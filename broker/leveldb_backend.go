package broker

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/256dpi/gomqtt/packet"
	sessionpkg "github.com/256dpi/gomqtt/session"
	"github.com/256dpi/gomqtt/topic"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// LevelDBBackendOptions configures the persistent broker backend.
type LevelDBBackendOptions struct {
	SessionQueueSize int
	KillTimeout time.Duration
	ClientMaximumKeepAlive time.Duration
	ClientParallelPublishes int
	ClientParallelSubscribes int
	ClientInflightMessages int
	ClientTokenTimeout time.Duration
	Credentials map[string]string
	Logger func(LogEvent, *Client, packet.Generic, *packet.Message, error)
}

func (o *LevelDBBackendOptions) defaults() {
	if o.SessionQueueSize <= 0 { o.SessionQueueSize = 1000 }
	if o.KillTimeout <= 0 { o.KillTimeout = 5 * time.Second }
}

// LevelDBBackend persists MQTT sessions, subscriptions, retained messages and
// queued deliveries. It owns the database opened by OpenLevelDBBackend.
type LevelDBBackend struct {
	db *leveldb.DB
	opts LevelDBBackendOptions
	writeOptions *opt.WriteOptions
	mu sync.Mutex
	setupMu sync.Mutex
	closeMu sync.Mutex
	closing bool
	dbClosed bool
	queueSeq uint64
	active map[string]*Client
	persistent map[string]*levelBrokerSession
	temporary map[*Client]*levelBrokerSession
}

type levelBrokerSession struct {
	id string
	persistent bool
	packets Session
	subscriptions *topic.Tree
	memoryQueue chan *packet.Message
	active *Client
}

type storedMessage struct {
	Message packet.Message `json:"message"`
}

var (
	errLevelDBBackendClosed = errors.New("leveldb backend closed")
	errInvalidStoredMessage = errors.New("invalid stored MQTT message")
)

const (
	levelBackendPrefix = "gomqtt:broker:v1:"
	levelSessionRegistry = "sessions:"
	levelRetainedPrefix = "retained:"
	levelQueuePrefix = "queue:"
)

// OpenLevelDBBackend opens a LevelDB-backed MQTT broker backend.
func OpenLevelDBBackend(path string, options ...LevelDBBackendOptions) (*LevelDBBackend, error) {
	if path == "" { return nil, errors.New("leveldb backend: database path is required") }
	var opts LevelDBBackendOptions
	if len(options) > 1 { return nil, errors.New("leveldb backend: at most one options value is allowed") }
	if len(options) == 1 { opts = options[0] }
	opts.defaults()
	db, err := leveldb.OpenFile(path, nil)
	if err != nil { return nil, fmt.Errorf("leveldb backend: open database: %w", err) }
	// MQTT session and queue writes are synchronously flushed before ACKs.
	write := &opt.WriteOptions{Sync: true}
	var queueSeq uint64
	if raw, getErr := db.Get([]byte(levelBackendPrefix+"queue-sequence"), nil); getErr == nil {
		if len(raw) != 8 { _ = db.Close(); return nil, errors.New("leveldb backend: invalid queue sequence") }
		queueSeq = binary.BigEndian.Uint64(raw)
	} else if getErr != leveldb.ErrNotFound { _ = db.Close(); return nil, fmt.Errorf("leveldb backend: read queue sequence: %w", getErr) }
	return &LevelDBBackend{
		db: db, opts: opts, writeOptions: write, queueSeq: queueSeq,
		active: make(map[string]*Client),
		persistent: make(map[string]*levelBrokerSession),
		temporary: make(map[*Client]*levelBrokerSession),
	}, nil
}

func encodeBackendID(id string) string { return base64.RawURLEncoding.EncodeToString([]byte(id)) }
func backendSessionPrefix(id string) []byte {
	return []byte(levelBackendPrefix + levelSessionRegistry + encodeBackendID(id) + ":")
}
func backendSessionKey(id string) []byte {
	return []byte(levelBackendPrefix + levelSessionRegistry + encodeBackendID(id))
}
func backendQueuePrefix(id string) []byte {
	return append(backendSessionPrefix(id), []byte(levelQueuePrefix)...)
}
func backendRetainedKey(topicName string) []byte {
	return []byte(levelBackendPrefix + levelRetainedPrefix + base64.RawURLEncoding.EncodeToString([]byte(topicName)))
}

func (b *LevelDBBackend) Authenticate(_ *Client, user, password string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing { return false, ErrClosing }
	if b.opts.Credentials == nil { return true, nil }
	pw, ok := b.opts.Credentials[user]
	return ok && pw == password, nil
}

// Setup creates a temporary session for clean/empty IDs, or restores a durable
// session for a persistent client ID. A previous connection with the same ID
// is closed before its session is attached to the new client.
func (b *LevelDBBackend) Setup(client *Client, id string, clean bool) (Session, bool, error) {
	if client == nil { return nil, false, errors.New("leveldb backend: nil client") }
	b.setupMu.Lock()
	defer b.setupMu.Unlock()

	b.mu.Lock()
	if b.closing { b.mu.Unlock(); return nil, false, ErrClosing }
	client.MaximumKeepAlive = b.opts.ClientMaximumKeepAlive
	client.ParallelPublishes = b.opts.ClientParallelPublishes
	client.ParallelSubscribes = b.opts.ClientParallelSubscribes
	client.InflightMessages = b.opts.ClientInflightMessages
	client.TokenTimeout = b.opts.ClientTokenTimeout

	var old *Client
	if id != "" { old = b.active[id] }
	b.mu.Unlock()

	if old != nil && old != client {
		old.Close()
		select {
		case <-old.Closed():
		case <-time.After(b.opts.KillTimeout):
			return nil, false, ErrKillTimeout
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing { return nil, false, ErrClosing }

	if id == "" || clean {
		if id != "" {
			if err := b.deletePersistentSession(id); err != nil { return nil, false, err }
			delete(b.persistent, id)
		}
		s := &levelBrokerSession{
			id: id, persistent: false, packets: sessionpkg.NewMemorySession(),
			subscriptions: topic.NewStandardTree(),
			memoryQueue: make(chan *packet.Message, b.opts.SessionQueueSize),
			active: client,
		}
		b.temporary[client] = s
		if id != "" { b.active[id] = client }
		return s.packets, false, nil
	}

	s, exists := b.persistent[id]
	if !exists {
		s, err := b.loadPersistentSession(id)
		if err != nil { return nil, false, err }
		if s == nil {
			s = &levelBrokerSession{
				id: id, persistent: true,
				subscriptions: topic.NewStandardTree(),
				memoryQueue: make(chan *packet.Message, b.opts.SessionQueueSize),
			}
			if err := b.saveSubscriptions(s); err != nil { return nil, false, err }
			if err := b.db.Put(backendSessionKey(id), []byte{1}, b.writeOptions); err != nil {
				return nil, false, fmt.Errorf("leveldb backend: register session: %w", err)
			}
		}
		b.persistent[id] = s
	}
	s.active = client
	b.active[id] = client
	return s.packets, exists, nil
}

func (b *LevelDBBackend) loadPersistentSession(id string) (*levelBrokerSession, error) {
	_, err := b.db.Get(backendSessionKey(id), nil)
	if err == leveldb.ErrNotFound { return nil, nil }
	if err != nil { return nil, fmt.Errorf("leveldb backend: check session: %w", err) }

	packets, err := sessionpkg.NewLevelDBSession(b.db, encodeBackendID(id))
	if err != nil { return nil, err }
	s := &levelBrokerSession{id:id, persistent:true, packets:packets, subscriptions:topic.NewStandardTree(), memoryQueue:make(chan *packet.Message, b.opts.SessionQueueSize)}
	data, err := b.db.Get(append(backendSessionPrefix(id), []byte("subscriptions")...), nil)
	if err != nil && err != leveldb.ErrNotFound { return nil, fmt.Errorf("leveldb backend: load subscriptions: %w", err) }
	if err == nil {
		var subs []packet.Subscription
		if err := json.Unmarshal(data, &subs); err != nil { return nil, fmt.Errorf("leveldb backend: decode subscriptions: %w", err) }
		for i := range subs { sub := subs[i]; s.subscriptions.Set(sub.Topic, &sub) }
	}
	return s, nil
}

func (b *LevelDBBackend) saveSubscriptions(s *levelBrokerSession) error {
	var subs []packet.Subscription
	for _, v := range s.subscriptions.All() {
		sub, ok := v.(*packet.Subscription)
		if !ok || sub == nil { continue }
		subs = append(subs, *sub)
	}
	sort.Slice(subs, func(i,j int) bool { return subs[i].Topic < subs[j].Topic })
	data, err := json.Marshal(subs)
	if err != nil { return fmt.Errorf("leveldb backend: encode subscriptions: %w", err) }
	if s.persistent {
		if err := b.db.Put(append(backendSessionPrefix(s.id), []byte("subscriptions")...), data, b.writeOptions); err != nil {
			return fmt.Errorf("leveldb backend: save subscriptions: %w", err)
		}
	}
	return nil
}

func (b *LevelDBBackend) Restore(*Client) error { return nil }

func (b *LevelDBBackend) sessionFor(client *Client) (*levelBrokerSession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.temporary[client]; s != nil { return s, nil }
	if client != nil {
		if s := b.persistent[client.ID()]; s != nil && s.active == client { return s, nil }
	}
	return nil, errors.New("leveldb backend: client session not found")
}

func (b *LevelDBBackend) Subscribe(client *Client, subs []packet.Subscription, ack Ack) error {
	s, err := b.sessionFor(client)
	if err != nil { return err }
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range subs {
		sub := subs[i]
		s.subscriptions.Set(sub.Topic, &sub)
	}
	if err := b.saveSubscriptions(s); err != nil { return err }

	batch := new(leveldb.Batch)
	for _, sub := range subs {
		values, err := b.retainedSearch(sub.Topic)
		if err != nil { return err }
		for _, value := range values {
			msg := value.(*packet.Message).Copy()
			msg.Retain = true
			if err := b.enqueueBatch(batch, s, msg); err != nil { return err }
		}
	}
	if err := b.db.Write(batch, b.writeOptions); err != nil { return fmt.Errorf("leveldb backend: queue retained messages: %w", err) }
	if ack != nil { ack() }
	return nil
}

func (b *LevelDBBackend) Unsubscribe(client *Client, topics []string, ack Ack) error {
	s, err := b.sessionFor(client)
	if err != nil { return err }
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, t := range topics { s.subscriptions.Empty(t) }
	if err := b.saveSubscriptions(s); err != nil { return err }
	if ack != nil { ack() }
	return nil
}

func (b *LevelDBBackend) Publish(_ *Client, msg *packet.Message, ack Ack) error {
	if msg == nil { return errors.New("leveldb backend: nil message") }
	if msg.Topic == "" { return errors.New("leveldb backend: empty topic") }

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing { return ErrClosing }

	batch := new(leveldb.Batch)
	if msg.Retain {
		if len(msg.Payload) == 0 {
			batch.Delete(backendRetainedKey(msg.Topic))
		} else {
			copyMsg := msg.Copy()
			copyMsg.Retain = true
			data, err := json.Marshal(storedMessage{Message:*copyMsg})
			if err != nil { return err }
			batch.Put(backendRetainedKey(msg.Topic), data)
		}
	}

	// Persistent sessions are enumerated from the durable registry, so offline
	// subscribers receive messages even after a broker restart.
	ids, err := b.listPersistentIDs()
	if err != nil { return err }
	seen := make(map[*levelBrokerSession]bool)
	for _, id := range ids {
		s := b.persistent[id]
		if s == nil {
			s, err = b.loadPersistentSession(id)
			if err != nil { return err }
			if s == nil { continue }
			b.persistent[id] = s
		}
		seen[s] = true
		if sub := s.subscriptions.MatchFirst(msg.Topic); sub != nil {
			copyMsg := msg.Copy()
			copyMsg.Retain = false
			if copyMsg.QOS > sub.(*packet.Subscription).QOS { copyMsg.QOS = sub.(*packet.Subscription).QOS }
			if err := b.enqueueBatch(batch, s, copyMsg); err != nil { return err }
		}
	}
	for _, s := range b.temporary {
		if seen[s] { continue }
		if sub := s.subscriptions.MatchFirst(msg.Topic); sub != nil {
			copyMsg := msg.Copy()
			copyMsg.Retain = false
			if copyMsg.QOS > sub.(*packet.Subscription).QOS { copyMsg.QOS = sub.(*packet.Subscription).QOS }
			if err := b.enqueueBatch(batch, s, copyMsg); err != nil { return err }
		}
	}
	if err := b.db.Write(batch, b.writeOptions); err != nil { return fmt.Errorf("leveldb backend: persist publish: %w", err) }
	if ack != nil { ack() }
	return nil
}

func (b *LevelDBBackend) enqueueBatch(batch *leveldb.Batch, s *levelBrokerSession, msg *packet.Message) error {
	if !s.persistent {
		select {
		case s.memoryQueue <- msg:
			return nil
		default:
			return ErrQueueFull
		}
	}
	// QoS 0 is not stored for offline persistent sessions. It is buffered
	// only while the persistent session is connected.
	if msg.QOS == packet.QOSAtMostOnce {
		if s.active == nil { return nil }
		select {
		case s.memoryQueue <- msg:
			return nil
		default:
			return ErrQueueFull
		}
	}
	seq, err := b.nextQueueID(batch)
	if err != nil { return err }
	data, err := json.Marshal(storedMessage{Message:*msg})
	if err != nil { return fmt.Errorf("leveldb backend: encode queued message: %w", err) }
	key := append(backendQueuePrefix(s.id), make([]byte, 8)...)
	binary.BigEndian.PutUint64(key[len(key)-8:], seq)
	batch.Put(key, data)
	return nil
}

func (b *LevelDBBackend) nextQueueID(batch *leveldb.Batch) (uint64, error) {
	if b.queueSeq == ^uint64(0) { return 0, errors.New("leveldb backend: queue sequence exhausted") }
	b.queueSeq++
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], b.queueSeq)
	batch.Put([]byte(levelBackendPrefix+"queue-sequence"), encoded[:])
	return b.queueSeq, nil
}

func (b *LevelDBBackend) deletePersistentSession(id string) error {
	batch := new(leveldb.Batch)
	for _, prefix := range [][]byte{
		backendSessionPrefix(id),
		[]byte("gomqtt:session:" + encodeBackendID(id) + ":"),
	} {
		iter := b.db.NewIterator(nil, nil)
		for iter.Seek(prefix); iter.Valid() && len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == string(prefix); iter.Next() {
			batch.Delete(append([]byte(nil), iter.Key()...))
		}
		if err := iter.Error(); err != nil { iter.Release(); return fmt.Errorf("leveldb backend: scan session deletion: %w", err) }
		iter.Release()
	}
	batch.Delete(backendSessionKey(id))
	if err := b.db.Write(batch, b.writeOptions); err != nil { return fmt.Errorf("leveldb backend: delete clean session: %w", err) }
	return nil
}

func (b *LevelDBBackend) listPersistentIDs() ([]string, error) {
	prefix := []byte(levelBackendPrefix + levelSessionRegistry)
	iter := b.db.NewIterator(nil, nil)
	defer iter.Release()
	var ids []string
	for iter.Seek(prefix); iter.Valid() && len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == string(prefix); iter.Next() {
		key := string(iter.Key())
		if len(key) == len(prefix) { continue }
		encoded := key[len(prefix):]
		// Ignore metadata and queue keys below a registry entry.
		if len(encoded) == 0 { continue }
		if separator := strings.IndexByte(encoded, ':'); separator >= 0 {
			continue
		}
		idBytes, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil { return nil, fmt.Errorf("leveldb backend: decode client ID: %w", err) }
		ids = append(ids, string(idBytes))
	}
	if err := iter.Error(); err != nil { return nil, fmt.Errorf("leveldb backend: list sessions: %w", err) }
	sort.Strings(ids)
	return ids, nil
}

func (b *LevelDBBackend) retainedSearch(filter string) ([]interface{}, error) {
	iter := b.db.NewIterator(nil, nil)
	defer iter.Release()
	prefix := []byte(levelBackendPrefix + levelRetainedPrefix)
	var values []interface{}
	for iter.Seek(prefix); iter.Valid() && len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == string(prefix); iter.Next() {
		encoded := string(iter.Key()[len(prefix):])
		topicBytes, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil { continue }
		data := append([]byte(nil), iter.Value()...)
		var stored storedMessage
		if err := json.Unmarshal(data, &stored); err != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidStoredMessage, err)
		}
		if topicMatches(filter, string(topicBytes)) { values = append(values, &stored.Message) }
	}
	if err := iter.Error(); err != nil { return nil, fmt.Errorf("leveldb backend: iterate retained messages: %w", err) }
	return values, nil
}

func topicMatches(filter, name string) bool {
	tree := topic.NewStandardTree()
	tree.Set(filter, true)
	return tree.Match(name) != nil
}

func (b *LevelDBBackend) Dequeue(client *Client) (*packet.Message, Ack, error) {
	s, err := b.sessionFor(client)
	if err != nil { return nil, nil, err }
	if !s.persistent {
		select {
		case msg := <-s.memoryQueue:
			return msg, nil, nil
		case <-client.Closing():
			return nil, nil, nil
		}
	}
	prefix := backendQueuePrefix(s.id)
	for {
		select {
		case msg := <-s.memoryQueue:
			return msg, nil, nil
		default:
		}
		iter := b.db.NewIterator(nil, nil)
		if !iter.Seek(prefix) {
			iter.Release()
			select {
			case msg := <-s.memoryQueue: return msg, nil, nil
			case <-time.After(100 * time.Millisecond):
			case <-client.Closing(): return nil, nil, nil
			}
			continue
		}
		if !iter.Valid() || len(iter.Key()) < len(prefix) || string(iter.Key()[:len(prefix)]) != string(prefix) {
			iter.Release()
			select {
			case <-time.After(100 * time.Millisecond):
			case <-client.Closing(): return nil, nil, nil
			}
			continue
		}
		key := append([]byte(nil), iter.Key()...)
		data := append([]byte(nil), iter.Value()...)
		iter.Release()
		var stored storedMessage
		if err := json.Unmarshal(data, &stored); err != nil { return nil, nil, fmt.Errorf("%w: %v", errInvalidStoredMessage, err) }
		msg := stored.Message.Copy()
		if msg.QOS == packet.QOSAtMostOnce {
			if err := b.db.Delete(key, b.writeOptions); err != nil { return nil, nil, fmt.Errorf("leveldb backend: delete qos0 message: %w", err) }
			return msg, nil, nil
		}
		var once sync.Once
		ack := func() {
			once.Do(func() {
				if err := b.db.Delete(key, b.writeOptions); err != nil {
					b.Log(BackendError, client, nil, msg, fmt.Errorf("leveldb backend: acknowledge queued message: %w", err))
				}
			})
		}
		return msg, ack, nil
	}
}

func (b *LevelDBBackend) Terminate(client *Client) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s := b.temporary[client]; s != nil {
		delete(b.temporary, client)
		if s.id != "" && b.active[s.id] == client { delete(b.active, s.id) }
		return nil
	}
	if client != nil {
		if s := b.persistent[client.ID()]; s != nil && s.active == client { s.active = nil }
		if b.active[client.ID()] == client { delete(b.active, client.ID()) }
	}
	return nil
}

func (b *LevelDBBackend) Log(event LogEvent, client *Client, pkt packet.Generic, msg *packet.Message, err error) {
	if b.opts.Logger != nil { b.opts.Logger(event, client, pkt, msg, err) }
}

// Close stops active clients and closes the owned LevelDB database. It returns
// false when clients did not finish within timeout; in that case the database
// is deliberately kept open so their cleanup paths can still use it.
func (b *LevelDBBackend) Close(timeout time.Duration) bool {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()

	b.mu.Lock()
	if b.dbClosed {
		b.mu.Unlock()
		return true
	}
	b.closing = true
	clients := make([]*Client, 0, len(b.active)+len(b.temporary))
	seen := make(map[*Client]bool)
	for _, client := range b.active {
		if client != nil && !seen[client] {
			seen[client] = true
			clients = append(clients, client)
			client.Close()
		}
	}
	for client := range b.temporary {
		if client != nil && !seen[client] {
			seen[client] = true
			clients = append(clients, client)
			client.Close()
		}
	}
	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for _, client := range clients {
		select {
		case <-client.Closed():
		case <-timer.C:
			return false
		}
	}
	if err := b.db.Close(); err != nil {
		b.Log(BackendError, nil, nil, nil, fmt.Errorf("leveldb backend: close database: %w", err))
		return false
	}
	b.mu.Lock()
	b.dbClosed = true
	b.mu.Unlock()
	return true
}
