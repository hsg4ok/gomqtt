package broker

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/256dpi/gomqtt/packet"
	"github.com/256dpi/gomqtt/topic"
	"github.com/syndtr/goleveldb/leveldb"
)

func openLevelDBBackendTest(t *testing.T) *LevelDBBackend {
	t.Helper()
	dir, err := ioutil.TempDir("", "gomqtt-broker-test-")
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := os.RemoveAll(dir); err != nil { t.Error(err) } })
	b, err := OpenLevelDBBackend(filepath.Join(dir, "broker.db"))
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := b.db.Close(); err != nil { t.Error(err) } })
	return b
}

func TestLevelDBBackendQueueSequenceUniqueWithinBatch(t *testing.T) {
	b := openLevelDBBackendTest(t)
	s := &levelBrokerSession{id:"device-1", persistent:true}
	batch := new(leveldb.Batch)
	first, err := b.nextQueueID(batch)
	if err != nil { t.Fatal(err) }
	second, err := b.nextQueueID(batch)
	if err != nil { t.Fatal(err) }
	if first == second { t.Fatalf("queue IDs collided: %d", first) }
	if err := b.db.Write(batch, b.writeOptions); err != nil { t.Fatal(err) }
	msg := &packet.Message{Topic:"jobs/print", Payload:[]byte("one"), QOS:packet.QOSAtLeastOnce}
	batch = new(leveldb.Batch)
	pendingCounts := make(map[string]int)
	if err := b.enqueueBatch(batch, s, msg, pendingCounts); err != nil { t.Fatal(err) }
	if err := b.enqueueBatch(batch, s, msg, pendingCounts); err != nil { t.Fatal(err) }
	if err := b.db.Write(batch, b.writeOptions); err != nil { t.Fatal(err) }
	iter := b.db.NewIterator(nil, nil)
	defer iter.Release()
	prefix := backendQueuePrefix(s.id)
	count := 0
	for iter.Seek(prefix); iter.Valid() && len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == string(prefix); iter.Next() { count++ }
	if err := iter.Error(); err != nil { t.Fatal(err) }
	if count != 2 { t.Fatalf("got %d queued messages, want 2", count) }
}

func TestLevelDBBackendListPersistentIDsIgnoresSubkeys(t *testing.T) {
	b := openLevelDBBackendTest(t)
	id := "tenant:device/01"
	if err := b.db.Put(backendSessionKey(id), []byte{1}, b.writeOptions); err != nil { t.Fatal(err) }
	if err := b.db.Put(append(backendSessionPrefix(id), []byte("subscriptions")...), []byte("[]"), b.writeOptions); err != nil { t.Fatal(err) }
	if err := b.db.Put(append(backendQueuePrefix(id), []byte{0,0,0,0,0,0,0,1}...), []byte("{}"), b.writeOptions); err != nil { t.Fatal(err) }
	ids, err := b.listPersistentIDs()
	if err != nil { t.Fatal(err) }
	if len(ids) != 1 || ids[0] != id { t.Fatalf("got IDs %#v, want [%q]", ids, id) }
}

func TestLevelDBBackendRestoresSubscriptions(t *testing.T) {
	b := openLevelDBBackendTest(t)
	id := "subscriber-1"
	if err := b.db.Put(backendSessionKey(id), []byte{1}, b.writeOptions); err != nil { t.Fatal(err) }
	subscriptions := []packet.Subscription{{Topic:"factory/+/status", QOS:packet.QOSAtLeastOnce}}
	data, err := json.Marshal(subscriptions)
	if err != nil { t.Fatal(err) }
	if err := b.db.Put(append(backendSessionPrefix(id), []byte("subscriptions")...), data, b.writeOptions); err != nil { t.Fatal(err) }
	s, err := b.loadPersistentSession(id)
	if err != nil { t.Fatal(err) }
	if s == nil { t.Fatal("session was not restored") }
	value := s.subscriptions.MatchFirst("factory/line-1/status")
	if value == nil { t.Fatal("restored subscription does not match topic") }
	if got := value.(*packet.Subscription).QOS; got != packet.QOSAtLeastOnce { t.Fatalf("qos = %d", got) }
}

func TestLevelDBBackendCleanSessionDeletesPersistedState(t *testing.T) {
	b := openLevelDBBackendTest(t)
	id := "clean-me"
	keys := [][]byte{
		backendSessionKey(id),
		append(backendSessionPrefix(id), []byte("subscriptions")...),
		append(backendQueuePrefix(id), []byte{0,0,0,0,0,0,0,1}...),
		[]byte("gomqtt:session:" + encodeBackendID(id) + ":counter"),
	}
	for i, key := range keys { if err := b.db.Put(key, []byte{byte(i+1)}, b.writeOptions); err != nil { t.Fatal(err) } }
	if err := b.deletePersistentSession(id); err != nil { t.Fatal(err) }
	for _, key := range keys { if _, err := b.db.Get(key, nil); err != leveldb.ErrNotFound { t.Fatalf("key %q still exists, err=%v", key, err) } }
}

func TestLevelDBBackendQueueSequenceStoredBigEndian(t *testing.T) {
	b := openLevelDBBackendTest(t)
	batch := new(leveldb.Batch)
	id, err := b.nextQueueID(batch)
	if err != nil { t.Fatal(err) }
	if err := b.db.Write(batch, b.writeOptions); err != nil { t.Fatal(err) }
	raw, err := b.db.Get([]byte(levelBackendPrefix+"queue-sequence"), nil)
	if err != nil { t.Fatal(err) }
	if got := binary.BigEndian.Uint64(raw); got != id { t.Fatalf("stored sequence = %d, want %d", got, id) }
}


type levelDBTestConn struct {
	closed chan struct{}
	once sync.Once
}

func newLevelDBTestConn() *levelDBTestConn { return &levelDBTestConn{closed:make(chan struct{})} }
func (c *levelDBTestConn) Send(packet.Generic, bool) error { return nil }
func (c *levelDBTestConn) Receive() (packet.Generic, error) { <-c.closed; return nil, io.EOF }
func (c *levelDBTestConn) Close() error { c.once.Do(func(){ close(c.closed) }); return nil }
func (c *levelDBTestConn) SetReadLimit(int64) {}
func (c *levelDBTestConn) SetReadTimeout(time.Duration) {}
func (c *levelDBTestConn) SetMaxWriteDelay(time.Duration) {}
func (c *levelDBTestConn) LocalAddr() net.Addr { return nil }
func (c *levelDBTestConn) RemoteAddr() net.Addr { return nil }

func newBackendTestClient(t *testing.T, backend *LevelDBBackend, id string) *Client {
	t.Helper()
	client := NewClient(backend, newLevelDBTestConn())
	client.id = id
	sess, _, err := backend.Setup(client, id, false)
	if err != nil { t.Fatal(err) }
	client.session = sess
	return client
}

func closeBackendTestClient(t *testing.T, backend *LevelDBBackend, client *Client) {
	t.Helper()
	client.Close()
	select {
	case <-client.Closed():
	case <-time.After(3*time.Second): t.Fatal("client did not close")
	}
	if err := backend.Terminate(client); err != nil { t.Fatal(err) }
}

func TestLevelDBBackendOfflineQoS1DeliverySurvivesRestart(t *testing.T) {
	dir, err := ioutil.TempDir("", "gomqtt-restart-test-")
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := os.RemoveAll(dir); err != nil { t.Error(err) } })
	path := filepath.Join(dir, "persistent-broker.db")
	backend, err := OpenLevelDBBackend(path)
	if err != nil { t.Fatal(err) }
	client := newBackendTestClient(t, backend, "printer-agent-01")

	if err := backend.Subscribe(client, []packet.Subscription{{Topic:"agent/jobs", QOS:packet.QOSAtLeastOnce}}, nil); err != nil { t.Fatal(err) }
	message := &packet.Message{Topic:"agent/jobs", Payload:[]byte("job-001"), QOS:packet.QOSAtLeastOnce}
	if err := backend.Publish(client, message, nil); err != nil { t.Fatal(err) }
	got, ack, err := backend.Dequeue(client)
	if err != nil { t.Fatal(err) }
	if got == nil || string(got.Payload) != "job-001" { t.Fatalf("unexpected dequeued message: %#v", got) }
	if ack == nil { t.Fatal("QoS1 queue item must return an acknowledgement callback") }

	// Simulate a process restart before the outgoing MQTT packet is acknowledged:
	// do not invoke ack, so the durable queue entry must remain.
	closeBackendTestClient(t, backend, client)
	if !backend.Close(3*time.Second) { t.Fatal("backend close timed out") }

	backend, err = OpenLevelDBBackend(path)
	if err != nil { t.Fatal(err) }
	defer func(){ if !backend.Close(3*time.Second) { t.Error("reopened backend close timed out") } }()
	client = newBackendTestClient(t, backend, "printer-agent-01")
	defer closeBackendTestClient(t, backend, client)

	got, ack, err = backend.Dequeue(client)
	if err != nil { t.Fatal(err) }
	if got == nil || string(got.Payload) != "job-001" { t.Fatalf("message was not restored: %#v", got) }
	if ack == nil { t.Fatal("restored QoS1 queue item has no acknowledgement callback") }
	ack()
}

func TestLevelDBBackendQoS0IsNotStoredForOfflineSession(t *testing.T) {
	backend := openLevelDBBackendTest(t)
	s := &levelBrokerSession{id:"offline", persistent:true, subscriptions:topic.NewStandardTree()}
	batch := new(leveldb.Batch)
	msg := &packet.Message{Topic:"agent/jobs", Payload:[]byte("qos0"), QOS:packet.QOSAtMostOnce}
	if err := backend.enqueueBatch(batch, s, msg, make(map[string]int)); err != nil { t.Fatal(err) }
	if batch.Len() != 0 { t.Fatalf("offline QoS0 unexpectedly persisted %d batch bytes", batch.Len()) }
}


func TestLevelDBBackendRetainedDeliveryRespectsSubscriptionQoS(t *testing.T) {
	backend := openLevelDBBackendTest(t)
	client := newBackendTestClient(t, backend, "retained-subscriber")
	retained := &packet.Message{
		Topic: "devices/printer-1/status",
		Payload: []byte("ready"),
		QOS: packet.QOSExactlyOnce,
		Retain: true,
	}
	encoded, err := json.Marshal(storedMessage{Message:*retained})
	if err != nil { t.Fatal(err) }
	if err := backend.db.Put(backendRetainedKey(retained.Topic), encoded, backend.writeOptions); err != nil { t.Fatal(err) }

	if err := backend.Subscribe(client, []packet.Subscription{{
		Topic: "devices/+/status",
		QOS: packet.QOSAtLeastOnce,
	}}, nil); err != nil { t.Fatal(err) }

	got, ack, err := backend.Dequeue(client)
	if err != nil { t.Fatal(err) }
	if got == nil { t.Fatal("expected retained message") }
	if got.QOS != packet.QOSAtLeastOnce { t.Fatalf("retained qos = %d, want qos1", got.QOS) }
	if !got.Retain { t.Fatal("retained delivery must keep Retain flag set") }
	if string(got.Payload) != "ready" { t.Fatalf("payload = %q", got.Payload) }
	if ack == nil { t.Fatal("QoS1 retained delivery must be acknowledged") }
	ack()
	closeBackendTestClient(t, backend, client)
}


func TestLevelDBBackendEnforcesPersistentQueueLimit(t *testing.T) {
	dir, err := ioutil.TempDir("", "gomqtt-queue-limit-")
	if err != nil { t.Fatal(err) }
	t.Cleanup(func() { if err := os.RemoveAll(dir); err != nil { t.Error(err) } })
	backend, err := OpenLevelDBBackend(filepath.Join(dir, "broker.db"), LevelDBBackendOptions{SessionQueueSize:1})
	if err != nil { t.Fatal(err) }
	defer func(){ if err := backend.db.Close(); err != nil { t.Error(err) } }()

	s := &levelBrokerSession{id:"limited", persistent:true}
	message := &packet.Message{Topic:"jobs/print", Payload:[]byte("one"), QOS:packet.QOSAtLeastOnce}
	batch := new(leveldb.Batch)
	pending := make(map[string]int)
	if err := backend.enqueueBatch(batch, s, message, pending); err != nil { t.Fatal(err) }
	if err := backend.db.Write(batch, backend.writeOptions); err != nil { t.Fatal(err) }

	batch = new(leveldb.Batch)
	pending = make(map[string]int)
	if err := backend.enqueueBatch(batch, s, message, pending); err != ErrQueueFull {
		t.Fatalf("second queued message error = %v, want ErrQueueFull", err)
	}
}
