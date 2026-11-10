package session

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/256dpi/gomqtt/packet"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	ErrLevelDBSessionClosed = errors.New("leveldb session: database unavailable")
	ErrInvalidPacketDirection = errors.New("leveldb session: invalid packet direction")
	ErrInvalidStoredPacket = errors.New("leveldb session: invalid stored packet")
)

// LevelDBSession persists MQTT inflight packets and the outgoing packet ID
// counter in a LevelDB database. The database is owned by the caller and is
// therefore not closed by this type. Each session must use a unique prefix.
type LevelDBSession struct {
	db *leveldb.DB
	prefix []byte
	mu sync.Mutex
	next packet.ID
	lastErr error
	writeOptions *opt.WriteOptions
}

// NewLevelDBSession creates or restores a persistent session under prefix.
// The caller must ensure that prefix is unique within db.
func NewLevelDBSession(db *leveldb.DB, prefix string) (*LevelDBSession, error) {
	if db == nil || prefix == "" {
		return nil, errors.New("leveldb session: db and non-empty prefix are required")
	}
	s := &LevelDBSession{
		db: db,
		prefix: []byte("gomqtt:session:" + prefix + ":"),
		next: 1,
		writeOptions: &opt.WriteOptions{Sync: true},
	}
	value, err := db.Get(s.key([]byte("counter")), nil)
	if err == nil {
		if len(value) != 2 {
			return nil, fmt.Errorf("leveldb session: invalid counter length %d", len(value))
		}
		s.next = packet.ID(binary.BigEndian.Uint16(value))
		if s.next == 0 {
			s.next = 1
		}
	} else if err != leveldb.ErrNotFound {
		return nil, fmt.Errorf("leveldb session: read counter: %w", err)
	}
	return s, nil
}

func (s *LevelDBSession) key(suffix []byte) []byte {
	key := make([]byte, 0, len(s.prefix)+len(suffix))
	key = append(key, s.prefix...)
	return append(key, suffix...)
}

func packetDirection(dir Direction) (byte, error) {
	switch dir {
	case Incoming:
		return 'i', nil
	case Outgoing:
		return 'o', nil
	default:
		return 0, ErrInvalidPacketDirection
	}
}

func (s *LevelDBSession) packetKey(dir Direction, id packet.ID) ([]byte, error) {
	d, err := packetDirection(dir)
	if err != nil { return nil, err }
	key := make([]byte, len(s.prefix)+4)
	copy(key, s.prefix)
	key[len(s.prefix)] = 'p'
	key[len(s.prefix)+1] = d
	binary.BigEndian.PutUint16(key[len(s.prefix)+2:], uint16(id))
	return key, nil
}

// NextID returns a non-zero packet ID and durably advances the counter.
// The legacy Session interface cannot return persistence errors; callers can
// inspect LastError and subsequent packet operations will also report errors.
func (s *LevelDBSession) NextID() packet.ID {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastErr != nil { return 0 }
	id := s.next
	next := id + 1
	if next == 0 { next = 1 }
	var data [2]byte
	binary.BigEndian.PutUint16(data[:], uint16(next))
	if err := s.db.Put(s.key([]byte("counter")), data[:], s.writeOptions); err != nil {
		s.lastErr = fmt.Errorf("leveldb session: persist packet counter: %w", err)
		return 0
	}
	s.next = next
	return id
}

// LastError returns the first asynchronous-interface error encountered by NextID.
func (s *LevelDBSession) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

func (s *LevelDBSession) SavePacket(dir Direction, pkt packet.Generic) error {
	if pkt == nil { return errors.New("leveldb session: packet is nil") }
	id, ok := packet.GetID(pkt)
	if !ok || !id.Valid() { return errors.New("leveldb session: packet has no valid identifier") }
	key, err := s.packetKey(dir, id)
	if err != nil { return err }
	data := make([]byte, pkt.Len())
	n, err := pkt.Encode(data)
	if err != nil { return fmt.Errorf("leveldb session: encode packet: %w", err) }
	if n != len(data) { return fmt.Errorf("leveldb session: encoded length mismatch: got %d want %d", n, len(data)) }
	if err := s.db.Put(key, data, s.writeOptions); err != nil {
		return fmt.Errorf("leveldb session: save packet: %w", err)
	}
	return s.LastError()
}

func (s *LevelDBSession) LookupPacket(dir Direction, id packet.ID) (packet.Generic, error) {
	if !id.Valid() { return nil, errors.New("leveldb session: packet ID is zero") }
	key, err := s.packetKey(dir, id)
	if err != nil { return nil, err }
	data, err := s.db.Get(key, nil)
	if err == leveldb.ErrNotFound { return nil, nil }
	if err != nil { return nil, fmt.Errorf("leveldb session: lookup packet: %w", err) }
	length, typ := packet.DetectPacket(data)
	if length != len(data) || !typ.Valid() { return nil, ErrInvalidStoredPacket }
	pkt, err := typ.New()
	if err != nil { return nil, fmt.Errorf("%w: %v", ErrInvalidStoredPacket, err) }
	n, err := pkt.Decode(data)
	if err != nil || n != len(data) {
		if err == nil { err = errors.New("decoded length mismatch") }
		return nil, fmt.Errorf("%w: %v", ErrInvalidStoredPacket, err)
	}
	return pkt, nil
}

func (s *LevelDBSession) DeletePacket(dir Direction, id packet.ID) error {
	key, err := s.packetKey(dir, id)
	if err != nil { return err }
	if err := s.db.Delete(key, s.writeOptions); err != nil {
		return fmt.Errorf("leveldb session: delete packet: %w", err)
	}
	return s.LastError()
}

func (s *LevelDBSession) AllPackets(dir Direction) ([]packet.Generic, error) {
	d, err := packetDirection(dir)
	if err != nil { return nil, err }
	prefix := make([]byte, len(s.prefix)+2)
	copy(prefix, s.prefix)
	prefix[len(s.prefix)] = 'p'
	prefix[len(s.prefix)+1] = d
	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()
	var packets []packet.Generic
	for iter.Seek(prefix); iter.Valid() && len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == string(prefix); iter.Next() {
		data := append([]byte(nil), iter.Value()...)
		length, typ := packet.DetectPacket(data)
		if length != len(data) || !typ.Valid() { return nil, ErrInvalidStoredPacket }
		pkt, err := typ.New()
		if err != nil { return nil, fmt.Errorf("%w: %v", ErrInvalidStoredPacket, err) }
		n, err := pkt.Decode(data)
		if err != nil || n != len(data) {
			if err == nil { err = errors.New("decoded length mismatch") }
			return nil, fmt.Errorf("%w: %v", ErrInvalidStoredPacket, err)
		}
		packets = append(packets, pkt)
	}
	if err := iter.Error(); err != nil { return nil, fmt.Errorf("leveldb session: iterate packets: %w", err) }
	return packets, nil
}

// Reset atomically clears inflight packets and resets the packet ID counter.
func (s *LevelDBSession) Reset() error {
	batch := new(leveldb.Batch)
	for _, dir := range []Direction{Incoming, Outgoing} {
		d, _ := packetDirection(dir)
		prefix := append(append([]byte(nil), s.prefix...), 'p', d)
		iter := s.db.NewIterator(nil, nil)
		for iter.Seek(prefix); iter.Valid() && len(iter.Key()) >= len(prefix) && string(iter.Key()[:len(prefix)]) == string(prefix); iter.Next() {
			batch.Delete(append([]byte(nil), iter.Key()...))
		}
		if err := iter.Error(); err != nil { iter.Release(); return fmt.Errorf("leveldb session: scan reset: %w", err) }
		iter.Release()
	}
	var counter [2]byte
	binary.BigEndian.PutUint16(counter[:], 1)
	batch.Put(s.key([]byte("counter")), counter[:])
	if err := s.db.Write(batch, s.writeOptions); err != nil { return fmt.Errorf("leveldb session: reset: %w", err) }
	s.mu.Lock()
	s.next = 1
	s.lastErr = nil
	s.mu.Unlock()
	return nil
}
