package broker

import (
	"encoding/binary"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/256dpi/gomqtt/packet"
	"github.com/syndtr/goleveldb/leveldb"
)

func openLevelDBBackendTest(t *testing.T) *LevelDBBackend {
	t.Helper()
	b, err := OpenLevelDBBackend(filepath.Join(t.TempDir(), "broker.db"))
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
	if err := b.enqueueBatch(batch, s, msg); err != nil { t.Fatal(err) }
	if err := b.enqueueBatch(batch, s, msg); err != nil { t.Fatal(err) }
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
	for i, key := range keys {
		if err := b.db.Put(key, []byte{byte(i+1)}, b.writeOptions); err != nil { t.Fatal(err) }
	}
	if err := b.deletePersistentSession(id); err != nil { t.Fatal(err) }
	for _, key := range keys {
		if _, err := b.db.Get(key, nil); err != leveldb.ErrNotFound { t.Fatalf("key %q still exists, err=%v", key, err) }
	}
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
