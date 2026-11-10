package session

import (
	"path/filepath"
	"testing"

	"github.com/256dpi/gomqtt/packet"
	"github.com/syndtr/goleveldb/leveldb"
)

func openTestDB(t *testing.T) (*leveldb.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	db, err := leveldb.OpenFile(path, nil)
	if err != nil { t.Fatal(err) }
	return db, path
}

func TestLevelDBSessionPacketRoundTripAndReopen(t *testing.T) {
	db, path := openTestDB(t)
	s, err := NewLevelDBSession(db, "client-a")
	if err != nil { t.Fatal(err) }
	p := packet.NewPublish()
	p.ID = 42
	p.Message.Topic = "sensor/temperature"
	p.Message.QOS = packet.QOSAtLeastOnce
	p.Message.Payload = []byte("23.5")
	if err := s.SavePacket(Outgoing, p); err != nil { t.Fatal(err) }
	if err := db.Close(); err != nil { t.Fatal(err) }

	db, err = leveldb.OpenFile(path, nil)
	if err != nil { t.Fatal(err) }
	defer db.Close()
	s, err = NewLevelDBSession(db, "client-a")
	if err != nil { t.Fatal(err) }
	got, err := s.LookupPacket(Outgoing, 42)
	if err != nil { t.Fatal(err) }
	publish, ok := got.(*packet.Publish)
	if !ok { t.Fatalf("got %T, want *packet.Publish", got) }
	if publish.Message.Topic != p.Message.Topic || string(publish.Message.Payload) != "23.5" {
		t.Fatalf("unexpected restored packet: %#v", publish)
	}
}

func TestLevelDBSessionCounterPersists(t *testing.T) {
	db, path := openTestDB(t)
	s, err := NewLevelDBSession(db, "counter")
	if err != nil { t.Fatal(err) }
	if got := s.NextID(); got != 1 { t.Fatalf("first ID = %d, want 1", got) }
	if err := db.Close(); err != nil { t.Fatal(err) }
	db, err = leveldb.OpenFile(path, nil)
	if err != nil { t.Fatal(err) }
	defer db.Close()
	s, err = NewLevelDBSession(db, "counter")
	if err != nil { t.Fatal(err) }
	if got := s.NextID(); got != 2 { t.Fatalf("second ID = %d, want 2", got) }
}

func TestLevelDBSessionDeleteAndAll(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()
	s, err := NewLevelDBSession(db, "delete")
	if err != nil { t.Fatal(err) }
	for _, id := range []packet.ID{10, 11} {
		p := packet.NewPuback()
		p.ID = id
		if err := s.SavePacket(Incoming, p); err != nil { t.Fatal(err) }
	}
	all, err := s.AllPackets(Incoming)
	if err != nil { t.Fatal(err) }
	if len(all) != 2 { t.Fatalf("got %d packets, want 2", len(all)) }
	if err := s.DeletePacket(Incoming, 10); err != nil { t.Fatal(err) }
	got, err := s.LookupPacket(Incoming, 10)
	if err != nil { t.Fatal(err) }
	if got != nil { t.Fatalf("deleted packet still exists: %v", got) }
}

func TestLevelDBSessionRejectsInvalidDirection(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()
	s, err := NewLevelDBSession(db, "invalid")
	if err != nil { t.Fatal(err) }
	if _, err := s.AllPackets(Direction(99)); err != ErrInvalidPacketDirection {
		t.Fatalf("got %v, want ErrInvalidPacketDirection", err)
	}
}


func TestLevelDBSessionConcurrentNextID(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()
	s, err := NewLevelDBSession(db, "concurrent")
	if err != nil { t.Fatal(err) }

	const workers = 128
	ids := make(chan packet.ID, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids <- s.NextID()
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[packet.ID]bool, workers)
	for id := range ids {
		if !id.Valid() { t.Fatal("NextID returned invalid ID") }
		if seen[id] { t.Fatalf("duplicate packet ID %d", id) }
		seen[id] = true
	}
	if len(seen) != workers { t.Fatalf("got %d unique IDs, want %d", len(seen), workers) }
}

func TestLevelDBSessionResetClearsPacketsAndCounter(t *testing.T) {
	db, _ := openTestDB(t)
	defer db.Close()
	s, err := NewLevelDBSession(db, "reset")
	if err != nil { t.Fatal(err) }
	p := packet.NewPuback()
	p.ID = 7
	if err := s.SavePacket(Outgoing, p); err != nil { t.Fatal(err) }
	if got := s.NextID(); got != 1 { t.Fatalf("first ID = %d, want 1", got) }
	if err := s.Reset(); err != nil { t.Fatal(err) }
	all, err := s.AllPackets(Outgoing)
	if err != nil { t.Fatal(err) }
	if len(all) != 0 { t.Fatalf("Reset left %d packets", len(all)) }
	if got := s.NextID(); got != 1 { t.Fatalf("ID after Reset = %d, want 1", got) }
}
