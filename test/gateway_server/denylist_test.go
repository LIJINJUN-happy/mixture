package gateway_server_test

import (
	"database/sql"
	gw "mixture/src/gateway_server"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDenyListThresholdAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), gw.DenyListDBFileName)
	store, err := gw.LoadDenyList(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < gw.DenyListStrikeLimit-1; i++ {
		if store.RecordInvalidPacket("127.0.0.1:1001") {
			t.Fatal("banned before strike limit")
		}
	}
	if store.IsDenied("127.0.0.1:1002") {
		t.Fatal("IP denied before strike limit")
	}
	if !store.RecordInvalidPacket("127.0.0.1:1003") {
		t.Fatal("IP not banned at strike limit")
	}
	if !store.IsDenied("127.0.0.1:1004") {
		t.Fatal("IP not denied after reaching strike limit")
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	loaded, err := gw.LoadDenyList(path)
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if !loaded.IsDenied("127.0.0.1:1005") {
		t.Fatal("ban did not survive reload")
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE denylist SET expires_at=? WHERE ip=?`, time.Now().Add(-time.Second).UnixMilli(), "127.0.0.1"); err != nil {
		t.Fatal(err)
	}
	if err := loaded.Refresh(); err != nil {
		t.Fatal(err)
	}
	if loaded.IsDenied("127.0.0.1:1006") {
		t.Fatal("expired ban survived refresh")
	}
	if err := loaded.CleanupExpired(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM denylist`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("expired ban remained in database: count=%d, err=%v", count, err)
	}
}

func TestDenyListRejectsMalformedDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), gw.DenyListDBFileName)
	if err := os.WriteFile(path, []byte("not a SQLite database"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := gw.LoadDenyList(path); err == nil {
		t.Fatal("malformed database was accepted")
	}
}

func TestDenyListMultipleGateways(t *testing.T) {
	path := filepath.Join(t.TempDir(), gw.DenyListDBFileName)
	first, err := gw.LoadDenyList(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := gw.LoadDenyList(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for i := 0; i < gw.DenyListStrikeLimit; i++ {
		first.RecordInvalidPacket("127.0.0.1:1001")
		second.RecordInvalidPacket("127.0.0.2:1002")
	}
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for _, store := range []*gw.DenyListStore{first, second} {
		wg.Add(1)
		go func(store *gw.DenyListStore) {
			defer wg.Done()
			errors <- store.Flush()
		}(store)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Refresh(); err != nil {
		t.Fatal(err)
	}
	if err := second.Refresh(); err != nil {
		t.Fatal(err)
	}
	for _, store := range []*gw.DenyListStore{first, second} {
		if !store.IsDenied("127.0.0.1:1003") || !store.IsDenied("127.0.0.2:1004") {
			t.Fatal("a gateway lost another gateway's ban")
		}
	}
}

func TestDenyListIPv6(t *testing.T) {
	store, err := gw.LoadDenyList(filepath.Join(t.TempDir(), gw.DenyListDBFileName))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < gw.DenyListStrikeLimit; i++ {
		store.RecordInvalidPacket("[::1]:1001")
	}
	if !store.IsDenied("[::1]:1002") {
		t.Fatal("IPv6 address was not denied")
	}
}
