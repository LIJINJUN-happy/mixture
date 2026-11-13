package gateway_server_test

import (
	"encoding/json"
	gw "mixture/src/gateway_server"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDenyListThresholdAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), gw.DenyListFileName)
	store, err := gw.LoadDenyList(path)
	if err != nil {
		t.Fatal(err)
	}
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
	if !loaded.IsDenied("127.0.0.1:1005") {
		t.Fatal("ban did not survive reload")
	}

	expired, err := json.Marshal(map[string]gw.DenyListBan{
		"127.0.0.1": {Reason: "invalid_packet", ExpiresAt: time.Now().Add(-time.Second)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, expired, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err = gw.LoadDenyList(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.IsDenied("127.0.0.1:1006") {
		t.Fatal("expired ban survived reload")
	}
}

func TestDenyListRejectsMalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), gw.DenyListFileName)
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := gw.LoadDenyList(path); err == nil {
		t.Fatal("malformed denylist was accepted")
	}
}

func TestDenyListIPv6(t *testing.T) {
	store, err := gw.LoadDenyList(filepath.Join(t.TempDir(), gw.DenyListFileName))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < gw.DenyListStrikeLimit; i++ {
		store.RecordInvalidPacket("[::1]:1001")
	}
	if !store.IsDenied("[::1]:1002") {
		t.Fatal("IPv6 address was not denied")
	}
}
