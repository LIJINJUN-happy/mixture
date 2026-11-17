package main

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"mixture/proto/mutual_pb/pb"
)

// TestWebSocketIdleCancellation 验证不发送首包时不会被压测端的一秒读超时提前打断。
func TestWebSocketIdleCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.ReadMessage()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var counters counters
	var conns sync.Map
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})
	go func() {
		worker(ctx, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", "connect", time.Second, time.Second, 0, nil, &counters, &conns, &wg)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("idle WS client exited early")
	case <-time.After(1200 * time.Millisecond):
	}
	cancel()
	conns.Range(func(key, value any) bool { key.(*WebSocketStream).Close(); return true })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle WS client failed to stop")
	}
	if counters.connected.Load() != 1 || counters.failed.Load() != 0 {
		t.Fatalf("connected=%d failed=%d", counters.connected.Load(), counters.failed.Load())
	}
}

func TestPacketPayloadSizes(t *testing.T) {
	for _, size := range []int{2, 1024, 4092} {
		p, err := packet(size)
		if err != nil {
			t.Fatal(err)
		}
		if len(p) != size+4 || int(binary.BigEndian.Uint16(p)) != size+2 {
			t.Fatalf("wrong packet length for %d", size)
		}
		var request pb.C2STest
		if err := proto.Unmarshal(p[4:], &request); err != nil || request.Num != 1 {
			t.Fatalf("invalid protobuf for size %d: %v", size, err)
		}
	}
	for _, size := range []int{0, 1, 4093} {
		if _, err := packet(size); err == nil {
			t.Errorf("accepted payload size %d", size)
		}
	}
}

func TestWebSocketTarget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if len(data) != 6 {
				return
			}
			if err := conn.WriteMessage(websocket.BinaryMessage, []byte{0, 4, 0, 2, 8, 2}); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	conn, err := DialTarget("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	request, _ := packet(2)
	for i := 0; i < 10; i++ {
		if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := writeAll(conn, request); err != nil {
			t.Fatal(err)
		}
		if err := readReply(conn); err != nil {
			t.Fatal(err)
		}
	}
}
