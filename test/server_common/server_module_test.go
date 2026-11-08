package server_common_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"mixture/src/common/printer"
	sc "mixture/src/common/server_common"
	gw "mixture/src/gateway_server"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// 测量单个内部包切包成本，不包含网络、业务或进程启动。
func BenchmarkSplitPacket(b *testing.B) {
	for _, size := range []int{64, 1024, 4098} {
		b.Run(fmt.Sprintf("body_%d", size), func(b *testing.B) {
			packet := make([]byte, size+gw.HeadLen)
			binary.BigEndian.PutUint16(packet, uint16(size))
			stream := new(gw.ReadStreamBufferObject)
			stream.Init(1)
			out := make([]byte, gw.ReadStreamBufferSize)
			var cuts gw.CutIndexList
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cuts.Reset()
				if !gw.DoSplit(stream, packet, len(packet), &cuts, out) {
					b.Fatal("split failed")
				}
			}
		})
	}
}

func TestOtherSplitModes(t *testing.T) {
	for _, mode := range []uint8{2, 3} {
		body := []byte{0, 1, 42, 0, 0}
		buf := make([]byte, gw.ReadStreamBufferSize)
		n := gw.DoPack(buf, body, mode)
		if n == 0 {
			t.Fatal("pack failed")
		}
		packet := bytes.Clone(buf[:n])
		input := append(bytes.Clone(packet), packet...)
		for split := 1; split < len(input); split++ {
			conn := new(gw.NewConnection)
			conn.Init(mode, nil, nil, "tcp")
			var cuts gw.CutIndexList
			out := make([]byte, 1024)
			var got []byte
			count := 0
			for _, chunk := range [][]byte{input[:split], input[split:]} {
				cuts.Reset()
				if !gw.DoSplit(conn.StreamBuffer, chunk, len(chunk), &cuts, out) {
					t.Fatalf("mode %d split %d rejected", mode, split)
				}
				if cuts.Len() > 0 {
					got = append(got, out[:cuts.Get(cuts.Len()-1)]...)
				}
				count += cuts.Len()
			}
			if count != 2 || !bytes.Equal(got, append(bytes.Clone(body), body...)) {
				t.Fatal("roundtrip changed payload")
			}
		}
		for _, tc := range []struct {
			input    []byte
			capacity int
		}{
			{packet, len(body) - 1},
			{bytes.Repeat(packet, gw.MaxPacketsPerRead+1), (gw.MaxPacketsPerRead + 1) * len(body)},
		} {
			conn := new(gw.NewConnection)
			conn.Init(mode, nil, nil, "tcp")
			var cuts gw.CutIndexList
			if gw.DoSplit(conn.StreamBuffer, tc.input, len(tc.input), &cuts, make([]byte, tc.capacity)) {
				t.Fatal("overflow accepted")
			}
		}
		conn := new(gw.NewConnection)
		conn.Init(mode, nil, nil, "tcp")
		var cuts gw.CutIndexList
		bad := []byte{1, 124}
		if mode == 3 {
			bad = []byte{0, 1}
		}
		if gw.DoSplit(conn.StreamBuffer, bad, len(bad), &cuts, buf) {
			t.Fatal("short body accepted")
		}
	}
	if gw.DoPack(make([]byte, 1024), []byte{0, 124}, 2) != 0 {
		t.Fatal("unescaped delimiter accepted")
	}
	if gw.DoPack(make([]byte, 1024), make([]byte, 555), 3) != 0 {
		t.Fatal("oversized fixed body accepted")
	}
}

func TestConnectionPacketLimits(t *testing.T) {
	client := new(gw.NewConnection)
	client.Init(1, nil, nil, "tcp")
	internal := new(gw.ReadStreamBufferObject)
	internal.Init(1)
	for _, tc := range []struct {
		name     string
		stream   *gw.ReadStreamBufferObject
		size     int
		accepted bool
	}{
		{"client empty", client.StreamBuffer, 0, false},
		{"client short", client.StreamBuffer, 1, false},
		{"client minimum", client.StreamBuffer, 2, true},
		{"internal short", internal, 5, false},
		{"internal minimum", internal, 6, true},
		{"client maximum", client.StreamBuffer, 4094, true},
		{"client oversized", client.StreamBuffer, 4095, false},
		{"forwarded maximum", internal, 4098, true},
		{"internal oversized", internal, 4099, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packet := make([]byte, tc.size+gw.HeadLen)
			binary.BigEndian.PutUint16(packet, uint16(tc.size))
			if tc.size > 0 {
				packet[len(packet)-1] = 123
			}
			var cuts gw.CutIndexList
			out := make([]byte, gw.ReadStreamBufferSize)
			// 分开输入包头与包体，覆盖跨Read的情况。
			ok := gw.DoSplit(tc.stream, packet[:gw.HeadLen], gw.HeadLen, &cuts, out)
			if ok {
				ok = gw.DoSplit(tc.stream, packet[gw.HeadLen:], tc.size, &cuts, out)
			}
			if ok != tc.accepted {
				t.Fatalf("accepted=%v, want %v", ok, tc.accepted)
			}
			if ok && (cuts.Len() != 1 || cuts.Get(0) != tc.size || out[tc.size-1] != 123) {
				t.Fatal("packet truncated")
			}
		})
	}
}

// Run in a child process so Windows releases the framework's log handles
// before the parent removes the temporary working directory.
func TestSynchronousModuleCall(t *testing.T) {
	if os.Getenv("MIXTURE_MODULE_TEST_CHILD") != "1" {
		dir := t.TempDir()
		cmd := exec.Command(os.Args[0], "-test.run=^TestSynchronousModuleCall$", "-test.v")
		cmd.Dir, _ = os.Getwd()
		cmd.Env = append(os.Environ(), "MIXTURE_MODULE_TEST_CHILD=1", "MIXTURE_MODULE_TEST_DIR="+dir)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%v\n%s", err, output)
		}
		return
	}
	if err := os.Chdir(os.Getenv("MIXTURE_MODULE_TEST_DIR")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join("log", "moduleLog"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("system.txt", []byte("slogLevel=0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	printer.InitPrinterMgr()
	printer.SetLoggerObj(printer.CreateLogger("test", "log/", false))
	t.Run("linker writer stop and socket failure", func(t *testing.T) {
		for _, failHandshake := range []bool{false, true} {
			local, remote := net.Pipe()
			stop, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				gw.DealWithLinkerData(new(gw.SerLinker), local, stop)
			}()
			if failHandshake {
				remote.Close()
			} else {
				remote.SetReadDeadline(time.Now().Add(time.Second))
				handshake := make([]byte, 1024)
				if _, err := remote.Read(handshake); err != nil {
					t.Fatal(err)
				}
				close(stop)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				local.Close()
				remote.Close()
				t.Fatal("writer failed to exit")
			}
			if _, err := local.Write([]byte{1}); err == nil {
				t.Fatal("writer did not close its socket")
			}
			remote.Close()
		}
	})

	t.Run("client write failure closes reader and rejects sends", func(t *testing.T) {
		local, remote := net.Pipe()
		defer remote.Close()
		conn := new(gw.NewConnection)
		conn.Init(1, &local, nil, "tcp")
		readDone, writeDone := make(chan error, 1), make(chan struct{})
		go func() { _, _, err := conn.Read(); readDone <- err }()
		// A write deadline fails only the write side; the reader needs Close to wake.
		local.SetWriteDeadline(time.Now().Add(-time.Second))
		for i := 0; i < 8; i++ {
			buf := gw.DistributeDataPool.Get().(*[]byte)
			*buf = (*buf)[:8]
			if !conn.TrySend(buf) {
				t.Fatal("enqueue failed")
			}
		}
		go func() { defer close(writeDone); gw.DoWriteMsg(conn) }()
		select {
		case <-writeDone:
		case <-time.After(time.Second):
			local.Close()
			t.Fatal("writer did not exit")
		}
		select {
		case err := <-readDone:
			if err == nil {
				t.Fatal("read unexpectedly succeeded")
			}
		case <-time.After(time.Second):
			local.Close()
			t.Fatal("reader remained blocked")
		}
		buf := []byte{1}
		if conn.TrySend(&buf) {
			t.Fatal("closed client accepted a packet")
		}
		conn.Close()
	})

	oldTimeout := sc.ModCallerTimeOut
	defer func() { sc.ModCallerTimeOut = oldTimeout }()

	t.Run("fast reply and message recycling", func(t *testing.T) {
		sc.ModCallerTimeOut = time.Second
		withModule(t, "fast", 1, func(args ...any) interface{} { return args[0] }, func() {
			for i := 0; i < 1000; i++ {
				if got := sc.SynchronousPack2Module("fast", "echo", []any{i}); got != i {
					t.Fatalf("got %v, want %d", got, i)
				}
			}
		})
	})
	t.Run("full queue times out", func(t *testing.T) {
		sc.ModCallerTimeOut = 20 * time.Millisecond
		started, release := make(chan struct{}), make(chan struct{})
		withModule(t, "full", 1, func(...any) interface{} { close(started); <-release; return nil }, func() {
			defer close(release)
			if !sc.AsynchronousPack2Module("full", "echo", nil) {
				t.Fatal("empty queue rejected async call")
			}
			<-started
			queue := sc.InterfaceName2ModMap["echo"]
			queue <- &sc.ModulePackInfo{InterfaceName: "noop"}
			asyncDone := make(chan bool, 1)
			go func() { asyncDone <- sc.AsynchronousPack2Module("full", "echo", nil) }()
			select {
			case accepted := <-asyncDone:
				if accepted {
					t.Fatal("full queue accepted async call")
				}
			case <-time.After(time.Second):
				t.Fatal("async enqueue blocked")
			}
			sc.ProtoName2ModMap["full_proto"] = queue
			defer delete(sc.ProtoName2ModMap, "full_proto")
			buf := sc.ReceivePacketBufPool.Get().(*[]byte)
			*buf = (*buf)[:6]
			protoDone := make(chan struct{})
			go func() { sc.TranspondProtoPack(buf, "full_proto", 1, 1); close(protoDone) }()
			select {
			case <-protoDone:
			case <-time.After(time.Second):
				t.Fatal("protocol enqueue blocked")
			}
			done := make(chan interface{}, 1)
			go func() { done <- sc.SynchronousPack2Module("full", "echo", nil) }()
			select {
			case got := <-done:
				if got != nil {
					t.Fatalf("got %v, want nil", got)
				}
			case <-time.After(time.Second):
				t.Fatal("call remained blocked on enqueue")
			}
			if len(queue) != 1 {
				t.Fatal("queue changed after timeout")
			}
		})
	})
	t.Run("late reply does not block module", func(t *testing.T) {
		sc.ModCallerTimeOut = 20 * time.Millisecond
		release := make(chan struct{})
		withModule(t, "late", 1, func(...any) interface{} { <-release; return 42 }, func() {
			defer close(release)
			if got := sc.SynchronousPack2Module("late", "echo", nil); got != nil {
				t.Fatalf("got %v, want timeout", got)
			}
		})
	})
}

func withModule(t *testing.T, name string, size int, fn func(...any) interface{}, run func()) {
	t.Helper()
	var wg sync.WaitGroup
	// Install handlers before starting the receive loop.
	ready := make(chan struct{})
	mod := sc.CreateMod(name, size, func() error { return nil }, func() error { return nil }, &wg)
	mod.RegisterInterfaceHandleCaller("echo", func(args ...any) interface{} { <-ready; return fn(args...) })
	mod.RegisterInterfaceHandleCaller("noop", func(...any) interface{} { return nil })
	queue := sc.InterfaceName2ModMap["echo"]
	close(ready)
	defer func() {
		// An invalid message is the public receive loop's termination path.
		done := make(chan struct{})
		go func() { queue <- struct{}{}; wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("module did not finish processing")
		}
		sc.ServerModMgr.Delete(name)
		delete(sc.InterfaceName2ModMap, "echo")
		delete(sc.InterfaceName2ModMap, "noop")
	}()
	run()
}

func TestConnectionCloseAndSend(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	conn := new(sc.SerConnObj)
	if err := conn.Init(&local); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				buf := []byte{1}
				conn.TrySend(&buf)
				conn.GetGateId()
			}
		}()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); conn.Close("test") }()
	}
	wg.Wait()
	buf := []byte{2}
	if conn.TrySend(&buf) {
		t.Fatal("closed connection accepted a message")
	}
	if conn.GetGateSerConn() == nil {
		t.Fatal("close changed socket pointer")
	}
	for {
		select {
		case <-conn.GetSendChannel():
		default:
			return
		}
	}
}
