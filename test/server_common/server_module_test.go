package server_common_test

import (
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
			sc.AsynchronousPack2Module("full", "echo", nil)
			<-started
			queue := sc.InterfaceName2ModMap["echo"]
			queue <- &sc.ModulePackInfo{InterfaceName: "noop"}
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
