package printer_test

import (
	"bytes"
	"mixture/src/common/printer"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestTextDebugBeforeInit 验证未初始化的日志对象不会误报DEBUG开启。
func TestTextDebugBeforeInit(t *testing.T) {
	defer printer.SetLoggerObj(nil)
	for _, logger := range []*printer.LoggerObj{nil, new(printer.LoggerObj)} {
		printer.SetLoggerObj(logger)
		if printer.IsTextDebugEnabled() {
			t.Fatal("uninitialized logger must not enable debug")
		}
	}
}

// TestTextDebugGuard 在独立进程中初始化真实logger，避免修改项目配置或留下未关闭的日志句柄。
func TestTextDebugGuard(t *testing.T) {
	for _, level := range []string{"0", "4", "-4"} {
		t.Run("level="+level, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestTextDebugGuardProcess$", "-test.v")
			cmd.Dir = t.TempDir()
			cmd.Env = append(os.Environ(), "MIXTURE_DEBUG_GUARD_LEVEL="+level)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("debug guard helper: %v\n%s", err, output)
			}
			t.Logf("%s", output)
		})
	}
}

// TestTextDebugGuardProcess 验证DEBUG开关、实参惰性构造及关闭时的分配成本。
func TestTextDebugGuardProcess(t *testing.T) {
	level := os.Getenv("MIXTURE_DEBUG_GUARD_LEVEL")
	if level == "" {
		return
	}
	if err := os.WriteFile("system.txt", []byte("slogLevel="+level+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	printer.InitPrinterMgr()
	printer.SetLoggerObj(printer.CreateLogger("debug-guard", "."+string(os.PathSeparator), false))
	enabled := level == "-4"
	if printer.IsTextDebugEnabled() != enabled {
		t.Fatalf("incorrect enabled state at level %s", level)
	}
	payload := bytes.Repeat([]byte{'x'}, 4096)
	builds := 0
	buildPayload := func() string { builds++; return string(payload) }
	if printer.IsTextDebugEnabled() {
		printer.TextDebug("doReadMsg", "num", len(payload), "message", buildPayload())
		printer.TextDebug("Write Success", "Send Byte Size", len(payload))
	}
	if enabled && builds != 1 || !enabled && builds != 0 {
		t.Fatalf("unexpected payload construction count: %d", builds)
	}
	paths, err := filepath.Glob(filepath.Join(".", "debug-guard.log*"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("expected one dated log file: paths=%v err=%v", paths, err)
	}
	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if enabled {
		for _, expected := range [][]byte{[]byte("level=DEBUG"), []byte("doReadMsg"), payload, []byte("Write Success")} {
			if !bytes.Contains(data, expected) {
				t.Fatalf("enabled DEBUG output missing %q", expected[:min(len(expected), 32)])
			}
		}
	} else {
		if len(data) != 0 {
			t.Fatal("disabled DEBUG must not emit output")
		}
		before := testing.AllocsPerRun(1000, func() {
			printer.TextDebug("doReadMsg", "num", len(payload), "message", string(payload))
		})
		after := testing.AllocsPerRun(1000, func() {
			if printer.IsTextDebugEnabled() {
				printer.TextDebug("doReadMsg", "num", len(payload), "message", string(payload))
			}
		})
		t.Logf("4096B payload: unguarded=%.0f allocs/op, guarded=%.0f allocs/op", before, after)
		if after != 0 || before <= after {
			t.Fatalf("allocation regression: unguarded=%v guarded=%v", before, after)
		}
	}
}
