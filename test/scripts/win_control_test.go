//go:build windows

package scripts_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Use isolated project copies: never run shutdown scripts against real services.
func TestWindowsControlScripts(t *testing.T) {
	root := t.TempDir()
	project := filepath.Join(root, "project with spaces")
	scriptDir := filepath.Join(project, "script", "win")
	for _, dir := range []string{scriptDir, filepath.Join(project, "bin"), filepath.Join(root, "other")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"stop.bat", "kill.bat", "logrm.bat"} {
		data, err := os.ReadFile(filepath.Join("..", "..", "script", "win", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(scriptDir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	fixtureSource := `package main
import("os";"os/signal";"time")
func main() {
 c:=make(chan os.Signal,1); signal.Notify(c,os.Interrupt)
 if err:=os.WriteFile(os.Args[1],[]byte("ready"),0600);err!=nil {panic(err)}
 for {select {case <-c: if os.Args[3]!="ignore" {os.WriteFile(os.Args[2],[]byte("clean"),0600);return}; case <-time.After(time.Second):}}
}`
	source := filepath.Join(root, "fixture.go")
	if err := os.WriteFile(source, []byte(fixtureSource), 0600); err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(project, "bin", "winMixture.exe")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "go", "build", "-o", exe, source).CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	data, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	otherExe := filepath.Join(root, "other", "winMixture.exe")
	if err := os.WriteFile(otherExe, data, 0700); err != nil {
		t.Fatal(err)
	}

	start := func(binary, mode string, flags uint32) (<-chan struct{}, string) {
		t.Helper()
		dir := t.TempDir()
		ready, clean := filepath.Join(dir, "ready"), filepath.Join(dir, "clean")
		cmd := exec.Command(binary, ready, clean, mode)
		cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: flags, HideWindow: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		t.Cleanup(func() {
			select {
			case <-done:
			default:
				_ = cmd.Process.Kill()
				<-done
			}
		})
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(ready); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("fixture not ready")
			}
			time.Sleep(20 * time.Millisecond)
		}
		return done, clean
	}
	run := func(name string, wantOK bool, contains string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "cmd.exe", "/d", "/c", "call", filepath.Join(scriptDir, name), "--no-pause")
		cmd.Env = append(os.Environ(), "MIXTURE_STOP_TIMEOUT_SECONDS=1")
		out, err := cmd.CombinedOutput()
		t.Logf("%s: %s", name, out)
		if (err == nil) != wantOK || !strings.Contains(string(out), contains) {
			t.Fatalf("%s: err=%v, output=%s", name, err, out)
		}
	}
	alive := func(done <-chan struct{}) {
		t.Helper()
		select {
		case <-done:
			t.Fatal("process unexpectedly exited")
		default:
		}
	}
	// Same-named executables outside this project must survive every script call.
	other, _ := start(otherExe, "ignore", 0x10) // CREATE_NEW_CONSOLE
	run("stop.bat", true, "Nothing to stop")
	run("kill.bat", true, "Nothing to kill")
	done, clean := start(exe, "handle", 0x10)
	second, secondClean := start(exe, "handle", 0x10)
	run("logrm.bat", false, "Project server is running")
	alive(done)
	alive(second)
	run("stop.bat", true, "Close Success")
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not exit")
	}
	if _, err := os.Stat(clean); err != nil {
		t.Fatal("CTRL+C cleanup not observed:", err)
	}
	select {
	case <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("stop skipped the second project process")
	}
	if _, err := os.Stat(secondClean); err != nil {
		t.Fatal("second process cleanup not observed:", err)
	}
	alive(other)
	done, clean = start(exe, "ignore", 0x10)
	run("stop.bat", false, "NOT force-killed")
	alive(done)
	run("kill.bat", true, "forcefully terminated")
	<-done
	if _, err := os.Stat(clean); !os.IsNotExist(err) {
		t.Fatal("forced exit unexpectedly ran cleanup")
	}
	alive(other)
	// A service without a console cannot receive CTRL+C; do not silently kill it.
	done, _ = start(exe, "handle", 0x8) // DETACHED_PROCESS
	run("stop.bat", false, "AttachConsole failed")
	alive(done)
	run("kill.bat", true, "forcefully terminated")
	<-done
	alive(other)
}

func TestWindowsLogRemoval(t *testing.T) {
	// Both project and caller paths include spaces; the caller's log must survive.
	root := t.TempDir()
	project := filepath.Join(root, "project with spaces")
	scriptDir := filepath.Join(project, "script", "win")
	caller := filepath.Join(root, "unrelated caller")
	logDir := filepath.Join(project, "log")
	for _, dir := range []string{scriptDir, caller, filepath.Join(logDir, "moduleLog")} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "script", "win", "logrm.bat"))
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(scriptDir, "logrm.bat")
	if err := os.WriteFile(script, data, 0644); err != nil {
		t.Fatal(err)
	}
	protected := filepath.Join(caller, "keep.log")
	for _, name := range []string{protected, filepath.Join(logDir, "server.log"), filepath.Join(logDir, "moduleLog", "user.log_2026-11-18")} {
		if err := os.WriteFile(name, []byte("test"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(wantOK bool, contains string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "cmd.exe", "/d", "/c", "call", script, "--no-pause")
		cmd.Dir = caller
		out, err := cmd.CombinedOutput()
		t.Logf("logrm.bat: %s", out)
		if (err == nil) != wantOK || !strings.Contains(string(out), contains) {
			t.Fatalf("err=%v, output=%s", err, out)
		}
	}
	run(true, "Clean Success")
	if _, err := os.Stat(logDir); !os.IsNotExist(err) {
		t.Fatal("entire log directory was not removed:", err)
	}
	if _, err := os.Stat(protected); err != nil {
		t.Fatal("unrelated log affected:", err)
	}
	run(true, "Nothing to delete")
	// A junction inside log must be rejected without touching its external target.
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatal(err)
	}
	junction := filepath.Join(logDir, "linked logs")
	cmd := exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", junction, caller)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create test junction: %v, %s", err, out)
	}
	// Remove only the junction, not its target, even if a subsequent assertion fails.
	t.Cleanup(func() { _ = os.Remove(junction) })
	run(false, "Link/junction detected")
	if _, err := os.Stat(protected); err != nil {
		t.Fatal("junction target affected:", err)
	}
	if _, err := os.Stat(logDir); err != nil {
		t.Fatal("unsafe log directory should have been preserved:", err)
	}
}
