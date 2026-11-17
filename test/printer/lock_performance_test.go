package printer_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// TestModuleLogLockPerformance 对照TrickLog的Get+JSON Info路径，仅切换外围读锁。
// 测试不修改生产代码；使用独立临时文件，需显式设置MIXTURE_LOG_LOCK_PERF=1。
// 不触发跨天轮转，也不测每条日志fsync的持久化延迟。
func TestModuleLogLockPerformance(t *testing.T) {
	if os.Getenv("MIXTURE_LOG_LOCK_PERF") != "1" {
		t.Skip("set MIXTURE_LOG_LOCK_PERF=1 to run file logging performance comparison")
	}
	const records, rounds = 10000, 9
	t.Logf("environment: %s/%s %s GOMAXPROCS=%d; records=%d per sample, rounds=%d", runtime.GOOS, runtime.GOARCH, runtime.Version(), runtime.GOMAXPROCS(0), records, rounds)
	root := t.TempDir()
	type scenario struct {
		name             string
		workers, modules int
	}
	for _, scene := range []scenario{{"single", 1, 1}, {"8_workers_1_module", 8, 1}, {"8_workers_8_modules", 8, 8}} {
		t.Run(scene.name, func(t *testing.T) {
			run := func(label string, locked bool) time.Duration {
				t.Helper()
				var logs sync.Map
				var mux sync.RWMutex
				files := make([]*os.File, scene.modules)
				names := make([]string, scene.modules)
				defer func() {
					for _, f := range files {
						if f != nil {
							f.Close()
						}
					}
				}()
				for i := range files {
					names[i] = fmt.Sprintf("module-%d", i)
					path := filepath.Join(root, fmt.Sprintf("%s-%s-%d.log", scene.name, label, i))
					f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0600)
					if err != nil {
						t.Fatal(err)
					}
					files[i] = f
					logs.Store(names[i], slog.New(slog.NewJSONHandler(f, &slog.HandlerOptions{Level: slog.LevelInfo})))
				}
				// 两条路径都包含相同的recover、sync.Map查找、参数和JSON写入。
				writeUnlocked := func(name string, sequence int) {
					defer func() {
						if r := recover(); r != nil {
							panic(r)
						}
					}()
					if value, ok := logs.Load(name); ok {
						value.(*slog.Logger).Info("lock performance test", "sequence", sequence, "event", "module_log")
					}
				}
				writeLocked := func(name string, sequence int) {
					defer func() {
						if r := recover(); r != nil {
							panic(r)
						}
					}()
					mux.RLock()
					defer mux.RUnlock()
					if value, ok := logs.Load(name); ok {
						value.(*slog.Logger).Info("lock performance test", "sequence", sequence, "event", "module_log")
					}
				}
				write := writeUnlocked
				if locked {
					write = writeLocked
				}
				var ready, done sync.WaitGroup
				ready.Add(scene.workers)
				done.Add(scene.workers)
				start := make(chan struct{})
				for worker := 0; worker < scene.workers; worker++ {
					go func(worker int) {
						defer done.Done()
						ready.Done()
						<-start
						for i := worker; i < records; i += scene.workers {
							write(names[worker%scene.modules], i)
						}
					}(worker)
				}
				ready.Wait()
				begin := time.Now()
				close(start)
				done.Wait()
				elapsed := time.Since(begin)
				// 在计时范围外检查实际写入条数，防止失败写入让结果虚快。
				count := 0
				for _, f := range files {
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
					data, err := os.ReadFile(f.Name())
					if err != nil {
						t.Fatal(err)
					}
					count += bytes.Count(data, []byte{'\n'})
				}
				if count != records {
					t.Fatalf("records=%d, want=%d", count, records)
				}
				return elapsed
			}
			// 预热不计入统计，正式采样交替执行顺序以降低先后顺序偏差。
			run("warmup-unlocked", false)
			run("warmup-locked", true)
			unlocked, locked := make([]time.Duration, rounds), make([]time.Duration, rounds)
			for i := 0; i < rounds; i++ {
				if i%2 == 0 {
					unlocked[i] = run(fmt.Sprintf("%d-unlocked", i), false)
					locked[i] = run(fmt.Sprintf("%d-locked", i), true)
				} else {
					locked[i] = run(fmt.Sprintf("%d-locked", i), true)
					unlocked[i] = run(fmt.Sprintf("%d-unlocked", i), false)
				}
				t.Logf("round=%d unlocked=%s locked=%s", i+1, unlocked[i], locked[i])
			}
			sort.Slice(unlocked, func(i, j int) bool { return unlocked[i] < unlocked[j] })
			sort.Slice(locked, func(i, j int) bool { return locked[i] < locked[j] })
			u, l := unlocked[rounds/2], locked[rounds/2]
			t.Logf("SUMMARY records=%d unlocked_median=%s locked_median=%s delta=%+.2f%% extra_ns_per_record=%.1f unlocked_range=%s..%s locked_range=%s..%s", records, u, l, (float64(l)/float64(u)-1)*100, float64(l-u)/records, unlocked[0], unlocked[rounds-1], locked[0], locked[rounds-1])
		})
	}
}
