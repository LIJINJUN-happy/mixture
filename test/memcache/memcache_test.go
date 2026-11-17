package main

import (
	"bytes"
	"errors"
	"mixture/src/database"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMemCacheGetOrLoad 验证缓存回源合并、不同key并行、错误重试及返回数据隔离。
func TestMemCacheGetOrLoad(t *testing.T) {
	e, err := newEndpoint("", "disconnect")
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	if _, err := initialize(e.listener.Addr().String()); err != nil {
		t.Fatal(err)
	}

	t.Run("SameKey", func(t *testing.T) {
		payload := []byte("loaded")
		s := measure("test:singleflight:hot", 30, 30, 50*time.Millisecond, payload)
		if s.failed.Load() != 0 || s.loads.Load() != 1 || s.peakLoads.Load() != 1 {
			t.Fatalf("failed=%d loads=%d peak=%d", s.failed.Load(), s.loads.Load(), s.peakLoads.Load())
		}
		// 返回时缓存已经回写，下一批请求不能再次调用loader。
		got, err := database.MemCacheGetOrLoad("test:singleflight:hot", func() ([]byte, error) {
			t.Error("cache was not written before return")
			return nil, nil
		})
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("got=%q err=%v", got, err)
		}
	})

	t.Run("DifferentKeys", func(t *testing.T) {
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		var wg sync.WaitGroup
		for _, key := range []string{"test:singleflight:a", "test:singleflight:b"} {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				_, err := database.MemCacheGetOrLoad(key, func() ([]byte, error) {
					entered <- struct{}{}
					<-release
					return []byte(key), nil
				})
				if err != nil {
					t.Error(err)
				}
			}(key)
		}
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		defer wg.Wait()
		defer close(release)
		for i := 0; i < 2; i++ {
			select {
			case <-entered:
			case <-timer.C:
				t.Fatal("different keys did not load concurrently")
			}
		}
	})

	t.Run("ErrorRetry", func(t *testing.T) {
		wantErr := errors.New("loader failed")
		_, err := database.MemCacheGetOrLoad("test:singleflight:retry", func() ([]byte, error) {
			return nil, wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("got %v, want loader error", err)
		}
		got, err := database.MemCacheGetOrLoad("test:singleflight:retry", func() ([]byte, error) {
			return []byte("retry succeeded"), nil
		})
		if err != nil || string(got) != "retry succeeded" {
			t.Fatalf("retry got=%q err=%v", got, err)
		}
	})

	t.Run("IndependentResults", func(t *testing.T) {
		var wg sync.WaitGroup
		var failed atomic.Int64
		start := make(chan struct{})
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				got, err := database.MemCacheGetOrLoad("test:singleflight:copy", func() ([]byte, error) {
					time.Sleep(50 * time.Millisecond)
					return []byte("original"), nil
				})
				if err != nil || string(got) != "original" {
					failed.Add(1)
					return
				}
				got[0] = 'X' // 每个调用者修改自己的结果；竞态检测也应通过。
			}()
		}
		close(start)
		wg.Wait()
		if failed.Load() != 0 {
			t.Fatalf("%d callers received modified data", failed.Load())
		}
	})
}
