// Package observe is an opt-in diagnostics helper for test deployments.
package observe

import (
	"encoding/json"
	sc "mixture/src/common/server_common"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"
)

// Start must run after module/protocol registration. Do not mutate routing maps afterwards.
// Bind to loopback only. Close the returned server to stop diagnostics.
func Start(address string) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		queues := map[string]any{}
		for name, q := range sc.ProtoName2ModMap {
			queues[name] = map[string]int{"length": len(q), "capacity": cap(q)}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"time": time.Now(), "goroutines": runtime.NumGoroutine(), "heap_alloc": m.HeapAlloc, "heap_inuse": m.HeapInuse, "total_alloc": m.TotalAlloc, "mallocs": m.Mallocs, "num_gc": m.NumGC, "gc_pause_ns": m.PauseTotalNs, "gc_cpu_fraction": m.GCCPUFraction, "protocol_queues": queues})
	})
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go server.Serve(listener)
	return server, nil
}

// SlowHandler injects deterministic service time in a test module.
func SlowHandler(delay time.Duration, fn func([]byte, int, uint32)) func([]byte, int, uint32) {
	return func(data []byte, gate int, session uint32) { time.Sleep(delay); fn(data, gate, session) }
}
