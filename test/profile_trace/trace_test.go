package profile_trace_test

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"mixture/src/common/profile_trace"
	sc "mixture/src/common/server_common"
	gw "mixture/src/gateway_server"
)

// TestRemotePortSQL 确认部署SQL只调整指定进程ID的调试端口，不覆盖业务配置。
func TestRemotePortSQL(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "net_sql.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	roles := []string{"gatewaySer", "actorSer", "modularizedSer", "interactiveSer", "webSer"}
	for _, role := range roles {
		if _, err := db.Exec("CREATE TABLE " + role + "Config (" + role + "Id INTEGER, deBugModePort TEXT, business TEXT)"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("INSERT INTO " + role + "Config VALUES (1,'9999','keep'),(2,'9998','keep')"); err != nil {
			t.Fatal(err)
		}
	}
	script, err := os.ReadFile("../performance/remote_ports.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(script)); err != nil {
		t.Fatal(err)
	}
	for i, role := range roles {
		var port int
		var business string
		if err := db.QueryRow("SELECT deBugModePort,business FROM "+role+"Config WHERE "+role+"Id=1").Scan(&port, &business); err != nil {
			t.Fatal(err)
		}
		if port != 6060+i || business != "keep" {
			t.Fatalf("%s: wrong update port=%d business=%q", role, port, business)
		}
		if err := db.QueryRow("SELECT deBugModePort FROM " + role + "Config WHERE " + role + "Id=2").Scan(&port); err != nil || port != 9998 {
			t.Fatalf("%s: unrelated instance changed: %v", role, err)
		}
	}
}

// TestDebugMetrics 验证远程诊断能识别进程、构建版本和运行期指标，并与默认路由隔离。
func TestDebugMetrics(t *testing.T) {
	mux := profile_trace.NewDebugMux("gateway_server", "1", profile_trace.TraceSources{
		ModuleQueues:       sc.GetModuleQueueMetrics,
		GatewayClients:     gw.GateClientMapSize.Load,
		GatewayWriteQueues: gw.GetGatewayWriteQueueMetrics,
	})
	if mux == http.DefaultServeMux {
		t.Fatal("diagnostic routes must use an independent mux")
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	var data map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if data["server_name"] != "gateway_server" || data["server_id"] != "1" || data["pid"] != float64(os.Getpid()) {
		t.Fatalf("unexpected process identity: %v", data)
	}
	if profile_trace.BuildRevision != "" && data["build_revision"] != profile_trace.BuildRevision {
		t.Fatalf("build revision not propagated: %v", data["build_revision"])
	}
	for _, key := range []string{"started_at", "uptime_seconds", "build_revision", "go_version", "goroutines", "heap_alloc", "total_alloc", "mallocs", "num_gc", "gc_pause_ns", "module_queues", "gateway_clients", "gateway_write_queues", "gateway_drops", "server_drops"} {
		if _, ok := data[key]; !ok {
			t.Errorf("missing metric %s", key)
		}
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Error("metrics response must not be cached")
	}
	for _, route := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/profile", "/debug/pprof/trace"} {
		_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, route, nil))
		if pattern == "" {
			t.Errorf("missing route %s", route)
		}
	}
}

// TestTraceSources 验证回调按请求采样，且未配置回调时仍能安全返回空指标。
func TestTraceSources(t *testing.T) {
	for _, configured := range []bool{false, true} {
		var sources profile_trace.TraceSources
		calls := 0
		if configured {
			sources.ModuleQueues = func() map[string]profile_trace.ModuleQueueMetrics {
				calls++
				return map[string]profile_trace.ModuleQueueMetrics{"test": {Length: 3, Capacity: 8}}
			}
			sources.GatewayClients = func() int32 { return 2 }
			sources.GatewayWriteQueues = func() profile_trace.GatewayWriteQueueMetrics {
				return profile_trace.GatewayWriteQueueMetrics{Clients: 2, Length: 4, MaxLength: 3, Capacity: 128}
			}
		}
		mux := profile_trace.NewDebugMux("test", "1", sources)
		if calls != 0 {
			t.Fatal("callbacks must not run before a metrics request")
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		var data struct {
			Queues  map[string]profile_trace.ModuleQueueMetrics `json:"module_queues"`
			Clients int32                                       `json:"gateway_clients"`
			Writes  profile_trace.GatewayWriteQueueMetrics      `json:"gateway_write_queues"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
			t.Fatal(err)
		}
		if configured {
			if calls != 1 || data.Queues["test"].Length != 3 || data.Clients != 2 || data.Writes.Length != 4 {
				t.Fatalf("callback values lost: %+v calls=%d", data, calls)
			}
		} else if data.Queues == nil || len(data.Queues) != 0 || data.Clients != 0 || data.Writes.Length != 0 {
			t.Fatalf("unexpected empty metrics: %+v", data)
		}
	}
}

// TestDropCounters 验证迁移后的每个计数项仍能在并发累加后读取，不重置进程累计值。
func TestDropCounters(t *testing.T) {
	serverBefore, gatewayBefore := profile_trace.GetServerDropMetrics(), profile_trace.GetGatewayDropMetrics()
	counters := []*atomic.Uint64{
		&profile_trace.ServerDropCounters.ReceiveQueueFull,
		&profile_trace.ServerDropCounters.ProtocolQueueFull,
		&profile_trace.ServerDropCounters.AsyncModuleQueueFull,
		&profile_trace.ServerDropCounters.DeliverySend,
		&profile_trace.GatewayDropCounters.LinkerQueueFull,
		&profile_trace.GatewayDropCounters.ClientSend,
		&profile_trace.GatewayDropCounters.BroadcastSend,
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				for _, counter := range counters {
					counter.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	s, g := profile_trace.GetServerDropMetrics(), profile_trace.GetGatewayDropMetrics()
	deltas := []uint64{s.ReceiveQueueFull - serverBefore.ReceiveQueueFull, s.ProtocolQueueFull - serverBefore.ProtocolQueueFull,
		s.AsyncModuleQueueFull - serverBefore.AsyncModuleQueueFull, s.DeliverySend - serverBefore.DeliverySend,
		g.LinkerQueueFull - gatewayBefore.LinkerQueueFull, g.ClientSend - gatewayBefore.ClientSend, g.BroadcastSend - gatewayBefore.BroadcastSend}
	for i, delta := range deltas {
		if delta != 800 {
			t.Errorf("counter %d: delta=%d, want 800", i, delta)
		}
	}
}

// TestModuleMetricsDuringRegistration 验证诊断在模块发布/删除期间读取快照不会访问普通注册map。
func TestModuleMetricsDuringRegistration(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			sc.ServerModMgr.Store("metrics-test", new(sc.ServerModule))
			sc.ServerModMgr.Delete("metrics-test")
		}
	}()
	for i := 0; i < 1000; i++ {
		sc.GetModuleQueueMetrics()
	}
	wg.Wait()
}
