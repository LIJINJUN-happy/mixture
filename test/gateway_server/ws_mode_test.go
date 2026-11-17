package gateway_server_test

import (
	gw "mixture/src/gateway_server"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWSRoutesCanRegisterAgain 验证多次启动可独立注册WS路由，不会因重复注册而panic。
func TestWSRoutesCanRegisterAgain(t *testing.T) {
	first := gw.DoRegister()
	second := gw.DoRegister()
	if first == second || first == http.DefaultServeMux || second == http.DefaultServeMux {
		t.Fatal("WS listeners must have independent route tables")
	}
	for _, mux := range []*http.ServeMux{first, second} {
		for _, path := range []string{"/ws", "/test"} {
			_, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil))
			if pattern != path {
				t.Fatalf("route %s not registered: pattern=%q", path, pattern)
			}
		}
	}
	first.HandleFunc("/first-only", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	response := httptest.NewRecorder()
	second.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/first-only", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("listener routes leaked across instances: status=%d", response.Code)
	}
}

// TestWSRoutesExcludeDebugHandlers 验证默认路由表的调试接口不会出现在WS监听上。
func TestWSRoutesExcludeDebugHandlers(t *testing.T) {
	original := http.DefaultServeMux
	http.DefaultServeMux = http.NewServeMux()
	defer func() { http.DefaultServeMux = original }()
	for _, path := range []string{"/debug/pprof/", "/metrics"} {
		http.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
	}
	mux := gw.DoRegister()
	for _, path := range []string{"/debug/pprof/", "/metrics"} {
		debugResponse := httptest.NewRecorder()
		http.DefaultServeMux.ServeHTTP(debugResponse, httptest.NewRequest(http.MethodGet, path, nil))
		if debugResponse.Code != http.StatusNoContent {
			t.Fatalf("debug route %s was changed", path)
		}
		gatewayResponse := httptest.NewRecorder()
		mux.ServeHTTP(gatewayResponse, httptest.NewRequest(http.MethodGet, path, nil))
		if gatewayResponse.Code != http.StatusNotFound {
			t.Fatalf("debug route %s exposed on WS listener: status=%d", path, gatewayResponse.Code)
		}
	}
}
