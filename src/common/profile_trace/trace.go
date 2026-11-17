// Package profile_trace 集中定义性能观测指标和诊断路由，不依赖网关及后端业务包。
package profile_trace

import (
	"encoding/json"
	"mixture/src/common/printer"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// 记录进程启动时间，用于区分重启及计算观测窗口。
var ProcessStartTime = time.Now()

// 部署构建版本，可通过 -ldflags "-X mixture/src/common/profile_trace.BuildRevision=提交号" 指定。
var BuildRevision string

// 后端服务进程自启动以来的累计消息丢弃统计。
type ServerDropMetrics struct {
	ReceiveQueueFull     uint64 `json:"receive_queue_full"`      //网关连接的接收处理队列已满
	ProtocolQueueFull    uint64 `json:"protocol_queue_full"`     //协议目标模块的接收队列已满
	AsyncModuleQueueFull uint64 `json:"async_module_queue_full"` //异步模块调用的目标队列已满
	DeliverySend         uint64 `json:"delivery_send"`           //返回网关时目标连接不存在、已关闭或发送队列已满
}

// 该结构保存后端服务各类消息丢弃场景的原子累计值。
var ServerDropCounters struct {
	ReceiveQueueFull     atomic.Uint64 //网关连接接收队列已满的累计次数
	ProtocolQueueFull    atomic.Uint64 //协议模块队列已满的累计次数
	AsyncModuleQueueFull atomic.Uint64 //异步模块调用队列已满的累计次数
	DeliverySend         atomic.Uint64 //返回网关时发送失败的累计次数
}

// 返回当前后端服务丢弃计数的只读快照。
func GetServerDropMetrics() ServerDropMetrics {
	// 各项独立原子读取，不清零计数；并发丢包时这些值并非同一时刻的事务快照。
	return ServerDropMetrics{
		ReceiveQueueFull:     ServerDropCounters.ReceiveQueueFull.Load(),
		ProtocolQueueFull:    ServerDropCounters.ProtocolQueueFull.Load(),
		AsyncModuleQueueFull: ServerDropCounters.AsyncModuleQueueFull.Load(),
		DeliverySend:         ServerDropCounters.DeliverySend.Load(),
	}
}

// 网关进程自启动以来的累计消息丢弃统计。
type GatewayDropMetrics struct {
	LinkerQueueFull uint64 `json:"linker_queue_full"` //发往后端服务的 Linker 队列已满
	ClientSend      uint64 `json:"client_send"`       //目标客户端不存在、已关闭或写队列已满
	BroadcastSend   uint64 `json:"broadcast_send"`    //广播目标不存在、已关闭、写队列已满或广播包过大
}

// 该结构保存网关各类消息丢弃场景的原子累计值。
var GatewayDropCounters struct {
	LinkerQueueFull atomic.Uint64 //发往后端的队列已满的累计次数
	ClientSend      atomic.Uint64 //发往客户端失败的累计次数
	BroadcastSend   atomic.Uint64 //广播发送失败的累计目标数
}

// 返回当前网关丢弃计数的只读快照。
func GetGatewayDropMetrics() GatewayDropMetrics {
	// 返回普通数值副本，不复制原子对象，也不影响发送路径继续累计。
	return GatewayDropMetrics{
		LinkerQueueFull: GatewayDropCounters.LinkerQueueFull.Load(),
		ClientSend:      GatewayDropCounters.ClientSend.Load(),
		BroadcastSend:   GatewayDropCounters.BroadcastSend.Load(),
	}
}

// 汇总已注册客户端写队列的瞬时状态。
type GatewayWriteQueueMetrics struct {
	Clients   int `json:"clients"`    //本次遍历采样到的客户端数，可能与并发变化中的在线计数略有差异
	Length    int `json:"length"`     //全部客户端写队列当前长度之和
	MaxLength int `json:"max_length"` //本次采样的最大单连接写队列长度
	Capacity  int `json:"capacity"`   //采样到的客户端写队列容量之和
}

// 保存模块队列的瞬时长度和容量，不代表历史峰值。
type ModuleQueueMetrics struct {
	Length   int `json:"length"`   //采样时等待处理的消息数
	Capacity int `json:"capacity"` //模块队列容量
}

// 提供业务队列的只读采样入口，避免观测包与业务包循环依赖。
// 回调应支持并发采样；未提供的回调返回对应的空指标。
type TraceSources struct {
	ModuleQueues       func() map[string]ModuleQueueMetrics //模块队列快照
	GatewayClients     func() int32                         //网关当前在线客户端数
	GatewayWriteQueues func() GatewayWriteQueueMetrics      //客户端写队列汇总
}

// 创建进程独立的诊断路由，不将调试接口挂载到业务监听上。
func NewDebugMux(serName, processID string, sources TraceSources) *http.ServeMux {
	revision, modified := BuildRevision, false // 优先使用构建参数注入的版本号，未指定时回退到Go记录的VCS信息。
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && revision == "" {
				revision = setting.Value
			}
			if setting.Key == "vcs.modified" {
				modified = setting.Value == "true" // 表示构建时工作区是否有修改，不表示运行期间的文件状态。
			}
		}
	}
	if revision == "" {
		revision = "unknown"
	}
	// 使用独立路由，避免业务HTTP监听意外挂载这里的诊断接口。
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		queues := make(map[string]ModuleQueueMetrics) // 回调只在采样请求时执行,不启动额外协程,也不持有业务连接。
		if sources.ModuleQueues != nil {
			queues = sources.ModuleQueues() // 允许未配置某类采样源,不同回调分步读取,结果不保证处于同一时刻。
		}
		var clients int32
		if sources.GatewayClients != nil {
			clients = sources.GatewayClients()
		}
		var writeQueues GatewayWriteQueueMetrics
		if sources.GatewayWriteQueues != nil {
			writeQueues = sources.GatewayWriteQueues()
		}

		// ReadMemStats有采样开销，仅在请求时读取，外部采集应避免过高频率。
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store") //防止客户端或代理复用旧的采样结果

		// total_alloc、mallocs、frees及GC总量为进程累计值，计算速率需取两次采样的差值。
		metrics := map[string]any{
			"time":                 time.Now(),
			"server_name":          serName,
			"server_id":            processID,
			"pid":                  os.Getpid(),
			"started_at":           ProcessStartTime,
			"uptime_seconds":       time.Since(ProcessStartTime).Seconds(),
			"build_revision":       revision,
			"build_modified":       modified,
			"go_version":           runtime.Version(),
			"goos":                 runtime.GOOS,
			"goarch":               runtime.GOARCH,
			"gomaxprocs":           runtime.GOMAXPROCS(0),
			"goroutines":           runtime.NumGoroutine(),
			"heap_alloc":           memory.HeapAlloc,
			"heap_inuse":           memory.HeapInuse,
			"heap_sys":             memory.HeapSys,
			"total_alloc":          memory.TotalAlloc,
			"mallocs":              memory.Mallocs,
			"frees":                memory.Frees,
			"num_gc":               memory.NumGC,
			"gc_pause_ns":          memory.PauseTotalNs,
			"gc_cpu_fraction":      memory.GCCPUFraction,
			"module_queues":        queues,
			"gateway_clients":      clients,
			"gateway_write_queues": writeQueues,
			"gateway_drops":        GetGatewayDropMetrics(),
			"server_drops":         GetServerDropMetrics(),
		}
		if err := json.NewEncoder(w).Encode(metrics); err != nil {
			printer.TextError("Encode metrics failed", "err", err)
		}
	})

	// Index同时处理heap、goroutine等命名profile,CPU采样与执行跟踪使用专用处理器。
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
