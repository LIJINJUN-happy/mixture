# 性能测试模板

本轮远程部署先按 [REMOTE_DEPLOY.md](REMOTE_DEPLOY.md) 准备，包含代码改动、6060～6064调试端口、SQL、直连/SSH隧道及系统采集方式。

实测报告：[113.45.222.72:7891，2026-11-09 公网短时阶梯压测](remote_113.45.222.72_2026-11-09.md)。

本轮详细记录：[2026-11-17 远程性能与稳定性实测](remote_113.45.222.72_2026-11-17.md)；分阶段采样和复现工具见 [remote/README.md](remote/README.md)。

所有命令从项目根目录执行。工具支持 TCP地址或 `ws://host:port/ws` / `wss://host:port/ws`，使用模式1、protobuf，发送协议1 `C2STest{Num:1}`，校验协议2 `S2CTestRet{Num:2}`。先启动数据库依赖、业务服务和网关，确认协议映射一致。JSON及另外两种切包模式尚未适配。
`-payload-bytes 4092` 通过合法protobuf unknown字段填充最大请求数据；它不改变业务回包大小，不代表已覆盖大回包或广播。

## 1. 基线与阶梯并发

先编译，避免把编译成本计入结果：

```powershell
go build -o test/performance/load.exe ./test/concurrent
./test/performance/load.exe -addr 127.0.0.1:10230 -connections 1 -ramp 1 -duration 30s -interval 0
./test/performance/load.exe -addr 127.0.0.1:10230 -connections 100 -ramp 50 -duration 120s -interval 0
./test/performance/load.exe -addr 127.0.0.1:10230 -connections 500 -ramp 100 -duration 120s -interval 0
./test/performance/load.exe -addr 127.0.0.1:10230 -connections 1000 -ramp 100 -duration 120s -interval 0
```

每档至少运行3次；先用30秒试跑预热，再运行正式样本。记录机器CPU、内存、Go版本、提交号、日志级别、模块队列容量、部署拓扑。正式容量测试建议压测端独立机器。不要用 `-race` 构建测性能。

每连接只允许一个未完成请求，是闭环压测。`-interval 0` 表示收到响应立即发下一次；非零间隔由ticker节拍控制，不是全局固定RPS。服务器变慢会自动降低发送速率，因此不能据此宣称固定到达率下的P99。错误后连接退出，不自动补足，必须同时观察在线数，不能把掉连接后的低延迟误认为改善。

延迟是成功请求的RTT，P95/P99为直方图区间上界，非精确分位数；超时不进入成功延迟分布。TOTAL的成功RPS含建连及退出阶段，用于整体比较；每秒回包速率更适合看稳态。`outstanding=sent-validated` 包含超时、异常响应及结束时未完成请求，不能直接当作服务端丢包数。

## 2. 连接与慢客户端

```powershell
./test/performance/load.exe -mode connect -connections 1000 -ramp 100 -duration 120s
./test/performance/load.exe -mode echo -connections 100 -interval 10ms -read-delay 100ms -timeout 5s -duration 120s
./test/performance/load.exe -mode noread -connections 20 -interval 1ms -duration 60s
```

`connect` 仅建连；当前服务器有10秒首包超时，未发送首包的连接约10秒断开是预期行为。`read-delay` 延迟已包含在RTT内，不是纯服务器耗时。`noread` 持续发送但完全不读取，用于观察写阻塞、队列满、连接退出；该模式不统计业务成功延迟。小回包可能很久才填满系统socket缓存，需结合服务端队列观察，不能仅凭运行60秒认定已覆盖慢连接故障。

## 3. 服务端观测（仅测试部署启用）

正式部署使用 `main.go` 已有的诊断启动入口，通过 `pprof=true`、`debugListenAddress` 和数据库各服务的 `deBugModePort` 启用。
端口6060～6064为本轮推荐分配，需按部署说明配置并启动，不代表服务器上已经开启。
默认回环监听；直连需显式配置外部监听并限制来源IP。不要在正式启动路径另外调用 `observe.Start` 争用这些端口。
`test/performance/observe` 仅用于隔离测试夹具；正式接口返回 `module_queues`（按模块名），夹具旧接口的 `protocol_queues` 则按协议名采样。

```powershell
Invoke-RestMethod http://127.0.0.1:6060/metrics | ConvertTo-Json -Depth 8
go tool pprof -http=127.0.0.1:8081 http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -http=127.0.0.1:8082 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
```

建议每2～5秒采集一次metrics。`module_queues` 和 `gateway_write_queues` 是瞬时状态，不是历史峰值。
已有 `gateway_drops`、`server_drops` 累计计数，应记录同进程同时间窗口的增量；日志只能作为辅助证据，这些计数也不是精确端到端丢失率。
正式接口还提供进程ID、启动时间、构建版本、在线客户端数、堆、GC与累计分配；重启后不能直接对计数做跨进程相减。

在稳定流量窗口开始和结束各采一份metrics：

- 分配字节/成功请求 ≈ `Δtotal_alloc / Δ成功响应数`。
- 分配次数/成功请求 ≈ `Δmallocs / Δ成功响应数`。
- GC次数 = `Δnum_gc`；累计暂停 = `Δgc_pause_ns`。

这些是包含后台工作、失败请求及观测成本的进程级摊销指标；多进程要分别统计。没有服务端完成计数时，客户端成功响应数只是近似分母。`gc_cpu_fraction` 是进程启动以来累计比例，不能当作当前窗口值。

## 4. 慢模块与恢复

测试部署中将一个处理函数注册为：

```go
userMod.RegisterProtoHandleCaller("C2STest", observe.SlowHandler(10*time.Millisecond, user.Test))
```

单模块串行执行，仅这段延迟就使处理上限不高于约100次/秒。逐步增加连接数，观察队列、失败、回包速率。将延迟恢复为0后重启再测。不要在运行中修改handler map。确认拥塞不会导致其他无关模块无法响应。结束压测后观察goroutine、队列和堆是否趋于稳定，不要求堆立即归零，sync.Pool和GC会影响回落时间。

日志开/关或不同级别各跑一组相同负载，比较CPU与吞吐，避免用关闭错误日志掩盖丢弃。另安排30～60分钟持续运行和服务重启试验；重启时本客户端退出的连接不会重连，应重新发起一轮。

## 5. 记录表

切包微基准（只测函数，不代表整个请求的分配量）：

```powershell
go test ./test/server_common -run '^$' -bench BenchmarkSplitPacket -benchmem -count 3
```

输出包含ns/op、B/op、allocs/op；与服务端窗口指标配合使用。

|提交/场景|连接目标/实际|窗口秒数|成功RPS|P95上界|P99上界|请求失败|拨号失败|队列长度趋势|日志丢弃记录|B/成功请求|alloc/成功请求|GC次数/暂停|CPU/内存|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|基线||||||||||||||
|阶梯并发||||||||||||||
|慢模块||||||||||||||
|慢客户端||||||||||||||

容量结论应以满足你的延迟目标、错误率目标且队列不持续增长的最高稳定负载为准，不能用已发送请求数或最高连接数代替。
