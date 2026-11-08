# 性能测试模板

所有命令从项目根目录执行。工具使用 TCP、模式1、protobuf，发送协议1 `C2STest{Num:1}`，校验协议2 `S2CTestRet{Num:2}`。先启动数据库依赖、业务服务和网关，确认协议映射一致。其他协议或 WS 需要另写客户端适配，不能直接套用本工具。

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

`connect` 仅建连；测试持续时间超过服务器空闲超时后断连是预期行为。`read-delay` 延迟已包含在RTT内，不是纯服务器耗时。`noread` 持续发送但完全不读取，用于观察写阻塞、队列满、连接退出；该模式不统计业务成功延迟。小回包可能很久才填满系统socket缓存，需结合服务端队列观察，不能仅凭运行60秒认定已覆盖慢连接故障。

## 3. 服务端观测（仅测试部署启用）

在各进程完成模块及协议注册后调用；运行期间不要修改协议路由map：

```go
import "mixture/test/performance/observe"

diagnostics, err := observe.Start("127.0.0.1:6060")
if err != nil { panic(err) }
defer diagnostics.Close() // 放在保持运行的启动函数中，不能放在立即返回的初始化函数中
```

不同进程使用不同端口，绑定回环地址。模板不会自动改动生产启动流程。

```powershell
Invoke-RestMethod http://127.0.0.1:6060/metrics | ConvertTo-Json -Depth 8
go tool pprof -http=127.0.0.1:8081 http://127.0.0.1:6060/debug/pprof/heap
go tool pprof -http=127.0.0.1:8082 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
```

每1～5秒采集一次metrics。`protocol_queues` 是瞬时长度，不是峰值；多个协议可能指向同一个模块队列，不要累加。框架当前没有独立丢弃计数器，可按压测时间窗口统计日志中的 `Queue Full`、`channel full`、`Drop msg`，但日志统计只是辅助证据，不是精确端到端丢失率。若需要精确丢弃指标，应在各失败分支加入原子计数；本模板未修改业务路径。

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
