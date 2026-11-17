# 远程性能测试部署说明

适用本轮更新后的正式入口 `main.go`。目标仍为此前的 `113.45.222.72`，业务 TCP 端口示例为 `7891`；按实际部署配置调整。
本文件提供准备步骤，不会自动重启或修改远程服务器。首次测试先保持 TCP、模式1、Protobuf；准备完成后再开始施压。

## 1. 这次需要提交什么

- `src/common/profile_trace/trace.go`：集中定义统计结构、原子丢包计数器和独立诊断路由；`main.go` 注入只读采样回调。现有 `/metrics` 增加进程/构建信息、堆、分配、GC、模块队列、客户端写队列；使用独立调试路由。
- `src/common/server_common/server_module.go`：通过 `ServerModMgr` 的 `sync.Map` 获取模块队列快照，允许在启动期间采样。
- `src/gateway_server/definition.go`：通过 `GateClientMap` 获取客户端写队列汇总，不改发送与回收流程。
- `system.txt`：新增 `debugListenAddress`，默认 `127.0.0.1`。
- `test/concurrent`：压测工具支持 TCP、WS/WSS 和合法大请求；大请求通过 protobuf unknown 字段填充。
- 本目录的端口 SQL、Linux 系统采集脚本、测试说明，以及对应测试代码。

`test/performance/local` 是可选本地隔离夹具，既不需要部署运行，也不是正式服务启动入口。
它不初始化真实 Mongo/Memcache，不能用其本地结果代替远程数据库或容量验收。

## 2. 编译与启动环境

- Linux 与服务当前架构匹配；Go 1.23 或更新的兼容版本，GCC/CGO 可用（项目使用 `go-sqlite3`）。建议直接在服务器或同平台构建机编译。
- 原有 MongoDB、Memcached 可达，四类后端的配置记录及数据库账号保持不变。
- 部署时完整保留 `system.txt`、`net_sql.db`、`proto/`、`settings/`，工作目录仍为项目/部署根目录。
- 至少预留日志及 profile 空间；建议测试期间空闲磁盘不低于 2GB，并检查 `ulimit -n` 能覆盖目标连接数及进程开销。
- 压测客户端使用独立机器；服务器本机回环测试另行执行，不能混为一组容量结果。

从项目根目录构建，正式性能采样不使用 `-race`：

```bash
mkdir -p bin log/moduleLog results/perf
CGO_ENABLED=1 go build -trimpath -ldflags "-X mixture/src/common/profile_trace.BuildRevision=$(git rev-parse HEAD)" -o bin/mixture .
go build -trimpath -o bin/load ./test/concurrent
go version -m bin/mixture
```

优先使用现有的 systemd/进程管理器替换可执行文件并启动，保留各服务原来的工作目录、账号和数据库配置。
若手动启动，命令形态仍为下列形式（示例ID均为1），每个服务由各自终端或进程管理器托管：

```bash
./bin/mixture actor_server 1
./bin/mixture modularized_server 1
./bin/mixture interactive_server 1
./bin/mixture web_server 1
./bin/mixture gateway_server 1
```

原有部署流程不变：先确认后端初始化及网关连接完成，再放行业务流量。
诊断 HTTP 会早于模块初始化完成开始监听，`/metrics` 能响应不等于服务已经就绪。

## 3. 配置 system.txt

保留其余配置，确认以下键各只有一条。修改这些启动配置后需要重启相关进程；settings JSON 热更不负责应用这些值。

```ini
pprof=true
debugListenAddress=127.0.0.1
slogLevel=4
workMode=tcp
streamBufferDealWay=1
protocolType=protobuf
```

`slogLevel=4` 为 WARN，仍保留警告和错误，减少每请求 DEBUG/INFO 日志干扰。之后会单独比较原日志级别，结束测试恢复原配置。
网关原有白名单和外部防火墙也要放行压测客户端的来源IP。

### 方式A：SSH隧道（默认）

保持 `debugListenAddress=127.0.0.1`；调试端口不需要开公网入站。在压测机器执行：

```bash
ssh -N -L 16060:127.0.0.1:6060 -L 16061:127.0.0.1:6061 \
  -L 16062:127.0.0.1:6062 -L 16063:127.0.0.1:6063 -L 16064:127.0.0.1:6064 USER@113.45.222.72
```

随后使用本机 `http://127.0.0.1:16060/metrics` 等地址。SSH实际端口不是22时加 `-p 实际端口`。

### 方式B：直接开放调试端口

如果要让我从压测客户端直接连接HTTP诊断，设：

```ini
debugListenAddress=0.0.0.0
```

重启后，在云安全组及主机防火墙放行以下TCP端口，**来源限定为压测客户端公网IP**。
仅开防火墙但仍绑定 `127.0.0.1`，外部仍无法访问。
这些接口没有应用层身份校验，会暴露诊断数据并允许触发CPU采样；不要设置为全网来源。
本文不提供防火墙自动修改命令，避免覆盖现有IP限制。

| 进程/用途 | 端口 | 是否需要本轮开放 |
| --- | --- | --- |
| TCP Gateway 业务 | 7891（沿用实际值） | 必须，来源放行压测客户端 |
| Gateway 调试 | 6060/TCP | 必须；隧道方式无需公网开放 |
| Actor 调试 | 6061/TCP | 必须；隧道方式无需公网开放 |
| Modularized 调试 | 6062/TCP | 建议，观察其他后端 |
| Interactive 调试 | 6063/TCP | 建议，观察其他后端 |
| Web 调试 | 6064/TCP | 建议，观察其他后端 |
| SSH | 现有管理端口 | 仅允许管理/采集端访问 |
| 后端内部TCP、MongoDB、Memcached | 沿用现有值 | 不需要为公网压测额外开放 |

WS 当前是启动配置中的另一种网关模式，**不会因为开放另一个端口就自动启动第二个WS监听**。
首次测试先部署TCP。后续可在测试窗口把同一网关切换为 `workMode=ws` 并重启，仍使用7891，通过 `ws://113.45.222.72:7891/ws` 访问。
若需要TCP/WS同时运行，需另建独立网关实例、配置目录、GatewayID及对应后端映射，不能共用相同GateID。
直接提供的是 `ws://`；只有配置了TLS终结或反向代理才使用 `wss://`。

## 4. 在 net_sql.db 配置调试端口

先查看当前值。以下操作假设五类进程ID均为1，不同ID须修改SQL中的WHERE条件。
每个进程必须独占监听端口；修改SQL只调整 `deBugModePort`，不会调整业务监听或数据库地址。

使用SQLite备份命令保存一致快照（可在有WAL的情况下使用，不要直接复制正在写入的单个db文件）：

```bash
perf_backup="net_sql.before-perf-$(date +%Y%m%d-%H%M%S).db"
sqlite3 net_sql.db ".backup '$perf_backup'"
sqlite3 -bail net_sql.db < test/performance/remote_ports.sql
```

SQL执行后应输出五行：Gateway 6060、Actor 6061、Modularized 6062、Interactive 6063、Web 6064。
如果记录不存在，UPDATE不会创建配置；应先用原部署工具补齐对应服务记录。数据库内其他内容不要用测试夹具覆盖。

## 5. 部署后的自检

服务器本机运行：

```bash
curl --fail --max-time 5 http://127.0.0.1:6060/metrics
curl --fail --max-time 5 http://127.0.0.1:6061/metrics
curl --fail --max-time 5 http://127.0.0.1:6060/debug/pprof/
ss -lntp
./bin/load -addr 127.0.0.1:7891 -connections 1 -ramp 1 -duration 5s -interval 1s
```

其余三个后端使用6062～6064检查。HTTP响应应具有正确的 `server_name`、`server_id`、`pid`、`build_revision`，
以及 `heap_alloc`、`total_alloc`、`mallocs`、`num_gc`、`module_queues`、`gateway_write_queues` 和两类丢弃计数。
Actor的 `module_queues` 应出现已加载模块（例如user）；网关上模块为空正常。PID用于系统采样并在每次进程重启后重新读取。
`build_modified` 表示编译时源码工作区状态，`build_revision` 是本次构建注入的提交号；这两个值不代表已验证全部文件一致。

压测端再通过已选的直连/隧道地址验证，例如直连方式：

```bash
curl --fail --max-time 5 http://113.45.222.72:6060/metrics
curl --fail --max-time 5 http://113.45.222.72:6061/metrics
```

只做上述低频自检；阶梯压测、慢客户端、重启实验和30分钟持续测试等准备环境确认后再运行。

## 6. 系统指标与profile

采集脚本只读取Linux `/proc`，需要Bash、常见coreutils；`ip`、`ss`可选。以服务进程所属用户运行可获取相应 `/proc/PID/io`。
不会读取进程环境变量、命令行或数据库密码。用刚从 `/metrics` 获取的实际PID替换示例值：

```bash
GATE_PID=12345
ACTOR_PID=12346
PERF_DURATION_SECONDS=1800 PERF_INTERVAL_SECONDS=2 \
  bash test/performance/collect_system.sh results/perf "$GATE_PID" "$ACTOR_PID"
```

脚本按时间戳创建独立日志，记录CPU时间计数、内存/RSS、进程IO、网卡流量、TCP重传等原始计数。
CPU和网卡需要计算相邻采样增量；重启后PID发生变化，应重新启动采集。云平台的2Mbit/s限速等指标仍需控制台导出，本脚本不能替代云端监控。

CPU profile建议负载稳定后按进程分别采集，每个进程同一时间只允许一份CPU采样：

```bash
curl --fail --max-time 40 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30' -o results/perf/gateway-cpu.pprof
curl --fail --max-time 40 'http://127.0.0.1:6061/debug/pprof/profile?seconds=30' -o results/perf/actor-cpu.pprof
curl --fail --max-time 10 'http://127.0.0.1:6060/debug/pprof/heap' -o results/perf/gateway-heap.pprof
```

连续两次CPU命令采到不同时间窗口；需要同窗口时分别后台发起并等待两者完成。使用 `go tool pprof -top` 分析保存的文件。
metrics建议每2～5秒一次；队列长度为瞬时值，不能证明两次采样之间从未满过，应结合丢弃计数增量。

## 7. 测试工具与环境边界

TCP/WS都支持模式1、Protobuf，校验 `S2CTestRet.Num=2`。服务端启动后，可由独立压测机器使用本项目的工具：

```bash
./bin/load -addr 113.45.222.72:7891 -connections 20 -ramp 20 -duration 60s -interval 0
./bin/load -addr ws://113.45.222.72:7891/ws -connections 20 -ramp 20 -duration 60s -interval 0
./bin/load -addr 113.45.222.72:7891 -connections 20 -ramp 20 -duration 60s -interval 0 -payload-bytes 4092
```

WS命令只在网关切换为WS后执行。第三条是最大合法请求数据，默认业务仍回小包；它不等于大回包/广播测试。
大回包、广播和慢模块注入需要额外的测试业务入口，后续单独部署，当前没有给正式业务函数增加延迟或改变回包内容。
客户端为闭环负载，每连接最多一个在途请求，P95/P99为成功请求延迟分桶上界；不能用该结果证明固定到达率的延迟SLO。
`-mode connect` 不发首包，当前服务约10s断开是预期结果，不再按旧版180s空闲超时解释。

请准备好后提供：构建提交号、部署服务ID、选择的诊断访问方式/地址、实际服务器资源和带宽，以及是否有其他业务流量。
只需提供连接方式和结果，不要在聊天中发送数据库密码或私钥。测试结束后关闭 `pprof` 并恢复原监听地址、日志级别和临时防火墙规则。
