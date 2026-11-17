# TCP / WebSocket 并发压测

在项目根目录启动完整服务端后运行。本程序只连接指定网关，不负责启动服务端。

```powershell
# 仅测试并发在线连接；建立 1000 条连接，每秒增加 100 条，全部发起后保持 1 分钟
go run ./test/concurrent -addr 127.0.0.1:10230 -mode connect -connections 1000 -ramp 100 -duration 1m

# 测试网关 → actor_server → 网关的完整请求回包链路
go run ./test/concurrent -addr 127.0.0.1:10230 -mode echo -connections 1000 -ramp 100 -duration 1m -interval 1s

# 网关配置为WS时，使用相同模式1、protobuf协议
go run ./test/concurrent -addr ws://127.0.0.1:10230/ws -connections 100 -duration 30s -interval 0

# 最大合法请求数据；正式业务仍返回原有小包
go run ./test/concurrent -addr 127.0.0.1:10230 -connections 20 -duration 30s -interval 0 -payload-bytes 4092
```

`-ramp` 是每秒发起的连接数。`-duration` 从全部连接发起后开始计算；总用时还包括建连阶段。建议从 100、1000 逐步增加到目标连接数，并结合服务器 CPU、内存、GC、模块队列和丢包日志观察。

`echo` 模式要求服务器为模式1、`protocolType=protobuf`，发 `C2STest`（协议号 1），校验 `S2CTestRet`（协议号 2）和返回的 `Num=2`。修改协议号、协议内容或切换 JSON 模式后，需要同步调整程序。延迟统计为单次请求到完整回包的时间；p50/p95/p99 使用分桶上界。`connect` 模式不发送首包，当前服务器约10秒断开属于预期防护，工具仍会将其计入失败字段，分析时应区分。

`-addr` 支持 TCP host:port、`ws://host:port/ws` 和 `wss://host:port/ws`，WSS要求服务端已有TLS终结。
`-payload-bytes` 默认为2，常用1024或4092，通过合法unknown字段填充请求，不改变服务端回包；少数无法精确编码的长度会报参数错误。
只有部署WS网关后才运行WS命令，工具不会自动修改服务端模式。

压测客户端本身也受本机可用端口、文件句柄、CPU 和内存限制。拨号失败不等于服务器拒绝；结合服务端日志判断原因。
