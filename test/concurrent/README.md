# TCP 并发压测

在项目根目录启动完整服务端后运行。本程序只连接指定网关，不负责启动服务端。

```powershell
# 仅测试并发在线连接；建立 1000 条连接，每秒增加 100 条，全部发起后保持 1 分钟
go run ./test/concurrent -addr 127.0.0.1:10230 -mode connect -connections 1000 -ramp 100 -duration 1m

# 测试网关 → actor_server → 网关的完整请求回包链路
go run ./test/concurrent -addr 127.0.0.1:10230 -mode echo -connections 1000 -ramp 100 -duration 1m -interval 1s
```

`-ramp` 是每秒发起的连接数。`-duration` 从全部连接发起后开始计算；总用时还包括建连阶段。建议从 100、1000 逐步增加到目标连接数，并结合服务器 CPU、内存、GC、模块队列和丢包日志观察。

`echo` 模式按当前 `system.txt` 的 `protocolType=protobuf` 发 `C2STest`（协议号 1），校验 `S2CTestRet`（协议号 2）和返回的 `Num=2`。修改协议号、协议内容或切换 JSON 模式后，需要同步调整程序。延迟统计为单次请求到完整回包的时间；p50/p95/p99 使用分桶上界。`connect` 模式只保持连接并检测提前断开，不发送心跳；服务器读超时为 180 秒，因此保持阶段应小于该值。

压测客户端本身也受本机可用端口、文件句柄、CPU 和内存限制。拨号失败不等于服务器拒绝；结合服务端日志判断原因。
