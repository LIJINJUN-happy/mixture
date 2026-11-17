# 远程性能验收编排器

只连接指定的业务端口及6060～6064诊断端口，不修改远程配置或重启服务。运行前必须获得目标所有者的压测授权，确认测试窗口和来源IP限制。

先从项目根目录编译两个客户端（Windows示例，输出目录自行指定）：

```powershell
go build -o "$env:TEMP/load.exe" ./test/concurrent
go build -o "$env:TEMP/remote.exe" ./test/performance/remote
```

分阶段执行；每阶段使用新的输出目录，不并行运行负载阶段：

```powershell
& "$env:TEMP/remote.exe" -host 113.45.222.72 -addr 113.45.222.72:7891 -load "$env:TEMP/load.exe" -out "$env:TEMP/perf-initial" -suite initial
& "$env:TEMP/remote.exe" -host 113.45.222.72 -addr 113.45.222.72:7891 -load "$env:TEMP/load.exe" -out "$env:TEMP/perf-edges" -suite edges
& "$env:TEMP/remote.exe" -host 113.45.222.72 -addr 113.45.222.72:7891 -load "$env:TEMP/load.exe" -out "$env:TEMP/perf-soak" -suite soak
```

- `initial`：1/10/25/50连接各重复3次，100连接带30秒CPU采样，250/500/1000连接每连接1秒发包；另有一条合法请求后的空闲超时探针。
- `edges`：合法分片、100包合并、首包截止时间、1KiB/最大合法请求、慢读、不读、首包超时和恢复。
- `soak`：25连接、每连接100ms节拍持续30分钟（约250 RPS），结束后10连接不限速恢复测试。限速用于控制远端逐请求日志量；不代表已经完成30分钟满载容量验收。

`recheck` 用于出现错误后单独复测：先10连接恢复，再100连接慢速建连，最后250/500/1000连接低频在线；应先查看上一阶段失败原因，不应盲目反复执行。

`edges-low` 用于大包不限速出现超时后的降档：最大请求改为20连接、每连接100ms节拍，再执行慢读、不读、首包超时和恢复。`stall` 则只占一条TCP连接，请求本地接收缓冲为1024字节并最多45秒只写不读，再验证恢复；实际TCP窗口由操作系统决定，仍需结合队列和日志判断是否触发写超时。

每2秒采集5个进程的JSON指标，保留本地时间、采集耗时及失败信息。每档保留前后快照、客户端逐秒原始日志和TOTAL摘要。阶段结束下载Gateway/Actor的heap和goroutine profile，不强制GC。合法echo场景出现请求或拨号错误时，该阶段停止升压，需检查结果再决定是否继续；连接超时和不读场景的错误需按防护语义解释。

客户端每连接仅一个在途请求（不读模式除外）；RPS是闭环成功响应率，延迟分位数是成功请求直方图桶上界。队列是采样瞬时值，不代表采样间隙从未积压。小回包不读测试可能填不满系统socket缓存，不能据其无错误就宣称验证了写超时。

本工具不覆盖WS模式切换、非法包封禁、进程重启、数据库故障、慢模块注入、真实业务大回包和广播，也不采集整机CPU、RSS或云带宽限速。系统层指标需配合 `../collect_system.sh`；缺失这些数据时不得把公网吞吐平台判为框架CPU上限。
