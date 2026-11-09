# 远程服务器性能实测报告

测试日期：2026-11-09（Asia/Shanghai）。目标：113.45.222.72:7891。
服务端配置由用户提供：Unix、2核、2GB、2Mbit/s 带宽；未登录服务器核验。
压测端：Windows amd64，Go 1.23.4；本地 HEAD：d512394cabaca750296c2ad4740656bfe988b8ca（构建使用当前工作区，远端版本未知）。

## 方法

使用现有 test/concurrent，TCP、模式1、Protobuf。发送协议1 C2STest{Num:1}，验证协议2 S2CTestRet{Num:2}。请求及响应各6字节应用数据，包含2字节长度头和2字节协议号。公网端到端RTT包括网络、网关、后端及排队时间，不代表纯业务执行时间。未验证远端实际部署拓扑。

每连接最多一个未完成请求；interval=0 为闭环发送。测试逐档串行执行，超时5秒。先低频探测，再10、50、100连接，最后降至10连接验证恢复。各高负载档保持30秒，额外计入建连时间。本轮为短时探索，每档未完成模板建议的3次独立重复，不是最终容量验收或长期稳定性报告。

## 结果

|场景|目标/关闭前在线|总耗时秒|发送|校验成功|成功RPS|P95上界|P99上界|请求失败/拨号失败|结束未完成|
|---|---|---:|---:|---:|---:|---|---|---|---:|
|低频协议探测，间隔1秒|1/1|10.0003|10|10|1.00|20ms|20ms|0/0|0|
|10连接，间隔0|10/10|30.9043|42408|42398|1371.91|10ms|20ms|0/0|10|
|50连接，间隔0|50/50|31.9811|107082|107042|3347.04|20ms|500ms|0/0|40|
|100连接，间隔0|100/100|34.0020|108993|108907|3202.96|500ms|1s|0/0|86|
|恢复10连接，间隔0|10/10|30.9037|42627|42619|1379.09|10ms|20ms|0/0|8|

采用进程退出后的 TOTAL 为准，退出期间仍可能处理少量响应，因此与“关闭前”回包数存在差异。结束未完成是 sent-validated，包含主动结束测试时取消的请求，不可视作服务端丢包。未观察到请求失败，不代表所有已发送请求都完成。

P95/P99 为直方图区间上界，成功请求才参与统计。例如500ms桶表示分位数位于200～500ms，1s桶表示500ms～1s，不是精确500ms或1秒。闭环负载会随服务变慢而降低发送速率，不能据此推断固定到达率下延迟。

## 观察与判断

- 10连接约1372 RPS，恢复测试约1379 RPS，P99均≤20ms，未发现测试结束后持续性能下降。
- 50连接早期曾出现约5710 RPS的单秒回包，后段多数约2900～3300 RPS。不能用启动阶段峰值作为持续容量。
- 100连接吞吐没有继续增长，P95/P99明显变差；该档连接逐步建立，保持阶段前几秒在线数由80、89升至100，没有拨号失败。
- 当前网络路径、部署和极小回包下，观察到约3000～3300 RPS的平台区。它不是框架CPU上限，也不能换算成真实玩家人数。对于实时战斗业务，50/100连接档的长尾已经需要关注。
- 尚未测10与50之间的拐点，也未定义业务延迟SLO；不能给出最终“最大安全并发”。

2Mbit/s 换算为250000字节/秒（约244KiB/s）。按3200 RPS、6字节回包计算，应用回包约19200字节/秒，但小TCP包另有TCP/IP头、ACK、重传等开销，实际网络占用不能只看6字节。带宽整形、丢包重传、CPU或日志开销均可能导致当前平台和长尾，本轮没有网卡、抓包或进程指标，不能确定根因。

作为量级参考，若每次回包应用数据为1KiB，2Mbit/s仅按应用数据计算也只有约244回包/秒的理论上限，实际还需扣除网络开销及其他流量。本次小包结果不可套用到大包或战斗广播。

## 原始 TOTAL 摘录

```text
TOTAL elapsed=10.0002804s sent=10 validated=10 failed=0 dial_failed=0 outstanding=0 success_rps=1.00 p95=≤20ms p99=≤20ms
TOTAL elapsed=30.9042625s sent=42408 validated=42398 failed=0 dial_failed=0 outstanding=10 success_rps=1371.91 p95=≤10ms p99=≤20ms
TOTAL elapsed=31.9811228s sent=107082 validated=107042 failed=0 dial_failed=0 outstanding=40 success_rps=3347.04 p95=≤20ms p99=≤500ms
TOTAL elapsed=34.0019689s sent=108993 validated=108907 failed=0 dial_failed=0 outstanding=86 success_rps=3202.96 p95=≤500ms p99=≤1s
TOTAL elapsed=30.9037466s sent=42627 validated=42619 failed=0 dial_failed=0 outstanding=8 success_rps=1379.09 p95=≤10ms p99=≤20ms
```

工具每秒输出的“运行”字段实际是距上次打印的间隔，不是累计运行时间；最后一次打印可能只覆盖极短窗口（恢复档出现9252.4/s），不应作为单秒峰值使用。以上结论使用 TOTAL 和完整秒窗口。

## 复现命令

在项目根目录依次执行：

```powershell
go build -o test/performance/load.exe ./test/concurrent
./test/performance/load.exe -addr 113.45.222.72:7891 -connections 1 -ramp 1 -duration 10s -interval 1s -timeout 5s
./test/performance/load.exe -addr 113.45.222.72:7891 -connections 10 -ramp 10 -duration 30s -interval 0 -timeout 5s
./test/performance/load.exe -addr 113.45.222.72:7891 -connections 50 -ramp 25 -duration 30s -interval 0 -timeout 5s
./test/performance/load.exe -addr 113.45.222.72:7891 -connections 100 -ramp 25 -duration 30s -interval 0 -timeout 5s
./test/performance/load.exe -addr 113.45.222.72:7891 -connections 10 -ramp 10 -duration 30s -interval 0 -timeout 5s
```

## 尚未测得

服务端CPU、RSS、每请求分配、GC、队列长度和丢弃计数均未采集：本次仅有业务端口访问，没有SSH或诊断接口访问资料。未执行慢模块注入、不读响应、进程重启、大包、最高空闲连接数及长时间耐久测试。没有服务端观测时不继续堆高并发，避免将更严重的排队误认为容量提升。

下一步应按 performance/README.md 通过回环诊断接口及SSH隧道采集各进程指标，配合云平台出网带宽和TCP重传；在20、30、40连接档各重复3次，并使用实际战斗包大小、频率和广播人数测试。
