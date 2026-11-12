# 协议使用与维护规则

本目录保存项目的通信协议定义及其路由配置。框架在启动时根据
`system.txt` 的 `protocolType` 选择 **protobuf** 或 **json**；同一次部署中的
客户端、网关和全部服务进程必须使用同一种编码及同一份协议号表。


## 目录说明

| 路径 | 内容 | 是否由工具生成 |
| --- | --- | --- |
| `mutual_pb/pb_src/` | 客户端与服务器之间的 Protobuf 源定义、`allprotocol.json` | `.proto` 为源文件；`allprotocol.json` 由脚本生成 |
| `mutual_pb/pb/` | Protobuf Go 代码 | 是，勿手改 |
| `mutual_json/` | 客户端与服务器之间的 JSON Go 结构体及协议号表 | `json_src/allprotocol.json` 由脚本生成 |
| `interior_pb/pb_src/` | 服务进程之间的 Protobuf 源定义 | `.proto` 为源文件 |
| `interior_pb/ipb/` | 内部 Protobuf Go 代码 | 是，勿手改 |
| `interior_json/` | 服务进程之间的 JSON Go 结构体 | 否 |
| `distribute_config.json` | 协议名到目标服务的路由表 | 否 |

## 传输格式

客户端 TCP/WebSocket 的业务包（默认拆包模式 1）为：

```text
[BodyLength: 2 字节，大端 uint16][ProtoNum: 2 字节，大端 uint16][Payload: 变长]
```

- `BodyLength` 是其后的 `ProtoNum + Payload` 长度，不含自身。
- 客户端完整包最大 4096 B，因此 Body 最大 4094 B，业务 `Payload` 最大 4092 B。
- `ProtoNum` 由 `allprotocol.json` 与 `distribute_config.json` 共同决定，客户端不得自行约定或复用协议号。

网关转发给服务进程、以及服务进程回网关时，会使用内部包体：

```text
[SessionID: 4 字节，大端 uint32][ProtoNum: 2 字节，大端 uint16][Payload: 变长]
```

`SessionID` 仅用于网关定位客户端；客户端不会收到该字段。业务处理函数拿到的 `data` 已去掉这两个头字段，只需要按当前编码反序列化 `Payload`。

## 当前协议与路由

`CS_` / `SS_` 是 `distribute_config.json` 的路由类别前缀，不是消息名称的一部分。实际消息名仍使用 `C2S`、`S2C`、`S2S` 表示方向。

| 协议名 | 协议号 | 类型 | 目标服务 | 说明 |
| --- | ---: | --- | --- | --- |
| `C2STest` | 1 | 客户端协议 | `actor_server` | 测试请求 |
| `S2CTestRet` | 2 | 客户端协议 | `gateway_server` | 测试响应，由网关投递给指定 Session |
| `S2CBroadcast` | 3 | 客户端协议 | `gateway_server` | 最终下发给客户端的广播封装 |
| `S2SHotFixModularizedSerSettings` | 10000 | 内部协议 | `modularized_server` | modularized 热更配置 |
| `S2SHotFixInteractiveSerSettings` | 15000 | 内部协议 | `interactive_server` | interactive 热更配置 |
| `S2SHotFixWebSerSettings` | 20000 | 内部协议 | `web_server` | web 热更配置 |
| `S2SGateBroadcast` | 30000 | 内部协议 | `gateway_server` | 服务进程发给网关的广播指令 |

## 消息字段注释


| 消息 | 字段 | 说明 |
| --- | --- | --- |
| `GateIdSyncInfo` | `ser_name`、`gate_id` | 服务进程与网关建立连接后的首包握手信息；握手使用协议号 `0`，不在路由表内。`gate_id` 用于服务端确认连接归属。 |
| `S2SGateBroadcast` | `session_ids`、`data` | `session_ids` 为空时向当前网关全部在线客户端广播；否则为逗号分隔的十进制 SessionID。非法 ID 会被忽略，重复 ID 只投递一次。`data` 是原始业务内容，网关会封装为 `S2CBroadcast` 后下发。 |
| `S2SHotFix*SerSettings` | `HotFixAll`、`SettingsInfo` | 热更控制信息。`HotFixAll=true` 表示全量热更；否则由 `SettingsInfo` 表示单项配置。JSON 与 Protobuf 使用相同字段含义。 |

## 业务代码中的使用方式

以 Protobuf 为例，模块注册协议处理器，并在处理器中解码：

```go
userMod.RegisterProtoHandleCaller("C2STest", func(data []byte, gateID int, sessionID uint32) {
    req := new(pb.C2STest)
    if err := proto.Unmarshal(data, req); err != nil {
        return
    }
    response, _ := proto.Marshal(&pb.S2CTestRet{Num: req.Num + 1})
    server_common.DeliveryMsg(gateID, sessionID, "S2CTestRet", response)
})
```

JSON 模式使用 `encoding/json` 及 `mutual_json` / `interior_json` 中的同名结构体。发送时始终传入**已序列化的业务载荷**；`DeliveryMsg` 会自动补齐 `SessionID` 与协议号，调用方不得自行拼接包头。

## 新增或修改协议的规则

1. 先确定协议范围：客户端协议放入 `mutual_pb` 与 `mutual_json`；仅服务进程使用的协议放入 `interior_pb` 与 `interior_json`。
2. 为两种编码保持相同的消息名和字段语义。允许编码表示不同，但不能像示例测试协议那样意外地让同名字段表达不同业务含义。
3. Protobuf 字段号一经发布不得修改或复用；新增字段使用新的字段号，删除字段应保留为废弃字段，保证旧客户端可解析。
4. 在 `proto/distribute_config.json` 对应的服务数组中登记协议名。客户端协议登记到 `CS_<服务名>`，内部协议登记到 `SS_<服务名>`；目标服务只能是 `actor_server`、`modularized_server`、`interactive_server`、`web_server` 或 `gateway_server`。
5. 修改 `.proto` 后，执行 `script/win/protobuf.bat`（Windows）或 `script/unix/protobuf.sh`（Unix）重新生成 Go 文件。脚本目前逐个列出需要编译的 `.proto`，新增文件时须先把它加入脚本（或手动执行等价的 `protoc` 命令）。修改客户端路由后也要重新生成 `mutual_pb/pb_src/allprotocol.json`；JSON 客户端路由使用对应的 `protojson` 脚本生成其 `allprotocol.json`。
6. 客户端协议号由生成的 `allprotocol.json` 固化，发布时将其 `_Version_` 与客户端一并更新。服务端启动会读取该版本；协议表不一致会导致协议号解析错误。
7. 在目标服务启动代码中用 `RegisterProtoHandleCaller` 注册处理函数；未注册处理器的协议无法进入业务模块。
8. 修改后以当前 `protocolType` 启动全链路验证：请求能路由到目标服务、响应能回到原 `SessionID`，并覆盖包长上限与反序列化失败场景。

## 路由与安全约束

- 内部协议号按服务分段自动递增：`modularized` 从 10000、`interactive` 从 15000、`web` 从 20000、`actor` 从 25000、`gateway` 从 30000 开始。数组内的排列会影响同一段的编号，已部署协议不要随意调整顺序。
- 客户端协议号由 `allprotocol.json` 指定。现有生成脚本按 `distribute_config.json` 中 `CS_` 条目的顺序分配；新增、删除或调整条目前要评估旧客户端兼容性。
- 不要将 `S2SGateBroadcast` 暴露为客户端可调用协议。网关只接受来自内部服务连接的该协议，并转换为 `S2CBroadcast`。
- `DeliveryMsg` 与 `SendMsg2Server` 会拒绝超过 4092 B 的业务载荷；广播封装后的整体包也必须满足同一上限。
- 广播不能保证必达：目标连接关闭、Session 不存在或客户端写队列满时，当前消息会丢弃并记录告警。因此关键状态应使用可确认、可重试的业务协议，而非广播。
