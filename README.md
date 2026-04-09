# Mixture

一个纯Golang语言编写的分布式游戏服务器框架，连接方式支持 TCP 和 WebSocket，协议格式可选择 JSON 或者 Protobuf。

---

## 目录

1. [开发环境](#开发环境)
2. [依赖包](#依赖包)
3. [配置](#配置)
4. [目录结构](#目录结构)
5. [整体架构](#整体架构)
6. [进程拓扑与启动流程](#进程拓扑与启动流程)
7. [数据包格式](#数据包格式)
8. [网关服务器（gateway_server）](#网关服务器-gateway_server)
9. [后端服务公共层（server_common）](#后端服务公共层-server_common)
10. [模块系统（ServerModule）](#模块系统-servermodule)
11. [协议路由表（ProtocolDistributeInfo）](#协议路由表-protocoldistributeinfo)
12. [内存池架构（sync.Pool）](#内存池架构-syncpool)
13. [CutIndexList 包索引结构](#cutindexlist-包索引结构)
14. [MongoDB 读写分离与健康检查](#mongodb-读写分离与健康检查)
15. [完整数据流](#完整数据流)
16. [关键常量速查](#关键常量速查)

---

## 开发环境

- Linux Kernel：Linux version 3.10.0-1160.108.1.el7.x86_64 | CentOS 7.9
- Golang 版本：go1.22.4（版本过低不支持内置 slog 库）
- SQLite 版本：SQLite-3.46.0
- MemCacheDB 版本：Memcached-1.4.15
- MongoDB 版本：7.0.20
- protoc 版本：protoc-27.1-win64

---

## 依赖包

```
google.golang.org/protobuf/cmd/protoc-gen-go@latest  // 将 .proto 文件编译为 Go 代码
github.com/gorilla/websocket v1.5.3                  // 支持 WebSocket 模式
github.com/mattn/go-sqlite3 v1.14.22                 // 读取 IP/Port 等网络配置
github.com/bradfitz/gomemcache/memcache              // NoSQL 缓存，减轻数据库压力
go.mongodb.org/mongo-driver/mongo                    // MongoDB 驱动
go.mongodb.org/mongo-driver/bson
```

---

## 配置

- **system.txt**：项目根目录下，存放调试模式、日志等级等系统配置。
- **net_sql.db**：根目录下的 SQLite 数据库，存放各进程的连接地址与端口，服务启动时自动读取。

---

## 目录结构

```
mixture/
├── bin/        编译好的可执行文件（Windows / Unix）
├── docs/       项目说明文档
├── proto/      协议文件
│   ├── mutual_json / mutual_pb/    外部协议（与客户端通信）
│   └── interior_json / interior_pb/ 内部协议（进程间通信）
├── robot/      机器人测试目录（json / protobuf 机器人，配置在 robot_config.txt）
├── script/     执行脚本（编译、启动、关闭、协议导出等）
├── settings/   项目配置文件（JSON 格式，支持热更）
└── src/        源码目录
    ├── gateway_server/     网关进程
    ├── actor_server/       Actor 服务进程
    ├── interactive_server/ 交互服务进程
    ├── modularized_server/ 模块化服务进程
    ├── web_server/         Web 服务进程
    ├── common/
    │   ├── server_common/  后端服务公共层（连接管理、模块系统、协议分发）
    │   ├── printer/        日志模块
    │   ├── settings/       配置读取
    │   └── tools/          通用工具
    └── database/           数据库访问层（SQLite / MongoDB / Memcache）
```

---

## 整体架构

```
                      ┌─────────────────────────────────┐
                      │          客户端 (Client)          │
                      │    TCP 或 WebSocket 连接          │
                      └────────────────┬────────────────┘
                                       │
                      ┌────────────────▼────────────────┐
                      │         gateway_server           │
                      │  DoReadMsg  ←→  DoWriteMsg       │
                      │  DoSplit → DoDistribute          │
                      │  SerLinker × 4                   │
                      └──┬──────┬───────┬──────┬────────┘
                         │      │       │      │
           ┌─────────────┘      │       │      └──────────────┐
           │              ┌─────┘       └─────┐               │
           ▼              ▼                   ▼               ▼
  ┌────────────┐  ┌───────────────┐  ┌────────────────┐  ┌──────────┐
  │actor_server│  │interactive_ser│  │modularized_ser │  │web_server│
  │ ServerInfo │  │  ServerInfo   │  │  ServerInfo    │  │ServerInfo│
  │ + Modules  │  │  + Modules    │  │  + Modules     │  │+Modules  │
  └────────────┘  └───────────────┘  └────────────────┘  └──────────┘
           │              │                   │               │
           └──────────────┴──────────┬────────┴───────────────┘
                                     │
                            ┌────────▼────────┐
                            │    MongoDB       │
                            │  读写分离         │
                            │  atomic.Pointer  │
                            │  健康检查         │
                            └─────────────────┘
```

**进程角色说明：**

| 进程 | 角色 | 说明 |
|------|------|------|
| `gateway_server` | 网关 | 唯一对外入口，负责 TCP/WebSocket 监听、包切割、协议路由、Session 管理 |
| `actor_server` | Actor 服务 | 处理玩家核心逻辑，每个功能封装为独立 Module |
| `interactive_server` | 交互服务 | 处理实时交互类逻辑（如聊天、房间） |
| `modularized_server` | 模块化服务 | 通用扩展服务，以 Module 为粒度横向扩展 |
| `web_server` | Web 服务 | HTTP/WebSocket 服务端接口 |

---

## 进程拓扑与启动流程

```
启动参数：./unixMixture <服务名> <服务ID>

gateway_server  ──── TCP dial ────►  actor_server        (ActorSerLinker)
                ──── TCP dial ────►  interactive_server  (InteractiveSerLinker)
                ──── TCP dial ────►  modularized_server  (ModularizedSerLinker)
                ──── TCP dial ────►  web_server          (WebSerLinker)
```

后端服务均通过 `RegisterMonitorLogic` 监听来自 gateway 的 TCP 主动连接。gateway 启动后主动 dial 各后端，断线后自动重连（`goto connTryAgain`）。

**网关初始化顺序（`gateway_server/launch.go: Run`）：**

```
Run()
 ├── database.GetSQLiteConnDB()         // 读取 SQLite 配置
 ├── DoInit(db)                         // 加载 IP/Port/白名单/Linker 地址
 ├── BeginGateServer()                  // 启动客户端监听（TCP 或 WebSocket）
 ├── time.Sleep(2s)
 ├── BeginLinkServer(WebSerLinker)
 ├── BeginLinkServer(ActorSerLinker)
 ├── BeginLinkServer(ModularizedSerLinker)
 ├── BeginLinkServer(InteractiveSerLinker)
 └── Looping()                          // 主循环（信号监听 + 定时统计）
```

---

## 数据包格式

### 客户端 ↔ 网关（TCP/WebSocket，模式1）

```
┌──────────────┬────────────────────────────┐
│  Head (2B)   │        Body (变长)          │
│  大端 uint16  │  最大 4096 字节             │
│  = Body 长度  │                            │
└──────────────┴────────────────────────────┘
```

### 网关内部路由缓冲（DoDistribute 填充）

```
┌────────────────┬──────────────┬──────────────────┐
│ SessionID (4B) │ ProtoNum (2B)│ Data (变长)       │
│  大端 uint32   │  大端 uint16 │  业务数据         │
│  客户端标识    │  路由依据    │                  │
└────────────────┴──────────────┴──────────────────┘
```

- 来自**客户端**：DoDistribute 自动在头部补充 `SessionID`。
- 来自**后端服务**：数据包已含完整结构，`sourceSession = 0`，直接转发。

### 进程间传输（DoPack，模式1）

```
[2B 大端包体长度] + [SessionID(4B)][ProtoNum(2B)][Data]
```

---

## 网关服务器（gateway_server）

### 客户端连接生命周期

```
Accept(conn)
 └── newConn.Init()
      ├── ReadStreamBufferObject.Init()    // 字节流状态机
      ├── writeChan = make(chan *[]byte, 64)
      └── rwChannel = make(chan string, 2) // 读写协程同步

 ├── go DoWriteMsg(newConn)
 │    └── for select {
 │          writeChan → bufPtr
 │            DoPack(去除 SessionID 前缀) → Write 给客户端
 │            *bufPtr 还原容量 → DistributeDataPool.Put(bufPtr)
 │          rwChannel = GoroutineSyncBreakSign → return
 │        }
 │
 └── go DoReadMsg(newConn)
      ├── AddIntoGateMap(writeChan)        // 注册 SessionID → writeChan
      └── for {
            SetReadDeadline(+900s)
            Read() → DealBufferData()
                      ├── DoSplit()        // 字节流切割
                      └── DoDistribute()  // 协议路由
            EOF/Error → rwChannel ← GoroutineSyncBreakSign
          }
```

### 字节流切割（DoSplit）

框架默认使用**模式 1**（包头长度前缀），支持三种模式：

| 模式 | 函数 | 说明 |
|------|------|------|
| 1 | `DoSplitByReadHeadLen` | 2字节包头声明包体长度，支持包头/包体跨 Read 拼接，包长超限断连 |
| 2 | `DoSplitBySign` | 按分隔符 `\|`（0x7C）切割 |
| 3 | `DoSplitByAppointLen` | 定长读取（556字节），空白处用 `\x00` 补充 |

切割结果写入 `CutIndexList`（各包在 `sliceBuffer` 中的结束下标），由 `DoDistribute` 逐包读取。

### 协议路由（DoDistribute）

```
DoDistribute(cutList, sliceBuffer, sourceSession)
 └── for each packet:
       DistributeDataPool.Get() → bufPtr
       填充 [SessionID][ProtoNum][Data]
       *bufPtr = data                        // 缩切到实际长度
       ProtoNumMap[protoNum] → protoName
       ProtoDistributeMap[protoName] → 目标服务
       switch 目标服务:
         actor/interactive/modularized/web  → SafeSendToLinker(linker, bufPtr)
         gateway_server                     → GateClientMap[sessionID].clientBuffChannel ← bufPtr
```

`SafeSendToLinker` 使用非阻塞 `select/default`，channel 满时直接回收缓冲，不阻塞路由协程。

### 进程连接器（SerLinker）

```
BeginLinkServer(sl) [goroutine]
 ├── defer: receiveChannel ← nil（通知 DealWithLinkerData 退出）→ 重连
 ├── net.Dial → 失败则 goto connTryAgain 循环重试
 ├── go DealWithLinkerData(linker)     // 发送协程
 │    └── receiveChannel → DoPack → (*serConn).Write
 │        → DistributeDataPool.Put(bufPtr)
 └── for: dial.Read → DoSplit → DoDistribute(sourceSession=0)  // 接收协程
```

---

## 后端服务公共层（server_common）

所有后端服务共享相同的 `server_common` 层，每个 gateway 连接对应一个 `SerConnObj`。

### 三协程模型

```
BeginService(sCoon) [goroutine]
 ├── go Transmit(sCoon)     // 发送协程：sendChannel → DoPack → Write 回 gateway
 ├── go Dispose(sCoon)      // 接收协程：receiveChannel → 解析 ProtoNum → 路由/握手
 ├── time.Sleep(3s)
 └── for: Read → DoSplit → receiveChannel ← newByte
```

**握手流程**（`protoNum == 0 && gateId < 0`）：
```
Dispose 接收首个数据包
 └── 解析 GateIdSyncInfo（JSON 或 Protobuf）→ 获取 gateID
      └── CheckConnObjIsExist → SetGateId → AddConnObj
           重复 gateID → 直接断开
```

### 回包路径（DeliveryMsg）

```
DeliveryMsg(gateID, sessionId, protoName, data)
 ├── ProtoNameMap[protoName] → protoNum
 ├── GetConnObj(gateID) → gateConn
 ├── DeliveryMsgBufPool.Get() → bufPtr
 │   填充 [SessionID(4B)][ProtoNum(2B)][Data]
 │   *bufPtr = sendDataBuf（缩切）
 └── select: gateConn.sendChannel ← bufPtr（满则 Put 回池）
      ↓
   Transmit → DoPack → Write 回 gateway TCP
      ↓
   gateway DoDistribute(sourceSession=0) → session.clientBuffChannel ← bufPtr
      ↓
   DoWriteMsg → DoPack(去除 SessionID) → Write 给客户端
```

---

## 模块系统（ServerModule）

每个模块独占一个 goroutine，通过 channel 接收协议包与模块调用包，完全无锁。

### 创建与注册

```go
// 创建模块
userMod := server_common.CreateMod("user", 2048, user.ModInit, user.ServerDown, &wg)

// 注册协议处理函数（建立 protoName → 模块通道 的映射）
userMod.RegisterProtoHandleCaller("C2STest", user.Test)

// 注册模块间调用接口
userMod.RegisterInterfaceHandleCaller("GetUserInfo", handler)
```

### 模块主循环（LoadMod）

```
LoadMod [goroutine]
 └── for select {
       closeChan    → ServerDown() → wg.Done()
       receiptChan  →
         *ProtoPackInfo  → HandleProtoPack  → HandleFuncMap[proName](data, gateId, sessionID)
                           defer ProtoPackInfoPool.Put(protoPack)
         *ModulePackInfo → HandleModulePack → InterfaceFuncMap[interfaceName](args...)
                           ReturnChan != nil → 同步返回结果
                           defer ModulePackInfoPool.Put(modulePack)
     }
```

### 模块间调用

```go
// 同步调用（阻塞，超时 10 秒）
result := server_common.SynchronousPack2Module("user", "GetUserInfo", args)

// 异步调用（不等返回）
server_common.AsynchronousPack2Module("user", "UpdateScore", args)
```

---

## 协议路由表（ProtocolDistributeInfo）

### 结构

```go
type DistributeInfo struct {
    ProtoNumMap        map[uint16]string // 协议号 → 协议名
    ProtoNameMap       map[string]uint16 // 协议名 → 协议号
    ProtoDistributeMap map[string]string // 协议名 → 目标服务名
    ProtocolType       string            // "json" 或 "protobuf"
    ProtoVersion       string            // 版本号
}
```

### 协议加载规则

路由表在 `init()` 阶段从配置文件加载，运行时只读：

| 前缀 | 来源 | 协议号 |
|------|------|--------|
| `CS_` | 客户端→服务器协议，从 `allprotocol.json` 读取 | 由 allprotocol.json 定义 |
| `SS_` | 服务器内部协议 | 自动递增，按服务分段 |

**SS_ 协议号分段：**

| 服务 | 起始号 |
|------|--------|
| modularized_server | 10000 |
| interactive_server | 15000 |
| web_server | 20000 |
| actor_server | 25000 |
| gateway_server | 30000 |

---

## 内存池架构（sync.Pool）

框架共有 4 个 `sync.Pool`，全部使用 `*T` 指针存储，避免堆逃逸：

| 名称 | 位置 | 存储类型 | 使用场景 |
|------|------|----------|----------|
| `DistributeDataPool` | `distribute.go` | `*[]byte`（cap=5120） | 网关协议转发：DoDistribute → SerLinker / writeChan |
| `DeliveryMsgBufPool` | `server_operate.go` | `*[]byte`（cap=5120） | 后端回包：DeliveryMsg → sendChannel → Transmit |
| `ProtoPackInfoPool` | `server_module.go` | `*ProtoPackInfo` | 协议分发：TranspondProtoPack → modChan → HandleProtoPack |
| `ModulePackInfoPool` | `server_module.go` | `*ModulePackInfo` | 模块间调用：Synchronous/AsynchronousPack2Module |

### *[]byte 池的正确使用模式

```go
// 取出并切到实际长度
bufPtr := DistributeDataPool.Get().(*[]byte)
data := (*bufPtr)[:actualLen]
*bufPtr = data                             // 缩切：接收方读 *bufPtr 得到有效数据

// 通过 channel 传递（传指针，不产生堆逃逸）
channel <- bufPtr

// 接收方用完后归还（还原满容量后用原始指针 Put）
*bufPtr = (*bufPtr)[:cap(*bufPtr)]
DistributeDataPool.Put(bufPtr)             // 零逃逸

// 错误示例（勿用）：Put(&localVar) 会导致 24B 堆逃逸/次
var buf []byte = *bufPtr
DistributeDataPool.Put(&buf)               // ← 错误！
```

---

## CutIndexList 包索引结构

替代 `container/list`，声明在调用方栈上，零堆分配，零 GC 压力：

```go
type CutIndexList struct {
    data [256]int  // 存储各包在 sliceBuffer 中的结束下标
    len  int
}
```

**与 DoSplit / DoDistribute 配合方式：**

```
sliceBuffer: [  pkg1_data  ][  pkg2_data  ][  pkg3_data  ]
cutList:         [end1]         [end2]         [end3]

DoDistribute:
  i=0 → sliceBuffer[0 : end1]       第1个包
  i=1 → sliceBuffer[end1 : end2]    第2个包
  i=2 → sliceBuffer[end2 : end3]    第3个包
```

每次 Read 前调用 `cutList.Reset()`（仅重置 `len=0`，O(1)），无任何堆分配。

---

## MongoDB 读写分离与健康检查

```go
var writeMongoClient atomic.Pointer[mongo.Client]  // 写客户端
var readMongoClient  atomic.Pointer[mongo.Client]  // 读客户端
```

`atomic.Pointer[T]` 保证客户端替换时无锁、无数据竞争。

**健康检查（`StartMongoHealthCheck`）：**
```
后台 goroutine，定期 Ping 读写节点
 └── Ping 失败 → atomic.Store(重建的新客户端)
                   不影响业务调用路径
```

**读写分离规则：**
- `MongoInsert / MongoDelete / MongoUpdate` → `GetWriteMongoClient()`
- `MongoFindOne / MongoCountLine` → `GetReadMongoClient()`

---

## 完整数据流

### 上行（客户端 → 后端服务）

```
客户端
 │ [Head(2B)][ProtoNum(2B)][Data]
 ▼
DoReadMsg → DoSplit(ReadStreamBufferObject)
 │ 切包 → CutIndexList
 ▼
DoDistribute(cutList, sliceBuffer, sessionID)
 │ DistributeDataPool.Get() → 填充[SessionID][ProtoNum][Data] → *bufPtr 缩切
 ▼
SafeSendToLinker → SerLinker.receiveChannel ← bufPtr
 ▼
DealWithLinkerData → DoPack → TCP Write → 后端进程
 │ DistributeDataPool.Put(bufPtr)
 ▼
BeginService: Read → DoSplit → receiveChannel ← newByte
 ▼
Dispose: 解析 ProtoNum → TranspondProtoPack
 │ ProtoPackInfoPool.Get() → 填充 → modChan ← info
 ▼
LoadMod: HandleProtoPack → HandleFuncMap[protoName](data, gateId, sessionID)
 │ defer ProtoPackInfoPool.Put(protoPack)
 ▼
业务逻辑
```

### 下行（后端服务 → 客户端）

```
业务逻辑
 ▼
DeliveryMsg(gateID, sessionId, protoName, data)
 │ DeliveryMsgBufPool.Get() → 填充[SessionID][ProtoNum][Data] → *bufPtr 缩切
 ▼
gateConn.sendChannel ← bufPtr
 ▼
Transmit → DoPack → TCP Write → gateway
 │ DeliveryMsgBufPool.Put(bufPtr)
 ▼
gateway BeginLinkServer: Read → DoSplit → DoDistribute(sourceSession=0)
 │ protoSendSerName == "gateway_server" → GateClientMap[sessionID]
 ▼
session.clientBuffChannel ← bufPtr
 ▼
DoWriteMsg → DoPack(去除 SessionID 前缀) → Write 给客户端
 │ DistributeDataPool.Put(bufPtr)
 ▼
客户端收到 [Head(2B)][ProtoNum(2B)][Data]
```

---

## 关键常量速查

| 常量 | 值 | 说明 |
|------|----|------|
| `ConnReadBufSize` | 4096 B | 每次 Read 的原始字节缓冲 |
| `ReadStreamBufferSize` | 5120 B | 池缓冲容量（包体上限 4096 + 余量） |
| `MaxPacketBodySize` | 4096 B | 单包体最大长度，超出视为非法包并断连 |
| `MaxPacketsPerRead` | 256 | 一次 Read 最多切出的包数（CutIndexList 容量） |
| `ConnWriteChanCaps` | 64 | `NewConnection.writeChan` 容量 |
| `channelBufferSize` | 1024 | `SerLinker.receiveChannel` 容量 |
| `sendChannelSize` | 1024 | `SerConnObj.sendChannel` 容量 |
| `receiveChannelSize` | 1024 | `SerConnObj.receiveChannel` 容量 |
| `MaxConnectClientNum` | 10000 | 最大客户端并发连接数 |
| `ConnReadTimeout` | 900 s | 客户端读超时（15 分钟） |
| `HeadLen` | 2 B | 包头长度（大端 uint16，存储包体字节数） |
| `SessionIdLen` | 4 B | SessionID 字节长度（大端 uint32） |
| `ProtoNumLen` | 2 B | 协议号字节长度（大端 uint16） |
| `ModCallerTimeOut` | 10 s | 模块同步调用超时 |
| `TransmitDisposeTickerDuration` | 500 ms | Transmit/Dispose 连接检测间隔 |
| SS_ 协议号起始 | modularized=10000, interactive=15000, web=20000, actor=25000, gateway=30000 | 内部协议号分段 |
