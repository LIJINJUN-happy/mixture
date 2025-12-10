# Mixture

一个纯Golang语言编写的服务器框架，连接方式支持 TCP 和 WebSocket， 协议格式可选择 JSON 或者 Protobuf 。

### 开发环境版本

- Linux Kernel：Linux version 3.10.0-1160.108.1.el7.x86_64 | CentOS 7.9

- 开发使用的golang版本：go1.22.4 （注意：版本过低不支持golang内置的slog库）

- SQLite版本：SQLite-3.46.0

- MemCacheDB版本：Memcached-1.4.15

- MongoDB版本：7.0.20

- protoc版本：protoc-27.1-win64 

### 依赖包

```
google.golang.org/protobuf/cmd/protoc-gen-go@latest （编译所需的插件库,该工具将.proto文件转换为Go代码）
```

```
github.com/gorilla/websocket v1.5.3 （用于支持ws模式）
```

```
github.com/mattn/go-sqlite3 v1.14.22 （设置网络IP地址以及端口等配置）
```

```
github.com/bradfitz/gomemcache/memcache（NO-SQL缓存数据的存取,减轻数据库压力）
```

```
go.mongodb.org/mongo-driver/mongo（MongoDB数据库依赖包）
go.mongodb.org/mongo-driver/bson
```

### 配置

- 项目根目录下的 **system.txt** 文本里面存放的是服务器的所需配置信息, 如：是否开发调试模式, 日志等级....等等。

- 不同进程间的连接端口以及连接对象的配置存放在根目录下名为 **net_sql.db** 的SQLite内置数据库中，启动进程后服务器会自行读取并连接，注意端口占用问题即可。

### 项目的目录结构

> **bin**：该目录下存放 ”windows“ 和 ”unix“ 平台下编译好的执行文件。
> 
> **docs**：项目说明文档存放目录

> **proto**：该目录用于存放协议文件，区分 ”json“ 协议以及 ”protobuf“ 协议。
> 
> 同时也区分了外部协议和内部协议文件目录，外部协议文件放置在前缀为 ”mutual“ 的目录下，用于和客户端进行通信，而内部协议则是用于服务器的进程之间的通信，存放在前缀为 ”interior“ 的文件夹。 

> **robot**：机器人目录，用于机器人测试以及调试，里面区分了 json 机器人以及protobuf 机器人，机器人的执行参数以及配置都在该目录下名为 ”robot_config“ 的文本内。

> **script**：用于存放执行脚本，包括：代码构建编译，启动服务器，关闭服务器, 协议编译和导出 ...... 等等
> 
> **settings**：项目配置文件存放处（json格式，支持热更）

> **src**：源码存放目录，目录下包含了常用模块以及各个服务器进程的逻辑代码。

### 其他

项目架构等可以通过docs目录下的文档了解
