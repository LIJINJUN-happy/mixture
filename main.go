package main

import (
	"mixture/robot"
	"mixture/src/actor_server"
	"mixture/src/common/printer"
	"mixture/src/common/profile_trace"
	"mixture/src/common/server_common"
	"mixture/src/common/settings"
	"mixture/src/common/tools"
	"mixture/src/database"
	"mixture/src/gateway_server"
	"mixture/src/interactive_server"
	"mixture/src/modularized_server"
	"mixture/src/web_server"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// 启动所有服务器进程，参数(1)进程服务名称 (2)编号ID（用于区分同类型服务的id编号） 其余参数.....
func main() {
	if len(os.Args) != 3 {
		panic("Os.Args size is not right, Make sure the three parameters are met")
	}
	if _, isNumber := strconv.Atoi(os.Args[2]); isNumber != nil {
		panic("Os.Args[2] Must Be Integer")
	}

	//加载配置(加载后监听配置文件变更自动热更)
	settings.DoLoad(true, "")
	settings.StartFileWatcher()

	//初始化服务进程Log日志
	serName, processId := os.Args[1], os.Args[2]
	DoLoggerInit(serName)

	//调试模式
	CheckIsOpenDeBugMode(serName, processId)

	//数据库管理池初始化(mongo持久化 以及 memcache缓存)
	database.MongoDBConnectClientInit(serName, processId)
	database.StartMongoHealthCheck(30 * time.Second)
	database.MemCacheConnectClientInit(serName, processId)
	database.StartMemCacheHealthCheck(30 * time.Second)

	defer func() {
		if err := recover(); err != nil {
			printer.PanicStack(err)
		}
	}()
	switch serName {
	case "robot_test":
		robot.Run()
	case "web_server":
		web_server.Run()
	case "actor_server":
		actor_server.Run()
	case "gateway_server":
		gateway_server.Run()
	case "modularized_server":
		modularized_server.Run()
	case "interactive_server":
		interactive_server.Run()
	default:
		printer.TextError("Server Launch Fail", "serName Not Find", serName)
		break
	}
}

// 初始化日志管理对象,并创建进程服务日志
func DoLoggerInit(serName string) {
	printer.InitPrinterMgr()

	//初始化进程日志命名
	logName := ""
	if serTitleIdx := strings.Index(serName, "_server"); serTitleIdx > -1 {
		logName = "server_" + serName[:serTitleIdx] //进程服务Log命名初始化：server_ + 服务进程名称
	} else if serName == "robot_test" {
		logName = serName //机器人服务进程就按原本的命名
	} else {
		panic("logName Init Fail")
	}
	l := printer.CreateLogger(logName, "log/", true)
	if l == nil {
		panic("Happen Error With Creating Server LoggerObj")
	}
	printer.SetLoggerObj(l)
}

/*调试模式*/
func CheckIsOpenDeBugMode(serName, processId string) {
	if serName == "robot_test" {
		return //机器人测试不需要调试模式
	}

	isOpenPprof := tools.GetSysValueByKey("pprof")
	if isOpenPprof != "true" {
		return
	}

	go func() {
		defer func(sn, pId string) {
			if quitErr := recover(); quitErr != nil {
				printer.PanicStack(quitErr)
			}
		}(serName, processId)

		resPort := ""
		netDB, res := database.GetSQLiteConnDB()
		if res == nil {
			resPort = database.GetDeBugModePort(serName, processId, netDB)

			//拿到端口后关闭数据库连接
			dbCloseResult := netDB.Close()
			if dbCloseResult != nil {
				printer.TextError("netDB.Close With CheckIsOpenDeBugMode", "dbCloseResult", dbCloseResult.Error())
			}

			//根据端口启动debug模式
			if resPort == "" {
				printer.TextError("database.GetDeBugModePort", "resPort", resPort)
				return
			} else {
				// 调试监听与业务端口分离,公网直连时应由防火墙限制来源IP,接口本身不提供鉴权。
				listenHost := tools.GetSysValueByKey("debugListenAddress")
				if listenHost == "" {
					listenHost = "127.0.0.1" //默认仅允许本机或SSH隧道访问,远程直连须显式配置并限制来源IP
				}

				// 传入采样函数而非当前值，请求/metrics时才读取最新状态,trace包无需依赖业务包。
				server := &http.Server{
					Addr: net.JoinHostPort(listenHost, resPort),
					Handler: profile_trace.NewDebugMux(serName, processId, profile_trace.TraceSources{
						ModuleQueues:       server_common.GetModuleQueueMetrics,
						GatewayClients:     gateway_server.GateClientMapSize.Load,
						GatewayWriteQueues: gateway_server.GetGatewayWriteQueueMetrics,
					}),
					ReadHeaderTimeout: 5 * time.Second, //限制请求头读取时间，不限制pprof性能采样的持续时间
				}

				// 调试入口早于业务初始化完成启动，能访问/metrics不代表服务已就绪。
				printer.TextInfo("Debug HTTP Listening", "address", server.Addr, "server", serName, "id", processId)
				errOpenDebug := server.ListenAndServe()
				if errOpenDebug != nil {
					printer.TextError("Error starting debug server", "errOpenDebug", errOpenDebug)
					return
				}
			}
		} else {
			printer.TextError("SQLite Connect Fail: " + res.Error())
		}
	}()
}
