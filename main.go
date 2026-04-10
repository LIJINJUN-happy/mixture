package main

import (
	"mixture/robot"
	"mixture/src/actor_server"
	"mixture/src/common/printer"
	"mixture/src/common/settings"
	"mixture/src/common/tools"
	"mixture/src/database"
	"mixture/src/gateway_server"
	"mixture/src/interactive_server"
	"mixture/src/modularized_server"
	"mixture/src/web_server"
	"net/http"
	_ "net/http/pprof"
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
		return
	}
	l := printer.CreateLogger(logName, "log/", true)
	if l == nil {
		panic("Happen Error With Creating Server LoggerObj")
		return
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
				errOpenDebug := http.ListenAndServe(":"+resPort, nil) // 启动pprof服务器http://localhost:xxx/debug/pprof/goroutine?debug=2
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
