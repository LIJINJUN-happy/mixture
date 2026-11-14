package interactive_server

import (
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
	"os"
	"strconv"
	"sync"
)

var interactiveSer = new(server_common.ServerInfo)

func Run() {
	defer func() {
		if r := recover(); r != nil {
			printer.PanicStack(r)
			os.Exit(1)
		}
	}()

	//创建进程监听服务以及进程接收的数据后的处理逻辑
	id, _ := strconv.Atoi(os.Args[2])
	if err := interactiveSer.InitInfo("interactiveSer", id); err != nil {
		panic(err)
	}
	interactiveSer.RegisterMonitorLogic(server_common.TranspondProtoPack)

	//加载模块功能（模块关闭同步）
	var modWaitGroup sync.WaitGroup

	//xxx系统
	xxxMod := server_common.CreateMod("interactiveSerxxx", 2048, func() error { return nil }, func() error { return nil }, &modWaitGroup)
	xxxMod.RegisterProtoHandleCaller("C2STestxxx", func(data []byte, gateId int, sessionID uint32) { return })

	modWaitGroup.Wait()
}
