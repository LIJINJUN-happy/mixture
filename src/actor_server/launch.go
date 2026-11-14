package actor_server

import (
	"mixture/src/actor_server/user"
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
	"os"
	"strconv"
	"sync"
)

var actorSer = new(server_common.ServerInfo)

func Run() {
	defer func() {
		if r := recover(); r != nil {
			printer.PanicStack(r)
			os.Exit(1)
		}
	}()

	id, _ := strconv.Atoi(os.Args[2])
	if err := actorSer.InitInfo("actorSer", id); err != nil {
		panic(err)
	}

	//加载模块功能（模块关闭同步）
	var modWaitGroup sync.WaitGroup

	//用户系统
	userMod := server_common.CreateMod("user", 2048, user.ModInit, user.ServerDown, &modWaitGroup)
	if userMod == nil {
		printer.TextError("actor server module init failed")
		os.Exit(1)
	}
	userMod.RegisterProtoHandleCaller("C2STest", user.Test)

	//模块及协议注册完成后，再开放进程监听服务
	ready := make(chan error, 1)
	go actorSer.RegisterMonitorLogic(server_common.TranspondProtoPack, ready)
	if err := <-ready; err != nil {
		printer.TextError("actor server startup failed", "err", err)
		os.Exit(1)
	}

	modWaitGroup.Wait()
}
