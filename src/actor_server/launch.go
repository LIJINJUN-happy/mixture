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
		}
	}()

	id, _ := strconv.Atoi(os.Args[2])
	if !actorSer.InitInfo("actorSer", id) {
		panic("ActorServer InitInfo Failed")
	}

	//创建进程监听服务以及进程接收的数据后的处理逻辑
	go actorSer.RegisterMonitorLogic(server_common.TranspondProtoPack)

	//加载模块功能（模块关闭同步）
	var modWaitGroup sync.WaitGroup

	//用户系统
	userMod := server_common.CreateMod("user", 2048, user.ModInit, user.ServerDown, &modWaitGroup)
	userMod.RegisterProtoHandleCaller("C2STest", user.Test)

	modWaitGroup.Wait()
}
