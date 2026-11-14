package modularized_server

import (
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
	"os"
	"strconv"
	"sync"
)

var modularizedSer = new(server_common.ServerInfo)

func Run() {
	defer func() {
		if r := recover(); r != nil {
			printer.PanicStack(r)
			os.Exit(1)
		}
	}()

	id, _ := strconv.Atoi(os.Args[2])
	if err := modularizedSer.InitInfo("modularizedSer", id); err != nil {
		panic(err)
	}

	//先绑定端口，再初始化模块并注册协议
	ready := make(chan error, 1)
	go modularizedSer.RegisterMonitorLogic(server_common.TranspondProtoPack, ready)
	if err := <-ready; err != nil {
		printer.TextError("modularized server startup failed", "err", err)
		os.Exit(1)
	}

	//加载模块功能（模块关闭同步）
	var modWaitGroup sync.WaitGroup

	//xxx系统
	xxxMod := server_common.CreateMod("modularizedSerxxx", 2048, func() error { return nil }, func() error { return nil }, &modWaitGroup)
	xxxMod.RegisterProtoHandleCaller("C2STestxxx", func(data []byte, gateId int, sessionID uint32) { return })

	modWaitGroup.Wait()
}
