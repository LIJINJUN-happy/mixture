package web_server

import (
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
	"os"
	"strconv"
	"sync"
)

var webSer = new(server_common.ServerInfo)

func Run() {
	defer func() {
		if r := recover(); r != nil {
			printer.PanicStack(r)
		}
	}()

	id, _ := strconv.Atoi(os.Args[2])
	if !webSer.InitInfo("webSer", id) {
		panic("webSer InitInfo Failed")
	}

	go webSer.RegisterMonitorLogic(server_common.TranspondProtoPack)

	//加载模块功能（模块关闭同步）
	var modWaitGroup sync.WaitGroup

	//xxx系统
	xxxMod := server_common.CreateMod("webSerxxx", 2048, func() error { return nil }, func() error { return nil }, &modWaitGroup)
	xxxMod.RegisterProtoHandleCaller("C2STestxxx", func(data []byte, gateId int, sessionID uint32) error { return nil })

	modWaitGroup.Wait()
}
