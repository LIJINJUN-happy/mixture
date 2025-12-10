package modularized_server

import (
	"google.golang.org/protobuf/proto"
	"mixture/proto/interior_pb/ipb"
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
	"os"
	"strconv"
)

var modularizedSer = new(server_common.ServerInfo)

func Run() {
	id, _ := strconv.Atoi(os.Args[2])
	if !modularizedSer.InitInfo("modularizedSer", id) {
		panic("modularizedSer InitInfo Failed")
	}

	server_common.CreateServerMonitor(modularizedSer, modularizedSerProcessor)
}

func modularizedSerProcessor(data []byte, proName string, gateId int, sessionID uint32) {
	printer.TextInfo("modularizedSerProcessor", "proName", proName, "gateId", gateId, "sessionID", sessionID, "data", string(data))
	pStruct := new(ipb.S2SHotFixModularizedSerSettings)
	errUnmarshal := proto.Unmarshal(data, pStruct)
	if errUnmarshal == nil {
		printer.TextInfo("modularizedSerProcessor", "pStruct.HotFixAll", pStruct.HotFixAll)
	}

	pbData := ipb.S2SHotFixInteractiveSerSettings{HotFixAll: true}
	sendData, mRes := proto.Marshal(&pbData)
	if mRes == nil {
		server_common.DeliveryMsg(gateId, sessionID, "S2SHotFixInteractiveSerSettings", sendData)
	}
}
