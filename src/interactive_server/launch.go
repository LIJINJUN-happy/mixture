package interactive_server

import (
	"google.golang.org/protobuf/proto"
	"mixture/proto/interior_pb/ipb"
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
	"os"
	"strconv"
)

var interactiveSer = new(server_common.ServerInfo)

func Run() {
	id, _ := strconv.Atoi(os.Args[2])
	if !interactiveSer.InitInfo("interactiveSer", id) {
		panic("InteractiveSer InitInfo Failed")
	}

	server_common.CreateServerMonitor(interactiveSer, interactiveSerProcessor)
}

func interactiveSerProcessor(data []byte, proName string, gateId int, sessionID uint32) {
	printer.TextInfo("interactiveSerProcessor", "proName", proName, "gateId", gateId, "sessionID", sessionID, "data", string(data))

	pStruct := new(ipb.S2SHotFixInteractiveSerSettings)
	errUnmarshal := proto.Unmarshal(data, pStruct)
	if errUnmarshal == nil {
		printer.TextInfo("interactiveSerProcessor", "pStruct.HotFixAll", pStruct.HotFixAll)
	}

	pbData := ipb.S2SHotFixWebSerSettings{HotFixAll: false, SettingsInfo: "./settings/const_cfg.json"}
	sendData, mRes := proto.Marshal(&pbData)
	if mRes == nil {
		if gateId == 1 {
			server_common.DeliveryMsg(2, sessionID, "S2SHotFixWebSerSettings", sendData)
		} else {
			server_common.DeliveryMsg(1, sessionID, "S2SHotFixWebSerSettings", sendData)
		}
	}

}
