package web_server

import (
	"google.golang.org/protobuf/proto"
	"mixture/proto/mutual_pb/pb"
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
	"os"
	"strconv"
)

var webSer = new(server_common.ServerInfo)

func Run() {
	id, _ := strconv.Atoi(os.Args[2])
	if !webSer.InitInfo("webSer", id) {
		panic("webSer InitInfo Failed")
	}
	webSer.RegisterMonitorLogic(webSerProcessor)
}

func webSerProcessor(data []byte, proName string, gateId int, sessionID uint32) {
	printer.TextInfo("webSerProcessor", "proName", proName, "gateId", gateId, "sessionID", sessionID, "data", string(data))
	/*pStruct := new(ipb.S2SHotFixWebSerSettings)
	errUnmarshal := proto.Unmarshal(data, pStruct)
	if errUnmarshal == nil {
		printer.TextInfo("webSerProcessor", "pStruct.HotFixAll", pStruct.HotFixAll, "path", pStruct.SettingsInfo)
	}*/

	ret := pb.S2CTestRet{Num: int32(100)}
	sendData, e := proto.Marshal(&ret)
	if e == nil {
		server_common.DeliveryMsg(gateId, sessionID, "S2CTestRet", sendData)
	}

	/*cfg := settings.ConstCfg.Load()
	if cfg != nil {
		printer.TextInfo("cfg print Fix Before", "pcfg.Const.HeroInitList", cfg.Const.HeroInitList)
	}
	settings.DoLoad(false, pStruct.SettingsInfo)
	cfg = settings.ConstCfg.Load()
	if cfg != nil {
		printer.TextInfo("cfg print Fix After", "pcfg.Const.HeroInitList", cfg.Const.HeroInitList)
	}*/
}
