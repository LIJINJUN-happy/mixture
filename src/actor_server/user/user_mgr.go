/*-----------------------------------------用户管理系统模块-------------------------------------------------*/
package user

import (
	"google.golang.org/protobuf/proto"
	"mixture/proto/interior_pb/ipb"
	"mixture/proto/mutual_pb/pb"
	"mixture/src/common/printer"
	"mixture/src/common/server_common"
)

// 模块初始化处理函数
func ModInit() error {
	return nil
}

// 服务器关闭处理函数
func ServerDown() error {
	printer.TextInfo("UseMode", "ServerDown", "CLoseSyn")
	return nil
}

func Test(data []byte, gateId int, sessionID uint32) error {
	num := 0
	pStruct := new(pb.C2STest)
	errUnmarshal := proto.Unmarshal(data, pStruct)
	if errUnmarshal == nil {
		num = int(pStruct.Num) + 1
		printer.TextInfo("UseMode", "Test", "num", num)
	}

	ret := ipb.S2SHotFixModularizedSerSettings{HotFixAll: true}
	sendData, e := proto.Marshal(&ret)
	if e == nil {
		server_common.DeliveryMsg(gateId, sessionID, "S2SHotFixModularizedSerSettings", sendData)
		printer.TrickLog("user", "S2SHotFixModularizedSerSettings", "S2SHotFixModularizedSerSettings' HotFixAll", true)
	}

	return nil
}
