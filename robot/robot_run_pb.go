package robot

import (
	"encoding/binary"
	"fmt"
	"google.golang.org/protobuf/proto"
	"mixture/proto/mutual_pb/pb"
	"mixture/src/gateway_server"
	"net"
	"os"
	"strconv"
	"time"
)

// createPBRot 根据配置创建 Protobuf 机器人。
func createPBRot() {
	connGateID := os.Args[2]
	finished := false
	for !finished {
		for i := 0; i < ConfigObj.CreateRobotNumOneSec; i++ {
			listNum := strconv.Itoa(RBList.Len())
			robotName := "Robot_" + connGateID + "_" + listNum
			uid, err := strconv.Atoi(connGateID + "00" + listNum)
			if err != nil {
				panic(err)
			}

			r := &TestRobot{
				uid:        uid,
				name:       robotName,
				createTime: time.Now(),
			}
			go robotSendPBMsg(r)

			ret := RBList.PushBack(r)
			if ret != nil && RBList.Len() < ConfigObj.RobotNum {
				continue
			}
			finished = true
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Println("All Robot Create Finish, robotList.Len() = ", RBList.Len())
}

func robotSendPBMsg(r *TestRobot) {
	Wg.Add(1)
	defer Wg.Done()

	conn, err := net.Dial("tcp", ConfigObj.serAddress)
	if err != nil {
		fmt.Println("Conn Ser Fail: ", ConfigObj.serAddress, ", Fail Error", err.Error())
		return
	}
	defer conn.Close()
	go robotRecPBMsg(r, conn)

	packetBuffer := make([]byte, gateway_server.ReadStreamBufferSize)
	interval := time.Duration(ConfigObj.RobotSendMsgIntervalSec) * time.Second
	for {
		if ConfigObj.RobotSendMsgIsLimit && ConfigObj.RobotSendMsgTimes == r.curSendNum {
			return
		}

		payload, err := proto.Marshal(&pb.C2STest{Num: 1})
		if err != nil {
			fmt.Println("marshal C2STest failed:", err)
			return
		}
		protoNum, ok := gateway_server.ProtocolDistributeInfo.ProtoNameMap["C2STest"]
		if !ok {
			panic("gateway_server.ProtocolDistributeInfo.ProtoNameMap[\"C2STest\"]")
		}
		body := make([]byte, gateway_server.ProtoNumLen, gateway_server.ProtoNumLen+len(payload))
		binary.BigEndian.PutUint16(body, protoNum)
		body = append(body, payload...)

		r.lastSendUnixNano.Store(time.Now().UnixNano())
		if err = writeRobotPacket(conn, packetBuffer, body); err != nil {
			fmt.Println("name: "+r.name+" ,write error:", err)
			return
		}
		r.curSendNum++

		if interval > 0 {
			time.Sleep(interval)
		}
	}
}

func robotRecPBMsg(r *TestRobot, conn net.Conn) {
	receiveRobotMessages(r, conn, robDealPBMsg)
}

func robDealPBMsg(r *TestRobot, data []byte) {
	if len(data) < gateway_server.ProtoNumLen {
		fmt.Println("Robot", r.name, "received a packet without protocol number")
		return
	}
	protoNum := binary.BigEndian.Uint16(data[:gateway_server.ProtoNumLen])
	if int(protoNum) >= len(gateway_server.ProtocolDistributeInfo.ProtoNumArray) {
		fmt.Println("Robot", r.name, "received an invalid protocol number:", protoNum)
		return
	}
	protoName := gateway_server.ProtocolDistributeInfo.ProtoNumArray[protoNum]
	readAt := time.Now().UnixNano()
	r.lastReadUnixNano.Store(readAt)
	if sendAt := r.lastSendUnixNano.Load(); sendAt > 0 {
		fmt.Println("Robot", r.name, " diffWithSend Milliseconds: ", time.Duration(readAt-sendAt).Milliseconds())
	}

	if protoName == "" {
		fmt.Println(data, ", proNum = "+strconv.Itoa(int(protoNum)), "Search Fail!")
		return
	}
	if protoName == "S2CTestRet" {
		message := new(pb.S2CTestRet)
		if err := proto.Unmarshal(data[gateway_server.ProtoNumLen:], message); err != nil {
			fmt.Println("unmarshal S2CTestRet failed:", err)
		}
	}
}
