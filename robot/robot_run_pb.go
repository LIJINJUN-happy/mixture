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
	"unsafe"
)

// 根据配置创建机器人
func createPBRot() {
	connGateId := os.Args[2]
	isFinishBreak := false
	for {
		if isFinishBreak {
			break
		}
		for i := 0; i < ConfigObj.CreateRobotNumOneSec; i++ {
			listNum := strconv.Itoa(RBList.Len())
			robotName := "Robot_" + connGateId + "_" + listNum
			num, err := strconv.Atoi(connGateId + "00" + listNum)
			if err != nil {
				panic(err)
				return
			}
			r := new(TestRobot)
			r.uid = num
			r.curSendNum = 0
			r.name = robotName
			r.createTime = time.Now()
			go robotSendPBMsg(r)

			ret := RBList.PushBack(r)
			if (ret != nil) && (RBList.Len() < ConfigObj.RobotNum) {
				continue
			} else {
				isFinishBreak = true
				break
			}
		}
		time.Sleep(1 * time.Second)
	}
	fmt.Println("All Robot Create Finish, robotList.Len() = ", RBList.Len())
}

// 机器人发送协议逻辑
func robotSendPBMsg(r *TestRobot) {
	Wg.Add(1)
	defer Wg.Done()
	dail, errDial := net.Dial("tcp", ConfigObj.serAddress)
	if errDial != nil {
		fmt.Println("Conn Ser Fail: ", ConfigObj.serAddress, ", Fail Error", errDial.Error())
		return
	} else {
		go robotRecPBMsg(r, &dail)

		byteList := make([]byte, gateway_server.ReadStreamBufferSize)
		dt := time.Duration(ConfigObj.RobotSendMsgIntervalSec)
		for {
			//判断是否有次数限制
			if ConfigObj.RobotSendMsgIsLimit && (ConfigObj.RobotSendMsgTimes == r.curSendNum) {
				break
			}

			//发送测试协议
			t := pb.C2STest{Num: 1}
			nb, _ := proto.Marshal(&t)
			num := 0
			byteList = byteList[0:0]
			newData := make([]byte, 60)
			va, res := gateway_server.ProtocolDistributeInfo.ProtoNameMap["C2STest"]
			if !res {
				panic("gateway_server.ProtocolDistributeInfo.ProtoNameMap[\"C2STest\"]")
				break
			}
			binary.BigEndian.PutUint16(newData[0:], va)
			newData = append(newData[:unsafe.Sizeof(va)], nb...)
			//fmt.Println("newData Size = ", len(newData))

			//打包数据,形成数据包体待发送给服务器
			if ConfigObj.way == 1 {
				num = gateway_server.DoPackByReadHeadLen(byteList, newData)
			} else if ConfigObj.way == 2 {
				num = gateway_server.DoPackBySign(byteList, newData)
			} else {
				num = gateway_server.DoPackByAppointLen(byteList, newData)
			}
			_, ee := dail.Write(byteList[:num])
			if ee == nil {
				r.curSendNum += 1
				r.lastSendMsgTime = time.Now()
				//fmt.Println("name: "+r.name+" ,SendMsgNumSize: "+strconv.Itoa(n), " ,curSendMsgNum = ", r.curSendNum)
			} else {
				fmt.Println("name: "+r.name+" ,write error：", ee)
				break
			}
			if ConfigObj.RobotSendMsgIntervalSec > 0 {
				time.Sleep(dt * time.Second)
			}
		}
		err := dail.Close()
		if err != nil {
			return
		}
	}
}

// 机器人接收字节数据
func robotRecPBMsg(r *TestRobot, newConn *net.Conn) {
	headData := make([]byte, 2)                                   //包头数据
	headSizeAlreadyRead := 0                                      //已读包头长度
	needReadSize := 0                                             //需要读取的长度大小
	curDataSize := 0                                              //当前包所读取的长度大小
	compData := make([]byte, gateway_server.ReadStreamBufferSize) //已读的数据长度
	for {
		readData := make([]byte, gateway_server.ReadStreamBufferSize)
		num, readError := (*newConn).Read(readData)
		if readError != nil {
			fmt.Println("robotRecMsg :" + readError.Error())
			return
		} else {
			//fmt.Println("doReadMsg", "num", num, "message", string(readData[:num])) //处理接收到的数据
			for {
				if headSizeAlreadyRead < gateway_server.HeadLen { //没读取完数据头（读取完数据头才知道包体长度）
					diffHead := gateway_server.HeadLen - headSizeAlreadyRead //数据头剩余所需长度
					if num >= diffHead {                                     //接受到的数据长度大于等于数据头证明数据头内容可以被填充好
						copy(headData[headSizeAlreadyRead:], readData[:diffHead]) //填充好包头数据内容
						headSizeAlreadyRead = headSizeAlreadyRead + diffHead      //更新包头已读长度
						num -= diffHead                                           //数据长度减少（因为读取了长度）,更新
						readData = append(readData[:0], readData[diffHead:]...)   //更新接受数据的内容（有部分已被转为包头被记录下来了）
						needReadSize = int(binary.BigEndian.Uint16(headData[:]))  //根据数据头的值得出数据包体长度
						if num <= 0 {
							break //假如没数据了直接break
						} else {
							continue //否则直接continue重新进来走包体切割逻辑
						}
					} else { //数据头被切割了,需要等待下次读取来完善
						headData = append(headData[:headSizeAlreadyRead], readData[:num]...) //填充好包头数据内容
						headSizeAlreadyRead += num                                           //更新包头已读长度
						break
					}
				} else { //包头被读完了（剩下是截取数据体内容长度）
					diff := needReadSize - curDataSize //当前包所需的读取剩余长度
					if diff <= num {                   //假如所需剩余读取数据的长度小于等于接收到的数据长度,直接形成一个包即可
						num -= diff
						compData = append(compData[:curDataSize], readData[:diff]...)
						robDealPBMsg(r, compData[:needReadSize], diff, curDataSize, num)

						//切好一个包需要重置相关数据内容，然后继续开始切下一个包
						headData = make([]byte, 2)
						compData = make([]byte, gateway_server.ReadStreamBufferSize)
						headSizeAlreadyRead = 0
						needReadSize = 0
						curDataSize = 0

						if num == 0 { //刚好形成一个包,直接break就行
							break
						} else {
							//还有剩余数据直接等待下次进入切割逻辑,剩余部分做新的readData
							readData = append(readData[:0], readData[diff:]...)
						}
					} else { //不足组合成一个包直接等下次读取数据再进来组合（有多少读多少）
						compData = append(compData[:curDataSize], readData[:num]...)
						curDataSize += num
						break
					}
				}
			}

			//处理完数据后设置下次读取的超时时间
			dLineTime := time.Now().Add(6000 * time.Second)
			err := (*(newConn)).SetReadDeadline(dLineTime)
			if err != nil {
				fmt.Println("Socket SetReadDeadline", "Error", err.Error())
			}
		}
		readData = nil
	}
}

func robDealPBMsg(r *TestRobot, data []byte, needReadSize, curDataSize, num int) {
	proNum := binary.BigEndian.Uint16(data[0:gateway_server.ProtoNumLen])
	proString := gateway_server.ProtocolDistributeInfo.ProtoNumMap[proNum]
	resSearch := proString != ""
	r.lastReadMsgTime = time.Now()
	diffWithSendTime := r.lastReadMsgTime.Sub(r.lastSendMsgTime).Milliseconds()
	fmt.Println("Robot", r.name, " diffWithSend Milliseconds: ", diffWithSendTime)

	//分析接收到的协议
	if resSearch {
		switch proString {
		case "S2CTestRet":
			t := new(pb.S2CTestRet)
			proto.Unmarshal(data[gateway_server.ProtoNumLen:], t)
			//fmt.Println("S2CTestRet 's Num = ", t.Num, "robotName = ", r.name, "robotID = ", r.uid)
		default:
		}
	} else {
		fmt.Println(data, ", proNum = "+strconv.Itoa(int(proNum)), "Search Fail! ", "needReadSize = ", needReadSize, "curDataSize = ", curDataSize, "num = ", num)
	}
}
