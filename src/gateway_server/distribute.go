package gateway_server

import (
	"container/list"
	"encoding/binary"
	"encoding/json"
	"mixture/src/common/printer"
	"mixture/src/common/tools"
	"os"
	"reflect"
	"strings"
	"unsafe"
)

// 协议格式：[[sessionID(4字节)][proNum(2字节)][data(xxx字节)]]
// 该函数负责数据的判断与转发(sourceSession 用于区别是来自服务器内部还是客户端,假如来自客户端需要添加上相关的字段内容再做转发)
func DoDistribute(cutList *list.List, sliceBuffer []byte, sourceSession uint32) {
	beginIdx := 0
	extraLen := 0
	if sourceSession > 0 {
		extraLen += SessionIdLen //假如来自客户端传输过来的数据,需要额外添加sessionID 值（4字节）的长度
	}
	for iterator := cutList.Front(); iterator != nil; iterator = iterator.Next() {
		element := iterator.Value.(int)
		//printer.TextDebug("doDistribute", "beginIdx", beginIdx, "element", element, "data", sliceBuffer[beginIdx:element])
		data := make([]byte, element-beginIdx+extraLen)
		if extraLen == 0 {
			copy(data, sliceBuffer[beginIdx:element]) //其他服务进程传输过来的协议数据包
		} else if extraLen > 0 {
			binary.BigEndian.PutUint32(data[:extraLen], sourceSession) //来自客户端socket的数据需要把sessionId做协议包体的前缀作为对应客户端的标识
			copy(data[extraLen:], sliceBuffer[beginIdx:element])
		}
		beginIdx = element

		//根据协议号来进行协议派发（包体中从4字节开始到6字节区间的数据，大端方式读取）
		elementProtoNum := binary.BigEndian.Uint16(data[SessionIdLen : SessionIdLen+ProtoNumLen])
		protoName, canFindProtoName := ProtocolDistributeInfo.ProtoNumMap[elementProtoNum]
		if !canFindProtoName {
			printer.TextWarn("DoDistribute", "protoName is nil, elementProtoNum", elementProtoNum)
			continue
		}
		protoSendSerName, canFindProtoSendSerName := ProtocolDistributeInfo.ProtoDistributeMap[protoName]
		if !canFindProtoSendSerName {
			printer.TextWarn("DoDistribute", "protoSendSerName is nil, protoName", protoName)
			continue
		}
		//printer.TextInfo("doDistribute", "protoName: ", protoName, "protoSendSerName: ", protoSendSerName)

		//注意此时的data依旧是包含协议号的数据
		switch protoSendSerName {
		case "actor_server":
			actorSerLinker.receiveChannel <- data
		case "modularized_server":
			modularizedSerLinker.receiveChannel <- data
		case "interactive_server":
			interactiveSerLinker.receiveChannel <- data
		case "web_server":
			webSerLinker.receiveChannel <- data
		case "gateway_server": //转发到对应客户端Write协程读取的chan中(发送给客户端前需要把SessionID前缀去掉)
			sendTarSessionID := binary.BigEndian.Uint32(data[:SessionIdLen])
			if client, resSearchFromMap := GateClientMap.Load(sendTarSessionID); resSearchFromMap {
				if session, changeTypeRes := client.(*clientSession); changeTypeRes {
					session.clientBuffChannel <- data[SessionIdLen:]
				}
			} else if sendTarSessionID == 0 {
				printer.TextWarn("DoDistribute Check serFunc", "protoName", protoName)
				switch protoName {
				case "xxxx":
				case "xxxxx":
				default:
				}
			}
		default:
		}
	}
}

type DistributeInfo struct {
	ProtoNumMap        map[uint16]string //协议号对应协议名字
	ProtoNameMap       map[string]uint16 //协议名字对应协议号
	ProtoDistributeMap map[string]string //协议名对应转发的服务名
	ProtocolType       string            //协议类型（JSON还是PB）
	ProtoVersion       string            //协议版本号
}

type distributeConfigInfo struct {
	AS  []string `json:"CS_actor_server"`
	MS  []string `json:"CS_modularized_server"`
	IS  []string `json:"CS_interactive_server"`
	GS  []string `json:"CS_gateway_server"`
	WB  []string `json:"CS_web_server"`
	IAS []string `json:"SS_actor_server"`
	IMS []string `json:"SS_modularized_server"`
	IIS []string `json:"SS_interactive_server"`
	IWS []string `json:"SS_web_server"`
	IGS []string `json:"SS_gateway_server"`
}

var ProtocolDistributeInfo = new(DistributeInfo)

// 在init先把协议内容先加载好
func init() {
	ProtocolDistributeInfo.ProtoNumMap = make(map[uint16]string)
	ProtocolDistributeInfo.ProtoNameMap = make(map[string]uint16)
	ProtocolDistributeInfo.ProtoDistributeMap = make(map[string]string)
	ProtocolDistributeInfo.ProtocolType = tools.GetSysValueByKey("protocolType")

	//根据协议类型加载不同的allproto文件的协议号
	allProtoPath := ""
	if ProtocolDistributeInfo.ProtocolType == "protobuf" {
		allProtoPath = "./proto/mutual_pb/pb_src/allprotocol.json"
	} else {
		allProtoPath = "./proto/mutual_json/json_src/allprotocol.json"
	}
	data, err := os.ReadFile(allProtoPath)
	if err != nil {
		panic("allprotocol.json read fail, allProtoPath:" + allProtoPath)
		return
	}

	//获取版本号（用于和前端区分协议版本）
	var allProto map[string]interface{}
	err = json.Unmarshal(data, &allProto)
	if err != nil {
		panic("allprotocol.json Unmarshal fail, err:" + err.Error())
		return
	}
	if versionString, findVersion := allProto["_Version_"]; findVersion {
		ProtocolDistributeInfo.ProtoVersion = versionString.(string)
	} else {
		panic("allprotocol.json Get _Version_ fail")
		return
	}

	//加载协议内容
	if !(ProtocolDistributeInfo.ProtocolType == "protobuf" || ProtocolDistributeInfo.ProtocolType == "json") {
		panic("ProtocolType is nil, protocolType:" + ProtocolDistributeInfo.ProtocolType)
		return
	}
	LoadProtocol("./proto/distribute_config.json", allProto)
	CheckLoadProtocol()
}

func LoadProtocol(path string, allProto map[string]interface{}) {
	ns, f := tools.ReadFileByName(path)
	if (ns == nil) || (f == nil) {
		panic("loadProtocol: Read " + path + " Error")
	}

	defer func(readFile *os.File) {
		if r := readFile.Close(); r != nil {
			panic(r)
		}
	}(f)

	sendTarCheckMap := make(map[string]bool)
	sendTarCheckMap["actor_server"] = true
	sendTarCheckMap["modularized_server"] = true
	sendTarCheckMap["interactive_server"] = true
	sendTarCheckMap["web_server"] = true
	sendTarCheckMap["gateway_server"] = true

	jsonStrBuilder := strings.Builder{}
	for ns.Scan() {
		line := ns.Text()
		jsonStrBuilder.WriteString(line)
	}

	jsonStr := jsonStrBuilder.String()
	distributeConfigObj := new(distributeConfigInfo)
	if json.Unmarshal([]byte(jsonStr), distributeConfigObj) != nil {
		panic("Unmarshal " + path + " Error")
	}

	//注意，obj不能为指针类型，否则会报：panic recovered: reflect: NumField of non-struct type
	t := reflect.TypeOf(*distributeConfigObj)
	v := reflect.ValueOf(*distributeConfigObj)
	for k := 0; k < t.NumField(); k++ {
		//去除SS_或者CS_,没有就直接报错吧
		distributeHead := ""
		sendServerTarget := t.Field(k).Tag.Get("json")
		if strings.HasPrefix(sendServerTarget, "SS_") || strings.HasPrefix(sendServerTarget, "CS_") {
			distributeHead = sendServerTarget[:3]
			sendServerTarget = sendServerTarget[3:]
			if !sendTarCheckMap[sendServerTarget] {
				panic("Unmarshal sendServerTarget Error: sendServerTarget = " + sendServerTarget)
			}
		} else {
			panic("Unmarshal sendServerTarget Can‘t Find SS_ Or CS_ Prefix")
		}
		//内部协议走自动递增
		if distributeHead == "SS_" {
			protoStartNum := uint16(0)
			switch sendServerTarget {
			case "modularized_server":
				protoStartNum = 10000
			case "interactive_server":
				protoStartNum = 15000
			case "web_server":
				protoStartNum = 20000
			case "actor_server":
				protoStartNum = 25000
			case "gateway_server":
				protoStartNum = 30000
			}
			if protoStartNum == 0 {
				panic("interior_pb switch sendServerTarget Fail, distributeHead = " + distributeHead + " ,sendServerTarget = " + sendServerTarget)
				return
			}
			lenOfValue := v.Field(k).Len()
			for idx := 0; idx < lenOfValue; idx++ {
				proName := v.Field(k).Interface().([]string)[idx]
				ptoNum := protoStartNum
				ProtocolDistributeInfo.ProtoDistributeMap[proName] = sendServerTarget
				ProtocolDistributeInfo.ProtoNumMap[ptoNum] = proName
				ProtocolDistributeInfo.ProtoNameMap[proName] = ptoNum
				protoStartNum += 1 //自增1
				//fmt.Println("sendServerTarget = ", sendServerTarget, "proName = ", proName, "ptoNum = ", ptoNum)
			}
		} else if distributeHead == "CS_" {
			//外部协议读取allproto
			lenOfValue := v.Field(k).Len()
			for idx := 0; idx < lenOfValue; idx++ {
				proName := v.Field(k).Interface().([]string)[idx]
				if num, findProtoNum := allProto[proName]; !findProtoNum {
					panic("mutual_pb read numStr Fail, proName = " + proName)
					return
				} else {
					ptoNum := uint16(num.(float64))
					ProtocolDistributeInfo.ProtoDistributeMap[proName] = sendServerTarget
					ProtocolDistributeInfo.ProtoNumMap[ptoNum] = proName
					ProtocolDistributeInfo.ProtoNameMap[proName] = ptoNum
					//fmt.Println("sendServerTarget = ", sendServerTarget, "proName = ", proName, "ptoNum = ", ptoNum)
				}
			}
		}
	}
}

func CheckLoadProtocol() {
	sizeNum := uint16(len(ProtocolDistributeInfo.ProtoNumMap))
	sizeName := uint16(len(ProtocolDistributeInfo.ProtoNameMap))
	sizeDistribute := uint16(len(ProtocolDistributeInfo.ProtoDistributeMap))
	if (sizeNum == sizeName) && (sizeName == sizeDistribute) {
		return
	}
	panic("CheckLoadProtocol Error")
}

// Gate进程发送协议去服务进程
func SendMsg2Server(sessionId uint32, protoName string, data []byte) {
	newData := make([]byte, 60)
	p, searchRes := ProtocolDistributeInfo.ProtoNameMap[protoName]
	if !searchRes {
		printer.TextError("SendMsg2Server Fail", "protoName", protoName, "data", data, "pnum", p)
		return
	}
	binary.BigEndian.PutUint16(newData[0:], p)
	newData = append(newData[:unsafe.Sizeof(p)], data...)
	printer.TextInfo("SendMsg2Server", "protoName", protoName, "data", data, "pnum", p, "newData", newData)

	//调用派发协议接口送出协议
	cutList := list.New()
	cutList = cutList.Init()
	cutList.PushBack(len(data) + HeadLen)
	DoDistribute(cutList, newData[0:], sessionId)
}
