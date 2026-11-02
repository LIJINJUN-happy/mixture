package gateway_server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"mixture/src/common/printer"
	"mixture/src/common/tools"
	"os"
	"reflect"
	"strings"
	"sync"
)

// 用于网关路由分发的对象池，避免频繁 GC
var DistributeDataPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, ReadStreamBufferSize)
		return &buf
	},
}

// 把 bufPtr 派发到目标服务进程（linker）的接收通道，任何失败路径内部 Put 归还池
func SafeSendToLinker(linker *SerLinker, bufPtr *[]byte) {
	select {
	case linker.receiveChannel <- bufPtr:
	default:
		printer.TextWarn("DoDistribute", "Linker channel full, drop packet", linker.linkerName)
		DistributeDataPool.Put(bufPtr)
	}
}

// 把 bufPtr 派发到目标客户端（sessionID）的写协程缓冲，任何失败路径内部 Put 归还池
func SafeSendToGateClient(bufPtr *[]byte, sessionID uint32, protoNum uint16) {
	if client, ok := GateClientMap.Load(sessionID); ok {
		if session, isSession := client.(*clientSession); isSession {
			select {
			case session.clientBuffChannel <- bufPtr:
				return // 成功投递，bufPtr 由接收方回收
			default:
				printer.TextWarn("DoDistribute", "Client Buffer Full, Drop Msg SessionID", sessionID)
			}
		}
	} else if sessionID == 0 {
		protoName := ProtocolDistributeInfo.ProtoNumArray[protoNum]
		printer.TextDebug("DoDistribute Check serFunc", "protoName", protoName)
		switch protoName {
		case "xxxx":
		case "xxxxx":
		default:
		}
	}
	DistributeDataPool.Put(bufPtr)
}

// 协议格式：[[sessionID(4字节)][proNum(2字节)][data(xxx字节)]]
// 该函数负责数据的判断与转发(sourceSession 用于区别是来自服务器内部还是客户端,假如来自客户端需要添加上相关的字段内容再做转发)
func DoDistribute(cutList *CutIndexList, sliceBuffer []byte, sourceSession uint32) {
	beginIdx := 0
	extraLen := 0
	if sourceSession > 0 {
		extraLen += SessionIdLen //假如来自客户端传输过来的数据,需要额外添加sessionID 值（4字节）的长度
	}
	for i := 0; i < cutList.Len(); i++ {
		element := cutList.Get(i)
		//printer.TextDebug("doDistribute", "beginIdx", beginIdx, "element", element, "data", sliceBuffer[beginIdx:element])

		//池化获取内存
		dataLen := element - beginIdx + extraLen
		bufPtr, ok := DistributeDataPool.Get().(*[]byte)
		if !ok {
			printer.TextError("DoDistribute", "Err", "DistributeDataPool Get Fail")
			return
		}
		data := (*bufPtr)[:dataLen]

		if extraLen == 0 {
			copy(data, sliceBuffer[beginIdx:element]) //其他服务进程传输过来的协议数据包
		} else if extraLen > 0 {
			binary.BigEndian.PutUint32(data[:extraLen], sourceSession) //来自客户端socket的数据需要把sessionId做协议包体的前缀作为对应客户端的标识
			copy(data[extraLen:], sliceBuffer[beginIdx:element])
		}
		beginIdx = element

		//根据协议号来进行协议派发（包体中从4字节开始到6字节区间的数据，大端方式读取）
		elementProtoNum := binary.BigEndian.Uint16(data[SessionIdLen : SessionIdLen+ProtoNumLen])
		if int(elementProtoNum) >= math.MaxUint16 {
			printer.TextWarn("DoDistribute", "elementProtoNum out of range", elementProtoNum, "sourceSession", sourceSession)
			DistributeDataPool.Put(bufPtr)
			continue
		}
		protoSendSerName := ProtocolDistributeInfo.ProtoDistributeArray[elementProtoNum]
		printer.TextDebug("DoDistribute", "elementProtoNum", elementProtoNum, "protoSendSerName", protoSendSerName)

		// 缩切到实际数据长度，接收端 *bufPtr 即为有效数据
		*bufPtr = data

		//注意此时的data依旧是包含协议号的数据
		switch protoSendSerName {
		case "actor_server":
			SafeSendToLinker(ActorSerLinker, bufPtr)
		case "modularized_server":
			SafeSendToLinker(ModularizedSerLinker, bufPtr)
		case "interactive_server":
			SafeSendToLinker(InteractiveSerLinker, bufPtr)
		case "web_server":
			SafeSendToLinker(WebSerLinker, bufPtr)
		case "gateway_server":
			sendTarSessionID := binary.BigEndian.Uint32(data[:SessionIdLen])
			SafeSendToGateClient(bufPtr, sendTarSessionID, elementProtoNum)
		default:
			DistributeDataPool.Put(bufPtr)
			printer.TextWarn("DoDistribute", "unknown protoSendSerName", protoSendSerName, "elementProtoNum", elementProtoNum)
		}
	}
}

type DistributeInfo struct {
	ProtoNameMap         map[string]uint16      //协议名字对应协议号
	ProtoNumArray        [math.MaxUint16]string //协议号对应协议名字
	ProtoDistributeArray [math.MaxUint16]string //协议号对应转发的服务名
	ProtocolType         string                 //协议类型（JSON还是PB）
	ProtoVersion         string                 //协议版本号
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
	ProtocolDistributeInfo.ProtoNameMap = make(map[string]uint16)
	ProtocolDistributeInfo.ProtocolType = tools.GetSysValueByKey("protocolType")

	//根据协议类型加载不同的allproto文件的协议号
	allProtoPath := ""
	if ProtocolDistributeInfo.ProtocolType == "protobuf" {
		allProtoPath = "./proto/mutual_pb/pb_src/allprotocol.json"
	} else {
		allProtoPath = "./proto/mutual_json/json_src/allprotocol.json"
	}
	resolvedProtoPath, err := tools.ResolveFilePath(allProtoPath)
	if err != nil {
		panic("allprotocol.json read fail, allProtoPath:" + allProtoPath)
	}
	data, err := os.ReadFile(resolvedProtoPath)
	if err != nil {
		panic("allprotocol.json read fail, resolvedProtoPath:" + resolvedProtoPath)
	}

	//获取版本号（用于和前端区分协议版本）
	var allProto map[string]interface{}
	err = json.Unmarshal(data, &allProto)
	if err != nil {
		panic("allprotocol.json Unmarshal fail, err:" + err.Error())
	}
	if versionString, findVersion := allProto["_Version_"]; findVersion {
		ProtocolDistributeInfo.ProtoVersion = versionString.(string)
	} else {
		panic("allprotocol.json Get _Version_ fail")
	}

	//加载协议内容
	if !(ProtocolDistributeInfo.ProtocolType == "protobuf" || ProtocolDistributeInfo.ProtocolType == "json") {
		panic("ProtocolType is nil, protocolType:" + ProtocolDistributeInfo.ProtocolType)
	}
	LoadProtocol("./proto/distribute_config.json", allProto)
	CheckLoadProtocol()
}

func LoadProtocol(path string, allProto map[string]interface{}) {
	ns, f := tools.ReadFileByName(path)
	if (ns == nil) || (f == nil) {
		panic(fmt.Sprintf("LoadProtocol: Failed to read %s", path))
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
		panic(fmt.Sprintf("Failed Unmarshal %s", path))
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
				panic(fmt.Sprintf("interior_pb switch sendServerTarget fail: distributeHead=%s sendServerTarget=%s", distributeHead, sendServerTarget))
			}
			lenOfValue := v.Field(k).Len()
			for idx := 0; idx < lenOfValue; idx++ {
				proName := v.Field(k).Interface().([]string)[idx]
				ptoNum := protoStartNum
				ProtocolDistributeInfo.ProtoDistributeArray[ptoNum] = sendServerTarget
				ProtocolDistributeInfo.ProtoNumArray[ptoNum] = proName
				ProtocolDistributeInfo.ProtoNameMap[proName] = ptoNum
				protoStartNum += 1 //自增1
				//fmt.Println("sendServerTarget = ", sendServerTarget, "proName = ", proName, "ptoNum = ", ptoNum)
			}
		} else if distributeHead == "CS_" {
			//外部协议读取allproto
			lenOfValue := v.Field(k).Len()
			for idx := 0; idx < lenOfValue; idx++ {
				proName := v.Field(k).Interface().([]string)[idx]
				num, findProtoNum := allProto[proName]
				if !findProtoNum {
					panic("mutual_pb read numStr Fail, proName = " + proName)
				}
				ptoNum := uint16(num.(float64))
				ProtocolDistributeInfo.ProtoDistributeArray[ptoNum] = sendServerTarget
				ProtocolDistributeInfo.ProtoNumArray[ptoNum] = proName
				ProtocolDistributeInfo.ProtoNameMap[proName] = ptoNum
				//fmt.Println("sendServerTarget = ", sendServerTarget, "proName = ", proName, "ptoNum = ", ptoNum)
			}
		}
	}
}

func CheckLoadProtocol() {
	sizeNum := 0
	for _, v := range ProtocolDistributeInfo.ProtoNumArray {
		if v != "" {
			sizeNum++
		}
	}
	sizeDistribute := 0
	for _, v := range ProtocolDistributeInfo.ProtoDistributeArray {
		if v != "" {
			sizeDistribute++
		}
	}
	sizeName := len(ProtocolDistributeInfo.ProtoNameMap)
	if (sizeNum == sizeName) && (sizeName == sizeDistribute) {
		return
	}
	panic("CheckLoadProtocol Error")
}

// Gate进程发送协议去服务进程
func SendMsg2Server(sessionId uint32, protoName string, data []byte) {
	p, searchRes := ProtocolDistributeInfo.ProtoNameMap[protoName]
	if !searchRes {
		printer.TextError("SendMsg2Server Fail", "protoName", protoName, "data", data, "pnum", p)
		return
	}
	// sliceBuffer 格式：[ProtoNum(2B)][Data]，SessionID 由 DoDistribute 追加
	if bufPtr, ok := DistributeDataPool.Get().(*[]byte); ok {
		dataLen := ProtoNumLen + len(data)
		newData := (*bufPtr)[:dataLen]
		binary.BigEndian.PutUint16(newData[0:], p)
		copy(newData[ProtoNumLen:], data)
		//printer.TextDebug("SendMsg2Server", "protoName", protoName, "data", data, "pnum", p, "newData", newData)

		//调用派发协议接口送出协议
		var cutList CutIndexList
		cutList.Push(dataLen)
		*bufPtr = newData
		DoDistribute(&cutList, *bufPtr, sessionId)
		DistributeDataPool.Put(bufPtr)
		return
	}
	printer.TextError("SendMsg2Server", "Err", "DistributeDataPool Get Fail", "sessionId", sessionId)
}
