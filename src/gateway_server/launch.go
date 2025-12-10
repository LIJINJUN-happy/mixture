package gateway_server

import (
	"container/list"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"mixture/proto/interior_json"
	"mixture/proto/interior_pb/ipb"
	"mixture/src/common/printer"
	"mixture/src/common/tools"
	"mixture/src/database"
	"net"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Run Gateway service: used to listen to client requests, accept protocols, decompose protocols, and send protocols
func Run() {
	isOk, db := database.GetSQLiteConnDB()
	if isOk != nil {
		return
	}

	//Init Gate Config With DBObjectPtr
	if !DoInit(db) {
		return
	}

	//Start Server
	BeginGateServer()
	time.Sleep(2 * time.Second)
	BeginLinkServer(webSerLinker)
	BeginLinkServer(actorSerLinker)
	BeginLinkServer(modularizedSerLinker)
	BeginLinkServer(interactiveSerLinker)

	//Do Check With Loop
	Looping()
}

func DoInit(db *sql.DB) bool {
	defer func(sq *sql.DB) {
		closeError := sq.Close() //调用db.Close() 函数关闭数据库
		if closeError != nil {
			printer.TextError("doInit", "SqlLite Database Close Failed", closeError)
		}
	}(db)

	//白名单初始化逻辑
	WhiteListPtr.WhiteMux.Lock()
	WhiteListPtr.WhiteMap = make(map[string]string)
	WhiteListPtr.WhiteMux.Unlock()
	if ipListStr := tools.GetSysValueByKey("whiteIPList"); len(ipListStr) > 0 {
		for _, ip := range strings.Split(ipListStr, "|") {
			WhiteListPtr.AddWhiteIP(ip) //加载IP白名单列表
		}
	}
	if whiteStatus := tools.GetSysValueByKey("whiteIPListDefaultStatus"); (whiteStatus == "true") || (whiteStatus == "false") {
		if whiteStatus == "true" {
			WhiteListPtr.IsActive = true
		}
	} else {
		printer.TextError("doInit", "whiteIPListDefaultStatus Error", "whiteIPListDefaultStatus", whiteStatus)
		return false
	}
	printer.TextInfo("doInit", "WhiteList Init Info", *WhiteListPtr)

	GateInfo.curDistributionSessionID = 0
	GateInfo.rebuildListenSec = 5
	GateInfo.rebuildListenTimes = 0
	GateInfo.maxConnSize = MaxConnectClientNum
	GateInfo.gateId, _ = strconv.Atoi(os.Args[2])
	checkAddressRes, address := database.GetGateIPAddressById(GateInfo.gateId, db)
	if !checkAddressRes {
		printer.TextError("doInit", "GetGateIPAddressById Result", "Failed", "GateId", GateInfo.gateId)
		return false
	} else {
		GateInfo.ip = address
	}
	checkPortRes, port := database.GetGatePortNumById(GateInfo.gateId, db)
	if !checkPortRes {
		printer.TextError("doInit", "GetGatePortNumById Result", "Failed", "GateId", GateInfo.gateId)
		return false
	} else {
		GateInfo.port = strconv.Itoa(port)
	}
	workMode := tools.GetSysValueByKey("workMode")
	GateInfo.workMode = workMode

	protocolType := tools.GetSysValueByKey("protocolType")
	GateInfo.protocol = protocolType

	//查找interactiveSer的ID标识
	checkInteractiveSerIdRes, interactiveSerId := database.GetInteractiveSerIdByGateId(GateInfo.gateId, db)
	if !checkInteractiveSerIdRes {
		printer.TextError("doInit", "checkInteractiveSerIdRes", checkInteractiveSerIdRes, "GateId", GateInfo.gateId)
		return false
	}

	//根据执行参数选择网络模式以及使用的协议方式
	checkWorkMode := GateInfo.workMode == "tcp" || GateInfo.workMode == "ws"
	checkProtocol := GateInfo.protocol == "json" || GateInfo.protocol == "protobuf"
	if !checkWorkMode {
		panic("\nArgs Error With GateWay's WorkMode: " + GateInfo.workMode + " , Should Be 'tcp' or 'ws'")
	}
	if !checkProtocol {
		panic("\nArgs Error With GateWay's Protocol: " + GateInfo.protocol + " , Should Be 'json' or 'protobuf'")
	}

	//ConnectionInfo初始化(TCP模式的相关参数)
	ConnectionInfo.connReadBufSize = 255
	ConnectionInfo.connReadTimeout = 900 * time.Second
	ConnectionInfo.connWriteChanCaps = 5

	//ActorSerLinker初始化
	actorSerLinker.InitLinker("actorSer", db, GateInfo.gateId)

	//ModularizedSerLinker初始化
	modularizedSerLinker.InitLinker("modularizedSer", db, GateInfo.gateId)

	//WebSerLinker初始化
	webSerLinker.InitLinker("webSer", db, GateInfo.gateId)

	//InteractiveSerLinker初始化
	interactiveSerLinker.InitLinker("interactiveSer", db, interactiveSerId)

	return true
}

/*链接xxx服务进程,收发gatewaySer和xxxSer之间的数据信息*/
func BeginLinkServer(sl *SerLinker) {
	go func(serLinker *SerLinker) {
		defer func(s *SerLinker) {
			//通知停止处理(*dialObject).Write操作,因为panic重启该协程,会造成旧的write协程没被销毁从而协程泄露
			s.receiveChannel <- GoroutineSyncBreakSign

			closeErr := (*s.serConn).Close() //关闭旧socket,链接成功会重新赋值一个新的socket
			if closeErr != nil {
				printer.TextError(s.linkerName, " dial.Close() Failed With Defer", closeErr)
			}
			time.Sleep(10 * time.Second)
			go BeginLinkServer(s)
		}(serLinker)

	connTryAgain:
		dial, err := net.Dial("tcp", serLinker.address)
		if err != nil {
			var strBuild strings.Builder
			errString := err.Error()
			strBuild.Grow(len(errString) + len(serLinker.linkerName) + 20)
			strBuild.WriteString("Link To ")
			strBuild.WriteString(serLinker.linkerName)
			strBuild.WriteString(" Fail: ")
			strBuild.WriteString(errString)
			printer.TextError(strBuild.String())
			time.Sleep(GateInfo.rebuildListenSec * time.Second)
			goto connTryAgain //链接失败再重新尝试,不成功就不创建dealWithLinkerData协程,避免协程泄露
		} else {
			printer.TextInfo("Link Success ", "serLinker.linkerName", serLinker.linkerName)
			serLinker.serConn = &dial
		}

		go DealWithLinkerData(serLinker)

		//读取字节流所需的结构对象以及容器
		rsbObject := &ReadStreamBufferObject{
			readWay:          uint8(1),
			headReadArray:    make([]uint8, 0, HeadLen),
			totalNeedReadLen: 0,
			curReadLen:       0,
			byteData:         make([]byte, 0, ReadStreamBufferSize),
		}
		serData := make([]byte, 255)
		sliceBuffer := make([]byte, ReadStreamBufferSize)
		cutList := list.New()

		//解析xxx服务进程发送来的数据(派发到对应进程的通信道中)
		for {
			cutList = cutList.Init()
			readNum, readErr := dial.Read(serData)
			if readErr != nil {
				printer.TextError("Read Data Fail ", "serLinker.linkerName", serLinker.linkerName, "readErr", readErr.Error())
				res, desc := CheckIsCloseByRemote(readErr)
				if !res {
					var strBuilder strings.Builder
					strBuilder.Grow(len(serLinker.linkerName) + 56)
					strBuilder.WriteString("Some Extraordinary Error With Fun beginLinkServer ")
					strBuilder.WriteString(serLinker.linkerName)
					strBuilder.WriteString("...")
					desc = strBuilder.String()
				}
				printer.TextError(desc)
				return
			} else {
				//printer.TextInfo("Get Data ", "From ", serLinker.linkerName, "serData", serData[:readNum])
				DoSplit(rsbObject, serData, readNum, cutList, sliceBuffer)

				//派发到对应的通信道中(来自其他服务所以sourceSessionID为0，标识不是来自客户端发送来的协议数据)
				DoDistribute(cutList, sliceBuffer, 0)
			}
		}

	}(sl)
}

// 处理连接器中的Channel收到的数据
func DealWithLinkerData(linker *SerLinker) {
	defer func(s *SerLinker) {
		if r := recover(); r != nil {
			//panic容错处理,由于Read协程panic或者退出都会通知dealWithLinkerData协程来同步协程的退出.所以假如该协程panic应让Read协程触发错误来通知该协程退出
			closeRes := (*s.serConn).Close()
			if closeRes != nil {
				printer.TextError("dealWithLinkerData Close Linker Conn Fail", "closeRes", closeRes.Error())
			} else {
				for range s.receiveChannel {
					printer.TextInfo("dealWithLinkerData Quit")
					return
				}
			}
		}
	}(linker)

	//连接上对应的服务进程后,发送本地gatewayID给予服务进程作为标识ID区分
	SendGateIDToRemoteSer(linker.serConn, linker.linkerName)

	dataSlice := make([]byte, ReadStreamBufferSize)
	for {
		select {
		case anyData := <-linker.receiveChannel:
			kind := reflect.TypeOf(anyData).Kind().String()
			//printer.TextInfo("dealWithLinkerData: ", "reflect.TypeOf(anyData).Kind().String()", kind)
			if kind == "string" {
				if anyData.(string) == GoroutineSyncBreakSign {
					return
				}
				continue
			} else if kind == "slice" {
				num := DoPack(dataSlice, anyData.([]byte), 1) //默认内部转发用方式1,包头+包体
				//printer.TextError("dealWithLinkerData", "DoPackResNum", num, "DataSlice", dataSlice[0:num])

				if num > 0 {
					_, writeErr := (*linker.serConn).Write(dataSlice[0:num])
					if writeErr != nil {
						printer.TextError("GateWay DialObject Write Fail", "writeErr", writeErr.Error())
					} else {
						//printer.TextDebug("GateWay DialObject Write Success", "writeNum", strconv.Itoa(writeNum), "dataSlice", string(dataSlice[0:num]))
					}
				}
			}
		}
	}
}

/*使用defer搭配递归解决监听线程的崩溃*/
func BeginGateServer() {
	if GateInfo.rebuildListenTimes >= maxGateRebuildListenTimes {
		printer.TextError("Rebuild ListenTimes Exceeded.")
		os.Exit(101)
	}
	printer.TextInfo("Begin Launching Server gateway_server")
	if GateInfo.workMode == "tcp" {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					GateInfo.rebuildListenTimes += 1
					time.Sleep(GateInfo.rebuildListenSec * time.Second)
					printer.TextWarn("重新启动tcp模式网关服务")
					go BeginGateServer()
				}
			}()
			BeginTcpSer()
		}()
	} else if GateInfo.workMode == "ws" {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					GateInfo.rebuildListenTimes += 1
					time.Sleep(GateInfo.rebuildListenSec * time.Second)
					printer.TextWarn("重新启动WebSocket模式网关服务")
					go BeginGateServer()
				}
			}()
			beginWebSocketSer()
		}()
	}
}

func Looping() {
	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case s := <-sigChannel:
			switch s {
			case syscall.SIGINT, syscall.SIGTERM:
				printer.TextWarn("收到 syscall.SIGINT 信号通知关闭进程")
				return
			default:
			}
		case <-time.After(intervalLoopingTime):
			printer.TextInfo("ClientGoroutine Statistics", "nowCountR", (*clientReadGoroutineNum).Load(), "nowCountW", clientWriteGoroutineNum.Load())
		}
	}
}

func SendGateIDToRemoteSer(cn *net.Conn, linkerName string) {
	dataSize := 0
	dateByte := make([]byte, 100)
	if GateInfo.protocol == "protobuf" {
		pbDate := ipb.GateIdSyncInfo{SerName: linkerName, GateId: int32(GateInfo.gateId)}
		protoBufData, errorGateInfo := proto.Marshal(&pbDate)
		if errorGateInfo != nil {
			printer.TextError("sendGateIDToRemoteSer", "proto.Marshal Error", errorGateInfo.Error())
			panic(errorGateInfo)
			return
		}
		dataSize = copy(dateByte, protoBufData)
	} else {
		jsStruct := interior_json.GateIdSyncInfo{SerName: linkerName, GateId: int32(GateInfo.gateId)}
		jsonData, errorGateInfo := json.Marshal(jsStruct)
		if errorGateInfo != nil {
			printer.TextError("sendGateIDToRemoteSer", "json.Marshal Error", errorGateInfo.Error())
			panic(errorGateInfo)
			return
		}
		dataSize = copy(dateByte, jsonData)
	}

	//打包数据发送给对应服务进程
	dateByte = append(dateByte[:dataSize])
	protoNum := 0 //默认通知服务标识网关的协议号指定为0
	dataIdxFrom := SessionIdLen + ProtoNumLen
	dataSlice := make([]byte, len(dateByte)+HeadLen+dataIdxFrom)
	body := make([]byte, len(dateByte)+dataIdxFrom)
	binary.BigEndian.PutUint32(body[0:SessionIdLen], uint32(0))
	binary.BigEndian.PutUint16(body[SessionIdLen:dataIdxFrom], uint16(protoNum))
	cpSize := copy(body[dataIdxFrom:], dateByte)
	num := DoPack(dataSlice, body, 1)
	n, _ := (*cn).Write(dataSlice[:num])
	printer.TextInfo("sendGateIpToRemoteSer", "sendNum", n, ",sendDataSlice", string(dataSlice[:num]), ", LinkerAddress", (*cn).RemoteAddr().String(), "BodySize", cpSize, "linkerName", linkerName)
}
