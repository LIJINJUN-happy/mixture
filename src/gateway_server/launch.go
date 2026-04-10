package gateway_server

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"mixture/proto/interior_json"
	"mixture/proto/interior_pb/ipb"
	"mixture/src/common/printer"
	"mixture/src/common/tools"
	"mixture/src/database"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Gateway service: used to listen to client requests, accept protocols, decompose protocols, and send protocols
func Run() {
	db, isOk := database.GetSQLiteConnDB()
	if isOk != nil {
		return
	}

	//Init Gate Config With DBObjectPtr
	if err := DoInit(db); err != nil {
		printer.TextError("Run", "DoInit Failed Err", err.Error())
		os.Exit(1)
	}

	//Start Server
	BeginGateServer()
	time.Sleep(2 * time.Second)
	BeginLinkServer(WebSerLinker)
	BeginLinkServer(ActorSerLinker)
	BeginLinkServer(ModularizedSerLinker)
	BeginLinkServer(InteractiveSerLinker)

	//Do Check With Loop
	Looping()
}

func DoInit(db *sql.DB) error {
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
		return fmt.Errorf("whiteIPListDefaultStatus Error Status &v", whiteStatus)
	}
	printer.TextInfo("doInit", "WhiteList Init Map Info", (*WhiteListPtr).WhiteMap, "Active", (*WhiteListPtr).IsActive)

	GateInfo.sessionIdDistributionRecord.Store(0)
	GateInfo.rebuildListenSec = 5
	GateInfo.rebuildListenTimes.Store(0)
	GateInfo.maxConnSize = MaxConnectClientNum
	GateInfo.gateId, _ = strconv.Atoi(os.Args[2])
	checkAddressRes, address := database.GetGateIPAddressById(GateInfo.gateId, db)
	if !checkAddressRes {
		return fmt.Errorf("GetGateIPAddressById Failed, GateId=%v", GateInfo.gateId)
	} else {
		GateInfo.ip = address
	}
	checkPortRes, port := database.GetGatePortNumById(GateInfo.gateId, db)
	if !checkPortRes {
		return fmt.Errorf("doInit", "GetGatePortNumById Failed, GateId=%v", GateInfo.gateId)
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
		return fmt.Errorf("checkInteractiveSerIdRes=%v, GateId=%v", checkInteractiveSerIdRes, GateInfo.gateId)
	}

	//根据执行参数选择网络模式以及使用的协议方式
	checkWorkMode := GateInfo.workMode == "tcp" || GateInfo.workMode == "ws"
	checkProtocol := GateInfo.protocol == "json" || GateInfo.protocol == "protobuf"
	if !checkWorkMode {
		return errors.New("Args Error With GateWay's WorkMode: " + GateInfo.workMode + " , Should Be 'tcp' or 'ws'")
	}
	if !checkProtocol {
		return errors.New("Args Error With GateWay's Protocol: " + GateInfo.protocol + " , Should Be 'json' or 'protobuf'")
	}

	//ActorSerLinker初始化
	ActorSerLinker.InitLinker("actorSer", db, GateInfo.gateId)

	//ModularizedSerLinker初始化
	ModularizedSerLinker.InitLinker("modularizedSer", db, GateInfo.gateId)

	//WebSerLinker初始化
	WebSerLinker.InitLinker("webSer", db, GateInfo.gateId)

	//InteractiveSerLinker初始化
	InteractiveSerLinker.InitLinker("interactiveSer", db, interactiveSerId)

	return nil
}

/*链接xxx服务进程,收发gatewaySer和xxxSer之间的数据信息*/
func BeginLinkServer(sl *SerLinker) {
	go func(serLinker *SerLinker) {
		defer func(s *SerLinker) {
			if s.serConn != nil {
				s.receiveChannel <- nil //通知停止处理(*dialObject).Write操作,因为panic重启该协程,会造成旧的write协程没被销毁从而协程泄露
				if closeErr := (*s.serConn).Close(); closeErr != nil {
					printer.TextError(s.linkerName, " dial.Close() Failed With Defer", closeErr)
				}
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
			printer.TextInfo("Link Success", "linkerName", serLinker.linkerName)
			serLinker.serConn = &dial
		}

		go DealWithLinkerData(serLinker)

		//读取字节流所需的结构对象以及容器
		rsbObject := &ReadStreamBufferObject{
			readWay:          ReadStreamBufferObjectWay,
			headReadArray:    make([]uint8, 0, HeadLen),
			totalNeedReadLen: 0,
			curReadLen:       0,
			byteData:         make([]byte, 0, ReadStreamBufferSize),
		}
		serData := make([]byte, ConnReadBufSize)
		sliceBuffer := make([]byte, ReadStreamBufferSize)
		var cutList CutIndexList

		//解析xxx服务进程发送来的数据(派发到对应进程的通信道中)
		for {
			cutList.Reset()
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
				DoSplit(rsbObject, serData, readNum, &cutList, sliceBuffer)

				//派发到对应的通信道中(来自其他服务所以sourceSessionID为0，标识不是来自客户端发送来的协议数据)
				DoDistribute(&cutList, sliceBuffer, 0)
			}
		}

	}(sl)
}

// 处理连接器中的Channel收到的数据
func DealWithLinkerData(linker *SerLinker) {
	defer func(s *SerLinker) {
		if r := recover(); r != nil {
			printer.TextError("DealWithLinkerData Panic", "Error", r)
			if s.serConn != nil {
				(*s.serConn).Close()
			}
		}
	}(linker)

	//连接上对应的服务进程后,发送本地gatewayID给予服务进程作为标识ID区分
	SendGateIDToRemoteSer(linker.serConn, linker.linkerName)

	dataSlice := make([]byte, ReadStreamBufferSize)
	for {
		select {
		case bufPtr := <-linker.receiveChannel:
			if bufPtr == nil {
				return // 关闭信号
			}
			num := DoPack(dataSlice, *bufPtr, ReadStreamBufferObjectWay) //默认内部转发用方式1,包头+包体
			if num > 0 {
				if _, writeErr := (*linker.serConn).Write(dataSlice[0:num]); writeErr != nil {
					printer.TextError("GateWay DialObject Write Fail", "writeErr", writeErr)
				} else {
					printer.TextDebug("GateWay DialObject Write Success", "writeNum", num, "dataSlice", string(dataSlice[0:num]))
				}
			}
			// 恢复容量后归还池，原始指针直接 Put，零逃逸
			*bufPtr = (*bufPtr)[:cap(*bufPtr)]
			DistributeDataPool.Put(bufPtr)
		}
	}
}

/*使用defer搭配递归解决监听线程的崩溃*/
func BeginGateServer() {
	if GateInfo.rebuildListenTimes.Load() >= maxGateRebuildListenTimes {
		printer.TextError("Rebuild ListenTimes Exceeded.")
		os.Exit(101)
	}
	printer.TextInfo("Begin Launching Server gateway_server")
	if GateInfo.workMode == "tcp" {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					GateInfo.rebuildListenTimes.Add(1)
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
					GateInfo.rebuildListenTimes.Add(1)
					time.Sleep(GateInfo.rebuildListenSec * time.Second)
					printer.TextWarn("重新启动WebSocket模式网关服务")
					go BeginGateServer()
				}
			}()
			BeginWebSocketSer()
		}()
	}
}

func Looping() {
	loopTimer := time.NewTimer(IntervalLoopingTime)
	defer loopTimer.Stop()

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
		case <-loopTimer.C:
			printer.TextInfo("ClientGoroutine Statistics", "Read-Goroutine", clientReadGoroutineNum.Load(), "Write-Goroutine", clientWriteGoroutineNum.Load())
			loopTimer.Stop()
			loopTimer.Reset(IntervalLoopingTime)
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
			printer.TextError("SendGateIDToRemoteSer", "proto.Marshal Error", errorGateInfo.Error())
			panic(errorGateInfo)
			return
		}
		dataSize = copy(dateByte, protoBufData)
	} else {
		jsStruct := interior_json.GateIdSyncInfo{SerName: linkerName, GateId: int32(GateInfo.gateId)}
		jsonData, errorGateInfo := json.Marshal(jsStruct)
		if errorGateInfo != nil {
			printer.TextError("SendGateIDToRemoteSer", "json.Marshal Error", errorGateInfo.Error())
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
	num := DoPack(dataSlice, body, ReadStreamBufferObjectWay)
	n, _ := (*cn).Write(dataSlice[:num])
	printer.TextInfo("SendGateIDToRemoteSer", "sendNum", n, "LinkerAddress", (*cn).RemoteAddr().String(), "PackSize", cpSize, "linkerName", linkerName)
}

func CheckIsCloseByRemote(readError error) (bool, string) {
	errString := readError.Error()
	if readError == io.EOF {
		return true, "Close Normal" //对端正常断开连接(socket)
	}
	if (strings.Index(errString, "websocket") > -1) && (strings.Index(errString, "EOF") > -1) {
		return true, "Close Normal" //对端正常断开连接(WebSocket)
	}
	if strings.Index(errString, "forcibly closed") > -1 {
		return true, "Close Forcibly" //异常断开
	}
	return false, ""
}

func CheckIsReadTimeOut(readError error) bool {
	theType := new(net.Error)
	switch {
	case errors.As(readError, theType):
		if (*theType).Timeout() {
			break
		}
	default:
		return false
	}
	return true
}
