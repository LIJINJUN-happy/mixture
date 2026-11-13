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

	BeginGateServer()
	BeginLinkServer(WebSerLinker)
	BeginLinkServer(ActorSerLinker)
	BeginLinkServer(ModularizedSerLinker)
	BeginLinkServer(InteractiveSerLinker)

	Looping()
}

func DoInit(db *sql.DB) error {
	defer func(sq *sql.DB) {
		closeError := sq.Close() //调用db.Close() 函数关闭数据库
		if closeError != nil {
			printer.TextError("DoInit", "SqlLite Database Close Failed", closeError)
		}
	}(db)

	//白名单初始化逻辑
	WhiteListPtr.whiteMux.Lock()
	WhiteListPtr.whiteMap = make(map[string]string)
	WhiteListPtr.whiteMux.Unlock()
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
		return fmt.Errorf("whiteIPListDefaultStatus Error Status %v", whiteStatus)
	}
	printer.TextInfo("DoInit", "WhiteList Init Map Info", WhiteListPtr.whiteMap, "Active", WhiteListPtr.IsActive)

	GateInfo.sessionIdDistributionRecord.Store(0)
	GateInfo.rebuildListenSec = 3
	GateInfo.rebuildListenTimes.Store(0)
	GateInfo.maxConnSize = MaxConnectClientNum
	GateInfo.gateId, _ = strconv.Atoi(os.Args[2])
	address, errAddr := database.GetGateIPAddressById(GateInfo.gateId, db)
	if errAddr != nil {
		return errAddr
	}
	GateInfo.ip = address

	port, errPort := database.GetGatePortNumById(GateInfo.gateId, db)
	if errPort != nil {
		return errPort
	}
	GateInfo.port = strconv.Itoa(port)
	workMode := tools.GetSysValueByKey("workMode")
	GateInfo.workMode = workMode

	protocolType := tools.GetSysValueByKey("protocolType")
	GateInfo.protocol = protocolType

	way := tools.GetSysValueByKey("streamBufferDealWay")
	if wayNum, errWay := strconv.Atoi(way); errWay != nil {
		return errWay
	} else {
		if wayNum < 1 || wayNum > 3 {
			return fmt.Errorf("streamBufferDealWay必须为1、2或3,当前值为%d", wayNum)
		}
		GateInfo.streamBufferDealWay = uint8(wayNum)
	}

	//查找interactiveSer的ID标识
	interactiveSerId, errIsId := database.GetInteractiveSerIdByGateId(GateInfo.gateId, db)
	if errIsId != nil {
		return errIsId
	}

	//根据执行参数选择网络模式以及使用的协议方式
	checkWorkMode := GateInfo.workMode == "tcp" || GateInfo.workMode == "ws"
	checkProtocol := GateInfo.protocol == "json" || GateInfo.protocol == "protobuf"
	if !checkWorkMode {
		return fmt.Errorf("Invalid WorkMode %q, Expected tcp or ws", GateInfo.workMode)
	}
	if !checkProtocol {
		return fmt.Errorf("Invalid Protocol %q, Expected json or protobuf", GateInfo.protocol)
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
		var dial net.Conn
		stop := make(chan struct{})  //本次连接的退出通知，不占用业务队列
		var writerDone chan struct{} //旧写协程退出后才允许重连
		defer func() {
			close(stop)
			if dial != nil {
				dial.Close()
			}
			if writerDone != nil {
				<-writerDone
			}
			BeginLinkServer(serLinker)
		}()
		time.Sleep(GateInfo.rebuildListenSec * time.Second)

		for {
			var err error
			if dial, err = net.Dial("tcp", serLinker.address); err != nil {
				printer.TextError("Link Fail", "LinkerName", serLinker.linkerName, "Error", err.Error())
				time.Sleep(GateInfo.rebuildListenSec * time.Second)
				continue //链接失败再重新尝试,不成功就不创建dealWithLinkerData协程,避免协程泄露
			}
			printer.TextInfo("Link Success", "LinkerName", serLinker.linkerName)
			break
		}

		writerDone = make(chan struct{})
		go func() {
			defer close(writerDone)
			DealWithLinkerData(serLinker, dial, stop)
		}()

		//读取字节流所需的结构对象以及容器
		rsbObject := new(ReadStreamBufferObject)
		rsbObject.Init(ReadStreamBufferObjectWay)
		serData := make([]byte, ConnReadBufSize)
		sliceBuffer := make([]byte, int(MaxPacketBodySize)+ConnReadBufSize) //上一包残留 + 本次Read
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
			}
			//printer.TextInfo("Get Data ", "From ", serLinker.linkerName, "serData", serData[:readNum])
			if !DoSplit(rsbObject, serData, readNum, &cutList, sliceBuffer) {
				printer.TextWarn("BeginLinkServer", "Illegal packet, closing linker", serLinker.linkerName)
				return
			}

			//派发到对应的通信道中(来自其他服务所以sourceSessionID为0，标识不是来自客户端发送来的协议数据)
			DoDistribute(&cutList, sliceBuffer, 0)
		}
	}(sl)
}

// 处理连接器中的Channel收到的数据
// 每个写协程绑定本次socket，退出完成后才启动下一次连接。
func DealWithLinkerData(linker *SerLinker, conn net.Conn, stop <-chan struct{}) {
	var current *[]byte
	defer func() {
		// 写失败时关闭本次socket，唤醒读协程并触发重连。
		conn.Close()
		if current != nil {
			*current = (*current)[:cap(*current)]
			DistributeDataPool.Put(current)
		}
		if r := recover(); r != nil {
			printer.TextError("DealWithLinkerData Panic", "Error", r)
		}
	}()

	SendGateIDToRemoteSer(&conn, linker.linkerName)
	dataSlice := make([]byte, ReadStreamBufferSize)
	for {
		select {
		case <-stop:
			return //外层 select：等待数据时，也能收到停止通知。
		case bufPtr := <-linker.receiveChannel:
			if bufPtr == nil {
				return
			}
			current = bufPtr
			if num := DoPack(dataSlice, *bufPtr, ReadStreamBufferObjectWay); num > 0 {
				n, err := conn.Write(dataSlice[:num])
				if err == nil && n != num {
					err = io.ErrShortWrite
				}
				if err != nil {
					printer.TextError("GateWay DialObject Write Fail", "writeErr", err)
					return
				}
			}
			*bufPtr = (*bufPtr)[:cap(*bufPtr)]
			DistributeDataPool.Put(bufPtr)
			current = nil
		}
	}
}

/*使用defer搭配递归解决监听线程的崩溃*/
func BeginGateServer() {
	if GateInfo.rebuildListenTimes.Load() >= maxGateRebuildListenTimes {
		printer.TextError("Rebuild ListenTimes Exceeded.")
		os.Exit(101)
	}
	printer.TextInfo("Launching GatewayServer")
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
	intervalLoopingTime := 30 * time.Second
	loopTimer := time.NewTimer(intervalLoopingTime)
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
			loopTimer.Reset(intervalLoopingTime)
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
		}
		dataSize = copy(dateByte, protoBufData)
	} else {
		jsStruct := interior_json.GateIdSyncInfo{SerName: linkerName, GateId: int32(GateInfo.gateId)}
		jsonData, errorGateInfo := json.Marshal(jsStruct)
		if errorGateInfo != nil {
			printer.TextError("SendGateIDToRemoteSer", "json.Marshal Error", errorGateInfo.Error())
			panic(errorGateInfo)
		}
		dataSize = copy(dateByte, jsonData)
	}

	//打包数据发送给对应服务进程
	dateByte = dateByte[:dataSize]
	protoNum := 0 //默认通知服务标识网关的协议号指定为0
	dataIdxFrom := SessionIdLen + ProtoNumLen
	dataSlice := make([]byte, len(dateByte)+HeadLen+dataIdxFrom)
	body := make([]byte, len(dateByte)+dataIdxFrom)
	binary.BigEndian.PutUint32(body[0:SessionIdLen], uint32(0))
	binary.BigEndian.PutUint16(body[SessionIdLen:dataIdxFrom], uint16(protoNum))
	cpSize := copy(body[dataIdxFrom:], dateByte)
	num := DoPack(dataSlice, body, ReadStreamBufferObjectWay)
	n, err := (*cn).Write(dataSlice[:num])
	if err != nil {
		panic(err)
	}
	printer.TextInfo("SendGateIDToRemoteSer", "WriteDataSize", n, "LinkerAddress", (*cn).RemoteAddr().String(), "DataPackageSize", cpSize, "LinkerName", linkerName)
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
