package server_common

import (
	"container/list"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"mixture/proto/interior_json"
	"mixture/proto/interior_pb/ipb"
	"mixture/src/common/printer"
	"mixture/src/database"
	"mixture/src/gateway_server"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const sendChannelSize = 1024
const receiveChannelSize = 1024
const TransmitDisposeTickerDuration = time.Millisecond * 500

// 通用服务指针（创建服务器结构体对象后赋值其地址给该指针）
var ServicePrt *ServerInfo

// 服务结构体（包含服务定义的所有信息）
type ServerInfo struct {
	ServerName          string                             //服务名
	ServerId            int                                //服务ID标识
	port                int                                //服务进程的接收端口
	address             string                             //服务进程的接收地址
	listenObj           *net.Listener                      //监听对象
	mux                 sync.RWMutex                       //SerConnObj集合读写锁(防止多个协程同时修改connObjMap)
	connObjMap          map[int]*SerConnObj                //用于存放SerConnObj对象指针的集合
	DisposeFunc         func([]byte, string, int, uint32)  //提供一个接口来处理接收到来的数据,参数：协议数据,协议名字,发送信息过来的gateID,用户的SessionID（可以有可以没有）
	ServerHandleFuncMap map[string]func(data []byte) error //服务处理函数集
}

func (s *ServerInfo) InitInfo(serName string, id int) bool {
	isOk, db := database.GetSQLiteConnDB()
	if isOk != nil {
		return false
	}
	defer func(dataBase *sql.DB) {
		if dbCloseRes := db.Close(); dbCloseRes != nil {
			printer.TextError("dbCloseRes Is Fail", "dbCloseReason", dbCloseRes)
		}
	}(db)

	address := database.GetSerAddressById(id, db, serName)
	if address == "" {
		printer.TextError(serName, "Server Address Is Null, ServerId", id)
		return false
	} else {
		s.address = address
	}

	port := database.GetSerPortById(id, db, serName)
	if port <= 0 {
		printer.TextError(serName, "Server port Is Error, ServerId", port)
		return false
	} else {
		s.port = port
	}
	s.ServerName = serName
	s.ServerId = id
	s.listenObj = nil
	s.connObjMap = make(map[int]*SerConnObj)
	s.ServerHandleFuncMap = make(map[string]func(data []byte) error)
	ServicePrt = s
	return true
}

func (s *ServerInfo) SetListener(l *net.Listener) {
	s.listenObj = l
}

func (s *ServerInfo) GetListener() *net.Listener {
	return s.listenObj
}

func (s *ServerInfo) GetPort() int {
	return s.port
}

func (s *ServerInfo) GetAddress() string {
	return s.address
}

func (s *ServerInfo) CheckConnObjIsExist(gateId int) bool {
	s.mux.RLock()
	defer s.mux.RUnlock() //检测只需要添加读锁即可
	if obj, ok := s.connObjMap[gateId]; ok {
		printer.TextError("CheckConnObjIsExist", "Exist Obj", obj)
		return true
	}
	return false
}

func (s *ServerInfo) GetConnObj(gateID int) *SerConnObj {
	s.mux.RLock()
	defer s.mux.RUnlock() //获取只需要添加读锁即可
	if obj, ok := s.connObjMap[gateID]; ok {
		return obj
	}
	return nil
}

// 添加gate连接对象，并返回添加后集合最新长度
func (s *ServerInfo) AddConnObj(c *SerConnObj) int {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.connObjMap[c.gateId] = c
	printer.TextInfo("AddConnObj New Gate Connect Success", "RemoteAddr", (*c).GetGateIPAddr())
	return len(s.connObjMap)
}

// 移除来自gateWay的连接对象
func (s *ServerInfo) DeleteConnObj(gateAddr string, gateId int) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if obj, ok := s.connObjMap[gateId]; ok {
		if (obj.GetGateIPAddr() == gateAddr) && (obj.GetGateId() == gateId) {
			//关闭通信道
			close(obj.sendChannel)
			close(obj.receiveChannel)

			//关闭socket, 并从连接链中移除连接对象
			obj.Close("DeleteConnObj")
			delete(s.connObjMap, gateId)
			printer.TextInfo("DeleteConnObj", "RemoteGateId", gateId, "Addr", gateAddr, "After DeleteConnObj Size", len(s.connObjMap))
		}
	}
}

func (s *ServerInfo) SetDisposeFunc(f func([]byte, string, int, uint32)) {
	s.DisposeFunc = f
}

func (s *ServerInfo) GetDisposeFunc() func([]byte, string, int, uint32) {
	return s.DisposeFunc
}

// 注册服务监视逻辑，所有连接到该服务进程的Gate的协议数据包都通过f函数处理
func (s *ServerInfo) RegisterMonitorLogic(f func([]byte, string, int, uint32)) {
	serName := s.ServerName
	serId := s.ServerId
	port, address := strconv.Itoa(s.GetPort()), s.GetAddress()

	//开始监听后设置响应函数,以及监听对象指针
	var strBuilder strings.Builder
	addrLen, portLen := len(address), len(port)
	strBuilder.Grow(addrLen + portLen + 1)
	strBuilder.WriteString(address)
	strBuilder.WriteString(":")
	strBuilder.WriteString(port)
	ip := strBuilder.String()
	listener, err := net.Listen("tcp", ip[addrLen:]) //ip = ":yy"(默认监听本地)
	if err != nil {
		printer.TextError("Listen Address Error", "ServerName", serName, "ServerId", serId, "Ip", ip, "ErrMsg", err)
		return
	} else {
		s.SetDisposeFunc(f)
		s.SetListener(&listener)
	}

	defer func(l *net.Listener, name string, id int) {
		if r := recover(); r != nil {
			printer.PanicStack(r)
		}
		errClose := (*l).Close()
		if errClose != nil {
			printer.TextError("listener Close Fail", "errClose", errClose.Error(), "ServerName", name, "ServerId", id)
		}
		printer.TextInfo("ServerMonitor Down", "ServerName", name, "ServerId", id)

	}(&listener, serName, serId)

	//监听网关的连接请求(接受多个网关连接,每连接一个新网关就把新连接对象加入到连接链表中)
	for {
		newConn, errorWithAccept := listener.Accept()
		if errorWithAccept != nil {
			printer.TextError("Accept Connect Error", "Error", errorWithAccept.Error(), "ServerName", serName, "ServerId", serId)
			continue
		}

		//创建连接对象（网关连接过来成功后创建的对象）,并且加入连接列表里
		SerConnObjPtr := new(SerConnObj)
		if res, desc := SerConnObjPtr.Init(&newConn); !res {
			printer.TextError("Init New Conn Error", desc, "ServerName", serName, "ServerId", serId)
			continue
		} else {
			go s.BeginService(SerConnObjPtr)
		}
	}
}

// 启动服务
func (s *ServerInfo) BeginService(sCoon *SerConnObj) {
	defer func(si *ServerInfo, sObj *SerConnObj) {
		if r := recover(); r != nil {
			printer.PanicStack(r)
		}
		si.DeleteConnObj(sObj.gateAddr, sObj.GetGateId())
		printer.TextInfo("BeginService Goroutine End........")
	}(s, sCoon)

	go s.Transmit(sCoon)
	go s.Dispose(sCoon)
	time.Sleep(2 * time.Second)

	//读取网关发送过来的数据
	readObj := new(gateway_server.ReadStreamBufferObject)
	readObj.Init(1) //进程间默认用模式1分割字节数据
	readBuff, sliceBuffer := make([]byte, 255), make([]byte, gateway_server.ReadStreamBufferSize)
	cutList := list.New()
	idx := 0
	for {
		idx = 0
		num, readError := (*sCoon.gateSerConn).Read(readBuff)
		if readError != nil {
			if checkRes, descString := gateway_server.CheckIsCloseByRemote(readError); checkRes {
				printer.TextInfo("GateSerConn Close With Client", "desc", descString, "ServerName", s.ServerName)
			} else {
				printer.TextError("GateSerConn Read Error: ", "readError", readError.Error(), "ServerName", s.ServerName)
			}
			return
		}
		//printer.TextInfo("sCoon.gateSerConn Read", "ReadResNum", num)

		//把读取到的数据发送到处理逻辑通信道中,待注册函数做逻辑处理
		cutList = cutList.Init()
		gateway_server.DoSplit(readObj, readBuff[0:], num, cutList, sliceBuffer)
		for it := cutList.Front(); it != nil; it = it.Next() {
			elementIDX := it.Value.(int)
			newByte := make([]byte, elementIDX-idx)
			copyRes := copy(newByte, sliceBuffer[idx:elementIDX])
			//printer.TextInfo("gateway_server DoSplit ", "CopyResNum", copyRes)
			if copyRes > 0 {
				sCoon.receiveChannel <- newByte
			}
			idx = elementIDX
		}
	}
}

func (s *ServerInfo) Transmit(sCoon *SerConnObj) {
	defer func(s *SerConnObj) {
		if r := recover(); r != nil {
			printer.PanicStack(r)
		}
		s.Close("Transmit")
		printer.TextInfo("Transmit Goroutine End........")
	}(sCoon)

	//创建定时器检测gate连接对象是否断开
	loopTicker := time.NewTicker(TransmitDisposeTickerDuration)
	defer loopTicker.Stop() //退出关闭定时器

	dataSlice := make([]byte, gateway_server.ReadStreamBufferSize)
	for {
		select {
		case transmitData := <-sCoon.sendChannel:
			if sCoon.gateSerConn != nil {
				if trueData, conversionResult := transmitData.([]byte); conversionResult {
					if num := gateway_server.DoPack(dataSlice, trueData, 1); num > 0 {
						_, writeErr := (*sCoon.gateSerConn).Write(dataSlice[0:num])
						if writeErr != nil {
							printer.TextError("DialObject Write Fail", "writeErr", writeErr.Error(), "ServerName", s.ServerName, "ServerId", s.ServerId)
							return
						}
						//printer.TextDebug("DialObject Write Success ", "writeNum", writeNum, "dataSlice", string(dataSlice[0:writeNum]))
					}
				}
			}
		case <-loopTicker.C:
			if sCoon.gateSerConn == nil {
				return
			}
		}
	}
}

func (s *ServerInfo) Dispose(sCoon *SerConnObj) {
	defer func(s *SerConnObj) {
		if r := recover(); r != nil {
			printer.PanicStack(r)
		}
		s.Close("Dispose")
		printer.TextInfo("Dispose Goroutine End........")
	}(sCoon)

	disposeTicker := time.NewTicker(TransmitDisposeTickerDuration)
	defer disposeTicker.Stop() //退出暂停定时器

	//收取通信道传输过来切割好的数据包内容
	dataIdxFrom := gateway_server.ProtoNumLen + gateway_server.SessionIdLen
	fun := s.GetDisposeFunc()
	for {
		select {
		case receiveData := <-sCoon.receiveChannel:
			dataSlice, conversionResult := receiveData.([]byte)
			if !conversionResult {
				//feedback := fmt.Sprintf("Ser:%v-%v, dispose receiveData conversion []byte Fail:%v", sInfo.ServerName, sInfo.ServerId, receiveData)
				//printer.PanicStack(feedback)
				break
			}
			sessionId := binary.BigEndian.Uint32(dataSlice)
			protoNum := binary.BigEndian.Uint16(dataSlice[gateway_server.SessionIdLen:])
			//printer.TextInfo("dispose receiveData", "receiveData := <-sCoon.receiveChannel", dataSlice, "sessionId", sessionId)
			if (protoNum > 0) && (sCoon.gateId > 0) {
				//已经标识了对应的网关并识别到对应的协议号后，直接调用协议处理函数
				protoName := gateway_server.ProtocolDistributeInfo.ProtoNumMap[protoNum]
				fun(dataSlice[dataIdxFrom:], protoName, sCoon.gateId, sessionId)
			} else if (protoNum == 0) && (sCoon.gateId < 0) {
				gateID := -1
				linkerName := ""
				if gateway_server.ProtocolDistributeInfo.ProtocolType == "protobuf" {
					gatePbStruct := new(ipb.GateIdSyncInfo)
					errUnmarshal := proto.Unmarshal(dataSlice[dataIdxFrom:], gatePbStruct)
					if errUnmarshal != nil {
						printer.PanicStack(errUnmarshal)
						break
					}
					gateID = int(gatePbStruct.GateId)
					linkerName = gatePbStruct.SerName
				} else {
					jsStruct := new(interior_json.GateIdSyncInfo)
					errUnmarshal := json.Unmarshal(dataSlice[dataIdxFrom:], jsStruct)
					if errUnmarshal != nil {
						printer.PanicStack(errUnmarshal)
						break
					}
					gateID = int(jsStruct.GateId)
					linkerName = jsStruct.SerName
				}

				//检测一下是否存已存在对应的网关ID标识，不可重复，需要保证唯一ID标识
				if !s.CheckConnObjIsExist(gateID) {
					sCoon.SetGateId(gateID) //设置网关标识
					curGateSize := s.AddConnObj(sCoon)
					printer.TextInfo("Dispose SYN GateId", "From GateLinker", linkerName, "GateID", gateID, "ConnObjMapSize", curGateSize)
				} else {
					//否者直接断开连接
					ptr := sCoon.gateSerConn
					sCoon.gateSerConn = nil
					(*ptr).Close()
					printer.TextError("Dispose SYN GateId", "From GateLinker", linkerName, "Already Exist GateID", gateID)
				}
			}
		case <-disposeTicker.C: //由于dispose协程内无法根据read或者write的报错触发error，只能根据conn是否被设为空来销毁协程
			if sCoon.gateSerConn == nil {
				return
			}
		}
	}
}

// 注册进程服务的响应函数（非进程内的模块）
func (s *ServerInfo) RegisterServerHandleFunc(protocol string, handleFunc func(data []byte) error) {
	if _, ok := s.ServerHandleFuncMap[protocol]; ok {
		printer.TextError("RegisterServerHandleFunc Fail", "Multiple Registration Func Name", protocol)
		return
	} else {
		s.ServerHandleFuncMap[protocol] = handleFunc
	}
}

// 尝试调用进程服务响应函数方法
func (s *ServerInfo) TryCallServerFunc(data []byte, proName string) {
	if serverFunc, searchRes := s.ServerHandleFuncMap[proName]; searchRes {
		err := serverFunc(data)
		if err != nil {
			printer.TextError("ServerHandleFuncMap Err", "proName", proName, "ErrString", err.Error())
		}
	}
}

// 打包协议数据到对应的模块通道或者是执行服务处理逻辑
func TranspondProtoPack(data []byte, proName string, gateId int, sessionID uint32) {
	//printer.TextInfo("TranspondProtoPack", "proName", proName, "gateId", gateId, "sessionID", sessionID, "data", string(data))
	if modChan, ok := ProtoName2ModMap[proName]; ok {
		info := new(ProtoPackInfo)
		info.Data = data
		info.GateId = gateId
		info.ProName = proName
		info.SessionID = sessionID
		modChan <- info
		return
	}

	//尝试执行是否为服务函数（非模块函数）
	ServicePrt.TryCallServerFunc(data, proName)
}

// 该协议用于发送服务中的协议返回gate进程处理,参数①发送目标网关的ID,②sessionId(非发送给客户端可以为0否者必须指定sessionID),③协议名字,④数据内容
func DeliveryMsg(gateID int, sessionId uint32, protoName string, data []byte) {
	protoNum, searchRes := gateway_server.ProtocolDistributeInfo.ProtoNameMap[protoName]
	if !searchRes {
		printer.TextError("ProtoNameMap Search Fail", "protoName", protoName)
		return
	}

	if ServicePrt == nil {
		printer.TextError("ServicePrt Is Null")
		return
	}
	if gateConn, dataLen := ServicePrt.GetConnObj(gateID), len(data); gateConn != nil {
		msgTotalSize := gateway_server.SessionIdLen + gateway_server.ProtoNumLen + dataLen //数据包结构=[session][协议号][数据内容]
		sendDataBuf := make([]byte, msgTotalSize)
		binary.BigEndian.PutUint32(sendDataBuf[:gateway_server.SessionIdLen], sessionId)
		binary.BigEndian.PutUint16(sendDataBuf[gateway_server.SessionIdLen:gateway_server.SessionIdLen+gateway_server.ProtoNumLen], protoNum)
		if n := copy(sendDataBuf[gateway_server.SessionIdLen+gateway_server.ProtoNumLen:], data); n == dataLen {
			gateConn.GetSendChannel() <- sendDataBuf
		} else {
			printer.TextError("copy sendDataBuf Not Same, copy result size = "+strconv.Itoa(n)+" dataLen =", strconv.Itoa(dataLen))
			sendDataBuf = nil
		}
	} else {
		printer.TextError("GetConnObj Fail", "GateID", gateID)
	}
}
