package server_common

import (
	"database/sql"
	"mixture/src/common/printer"
	"mixture/src/database"
	"net"
	"sync"
)

const sendChannelSize = 1024
const receiveChannelSize = 1024

// ServicePrt 通用服务指针（创建服务器结构体对象后赋值其地址给该指针）
var ServicePrt *ServerInfo

// ServerInfo 服务结构体（包含服务定义的所有信息）
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

// SerConnObj 网关连接到服务的连接结构体定义
type SerConnObj struct {
	gateId         int       //连接过来的网关的ID标识
	gateAddr       string    //连接过来的网关的IP地址
	gateSerConn    *net.Conn //和gateway_ser连接的connection
	sendChannel    chan any  //发送数据的通信道(由transmit协程发数据回GateWay)
	receiveChannel chan any  //接收数据的通信道（接收后存入该管道,由dispose协程处理）
}

func (s *SerConnObj) Init(conn *net.Conn) (bool, string) {
	if conn == nil {
		return false, "conn Can't Be Nil"
	}
	s.gateId = -1
	s.gateSerConn = conn
	s.gateAddr = (*conn).RemoteAddr().String()
	s.sendChannel = make(chan any, sendChannelSize)
	s.receiveChannel = make(chan any, receiveChannelSize)
	return true, ""
}

func (s *SerConnObj) GetGateId() int {
	return s.gateId
}

func (s *SerConnObj) SetGateId(id int) {
	s.gateId = id
}

func (s *SerConnObj) GetSendChannel() chan any {
	return s.sendChannel
}

func (s *SerConnObj) GetReceiveChannel() chan any {
	return s.receiveChannel
}

func (s *SerConnObj) GetGateSerConn() *net.Conn {
	return s.gateSerConn
}

func (s *SerConnObj) GetGateIPAddr() string {
	return s.gateAddr
}

func (s *SerConnObj) Close(funcCmd string) {
	if s.gateSerConn != nil {
		if closeError := (*s.gateSerConn).Close(); closeError != nil {
			printer.TextError("GateSerConn Close Fail", "Close Error", closeError.Error(), "funcCmd", funcCmd)
		}
		s.gateSerConn = nil
	}
}
