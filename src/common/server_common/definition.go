package server_common

import (
	"container/list"
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
	mux                 sync.RWMutex                       //链表读写锁(防止多个协程同时修改connObjList)
	connObjList         *list.List                         //用于存放SerConnObj对象指针的链表
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
	s.connObjList = list.New()
	s.connObjList = s.connObjList.Init()
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
	for iterator := s.connObjList.Front(); iterator != nil; iterator = iterator.Next() {
		element := iterator.Value.(*SerConnObj)
		if element.GetGateId() == gateId {
			return true
		}
	}
	return false
}

func (s *ServerInfo) GetConnObj(gateID int) *SerConnObj {
	s.mux.RLock()
	defer s.mux.RUnlock() //获取只需要添加读锁即可
	for iterator := s.connObjList.Front(); iterator != nil; iterator = iterator.Next() {
		element := iterator.Value.(*SerConnObj)
		if element.GetGateId() == gateID {
			return element
		}
	}
	return nil
}

func (s *ServerInfo) AddConnObj(c *SerConnObj) {
	remoteIp := (*c.gateSerConn).RemoteAddr().String()
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.connObjList.PushBack(c) != nil {
		printer.TextInfo("AddConnObj New Conn Success", "RemoteAddr", remoteIp, "ServerName", s.ServerName, "ServerId", s.ServerId)
	}
}

// DeleteConnObj 移除来自gateWay的连接对象
func (s *ServerInfo) DeleteConnObj(gateId int) {
	s.mux.Lock()
	defer s.mux.Unlock()
	for iterator := s.connObjList.Front(); iterator != nil; iterator = iterator.Next() {
		element := iterator.Value.(*SerConnObj)
		if element.GetGateId() == gateId {
			//关闭通信道
			close(element.sendChannel)
			close(element.receiveChannel)

			//关闭socket, 并从连接链中移除连接对象
			if element.gateSerConn != nil {
				closeError := (*element.gateSerConn).Close()
				if closeError != nil {
					printer.TextError("gateSerConn Close Fail With DeleteConnObj", "Close Error", closeError.Error())
				}
				element.gateSerConn = nil
			}
			s.connObjList.Remove(iterator)
			printer.TextInfo("DeleteConnObj Success", "RemoteGateId", gateId, "ServerName", s.ServerName, "ServerId", s.ServerId, "After DeleteConnObj Size", s.connObjList.Len())
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
	gateId         int       //gateObj的ID（连接过来的网关的ID标识）
	sendChannel    chan any  //发送数据的通信道(由transmit协程发数据回GateWay)
	receiveChannel chan any  //接收数据的通信道（接收后存入该管道,由dispose协程处理）
	gateSerConn    *net.Conn //和gateway_ser连接的connection
}

func (s *SerConnObj) Init(conn *net.Conn) (bool, string) {
	if conn == nil {
		return false, "conn Can't Be Nil"
	}
	s.gateId = -1
	s.gateSerConn = conn
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
