package server_common

import (
	"mixture/src/common/printer"
	"net"
)

// 网关连接到服务的连接结构体定义
type SerConnObj struct {
	gateId         int       //连接过来的网关的ID标识
	gateAddr       string    //连接过来的网关的IP地址
	gateSerConn    *net.Conn //和gateway_ser连接的connection
	sendChannel    chan *[]byte //发送数据的通信道(由transmit协程发数据回GateWay)
	receiveChannel chan any  //接收数据的通信道（接收后存入该管道,由dispose协程处理）
}

func (s *SerConnObj) Init(conn *net.Conn) (bool, string) {
	if conn == nil {
		return false, "conn Can't Be Nil"
	}
	s.gateId = -1
	s.gateSerConn = conn
	s.gateAddr = (*conn).RemoteAddr().String()
	s.sendChannel = make(chan *[]byte, sendChannelSize)
	s.receiveChannel = make(chan any, receiveChannelSize)
	return true, ""
}

func (s *SerConnObj) GetGateId() int {
	return s.gateId
}

func (s *SerConnObj) SetGateId(id int) {
	s.gateId = id
}

func (s *SerConnObj) GetSendChannel() chan *[]byte {
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
