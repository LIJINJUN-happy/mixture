package server_common

import (
	"errors"
	"mixture/src/common/printer"
	"net"
	"sync"
	"sync/atomic"
)

// 网关连接到服务的连接结构体定义
type SerConnObj struct {
	gateId         atomic.Int32  //网关ID，-1表示尚未握手；原子读写避免握手与关闭并发访问
	gateAddr       string        //连接过来的网关的IP地址
	gateSerConn    *net.Conn     //和gateway_ser连接的connection
	sendChannel    chan *[]byte  //发送数据的通信道(由transmit协程发数据回GateWay)
	receiveChannel chan *[]byte  //接收数据的通信道（接收后存入该管道,由dispose协程处理）
	closeMux       sync.Mutex    //保护关闭状态，使检查状态并入队与关闭操作互斥
	closed         bool          //是否已停止接受新数据，只能在持有closeMux时访问
	closeSign      chan struct{} //关闭通知，由Close关闭，通知发送和处理协程退出
	closeOnce      sync.Once     //保证关闭通知和socket关闭只执行一次
}

// Init 初始化一个新连接对象；调用后调用方不得再给 *conn 赋值。
func (s *SerConnObj) Init(conn *net.Conn) error {
	if conn == nil || *conn == nil {
		return errors.New("SerConnObj.Init: conn is nil")
	}
	s.closeSign = make(chan struct{})
	s.gateId.Store(-1)
	s.gateSerConn = conn
	s.gateAddr = (*conn).RemoteAddr().String()
	s.sendChannel = make(chan *[]byte, sendChannelSize)
	s.receiveChannel = make(chan *[]byte, receiveChannelSize)
	return nil
}

func (s *SerConnObj) GetGateId() int {
	return int(s.gateId.Load())
}

func (s *SerConnObj) SetGateId(id int) {
	s.gateId.Store(int32(id))
}

func (s *SerConnObj) GetSendChannel() <-chan *[]byte {
	return s.sendChannel
}

func (s *SerConnObj) GetReceiveChannel() <-chan *[]byte {
	return s.receiveChannel
}

func (s *SerConnObj) GetGateSerConn() *net.Conn {
	return s.gateSerConn
}

func (s *SerConnObj) GetGateIPAddr() string {
	return s.gateAddr
}

func (s *SerConnObj) Close(funcCmd string) {
	s.closeOnce.Do(func() {
		s.closeMux.Lock()
		s.closed = true
		close(s.closeSign)
		s.closeMux.Unlock()
		if s.gateSerConn != nil {
			if closeError := (*s.gateSerConn).Close(); closeError != nil {
				printer.TextError("GateSerConn Close Fail", "Close Error", closeError.Error(), "funcCmd", funcCmd)
			}
		}
	})
}

// TrySend 成功时将缓冲区交给连接，失败时仍由调用方回收。
func (s *SerConnObj) TrySend(buf *[]byte) bool {
	return s.TryEnqueue(s.sendChannel, buf)
}

// TryEnqueue 在关闭锁保护下向指定队列非阻塞入队，失败时由调用方回收缓冲区。
func (s *SerConnObj) TryEnqueue(ch chan *[]byte, buf *[]byte) bool {
	s.closeMux.Lock()
	defer s.closeMux.Unlock()
	if s.closed || buf == nil {
		return false
	}
	select {
	case ch <- buf:
		return true
	default:
		return false
	}
}

// 消费协程在Close禁止所有生产者入队后，回收自己队列中的剩余缓冲区。
func DrainConnQueue(ch <-chan *[]byte, pool *sync.Pool) {
	for {
		select {
		case buf := <-ch:
			*buf = (*buf)[:cap(*buf)]
			pool.Put(buf)
		default:
			return
		}
	}
}
