package gateway_server

import (
	"mixture/src/common/printer"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

func BeginTcpSer() {
	address := strings.Join([]string{GateInfo.ip + ":", ""}, GateInfo.port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic("Listen Address Error: " + address)
	}

	defer func(l *net.Listener) {
		errClose := (*l).Close()
		if errClose != nil {
			printer.TextError("listener Close Fail", "Error", errClose.Error())
		}
	}(&listener)

	printer.TextInfo(strings.Join([]string{"Listening: ", " ....."}, address))
	for {
		conn, acceptErr := listener.Accept() //使用Accept方法，等待用户连接，如果没有请求就一直卡在这里
		if acceptErr != nil {
			printer.TextError("Accept Connect Error", "acceptErr", acceptErr.Error())
		} else {
			//白名单过滤
			if !WhiteListPtr.CheckWhiteIP(conn.RemoteAddr().String()) {
				conn.Close()
				continue
			}

			printer.TextDebug("Accept Connect Success", "conn", conn)
			if !ReserveClientConnection() {
				closeError := conn.Close()
				if closeError != nil {
					printer.TextDebug("conn.Close() Error With BeginTcpSer Accept", closeError.Error())
				}
				printer.TextDebug("Too Many Client....")
				continue
			}

			//当有客户端连接进来就创一个newConnection对象,并创建一个goroutine来执行客户端请求和回应
			newConn := new(NewConnection)
			newConn.Init(GateInfo.streamBufferDealWay, &conn, nil, "tcp")
			newConn.slotReserved = true

			//每个Actor连接读写分离
			newConn.AddIntoGateMap()
			go DoWriteMsg(newConn)
			go DoReadMsg(newConn)
		}
	}

}

func DoWriteMsg(newConn *NewConnection) {
	clientWriteGoroutineNum.Add(1)
	printer.TextDebug("clientWriteGoroutineNum current value", "clientWriteGoroutineNum", clientWriteGoroutineNum.Load())

	var current *[]byte
	defer func() {
		newConn.Close()
		if current != nil {
			*current = (*current)[:cap(*current)]
			DistributeDataPool.Put(current)
		}
		newConn.drainWriteQueue()
		clientWriteGoroutineNum.Add(-1) //写协程数量同步变动减少
	}()

	sendBuffer := make([]byte, ReadStreamBufferSize)
	for {
		select {
		case <-newConn.closeSign:
			return
		case bufPtr := <-newConn.writeChan:
			current = bufPtr
			num := DoPack(sendBuffer, (*bufPtr)[SessionIdLen:], newConn.StreamBuffer.readWay) //发送给客户端前需要把SessionID前缀去掉
			if num > 0 {
				sendByteNum, writeError := (*newConn).Write(sendBuffer[:num])
				if writeError != nil {
					printer.TextError("Write Error", "err", writeError.Error())
					return
				} else {
					printer.TextDebug("Write Success", "Send Byte Size", strconv.Itoa(sendByteNum))
				}
			}
			// 恢复容量后归还池，原始指针直接 Put，零逃逸
			*bufPtr = (*bufPtr)[:cap(*bufPtr)]
			DistributeDataPool.Put(bufPtr)
			current = nil
		}
	}
}

func DoReadMsg(newConn *NewConnection) {
	clientReadGoroutineNum.Add(1)
	printer.TextDebug("clientReadGoroutineNum current value", "clientReadGoroutineNum", clientReadGoroutineNum.Load())

	defer func() {
		if r := recover(); r != nil {
			printer.TextError("GateInfo.newConnPanic", "Recover", r, "Stack Trace", string(debug.Stack()))
		}
		(*newConn).Close()             //关闭套接字（关闭conn对象）
		clientReadGoroutineNum.Add(-1) //读协程数量同步变动减少
	}()

	sliceBuffer := make([]byte, int(MaxPacketBodySize)+ConnReadBufSize) //上一包残留 + 本次Read
	var cutList CutIndexList
	for {
		waitingFirstPacket := !newConn.firstPacketReceived
		dLineTime := time.Now().Add(ConnReadTimeout)
		if waitingFirstPacket {
			// 假如没收到过完整的协议包,设置超时时间，防止恶意长期连接不发送协议占用资源
			dLineTime = newConn.firstPacketDeadline
		}
		err := (*(newConn.conn)).SetReadDeadline(dLineTime)
		if err != nil {
			printer.TextError("Socket SetReadDeadline", "Error", err.Error())
		}

		cutList.Reset()
		num, _, readError := (*newConn).Read()
		if readError != nil {
			if checkRes, descString := CheckIsCloseByRemote(readError); checkRes {
				printer.TextInfo("Conn Close With Client", "descString", descString)
			} else if CheckIsReadTimeOut(readError) && waitingFirstPacket {
				printer.TextInfo("First Packet Timeout", "RemoteAddr", (*newConn).RemoteAddr().String())
			} else if CheckIsReadTimeOut(readError) {
				printer.TextInfo("Read Timeout", "RemoteAddr", (*newConn).RemoteAddr().String())
			} else {
				printer.TextError("Read ErrorMsg", "err", readError)
			}
			return
		} else {
			//处理接收到的数据
			printer.TextDebug("doReadMsg", "num", num, "message", string(newConn.ReadBuf[:num]))
			if !DealBufferData(newConn, num, &cutList, sliceBuffer) {
				printer.TextWarn("DoReadMsg", "Illegal packet, closing connection", (*newConn).RemoteAddr().String())
				return
			}
			//判断是否成功切出第一个包
			if waitingFirstPacket && cutList.Len() > 0 {
				newConn.firstPacketReceived = true
			}
		}
	}
}
