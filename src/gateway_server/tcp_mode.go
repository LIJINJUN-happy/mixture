package gateway_server

import (
	"container/list"
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
			nowCount := clientReadGoroutineNum.Load()
			if nowCount >= GateInfo.maxConnSize {
				closeError := conn.Close()
				if closeError != nil {
					printer.TextDebug("conn.Close() Error With BeginTcpSer Accept", closeError.Error())
				}
				printer.TextDebug("Too Many Client....")
				continue
			}

			//当有客户端连接进来就创一个newConnection对象,并创建一个goroutine来执行客户端请求和回应
			newConn := new(NewConnection)
			newConn.Init(1, &conn, nil, "tcp")

			//每个Actor连接读写分离
			go DoWriteMsg(newConn)
			go DoReadMsg(newConn)
		}
	}

}

func DoWriteMsg(newConn *NewConnection) {
	clientWriteGoroutineNum.Add(1)
	printer.TextDebug("clientWriteGoroutineNum current value", "clientWriteGoroutineNum", clientWriteGoroutineNum.Load())

	defer func() {
		newConn.ReduceFromGateMap() //移除Map中的客户端元素
		(*newConn).Close()
		clientWriteGoroutineNum.Add(-1) //写协程数量同步变动减少
	}()

	writeTimer := time.NewTimer(time.Hour)
	defer writeTimer.Stop()

	sendBuffer := make([]byte, ReadStreamBufferSize)
	for {
		select {
		case SynFromReadGoroutine := <-newConn.rwChannel:
			if SynFromReadGoroutine == GoroutineSyncBreakSign {
				printer.TextDebug("收到" + SynFromReadGoroutine)
				return
			}
		case gameReturnData := <-newConn.writeChan:
			num := DoPack(sendBuffer, gameReturnData, newConn.StreamBuffer.readWay)
			if num > 0 {
				sendByteNum, writeError := (*newConn).Write(sendBuffer[:num])
				if writeError != nil {
					printer.TextError("Write Error", "err", writeError.Error())
					return
				} else {
					printer.TextDebug("Write Success", "Send Byte Size", strconv.Itoa(sendByteNum))
				}
			}
		case <-writeTimer.C:
			writeTimer.Stop()
			writeTimer.Reset(time.Hour)
		}
	}
}

func DoReadMsg(newConn *NewConnection) {
	clientReadGoroutineNum.Add(1)
	newConn.AddIntoGateMap(newConn.writeChan)
	printer.TextDebug("clientReadGoroutineNum current value", "clientReadGoroutineNum", clientReadGoroutineNum.Load())

	defer func() {
		newConn.rwChannel <- GoroutineSyncBreakSign
		close(newConn.rwChannel) //通知Write线程并关闭通知通道
		(*newConn).Close()       //关闭套接字（关闭conn对象）
		if r := recover(); r != nil {
			printer.TextError("GateInfo.newConnPanic", "Recover", r, "Stack Trace", string(debug.Stack()))
		}
		clientReadGoroutineNum.Add(-1) //读协程数量同步变动减少
	}()

	sliceBuffer := make([]byte, ReadStreamBufferSize)
	cutList := list.New()
	for {
		//设置下次读取的超时时间
		dLineTime := time.Now().Add(ConnReadTimeout)
		err := (*(newConn.conn)).SetReadDeadline(dLineTime)
		if err != nil {
			printer.TextError("Socket SetReadDeadline", "Error", err.Error())
		}

		cutList = cutList.Init()
		num, _, readError := (*newConn).Read()
		if readError != nil {
			if checkRes, descString := CheckIsCloseByRemote(readError); checkRes {
				printer.TextInfo("Conn Close With Client", "descString", descString)
			} else if CheckIsReadTimeOut(readError) {
				printer.TextInfo("Read Timeout", "RemoteAddr", (*newConn).RemoteAddr().String())
			} else {
				printer.TextError("Read Error: " + readError.Error())
			}
			return
		} else {
			//处理接收到的数据
			printer.TextDebug("doReadMsg", "num", num, "message", string(newConn.ReadBuf[:num]))
			DealBufferData(newConn, num, cutList, sliceBuffer)
		}
	}
}
