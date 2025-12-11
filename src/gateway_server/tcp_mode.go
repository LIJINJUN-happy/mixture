package gateway_server

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"github.com/gorilla/websocket"
	"io"
	"mixture/src/common/printer"
	"net"
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

			// 当有客户端连接进来就创一个newConnection对象,并创建一个goroutine来执行客户端请求和回应
			newConn := CreateNewConnObject(1, &conn, nil, "tcp")
			DealConnect(newConn)
		}
	}

}

// 读写分离
func DealConnect(newConn *NewConnection) {
	go func() {
		go DoWriteMsg(newConn)
		DoReadMsg(newConn)
	}()
}

func DoWriteMsg(newConn *NewConnection) {
	clientWriteGoroutineNum.Add(1)
	printer.TextDebug("clientWriteGoroutineNum current value", "clientWriteGoroutineNum", clientWriteGoroutineNum.Load())

	defer func() {
		ReduceClientToGateMap(newConn) //移除Map中的客户端元素
		(*newConn).Close()
		clientWriteGoroutineNum.Add(-1) //写协程数量同步变动减少
	}()

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
		case <-time.After(time.Hour):
		}
	}
}

func DoReadMsg(newConn *NewConnection) {
	clientReadGoroutineNum.Add(1)
	AddClientToGateMap(newConn, newConn.writeChan)
	printer.TextDebug("clientReadGoroutineNum current value", "clientReadGoroutineNum", clientReadGoroutineNum.Load())

	defer func() {
		newConn.rwChannel <- GoroutineSyncBreakSign
		close(newConn.rwChannel) //通知Write线程并关闭通知通道
		(*newConn).Close()       //关闭套接字（关闭conn对象）
		if r := recover(); r != nil {
			printer.TextError("GateInfo.newConnPanic", "Recover", r)
		}
		clientReadGoroutineNum.Add(-1) //读协程数量同步变动减少
	}()

	sliceBuffer := make([]byte, ReadStreamBufferSize)
	cutList := list.New()
	for {
		//置下次读取的超时时间
		dLineTime := time.Now().Add(ConnectionInfo.connReadTimeout)
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

func DealBufferData(newConn *NewConnection, readNum int, l *list.List, sliceBuffer []byte) {
	DoSplit(newConn.StreamBuffer, newConn.ReadBuf, readNum, l, sliceBuffer)
	DoDistribute(l, sliceBuffer[0:], newConn.connSession)
}

// DoSplit /* 三种读取字节流切割方式： 1.根据字节长度来读取数据   2.根据标记来切割字节流  3定长读取，每个包都是固定长度*/
func DoSplit(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *list.List, sliceBuffer []byte) {
	switch j.readWay {
	case 1:
		DoSplitByReadHeadLen(j, byteSlice, readNum, list, sliceBuffer, 0)
	case 2:
		DoSplitBySign(j, byteSlice, readNum, list, sliceBuffer, 0)
	case 3:
		DoSplitByAppointLen(j, byteSlice, readNum, list, sliceBuffer, 0)
	default:
	}
}

// 1.根据字节长度来读取数据(注意切割的时候有可能是包头被切割了,需要判断下包头字节问题,除非包头是1字节)
func DoSplitByReadHeadLen(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *list.List, sliceBuffer []byte, beginIdx int) {
	if j.totalNeedReadLen == 0 {
		if len(byteSlice) >= HeadLen {
			//需要判断包头是否被切割了（不仅仅只考虑包体拼接）
			diffLen := HeadLen - len(j.headReadArray) //头包剩余所需长度
			j.headReadArray = append(j.headReadArray, byteSlice[:diffLen]...)
			j.totalNeedReadLen = binary.BigEndian.Uint16(j.headReadArray[:HeadLen])
			byteSlice = byteSlice[diffLen:]
			readNum -= diffLen
		} else {
			//不足够读取整个头包长度（可能有1字节被分成下次Read的数据中）
			j.headReadArray = append(j.headReadArray, byteSlice...)
			return
		}
	}
	packageNeedLen := j.totalNeedReadLen - j.curReadLen //该完整包剩余所需读取字节数
	//printer.TextDebug("doSplitByReadHeadLen", "包头对应的值", j.totalNeedReadLen, ", 该完整包剩余所需字节数为", packageNeedLen, ", 切片内剩余字节数为", readNum)

	if readNum < int(packageNeedLen) {
		j.byteData = append(j.byteData, byteSlice...) //剩余的不足够拼成一个完整包（先直接追加进内存等下次Read执行再继续拼接后半段）
		j.curReadLen += uint16(readNum)
	} else if readNum >= int(packageNeedLen) {
		j.byteData = append(j.byteData, byteSlice[:packageNeedLen]...)
		//printer.TextDebug("newString = " + string(j.byteData))
		retNum := copy(sliceBuffer[beginIdx:], j.byteData[0:])
		beginIdx += retNum
		if retNum >= 1 {
			list.PushBack(beginIdx)
		}

		//重置readStreamBufferObject结构对象成员值
		j.totalNeedReadLen, j.curReadLen = 0, 0
		j.byteData = j.byteData[0:0]
		j.headReadArray = j.headReadArray[0:0]
		byteSlice = byteSlice[packageNeedLen:]

		if readNum != int(packageNeedLen) {
			DoSplitByReadHeadLen(j, byteSlice, readNum-int(packageNeedLen), list, sliceBuffer, beginIdx) //剩余的数据大于等于所需的(先拼接好传到发送通信到等待传输去Actor服务进程，然后用递归继续执行剩余的数据拼接新的数据)
		}
	}
}

// 2.根据标记来切割字节流(这里用splitSign常量表示,该方法缺点是前端发送的数据中带有该切割符就会让该方法切割错误)
func DoSplitBySign(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *list.List, sliceBuffer []byte, beginIdx int) {
	byteSlice = byteSlice[:readNum] //实际数据
	//printer.TextDebug("doSplitBySign", "要处理的数据大小", readNum)
	//printer.TextDebug("doSplitBySign", "要处理的数据", string(byteSlice))
	for {
		index := bytes.Index(byteSlice, []byte{splitSign})
		//没找到就直接追加上去然后break
		if index == -1 {
			j.byteData = append(j.byteData, byteSlice...)
			break
		} else {
			//在字符串中有切割符号
			j.byteData = append(j.byteData, byteSlice[:index]...)
			lenData := len(j.byteData)
			if lenData > 0 {
				//printer.TextDebug("doSplitBySign","The String", j.byteData)
				retNum := copy(sliceBuffer[beginIdx:], j.byteData[0:])
				beginIdx += retNum
				if retNum >= 1 {
					list.PushBack(beginIdx)
				}
			}
			j.byteData = j.byteData[0:0]
			readNum -= index //剩余多少字节没处理

			byteSlice = byteSlice[index:]
			//printer.TextDebug("剩余未处理字节大小" + strconv.Itoa(readNum))
			//printer.TextDebug("剩余内容： " + string(byteSlice))
			if (readNum == 1) || (readNum == 0) {
				break //只剩下切割符了或者没有字符串了（直接无视）
			} else {
				//到了这里必定是两个以上的数据长度（包含一个切割符，且切割符在第一位）
				DoSplitBySign(j, byteSlice[1:], readNum-1, list, sliceBuffer, beginIdx)
				return
			}
		}
	}
}

// 3.根据定长取字节流数据(缺点是假如数据量少就会浪费流量资源,空白部分用'\0'补充)
func DoSplitByAppointLen(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *list.List, sliceBuffer []byte, beginIdx int) {
	byteSlice = byteSlice[:readNum]
	diffLen := int(appointLen - j.curReadLen) //剩余形成完整数据包要读的数据大小
	if diffLen <= readNum {
		j.byteData = append(j.byteData, byteSlice[:diffLen]...)
		j.byteData = bytes.TrimRight(j.byteData, "\x00") //去除空白部分
		byteSlice = byteSlice[diffLen:]
		//printer.TextDebug("doSplitByAppointLen", "newStringData", string(j.byteData), "newStringData Len", len(string(j.byteData)))
		retNum := copy(sliceBuffer[beginIdx:], j.byteData[0:])
		beginIdx += retNum
		if retNum >= 1 {
			list.PushBack(beginIdx)
		}
		j.byteData = j.byteData[0:0]
		j.curReadLen = 0
		readNum -= diffLen

		//读取完需要判断下还有没有可数据,有就递归继续处理
		if readNum > 0 {
			DoSplitByAppointLen(j, byteSlice, readNum, list, sliceBuffer, beginIdx)
		} else {
			return
		}
	} else {
		j.byteData = append(j.byteData, byteSlice...)
		j.curReadLen += uint16(readNum)
		return
	}
}

// DoPack 流数据打包处理流程
func DoPack(byteList []byte, data []byte, packWay uint8) int {
	byteList = byteList[0:0]
	switch packWay {
	case 1:
		return DoPackByReadHeadLen(byteList, data)
	case 2:
		return DoPackBySign(byteList, data)
	case 3:
		return DoPackByAppointLen(byteList, data)
	default:
	}
	return 0
}

// DoPackByReadHeadLen 按包头+包体数据的格式打包
func DoPackByReadHeadLen(byteList []byte, data []byte) int {
	dataLen := len(data)
	allLen := dataLen + HeadLen
	byteList = append(byteList[:HeadLen], data[0:]...)        //先存放数据用append扩大长度（预留HeadLen存放包头）
	binary.BigEndian.PutUint16(byteList[0:], uint16(dataLen)) //大端存放包头字节数据
	return allLen
}

// DoPackBySign 用切割符号作为标志打包
func DoPackBySign(byteList []byte, data []byte) int {
	byteList = append(byteList, data[0:]...)
	byteList = append(byteList, splitSign)
	return len(byteList)
}

// DoPackByAppointLen 定长方式打包（需要和接受方一致,空白处需要补全）
func DoPackByAppointLen(byteList []byte, data []byte) int {
	byteList = append(byteList, data[0:]...)
	limitSize, byteListSize := int(appointLen), len(byteList)
	if byteListSize > limitSize {
		return 0
	} else {
		//补充空白处
		diff := limitSize - byteListSize
		if diff > 0 {
			byteList = append(byteList, bytes.Repeat([]byte("\x00"), diff)...)
			//printer.TextDebug("doPackByAppointLen", "Len byteList", len(byteList))
		}
	}
	return limitSize
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

func AddClientToGateMap(socketConn *NewConnection, writeChan chan []byte) {
	clientAddress := (*socketConn).RemoteAddr().String()
	//printer.TextDebug("clientAddress = " + clientAddress)
	GateInfo.curDistributionSessionID += 1
	sessionId := GateInfo.curDistributionSessionID
	if _, ok := GateClientMap.Load(sessionId); ok {
		return
	}

	//修改gateMap的对应客户端数据
	ip, port, _ := strings.Cut(clientAddress, ":")
	realPort, _ := strconv.Atoi(port)
	sessionPtr := new(clientSession)
	sessionPtr.ip = net.ParseIP(ip)
	sessionPtr.port = uint16(realPort)
	sessionPtr.wholeAddress = clientAddress
	sessionPtr.clientBuffChannel = writeChan
	GateClientMap.Store(sessionId, sessionPtr)

	//标记该连接的SessionID
	socketConn.connSession = sessionId
	sessionPtr.session = sessionId
	GateClientMapSize.Add(1)
	printer.TextDebug("After add client to GateMap", "The Size", GateClientMapSize.Load())
}

// 移除map中的 writeChan,防止有数据继续写入而触发panic
func ReduceClientToGateMap(socketConn *NewConnection) {
	//clientAddress := (*socketConn).RemoteAddr().String()
	if socketConn.connSession > 0 {
		GateClientMap.Delete(socketConn.connSession)
		GateClientMapSize.Add(-1)
	}
	printer.TextDebug("After reduce client to GateMap", "The Size", GateClientMapSize.Load())
}

// CreateNewConnObject 创建一个socket Conn对象
func CreateNewConnObject(way int, socketConn *net.Conn, wSocketConn *websocket.Conn, typeofConn string) *NewConnection {
	rsbObject := CreateReadStreamBufferObject(way)
	newConn := NewConnection{
		wsConn:       wSocketConn,
		conn:         socketConn,
		connSession:  0,
		connType:     typeofConn,
		connTime:     time.Now(),
		ReadBuf:      make([]byte, ConnectionInfo.connReadBufSize),
		writeChan:    make(chan []byte, ConnectionInfo.connWriteChanCaps),
		rwChannel:    make(chan string, 2),
		StreamBuffer: rsbObject,
	}

	//设置ipString作为Goroutine的标识映射
	if socketConn != nil {
		newConn.ipString = (*socketConn).RemoteAddr().String()
	} else if wSocketConn != nil {
		newConn.ipString = (*wSocketConn).RemoteAddr().String()
	}

	return &newConn
}

func CreateReadStreamBufferObject(way int) *ReadStreamBufferObject {
	rsbObject := ReadStreamBufferObject{
		readWay:          uint8(way),
		headReadArray:    make([]uint8, 0, HeadLen),
		totalNeedReadLen: 0,
		curReadLen:       0,
		byteData:         make([]byte, 0, ReadStreamBufferSize),
	}
	return &rsbObject
}
