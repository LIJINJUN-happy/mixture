package gateway_server

import (
	"container/list"
	"mixture/src/common/printer"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func doRegister() {
	http.HandleFunc("/ws", DoUpgradeWS) //升级成WS请求处理
	http.HandleFunc("/test", TestFunc)  //测试例子
}

func beginWebSocketSer() {
	doRegister()
	address := strings.Join([]string{GateInfo.ip + ":", ""}, GateInfo.port)
	var strBuild strings.Builder
	addrLen := len(address)
	strBuild.Grow(addrLen + 33)
	strBuild.WriteString("Starting WebSocket Server: ")
	strBuild.WriteString(address)
	strBuild.WriteString(" ...")
	printer.TextInfo(strBuild.String())
	err := http.ListenAndServe(address[addrLen:], nil) //默认监听本地,“:端口号”
	if err != nil {
		printer.TextError("Error Starting Gate_Server With WS Mode", "err", err)
	}
}

// 注意：gorilla/websocket库中的Respond会自动创建一个Goroutine来处理请求,所以无需像beginTcpSer一样再创建2个协程来读写分离
func DoUpgradeWS(w http.ResponseWriter, r *http.Request) {
	wsConn, errUpgrade := upgrade.Upgrade(w, r, nil)
	if errUpgrade != nil {
		printer.TextError("Failed To Set up WebSocket Connection", "errUpgrade", errUpgrade)
		return
	}

	//白名单过滤
	if !WhiteListPtr.CheckWhiteIP(wsConn.RemoteAddr().String()) {
		wsConn.Close()
		return
	}

	printer.TextInfo("Upgraded WebSocket Connection Successfully")
	newConn := CreateNewConnObject(1, nil, wsConn, "ws")
	clientReadGoroutineNum.Add(1)
	AddClientToGateMap(newConn, newConn.writeChan)

	defer func(wsc *NewConnection) {
		wsc.rwChannel <- GoroutineSyncBreakSign
		closeError := wsc.Close()
		if closeError != nil {
			printer.TextError("Failed To Close WsConn", "closeError", closeError)
		}
		clientReadGoroutineNum.Add(-1)
	}(newConn)

	//Do Write
	go DoWriteMsg(newConn)

	//Do Read
	sliceBuffer := make([]byte, ReadStreamBufferSize)
	cutList := list.New()
	for {
		//设置读取的超时时间
		dLineTime := time.Now().Add(ConnectionInfo.connReadTimeout)
		err := newConn.wsConn.SetReadDeadline(dLineTime)
		if err != nil {
			printer.TextError("WebSocket SetReadDeadline", "Error", err.Error())
		}

		cutList = cutList.Init()
		num, _, readError := (*newConn).Read()
		if readError != nil {
			if checkRes, descString := CheckIsCloseByRemote(readError); checkRes {
				printer.TextInfo("Conn Close With Client", "descString", descString)
			} else if CheckIsReadTimeOut(readError) {
				printer.TextWarn("Read Timeout", "RemoteAddr", (*newConn).RemoteAddr().String())
			} else {
				printer.TextError("Failed to read WebSocket message", "readError", readError)
			}
			return
		} else {
			//处理接收到的数据
			//printer.TextInfo("doUpgradeWS", "num", num, "message", string(newConn.ReadBuf[:num]))
			DealBufferData(newConn, num, cutList, sliceBuffer)
		}
	}
}

func TestFunc(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		n, e := w.Write([]byte("here" + strconv.Itoa(time.Now().Second())))
		if e != nil {
			printer.TextError("Failed to write response:" + e.Error())
		} else {
			printer.TextInfo("testFunc", "n", n, "r", r)
		}
	} else if r.Method == "POST" {

	}
}
