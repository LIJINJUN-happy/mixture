package gateway_server

import (
	"mixture/src/common/printer"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func DoRegister() {
	http.HandleFunc("/ws", DoUpgradeWS) //升级成WS请求处理
	http.HandleFunc("/test", TestFunc)  //测试例子
}

func BeginWebSocketSer() {
	DoRegister()
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
		panic("Listen Address Error: " + address)
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

	newConn := new(NewConnection)
	newConn.Init(ReadStreamBufferObjectWay, nil, wsConn, "ws")
	clientReadGoroutineNum.Add(1)
	newConn.AddIntoGateMap()

	defer func(wsc *NewConnection) {
		closeError := wsc.Close()
		if closeError != nil {
			printer.TextError("Failed To Close WsConn", "closeError", closeError)
		}
		clientReadGoroutineNum.Add(-1)
	}(newConn)

	//Do Write
	go DoWriteMsg(newConn)

	//Do Read
	sliceBuffer := make([]byte, int(MaxPacketBodySize)+ConnReadBufSize) //超过输出容量时切包返回false
	var cutList CutIndexList
	for {
		//设置读取的超时时间
		dLineTime := time.Now().Add(ConnReadTimeout)
		err := newConn.wsConn.SetReadDeadline(dLineTime)
		if err != nil {
			printer.TextError("WebSocket SetReadDeadline", "Error", err.Error())
		}

		cutList.Reset()
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
			if !DealBufferData(newConn, num, &cutList, sliceBuffer) {
				printer.TextWarn("DoUpgradeWS", "Illegal packet, closing connection", (*newConn).RemoteAddr().String())
				return
			}
		}
	}
}

func TestFunc(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		n, e := w.Write([]byte("here" + strconv.Itoa(time.Now().Second())))
		if e != nil {
			printer.TextError("Write response failed", "err", e)
		} else {
			printer.TextInfo("testFunc", "n", n, "r", r)
		}
	} else if r.Method == "POST" {

	}
}
