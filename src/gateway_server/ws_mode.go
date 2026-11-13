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
	server := &http.Server{
		Addr:              address[:addrLen],
		ReadHeaderTimeout: 5 * time.Second,
	}
	err := server.ListenAndServe() // 限制 Upgrade 前 HTTP 请求头读取时间，防止慢速请求长期占用连接。
	if err != nil {
		printer.TextError("Error Starting Gate_Server With WS Mode", "err", err)
		panic("Listen Address Error: " + address)
	}
}

// 注意：gorilla/websocket库中的Respond会自动创建一个Goroutine来处理请求,所以无需像beginTcpSer一样再创建2个协程来读写分离
func DoUpgradeWS(w http.ResponseWriter, r *http.Request) {
	if !WhiteListPtr.CheckWhiteIP(r.RemoteAddr) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return // 在 Upgrade 前完成准入，连接满时不再建立 WebSocket 会话。
	}
	if !ReserveClientConnection() {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	needReleaseReservation := true //是否需要需归还连接名额
	defer func() {
		if needReleaseReservation {
			activeClientConnections.Add(-1)
		}
	}()

	wsConn, errUpgrade := upgrade.Upgrade(w, r, nil)
	if errUpgrade != nil {
		printer.TextError("Failed To Set up WebSocket Connection", "errUpgrade", errUpgrade)
		return
	}

	// 限制单条WS消息（包含全部分片）的应用数据总长度，避免ReadMessage无限制分配内存。
	wsConn.SetReadLimit(int64(MaxClientPacketSize))
	printer.TextInfo("Upgraded WebSocket Connection Successfully")

	//创建且初始化conn对象
	newConn := new(NewConnection)
	newConn.Init(GateInfo.streamBufferDealWay, nil, wsConn, "ws")
	newConn.slotReserved = true
	needReleaseReservation = false //后续通过close归还连接名额
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
