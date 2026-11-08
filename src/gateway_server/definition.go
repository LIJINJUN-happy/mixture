package gateway_server

import (
	"database/sql"
	"fmt"
	"github.com/gorilla/websocket"
	"mixture/src/common/printer"
	"mixture/src/database"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ConnReadBufSize                  = 4096              //每次读取客户端传来的数据长度大小
	ConnWriteChanCaps                = 64                //写信道缓存容量
	ReadStreamBufferSize             = 5120              //字节流读取对象的已读书缓存长度(1024 * 5 )
	ReadStreamBufferObjectWay uint8  = 1                 //字节流切割方式
	MaxConnectClientNum              = 10000             //最大客户端同时连接数量
	messageType               int    = 1                 //webSocket的消息类型（默认为1文本类型）
	maxGateRebuildListenTimes int32  = 10                //网关监听最大重启次数
	splitSign                        = 124               //字节流切割的标识符（’|’）
	appointLen                uint16 = 556               //定长读取字节流的长度
	channelBufferSize                = 2048              //channelArray中每个chan的缓存大小
	ConnReadTimeout                  = 180 * time.Second //读取用户超时间隔（为了防止长时间无响应的情况）
	IntervalLoopingTime              = 30 * time.Second
	GoroutineSyncBreakSign           = "_CLOSE"
)

const (
	HeadLen                 int = 2                                              //数据包头部长度（多少字节）,大端
	ProtoNumLen             int = 2                                              //数据包中的协议占的字节长度,大端
	SessionIdLen            int = 4                                              //数据包来自哪个用户标识的字节长度（来自客户端或者是发送给客户端的时候必须有这个字段内容值,否者找不到发送目标）
	MaxClientPacketSize         = 4096                                           //客户端完整包上限，包含包头和协议号
	MaxPacketsPerRead           = 1024                                           //一次 Read 最多切出的包数上限，超出丢弃并报警
	MaxClientPacketBodySize     = uint16(MaxClientPacketSize - HeadLen)          //客户端包体上限（不包含头部）
	MaxPacketBodySize           = MaxClientPacketBodySize + uint16(SessionIdLen) //内部包体上限，预留转发增加的sessionId
)

// 声明在调用方栈上，零堆分配，零 GC 压力（替代 container/list，存放每次DoSplit方法切出的包尾下标）
type CutIndexList struct {
	data [MaxPacketsPerRead]int
	len  int
}

// 白名单指针对象
var WhiteListPtr = new(WhiteList)

// 网关服务器的全局对象
var GateClientMap = new(sync.Map)
var GateClientMapSize = new(atomic.Int32)
var GateInfo = new(GateInformation)

// IO/协程数量
var clientReadGoroutineNum = new(atomic.Int32)
var clientWriteGoroutineNum = new(atomic.Int32)

// WebSocket定义的全局结构对象
var upgrade = websocket.Upgrader{
	ReadBufferSize:  ReadStreamBufferSize,
	WriteBufferSize: ReadStreamBufferSize,

	//该函数用于拦截或放行跨域请求。函数返回值为bool类型，即true放行，false拦截。如果请求不是跨域请求可以不赋值
	CheckOrigin: func(r *http.Request) bool {
		/*origin := r.Header.Get("Origin")
		if origin == "" {
			// 浏览器前端为主的业务，建议拒绝空 Origin
			return false
		}

		u, err := url.Parse(origin)
		if err != nil {
			return false
		}

		// 方案1：固定白名单
		allowedHosts := map[string]struct{}{
			"app.example.com":   {},
			"admin.example.com": {},
		}
		if _, ok := allowedHosts[strings.ToLower(u.Hostname())]; !ok {
			return false
		}

		// 可选：限制协议
		if u.Scheme != "https" {
			return false
		}*/
		return true // 允许所有源，生产环境中应根据需要设置更严格的CORS策略
	},
}

// 链接器对象
var ActorSerLinker = new(SerLinker)
var InteractiveSerLinker = new(SerLinker)
var ModularizedSerLinker = new(SerLinker)
var WebSerLinker = new(SerLinker)

// 白名单结构
type WhiteList struct {
	whiteMux sync.RWMutex      //使用读写锁,因为绝大部分都是读比较多
	whiteMap map[string]string //白名单字典
	IsActive bool              //是否开启
}

// NewConnection for Client Connect Object conn对象结构
type NewConnection struct {
	connSession  uint32
	connType     string
	connTime     time.Time
	conn         *net.Conn       //socket对象(tcp模式)
	wsConn       *websocket.Conn //socket对象(ws模式)
	ReadBuf      []byte          //socket读取的数据存放切片
	writeChan    chan *[]byte    //socket获取数据的通信道（来自其他进程进程的数据）
	ipString     string          //网络地址
	StreamBuffer *ReadStreamBufferObject
	closeMux     sync.RWMutex  //入队持读锁，关闭和会话注册持写锁
	closed       bool          //是否停止入队，由closeMux保护
	closeSign    chan struct{} //关闭时通知写协程退出
	closeOnce    sync.Once     //保证关闭socket和移除会话只执行一次
	closeErr     error         //保存关闭结果，通过closeOnce同步访问
}

// SerLinker进程连接器（和其他服务进程的链接对象）
type SerLinker struct {
	linkerName     string
	address        string
	receiveChannel chan *[]byte //发送缓存队列，重连期间保留；退出通知使用每次连接独立的通道
}

// GateInformation for GateWaySer Object 网关服务相关参数
type GateInformation struct {
	gateId                      int
	ip                          string
	port                        string
	workMode                    string        //tcp or ws(webSocket)
	protocol                    string        //json or protobuf
	streamBufferDealWay         uint8         //切割数据流方式(作用于客户端和Gate之间)
	maxConnSize                 int32         //接收客户端数据协程最大并发数量
	rebuildListenTimes          atomic.Int32  //重启次数（太多次直接Exit）
	rebuildListenSec            time.Duration //重新启动监听延迟（监听panic后重启监听的间隔时间）
	sessionIdDistributionRecord atomic.Uint32 //当前派发的SessionID（每次有新连接进来都会派发一个新的sessionID作为标识）
}

// 客户端的session元素
type clientSession struct {
	session      uint32         //session分配数值
	ip           net.IP         //客户端的IP地址
	port         uint16         //端口值
	wholeAddress string         //端口+地址
	connection   *NewConnection //回包经连接对象入队，与关闭状态同步
}

// ReadStreamBufferObject读取字节流的数据存放处,每个包都是头部2字节，2字节保存的值是数据的长度
type ReadStreamBufferObject struct {
	minPacketBodySize uint16  //当前连接包体必须包含的协议字段长度
	maxPacketBodySize uint16  //当前连接允许的最大包体长度
	readWay           uint8   //字节流切割方式
	headReadArray     []uint8 //包头已读字节（tcp属于字节流,被分割的有可能是包头部分）
	totalNeedReadLen  uint16  //累计需要读取的长度
	curReadLen        uint16  //目前读取了的长度
	byteData          []byte  //目前读取的字节数据
}

// ReadStreamBufferObject初始化方法
func (streanBuff *ReadStreamBufferObject) Init(way uint8) {
	streanBuff.minPacketBodySize = uint16(SessionIdLen + ProtoNumLen)
	streanBuff.maxPacketBodySize = MaxPacketBodySize //默认用于进程间连接
	streanBuff.readWay = way
	streanBuff.headReadArray = make([]uint8, 0, HeadLen)
	streanBuff.totalNeedReadLen = 0
	streanBuff.curReadLen = 0
	streanBuff.byteData = make([]byte, 0, ReadStreamBufferSize)
}

// 初始化NewConnection对象
func (netObj *NewConnection) Init(way uint8, socketConn *net.Conn, wSocketConn *websocket.Conn, typeofConn string) {
	rsbObject := new(ReadStreamBufferObject)
	rsbObject.Init(way)
	rsbObject.maxPacketBodySize = MaxClientPacketBodySize
	rsbObject.minPacketBodySize = uint16(ProtoNumLen)
	netObj.StreamBuffer = rsbObject
	netObj.wsConn = wSocketConn
	netObj.conn = socketConn
	netObj.connSession = 0
	netObj.connType = typeofConn
	netObj.connTime = time.Now()
	netObj.ReadBuf = make([]byte, ConnReadBufSize)
	netObj.writeChan = make(chan *[]byte, ConnWriteChanCaps)
	netObj.closeSign = make(chan struct{})

	//设置ipString作为Goroutine的标识映射
	if socketConn != nil {
		netObj.ipString = (*socketConn).RemoteAddr().String()
	} else if wSocketConn != nil {
		netObj.ipString = (*wSocketConn).RemoteAddr().String()
	}
}

// 往map中添加newConnection对应的writeChan
func (netObj *NewConnection) AddIntoGateMap() {
	netObj.closeMux.Lock()
	defer netObj.closeMux.Unlock()
	if netObj.closed || netObj.connSession != 0 {
		return
	}
	clientAddress := (*netObj).RemoteAddr().String()
	//printer.TextDebug("clientAddress = " + clientAddress)
	//不用担心超过uint32后溢出的情况，Add方法在溢出时会自动回绕（wrap around），这是无符号整数在底层硬件上的标准行为
	sessionId := GateInfo.sessionIdDistributionRecord.Add(1)
	if _, ok := GateClientMap.Load(sessionId); ok {
		printer.TextError("AddIntoGateMap Error SessionId Already Exists", "sessionId", sessionId)
		return
	}

	//修改gateMap的对应客户端数据
	ip, port, _ := strings.Cut(clientAddress, ":")
	realPort, _ := strconv.Atoi(port)
	sessionPtr := new(clientSession)
	sessionPtr.ip = net.ParseIP(ip)
	sessionPtr.port = uint16(realPort)
	sessionPtr.wholeAddress = clientAddress
	sessionPtr.connection = netObj
	GateClientMap.Store(sessionId, sessionPtr)

	//标记该连接的SessionID
	netObj.connSession = sessionId
	sessionPtr.session = sessionId
	GateClientMapSize.Add(1)
	printer.TextDebug("After add client to GateMap", "The Size", GateClientMapSize.Load())
}

// 移除map中的 writeChan,防止有数据继续写入而触发panic
func (netObj *NewConnection) ReduceFromGateMap() {
	netObj.closeMux.Lock()
	defer netObj.closeMux.Unlock()
	//clientAddress := (*netObj).RemoteAddr().String()
	if netObj.connSession > 0 {
		if _, removed := GateClientMap.LoadAndDelete(netObj.connSession); removed {
			GateClientMapSize.Add(-1)
		}
	}
	printer.TextDebug("After reduce client to GateMap", "The Size", GateClientMapSize.Load())
}

func (netObj *NewConnection) Write(b []byte) (n int, err error) {
	if netObj.conn != nil {
		return (*(netObj.conn)).Write(b)
	}

	//websocket.Write
	return len(b), (netObj.wsConn).WriteMessage(messageType, b)
}

func (netObj *NewConnection) Read() (int, []byte, error) {
	if netObj.conn != nil {
		n, errWithTcpRead := (*(netObj.conn)).Read(netObj.ReadBuf)
		return n, netObj.ReadBuf, errWithTcpRead
	}

	_, p, errWithWsRead := (netObj.wsConn).ReadMessage()
	if errWithWsRead != nil {
		return 0, nil, errWithWsRead
	}
	netObj.ReadBuf = append(netObj.ReadBuf[0:0], p...)
	//printer.TextInfo("netObj.readBuf","readBuf", netObj.ReadBuf)
	return len(netObj.ReadBuf), netObj.ReadBuf, nil
}

func (netObj *NewConnection) Close() error {
	netObj.closeOnce.Do(func() {
		netObj.closeMux.Lock()
		netObj.closed = true
		close(netObj.closeSign)
		netObj.closeMux.Unlock()
		if netObj.conn != nil {
			netObj.closeErr = (*netObj.conn).Close()
		} else {
			netObj.closeErr = netObj.wsConn.Close()
		}
		netObj.ReduceFromGateMap()
	})
	return netObj.closeErr
}

// TrySend 成功后由写协程回收缓冲区，失败时仍由调用方回收。
func (netObj *NewConnection) TrySend(buf *[]byte) bool {
	netObj.closeMux.RLock()
	defer netObj.closeMux.RUnlock()
	if netObj.closed || buf == nil {
		return false
	}
	select {
	case netObj.writeChan <- buf:
		return true
	default:
		return false
	}
}

// Close禁止入队后，仅由写协程清理剩余缓冲区。
func (netObj *NewConnection) drainWriteQueue() {
	for {
		select {
		case buf := <-netObj.writeChan:
			*buf = (*buf)[:cap(*buf)]
			DistributeDataPool.Put(buf)
		default:
			return
		}
	}
}

func (netObj *NewConnection) RemoteAddr() net.Addr {
	if netObj.conn != nil {
		return (*(netObj.conn)).RemoteAddr()
	}
	return (netObj.wsConn).RemoteAddr()
}

func (linker *SerLinker) InitLinker(linkerName string, db *sql.DB, linkerID int) {
	address := database.GetSerAddressById(linkerID, db, linkerName)
	port := database.GetSerPortById(linkerID, db, linkerName)
	if len(address) < 1 || port <= 0 {
		err := fmt.Errorf("error initLinker:%s,linkerID:%d", linkerName, linkerID)
		panic(err)
	}

	linker.receiveChannel = make(chan *[]byte, channelBufferSize)
	linker.linkerName = linkerName

	var strBuilder strings.Builder
	portString := strconv.Itoa(port)
	strBuilder.Grow(len(address) + 1 + len(portString))
	strBuilder.WriteString(address)
	strBuilder.WriteString(":")
	strBuilder.WriteString(portString)
	linker.address = strBuilder.String()
	return
}

func (w *WhiteList) CheckWhiteIP(addr string) bool {
	w.whiteMux.RLock()
	defer w.whiteMux.RUnlock()
	if !w.IsActive {
		return true //假如没开启白名单则直接默认返回true即可,无需检测
	}

	// addr 格式为 "ip:port"，提取纯 IP 部分后再比对白名单
	ip, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if _, ok := w.whiteMap[ip]; ok {
		return true
	}
	return false
}

func (w *WhiteList) AddWhiteIP(addr string) {
	w.whiteMux.Lock()
	defer w.whiteMux.Unlock()
	w.whiteMap[addr] = addr
}

func (w *WhiteList) DelWhiteIP(addr string) {
	w.whiteMux.Lock()
	defer w.whiteMux.Unlock()
	delete(w.whiteMap, addr)
}

func (w *WhiteList) ChangeActiveStatus(cmd bool) {
	w.whiteMux.Lock()
	defer w.whiteMux.Unlock()
	w.IsActive = cmd
}

func (c *CutIndexList) Reset() {
	c.len = 0
}

func (c *CutIndexList) Push(v int) {
	if c.len < MaxPacketsPerRead {
		c.data[c.len] = v
		c.len++
	} else {
		printer.TextWarn("CutIndexList MaxPacketsPerRead exceeded, packet dropped")
	}
}

func (c *CutIndexList) Get(i int) int {
	return c.data[i]
}

func (c *CutIndexList) Len() int {
	return c.len
}
