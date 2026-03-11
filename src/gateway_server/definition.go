package gateway_server

import (
	"database/sql"
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

const ConnReadBufSize = 255                //每次读取客户端传来的数据长度大小
const ConnWriteChanCaps = 5                //写信道缓存容量
const ReadStreamBufferSize = 5120          //字节流读取对象的已读书缓存长度(1024 * 5 )
const ReadStreamBufferObjectWay uint8 = 1  //字节流切割方式
const MaxConnectClientNum = 10000          //最大客户端同时连接数量
const messageType int = 1                  //webSocket的消息类型（默认为1文本类型）
const maxGateRebuildListenTimes int32 = 10 //网关监听最大重启次数
const splitSign = 124                      //字节流切割的标识符（‘|’）
const HeadLen int = 2                      //数据包头部长度（多少字节）,大端
const ProtoNumLen int = 2                  //数据包中的协议占的字节长度,大端
const SessionIdLen int = 4                 //数据包来自哪个用户标识的字节长度（来自客户端或者是发送给客户端的时候必须有这个字段内容值,否者找不到发送目标）
const appointLen uint16 = 556              //定长读取字节流的长度
const channelBufferSize = 1024             //channelArray中每个chan的缓存大小
const ConnReadTimeout = 900 * time.Second  //读取用户超时间隔（为了防止长时间无响应的情况）
const IntervalLoopingTime = 30 * time.Second
const GoroutineSyncBreakSign = "_CLOSE"

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
	WhiteMux sync.RWMutex      //使用读写锁,因为绝大部分都是读比较多
	WhiteMap map[string]string //白名单字典
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
	writeChan    chan []byte     //socket获取数据的通信道（来自其他进程进程的数据）
	rwChannel    chan string     //用于conn对象的Read和Write协程间的通知,实现同步关闭,不同步关闭的话会造成内存泄露
	ipString     string          //网络地址
	StreamBuffer *ReadStreamBufferObject
}

// SerLinker进程连接器（和其他服务进程的链接对象）
type SerLinker struct {
	linkerName     string
	address        string
	receiveChannel chan any  //用来接收所有将要派发到对应进程的数据（相当于连接器发送缓存通道）
	serConn        *net.Conn //链接xx服务进程的socket对象
}

// GateInformation for GateWaySer Object 网关服务相关参数
type GateInformation struct {
	gateId                      int
	ip                          string
	port                        string
	workMode                    string        //tcp or ws(webSocket)
	protocol                    string        //json or protobuf
	maxConnSize                 int32         //接收客户端数据协程最大并发数量
	rebuildListenTimes          atomic.Int32  //重启次数（太多次直接Exit）
	rebuildListenSec            time.Duration //重新启动监听延迟（监听panic后重启监听的间隔时间）
	sessionIdDistributionRecord atomic.Uint32 //当前派发的SessionID（每次有新连接进来都会派发一个新的sessionID作为标识）
}

// 客户端的session元素
type clientSession struct {
	session           uint32      //session分配数值
	ip                net.IP      //客户端的IP地址
	port              uint16      //端口值
	wholeAddress      string      //端口+地址
	clientBuffChannel chan []byte //客户端的接收协议的通信道
}

// ReadStreamBufferObject读取字节流的数据存放处,每个包都是头部2字节，2字节保存的值是数据的长度
type ReadStreamBufferObject struct {
	readWay          uint8   //字节流切割方式
	headReadArray    []uint8 //包头已读字节（tcp属于字节流,被分割的有可能是包头部分）
	totalNeedReadLen uint16  //累计需要读取的长度
	curReadLen       uint16  //目前读取了的长度
	byteData         []byte  //目前读取的字节数据
}

// ReadStreamBufferObject初始化方法
func (streanBuff *ReadStreamBufferObject) Init(way uint8) {
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
	netObj.StreamBuffer = rsbObject
	netObj.wsConn = wSocketConn
	netObj.conn = socketConn
	netObj.connSession = 0
	netObj.connType = typeofConn
	netObj.connTime = time.Now()
	netObj.ReadBuf = make([]byte, ConnReadBufSize)
	netObj.writeChan = make(chan []byte, ConnWriteChanCaps)
	netObj.rwChannel = make(chan string, 2)

	//设置ipString作为Goroutine的标识映射
	if socketConn != nil {
		netObj.ipString = (*socketConn).RemoteAddr().String()
	} else if wSocketConn != nil {
		netObj.ipString = (*wSocketConn).RemoteAddr().String()
	}
}

// 往map中添加newConnection对应的writeChan
func (netObj *NewConnection) AddIntoGateMap(writeChan chan []byte) {
	clientAddress := (*netObj).RemoteAddr().String()
	//printer.TextDebug("clientAddress = " + clientAddress)
	//不用担心超过uint32后溢出的情况，Add方法在溢出时会自动回绕（wrap around），这是无符号整数在底层硬件上的标准行为
	sessionId := GateInfo.sessionIdDistributionRecord.Add(1)
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
	netObj.connSession = sessionId
	sessionPtr.session = sessionId
	GateClientMapSize.Add(1)
	printer.TextDebug("After add client to GateMap", "The Size", GateClientMapSize.Load())
}

// 移除map中的 writeChan,防止有数据继续写入而触发panic
func (netObj *NewConnection) ReduceFromGateMap() {
	//clientAddress := (*netObj).RemoteAddr().String()
	if netObj.connSession > 0 {
		GateClientMap.Delete(netObj.connSession)
		GateClientMapSize.Add(-1)
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
	netObj.ReadBuf = append(netObj.ReadBuf[0:0], p...)
	//printer.TextInfo("netObj.readBuf","readBuf", netObj.ReadBuf)
	return len(netObj.ReadBuf), netObj.ReadBuf, errWithWsRead
}

func (netObj *NewConnection) Close() error {
	if netObj.conn != nil {
		return (*(netObj.conn)).Close()
	}
	return (netObj.wsConn).Close()
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
		panic("Error initLinker " + linkerName + ": " + strconv.Itoa(linkerID))
	}

	linker.receiveChannel = make(chan any, channelBufferSize)
	linker.serConn = nil
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
	w.WhiteMux.RLock()
	defer w.WhiteMux.RUnlock()
	if !w.IsActive {
		return true //假如没开启白名单则直接默认返回true即可,无需检测
	}

	if _, ok := w.WhiteMap[addr]; ok {
		return true
	}
	return false
}

func (w *WhiteList) AddWhiteIP(addr string) {
	w.WhiteMux.Lock()
	defer w.WhiteMux.Unlock()
	w.WhiteMap[addr] = addr
}

func (w *WhiteList) DelWhiteIP(addr string) {
	w.WhiteMux.Lock()
	defer w.WhiteMux.Unlock()
	delete(w.WhiteMap, addr)
}

func (w *WhiteList) ChangeActiveStatus(cmd bool) {
	w.WhiteMux.Lock()
	defer w.WhiteMux.Unlock()
	w.IsActive = cmd
}
