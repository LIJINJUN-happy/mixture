// 本地隔离性能回归。运行方式与测试边界见 ../README.md。
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mixture/src/common/printer"
	sc "mixture/src/common/server_common"
	gw "mixture/src/gateway_server"
	"mixture/test/performance/observe"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"mixture/proto/mutual_pb/pb"
)

const ProbeRequest = 60002 // 仅隔离测试进程注册，不修改正式协议文件。
const ProbeResponse = 60003

type Metrics struct {
	Time         time.Time                 `json:"time"`
	Goroutines   int                       `json:"goroutines"`
	HeapAlloc    uint64                    `json:"heap_alloc"`
	TotalAlloc   uint64                    `json:"total_alloc"`
	Mallocs      uint64                    `json:"mallocs"`
	NumGC        uint32                    `json:"num_gc"`
	Pause        uint64                    `json:"gc_pause_ns"`
	Clients      int32                     `json:"gateway_clients"`
	Queues       map[string]map[string]int `json:"protocol_queues"`
	GatewayDrops map[string]uint64         `json:"gateway_drops"`
	ServerDrops  map[string]uint64         `json:"server_drops"`
}

type Child struct {
	Command    *exec.Cmd
	Done       chan error
	Diagnostic string
	Log        *os.File
}
type Rig struct {
	Directory, Transport string
	Address              string
	Ports                map[string]int
	Children             map[string]*Child
	Delay                time.Duration
	Queue, LogLevel      int
}

type Options struct {
	Name                           string
	Connections                    int
	Duration                       time.Duration
	Payload                        int
	ReadDelay                      time.Duration
	NoRead, Silent, Trickle, Probe bool
	Interval                       time.Duration
}

type Result struct {
	Name                                                        string  `json:"name"`
	Transport                                                   string  `json:"transport"`
	Connections                                                 int     `json:"connections"`
	Payload                                                     int     `json:"payload_bytes"`
	Seconds                                                     float64 `json:"seconds"`
	Connected, DialFailed, Sent, Validated, Failed, Interrupted int64
	SuccessRPS                                                  float64 `json:"success_rps"`
	P50, P95, P99                                               string
	ProbeOK, ProbeFailed                                        int64
	DisconnectMin, DisconnectMax                                float64
	Before, During, After                                       map[string]Metrics
	Notes                                                       string `json:"notes,omitempty"`
}

type Counters struct {
	Connected, DialFailed, Sent, Validated, Failed, Interrupted atomic.Int64
	Buckets                                                     [15]atomic.Int64
	ProbeOK, ProbeFailed                                        atomic.Int64
	mu                                                          sync.Mutex
	Disconnects                                                 []float64
}

var Limits = []time.Duration{100 * time.Microsecond, 200 * time.Microsecond, 500 * time.Microsecond, time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second}
var OutputMu sync.Mutex
var ResultFile, TimelineFile *os.File

// RegisterProbe 在子进程启动阶段注册独立模块探针，运行期间不再修改路由表。
func RegisterProbe() {
	for _, p := range []struct {
		Name   string
		Number uint16
		Target string
	}{
		{"C2SPerfProbe", ProbeRequest, "actor_server"}, {"S2CPerfProbe", ProbeResponse, "gateway_server"},
	} {
		gw.ProtocolDistributeInfo.ProtoNameMap[p.Name] = p.Number
		gw.ProtocolDistributeInfo.ProtoNumArray[p.Number] = p.Name
		gw.ProtocolDistributeInfo.ProtoDistributeArray[p.Number] = p.Target
	}
}

// ChildMain 复用真实网络公共层及模块系统，不初始化或模拟 MongoDB/Memcached。
func ChildMain() error {
	f := flag.NewFlagSet("child", flag.ExitOnError)
	role := f.String("role", "actorSer", "process role")
	diagnostic := f.String("diagnostic", "", "loopback diagnostic address")
	delay := f.Duration("delay", 0, "test module delay")
	queue := f.Int("queue", 2048, "test module queue capacity")
	f.Parse(os.Args[3:]) // os.Args[2] 保持为网关/服务ID，兼容正式初始化代码。
	printer.InitPrinterMgr()
	printer.SetLoggerObj(printer.CreateLogger(*role, "log/", false))
	RegisterProbe()
	if *role == "gateway" {
		diagnostics, err := observe.Start(*diagnostic)
		if err != nil {
			return err
		}
		defer diagnostics.Close()
		gw.Run()
		return nil
	}
	service := new(sc.ServerInfo)
	if err := service.InitInfo(*role, 1); err != nil {
		return err
	}
	var wg sync.WaitGroup
	if *role == "actorSer" {
		mod := sc.CreateMod("user", *queue, func() error { return nil }, func() error { return nil }, &wg)
		mod.RegisterProtoHandleCaller("C2STest", func(data []byte, gate int, sid uint32) {
			if *delay > 0 {
				time.Sleep(*delay)
			}
			Reply(data, gate, sid, "S2CTestRet")
		})
		probe := sc.CreateMod("probe", 64, func() error { return nil }, func() error { return nil }, &wg)
		probe.RegisterProtoHandleCaller("C2SPerfProbe", func(data []byte, gate int, sid uint32) { Reply(data, gate, sid, "S2CPerfProbe") })
	}
	service.RegisterMonitorLogic(sc.TranspondProtoPack)
	diagnostics, err := observe.Start(*diagnostic)
	if err != nil {
		return err
	}
	defer diagnostics.Close()
	select {}
}

// Reply 保留 C2STest.Num+1 语义，额外回传 protobuf unknown 字段用于合法大包测试。
func Reply(data []byte, gate int, sid uint32, response string) {
	var request pb.C2STest
	if err := proto.Unmarshal(data, &request); err != nil {
		return
	}
	printer.TextInfo("Performance Test", "num", request.Num+1)
	reply := &pb.S2CTestRet{Num: request.Num + 1}
	reply.ProtoReflect().SetUnknown(bytes.Clone(request.ProtoReflect().GetUnknown()))
	body, err := proto.Marshal(reply)
	if err == nil {
		sc.DeliveryMsg(gate, sid, response, body)
	}
}

func Port() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	return port, l.Close()
}

func NewRig(transport string, delay time.Duration, queue, level int) (*Rig, error) {
	dir, err := os.MkdirTemp("", "mixture-perf-"+transport+"-")
	if err != nil {
		return nil, err
	}
	r := &Rig{Directory: dir, Transport: transport, Ports: map[string]int{}, Children: map[string]*Child{}, Delay: delay, Queue: queue, LogLevel: level}
	for _, name := range []string{"gateway", "actorSer", "interactiveSer", "modularizedSer", "webSer"} {
		for _, suffix := range []string{"", "-diag"} {
			p, e := Port()
			if e != nil {
				return nil, e
			}
			r.Ports[name+suffix] = p
		}
	}
	r.Address = net.JoinHostPort("127.0.0.1", fmt.Sprint(r.Ports["gateway"]))
	for _, name := range []string{"log/moduleLog", "proto/mutual_pb/pb_src"} {
		if err = os.MkdirAll(filepath.Join(dir, name), 0755); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{"proto/distribute_config.json", "proto/mutual_pb/pb_src/allprotocol.json"} {
		data, e := os.ReadFile(name)
		if e != nil {
			return nil, e
		}
		if e = os.WriteFile(filepath.Join(dir, name), data, 0600); e != nil {
			return nil, e
		}
	}
	config := fmt.Sprintf("slogLevel=%d\nprotocolType=protobuf\nworkMode=%s\nstreamBufferDealWay=1\nwhiteIPListDefaultStatus=false\npprof=false\n", level, transport)
	if err = os.WriteFile(filepath.Join(dir, "system.txt"), []byte(config), 0600); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite3", filepath.Join(dir, "net_sql.db"))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE gatewaySerConfig(gatewaySerId INTEGER, gatewaySerAddress TEXT, gatewaySerPort TEXT, interactiveSerId INTEGER)`); err != nil {
		return nil, err
	}
	if _, err = db.Exec(`INSERT INTO gatewaySerConfig VALUES(1,'127.0.0.1',?,1)`, r.Ports["gateway"]); err != nil {
		return nil, err
	}
	for _, name := range []string{"actorSer", "interactiveSer", "modularizedSer", "webSer"} {
		query := fmt.Sprintf("CREATE TABLE %sConfig(%sId INTEGER,%sAddress TEXT,%sPort TEXT)", name, name, name, name)
		if _, err = db.Exec(query); err != nil {
			return nil, err
		}
		if _, err = db.Exec(fmt.Sprintf("INSERT INTO %sConfig VALUES(1,'127.0.0.1',?)", name), r.Ports[name]); err != nil {
			return nil, err
		}
	}
	for _, name := range []string{"actorSer", "interactiveSer", "modularizedSer", "webSer", "gateway"} {
		if err = r.Start(name); err != nil {
			r.Close()
			return nil, err
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err = r.Probe(); err == nil {
			fmt.Printf("READY transport=%s address=%s fixture=%s\n", transport, r.Address, dir)
			return r, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.Close()
	return nil, fmt.Errorf("rig readiness timeout: %w; logs=%s", err, dir)
}

func (r *Rig) Start(role string) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	log, err := os.OpenFile(filepath.Join(r.Directory, role+"-process.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	diag := fmt.Sprintf("http://127.0.0.1:%d", r.Ports[role+"-diag"])
	cmd := exec.Command(executable, "child", "1", "-role", role, "-diagnostic", strings.TrimPrefix(diag, "http://"), "-delay", r.Delay.String(), "-queue", fmt.Sprint(r.Queue))
	cmd.Dir = r.Directory
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		log.Close()
		return err
	}
	child := &Child{Command: cmd, Diagnostic: diag, Done: make(chan error, 1), Log: log}
	r.Children[role] = child
	go func() { child.Done <- cmd.Wait(); log.Close() }()
	return nil
}

func (r *Rig) Stop(role string) {
	c := r.Children[role]
	if c == nil {
		return
	}
	c.Command.Process.Kill()
	<-c.Done
	delete(r.Children, role)
}
func (r *Rig) Close() {
	for _, role := range []string{"gateway", "actorSer", "interactiveSer", "modularizedSer", "webSer"} {
		r.Stop(role)
	}
}

type Session interface {
	Send([]byte) error
	Receive() ([]byte, error)
	Deadline(time.Time) error
	Close() error
}
type TCPSession struct {
	net.Conn
	Buffer [4096]byte
}

func (s *TCPSession) Send(p []byte) error {
	n, e := s.Write(p)
	if e == nil && n != len(p) {
		e = io.ErrShortWrite
	}
	return e
}
func (s *TCPSession) Receive() ([]byte, error) {
	var h [2]byte
	if _, e := io.ReadFull(s.Conn, h[:]); e != nil {
		return nil, e
	}
	n := int(binary.BigEndian.Uint16(h[:]))
	if n < 2 || n > 4094 {
		return nil, fmt.Errorf("invalid length %d", n)
	}
	_, e := io.ReadFull(s.Conn, s.Buffer[:n])
	return s.Buffer[:n], e
}
func (s *TCPSession) Deadline(t time.Time) error { return s.SetDeadline(t) }

type WSSession struct{ Conn *websocket.Conn }

func (s *WSSession) Send(p []byte) error { return s.Conn.WriteMessage(websocket.BinaryMessage, p) }
func (s *WSSession) Receive() ([]byte, error) {
	kind, p, e := s.Conn.ReadMessage()
	if e != nil {
		return nil, e
	}
	if kind != websocket.BinaryMessage || len(p) < 4 || int(binary.BigEndian.Uint16(p[:2])) != len(p)-2 {
		return nil, errors.New("invalid WS reply")
	}
	return p[2:], nil
}
func (s *WSSession) Deadline(t time.Time) error {
	if e := s.Conn.SetReadDeadline(t); e != nil {
		return e
	}
	return s.Conn.SetWriteDeadline(t)
}
func (s *WSSession) Close() error { return s.Conn.Close() }

func (r *Rig) Dial() (Session, error) {
	if r.Transport == "ws" {
		d := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
		c, response, e := d.Dial("ws://"+r.Address+"/ws", nil)
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if e != nil {
			return nil, e
		}
		return &WSSession{c}, nil
	}
	c, e := net.DialTimeout("tcp", r.Address, 5*time.Second)
	if e != nil {
		return nil, e
	}
	return &TCPSession{Conn: c}, nil
}

func Packet(size int, number uint16) ([]byte, error) {
	body, _ := proto.Marshal(&pb.C2STest{Num: 1})
	if size > len(body) {
		padding := -1
		for n := 0; n <= size; n++ {
			if len(body)+protowire.SizeTag(127)+protowire.SizeBytes(n) == size {
				padding = n
				break
			}
		}
		if padding < 0 {
			return nil, fmt.Errorf("cannot encode payload size %d", size)
		}
		body = protowire.AppendTag(body, 127, protowire.BytesType)
		body = protowire.AppendBytes(body, bytes.Repeat([]byte{42}, padding))
	}
	p := make([]byte, len(body)+4)
	binary.BigEndian.PutUint16(p, uint16(len(body)+2))
	binary.BigEndian.PutUint16(p[2:], number)
	copy(p[4:], body)
	return p, nil
}

func Validate(body []byte, number uint16, payload int) error {
	if len(body) != payload+2 || binary.BigEndian.Uint16(body) != number {
		return errors.New("reply size or protocol mismatch")
	}
	var response pb.S2CTestRet
	if e := proto.Unmarshal(body[2:], &response); e != nil {
		return e
	}
	if response.Num != 2 {
		return errors.New("reply value mismatch")
	}
	return nil
}

func (r *Rig) Probe() error {
	s, e := r.Dial()
	if e != nil {
		return e
	}
	defer s.Close()
	s.Deadline(time.Now().Add(time.Second))
	p, _ := Packet(2, ProbeRequest)
	if e = s.Send(p); e != nil {
		return e
	}
	body, e := s.Receive()
	if e != nil {
		return e
	}
	return Validate(body, ProbeResponse, 2)
}

func (r *Rig) Snapshot() map[string]Metrics {
	out := map[string]Metrics{}
	client := http.Client{Timeout: 2 * time.Second}
	for _, role := range []string{"gateway", "actorSer"} {
		c := r.Children[role]
		if c == nil {
			continue
		}
		response, e := client.Get(c.Diagnostic + "/metrics")
		if e != nil {
			continue
		}
		var m Metrics
		e = json.NewDecoder(response.Body).Decode(&m)
		response.Body.Close()
		if e == nil {
			out[role] = m
		}
	}
	return out
}

func (c *Counters) Observe(d time.Duration) {
	i := sort.Search(len(Limits), func(i int) bool { return d <= Limits[i] })
	c.Buckets[i].Add(1)
}
func (c *Counters) Percentile(p float64) string {
	n := c.Validated.Load()
	if n == 0 {
		return "-"
	}
	target := int64(float64(n)*p + 0.999999)
	var sum int64
	for i := range c.Buckets {
		sum += c.Buckets[i].Load()
		if sum >= target {
			if i == len(Limits) {
				return ">2s"
			}
			return "≤" + Limits[i].String()
		}
	}
	return "-"
}

func Record(result Result) {
	OutputMu.Lock()
	defer OutputMu.Unlock()
	json.NewEncoder(ResultFile).Encode(result)
	ResultFile.Sync()
	fmt.Printf("RESULT %s/%s conn=%d valid=%d failed=%d dial_failed=%d rps=%.1f p99=%s\n", result.Transport, result.Name, result.Connections, result.Validated, result.Failed, result.DialFailed, result.SuccessRPS, result.P99)
}
func Timeline(name, transport string, m map[string]Metrics) {
	OutputMu.Lock()
	defer OutputMu.Unlock()
	json.NewEncoder(TimelineFile).Encode(map[string]any{"scenario": name, "transport": transport, "metrics": m})
}

// Load 使用闭环请求；silent/trickle 的断开计入 Failed，并作为预期防护结果单独解释。
func (r *Rig) Load(o Options) Result {
	result := Result{Name: o.Name, Transport: r.Transport, Connections: o.Connections, Payload: o.Payload}
	request, err := Packet(o.Payload, 1)
	if err != nil {
		result.Notes = err.Error()
		Record(result)
		return result
	}
	var c Counters
	var ready, workers sync.WaitGroup
	var sessions sync.Map
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := make(chan struct{})
	for i := 0; i < o.Connections; i++ {
		ready.Add(1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			s, e := r.Dial()
			if e != nil {
				c.DialFailed.Add(1)
				ready.Done()
				return
			}
			connected := time.Now()
			c.Connected.Add(1)
			sessions.Store(s, true)
			defer func() { s.Close(); sessions.Delete(s) }()
			ready.Done()
			<-start
			if o.Silent || o.Trickle {
				if o.Trickle {
					go func() {
						for j := 0; j < len(request); j++ {
							select {
							case <-ctx.Done():
								return
							case <-time.After(3 * time.Second):
							}
							if s.Send(request[j:j+1]) != nil {
								return
							}
						}
					}()
				}
				s.Deadline(time.Now().Add(o.Duration + 3*time.Second))
				_, e = s.Receive()
				if e != nil && ctx.Err() == nil {
					c.Failed.Add(1)
					c.mu.Lock()
					c.Disconnects = append(c.Disconnects, time.Since(connected).Seconds())
					c.mu.Unlock()
				}
				return
			}
			for ctx.Err() == nil {
				begin := time.Now()
				s.Deadline(begin.Add(5 * time.Second))
				e = s.Send(request)
				if e == nil {
					c.Sent.Add(1)
				}
				if e == nil && !o.NoRead {
					if o.ReadDelay > 0 {
						select {
						case <-ctx.Done():
							c.Interrupted.Add(1)
							return
						case <-time.After(o.ReadDelay):
						}
					}
					var body []byte
					body, e = s.Receive()
					if e == nil {
						e = Validate(body, 2, o.Payload)
					}
				}
				if e != nil {
					if ctx.Err() == nil {
						c.Failed.Add(1)
					} else {
						c.Interrupted.Add(1)
					}
					return
				}
				if !o.NoRead {
					c.Observe(time.Since(begin))
					c.Validated.Add(1)
				}
				if o.Interval > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(o.Interval):
					}
				}
			}
		}()
	}
	ready.Wait()
	result.Before = r.Snapshot()
	begin := time.Now()
	close(start)
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		if !o.Probe {
			return
		}
		time.Sleep(time.Second)
		for i := 0; i < 10; i++ {
			if r.Probe() == nil {
				c.ProbeOK.Add(1)
			} else {
				c.ProbeFailed.Add(1)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	end := time.NewTimer(o.Duration)
	tick := time.NewTicker(5 * time.Second)
	progress := time.NewTicker(30 * time.Second)
	for running := true; running; {
		select {
		case <-end.C:
			running = false
		case <-tick.C:
			Timeline(o.Name, r.Transport, r.Snapshot())
		case <-progress.C:
			fmt.Printf("PROGRESS %s/%s elapsed=%s validated=%d failed=%d\n", r.Transport, o.Name, time.Since(begin).Round(time.Second), c.Validated.Load(), c.Failed.Load())
		}
	}
	result.During = r.Snapshot()
	result.Seconds = time.Since(begin).Seconds()
	//结束时取消的在途请求单列，不当作服务端丢包或请求超时。
	cancel()
	sessions.Range(func(key, value any) bool { key.(Session).Close(); return true })
	workers.Wait()
	<-probeDone
	tick.Stop()
	progress.Stop()
	end.Stop()
	result.Connected = c.Connected.Load()
	result.DialFailed = c.DialFailed.Load()
	result.Sent = c.Sent.Load()
	result.Validated = c.Validated.Load()
	result.Failed = c.Failed.Load()
	result.Interrupted = c.Interrupted.Load()
	result.SuccessRPS = float64(result.Validated) / result.Seconds
	result.P50 = c.Percentile(.5)
	result.P95 = c.Percentile(.95)
	result.P99 = c.Percentile(.99)
	result.ProbeOK = c.ProbeOK.Load()
	result.ProbeFailed = c.ProbeFailed.Load()
	if len(c.Disconnects) > 0 {
		sort.Float64s(c.Disconnects)
		result.DisconnectMin = c.Disconnects[0]
		result.DisconnectMax = c.Disconnects[len(c.Disconnects)-1]
	}
	time.Sleep(time.Second)
	result.After = r.Snapshot()
	Record(result)
	return result
}

func (r *Rig) Restart() error {
	before := r.Snapshot()
	begin := time.Now()
	r.Stop("actorSer")
	time.Sleep(500 * time.Millisecond)
	if e := r.Start("actorSer"); e != nil {
		return e
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if e := r.Probe(); e == nil {
			Record(Result{Name: "backend_restart", Transport: r.Transport, Seconds: time.Since(begin).Seconds(), Validated: 1, Before: before, After: r.Snapshot(), Notes: "仅重启隔离Actor进程；恢复时间为探针观察值，非精确故障切换耗时"})
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return errors.New("backend restart did not recover")
}

func Profile(r *Rig, out string) {
	var wg sync.WaitGroup
	for _, role := range []string{"gateway", "actorSer"} {
		wg.Add(1)
		go func(role string) {
			defer wg.Done()
			client := http.Client{Timeout: 15 * time.Second}
			response, e := client.Get(r.Children[role].Diagnostic + "/debug/pprof/profile?seconds=10")
			if e != nil {
				return
			}
			defer response.Body.Close()
			body, e := io.ReadAll(response.Body)
			if e == nil && response.StatusCode == 200 {
				os.WriteFile(filepath.Join(out, r.Transport+"-"+role+".pprof"), body, 0600)
			}
		}(role)
	}
	wg.Wait()
}

func Suite() error {
	out := flag.String("out", "", "result directory (required)")
	sample := flag.Duration("sample", 30*time.Second, "duration of each steady sample")
	repeats := flag.Int("repeat", 3, "independent samples per connection tier")
	soak := flag.Duration("soak", 30*time.Minute, "simultaneous TCP/WS moderate-load endurance duration")
	quick := flag.Bool("quick", false, "short smoke run; not a performance acceptance run")
	flag.Parse()
	if *out == "" || *sample <= 0 || *repeats < 1 || *soak < 0 {
		return errors.New("invalid parameters")
	}
	if *quick {
		*sample = 2 * time.Second
		*repeats = 1
		*soak = 0
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	var err error
	ResultFile, err = os.Create(filepath.Join(*out, "results.jsonl"))
	if err != nil {
		return err
	}
	defer ResultFile.Close()
	TimelineFile, err = os.Create(filepath.Join(*out, "metrics.jsonl"))
	if err != nil {
		return err
	}
	defer TimelineFile.Close()
	metadata := map[string]any{"started": time.Now(), "go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "cpus": runtime.NumCPU(), "sample": sample.String(), "repeat": *repeats, "soak": soak.String(), "quick": *quick, "scope": "本机隔离网络/模块测试；五个服务进程与压测端共享机器，无真实Mongo/Memcached；大包用测试handler回传unknown字段"}
	data, _ := json.MarshalIndent(metadata, "", "  ")
	os.WriteFile(filepath.Join(*out, "metadata.json"), data, 0600)
	var failures []string
	for _, transport := range []string{"tcp", "ws"} {
		r, err := NewRig(transport, 0, 2048, 4)
		if err != nil {
			return err
		}
		func() {
			defer r.Close()
			warm := r.Load(Options{Name: "warmup", Connections: 10, Duration: *sample, Payload: 2})
			if warm.Failed > 0 || warm.DialFailed > 0 || warm.Validated == 0 {
				failures = append(failures, transport+" warmup")
			}
			tiers := []int{1, 100, 500, 1000}
			if *quick {
				tiers = []int{1, 20}
			}
			for _, n := range tiers {
				for repeat := 1; repeat <= *repeats; repeat++ {
					result := r.Load(Options{Name: fmt.Sprintf("tier_%d_run_%d", n, repeat), Connections: n, Duration: *sample, Payload: 2})
					if result.Failed > 0 || result.DialFailed > 0 || result.Validated == 0 {
						failures = append(failures, result.Name+transport)
					}
				}
			}
			result := r.Load(Options{Name: "max_legal_packet", Connections: 100, Duration: *sample, Payload: 4092})
			if result.Failed > 0 || result.DialFailed > 0 {
				failures = append(failures, transport+" large")
			}
			if !*quick {
				result = r.Load(Options{Name: "slow_reader", Connections: 20, Duration: 15 * time.Second, Payload: 4092, ReadDelay: 100 * time.Millisecond})
				if result.Failed > 0 {
					failures = append(failures, transport+" slow_reader")
				}
				result = r.Load(Options{Name: "no_read", Connections: 4, Duration: 20 * time.Second, Payload: 4092, NoRead: true, Interval: time.Millisecond})
				if result.Failed == 0 {
					failures = append(failures, transport+" no_read did not trigger closure")
				}
				if err = r.Probe(); err != nil {
					failures = append(failures, transport+" recovery after no_read")
				}
				for _, trickle := range []bool{false, true} {
					name := "silent_first_packet"
					count := 1000
					if trickle {
						name = "trickle_first_packet"
						count = 20
					}
					result = r.Load(Options{Name: name, Connections: count, Duration: 14 * time.Second, Payload: 2, Silent: !trickle, Trickle: trickle})
					if result.Failed != int64(count) || result.DisconnectMax > 13 || result.DisconnectMin < 9 {
						failures = append(failures, transport+" "+name)
					}
				}
				if err = r.Restart(); err != nil {
					failures = append(failures, transport+" restart: "+err.Error())
				}
				profileDone := make(chan struct{})
				go func() { defer close(profileDone); time.Sleep(time.Second); Profile(r, *out) }()
				r.Load(Options{Name: "cpu_profile_load", Connections: 100, Duration: 15 * time.Second, Payload: 2})
				<-profileDone
			}
		}()
	}
	if !*quick {
		r, err := NewRig("tcp", 10*time.Millisecond, 16, 4)
		if err != nil {
			return err
		}
		result := r.Load(Options{Name: "slow_module_isolation", Connections: 100, Duration: 15 * time.Second, Payload: 2, Probe: true})
		r.Close()
		if result.ProbeOK != 10 || result.ProbeFailed != 0 || result.During["actorSer"].ServerDrops["protocol_queue_full"] <= result.Before["actorSer"].ServerDrops["protocol_queue_full"] {
			failures = append(failures, "slow module isolation")
		}
		for _, level := range []int{0, 4} {
			r, err = NewRig("tcp", 0, 2048, level)
			if err != nil {
				return err
			}
			r.Load(Options{Name: fmt.Sprintf("logging_level_%d", level), Connections: 10, Duration: 10 * time.Second, Payload: 2})
			r.Close()
		}
	}
	if *soak > 0 {
		var rigs []*Rig
		for _, transport := range []string{"tcp", "ws"} {
			r, e := NewRig(transport, 0, 2048, 4)
			if e != nil {
				for _, started := range rigs {
					started.Close()
				}
				return e
			}
			rigs = append(rigs, r)
		}
		var wg sync.WaitGroup
		results := make(chan Result, 2)
		for _, r := range rigs {
			wg.Add(1)
			go func(r *Rig) {
				defer wg.Done()
				defer r.Close()
				results <- r.Load(Options{Name: "endurance", Connections: 100, Duration: *soak, Payload: 1024, Interval: 10 * time.Millisecond})
			}(r)
		}
		wg.Wait()
		close(results)
		for result := range results {
			if result.Failed > 0 || result.DialFailed > 0 || result.Validated == 0 {
				failures = append(failures, result.Transport+" endurance")
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("failed scenario checks: %s", strings.Join(failures, ", "))
	}
	fmt.Println("PASS: local scenarios passed; inspect results and metrics before making capacity claims")
	return nil
}

func main() {
	var err error
	if len(os.Args) > 2 && os.Args[1] == "child" {
		err = ChildMain()
	} else {
		err = Suite()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
