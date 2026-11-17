// 并发压测客户端。运行方式见 test/concurrent/README.md。
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"mixture/proto/mutual_pb/pb"
)

type counters struct {
	active, connected, dialFailed atomic.Int64
	sent, received, failed        atomic.Int64
	latency                       [12]atomic.Int64
}

var limits = [...]time.Duration{
	time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond,
	10 * time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2 * time.Second,
}

func (s *counters) observe(d time.Duration) {
	for i, limit := range limits {
		if d <= limit {
			s.latency[i].Add(1)
			return
		}
	}
	s.latency[len(s.latency)-1].Add(1)
}

func (s *counters) percentile(p float64) string {
	total := s.received.Load()
	if total == 0 {
		return "-"
	}
	target := int64(float64(total)*p + 0.999999)
	var sum int64
	for i := range s.latency {
		sum += s.latency[i].Load()
		if sum >= target {
			if i == len(limits) {
				return ">2s"
			}
			return "≤" + limits[i].String()
		}
	}
	return "-"
}

func (s *counters) print(elapsed time.Duration, previous *int64) {
	recv := s.received.Load()
	seconds := elapsed.Seconds()
	if seconds <= 0 {
		seconds = 1
	}
	fmt.Printf("运行=%s 在线=%d 累计连接=%d 拨号失败=%d 已发=%d 回包=%d 请求失败=%d 回包速率=%.1f/s p50=%s p95=%s p99=%s\n",
		elapsed.Truncate(time.Second), s.active.Load(), s.connected.Load(), s.dialFailed.Load(),
		s.sent.Load(), recv, s.failed.Load(), float64(recv-*previous)/seconds,
		s.percentile(0.50), s.percentile(0.95), s.percentile(0.99))
	*previous = recv
}

func packet(payloadBytes int) ([]byte, error) {
	data, err := proto.Marshal(&pb.C2STest{Num: 1})
	if err != nil {
		return nil, err
	}
	if payloadBytes < len(data) || payloadBytes > 4092 {
		return nil, errors.New("payload-bytes must be between 2 and 4092")
	}
	if payloadBytes > len(data) {
		padding := -1
		for n := 0; n <= payloadBytes; n++ {
			if len(data)+protowire.SizeTag(127)+protowire.SizeBytes(n) == payloadBytes {
				padding = n
				break
			}
		}
		if padding < 0 {
			return nil, fmt.Errorf("payload size %d cannot be encoded exactly", payloadBytes)
		}
		//使用合法unknown字段增加请求大小，不修改正式protobuf定义或业务回包逻辑。
		data = protowire.AppendTag(data, 127, protowire.BytesType)
		data = protowire.AppendBytes(data, make([]byte, padding))
	}
	// 外层长度只计算包体；包体为 2 字节协议号 + protobuf 数据。
	bodyLen := 2 + len(data)
	out := make([]byte, 2+bodyLen)
	binary.BigEndian.PutUint16(out[:2], uint16(bodyLen))
	binary.BigEndian.PutUint16(out[2:4], 1) // C2STest
	copy(out[4:], data)
	return out, nil
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func readReply(conn net.Conn) error {
	var head [2]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return err
	}
	n := int(binary.BigEndian.Uint16(head[:]))
	if n < 2 || n > 4094 {
		return fmt.Errorf("非法回包长度 %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(conn, body); err != nil {
		return err
	}
	if binary.BigEndian.Uint16(body[:2]) != 2 { // S2CTestRet
		return fmt.Errorf("回包协议号不是 2: %d", binary.BigEndian.Uint16(body[:2]))
	}
	var reply pb.S2CTestRet
	if err := proto.Unmarshal(body[2:], &reply); err != nil {
		return err
	}
	if reply.Num != 2 {
		return fmt.Errorf("回包 Num=%d，期望 2", reply.Num)
	}
	return nil
}

// WebSocketStream 将二进制WS消息适配为现有压测工具的协议字节流；每连接仍只有一个读取者和写入者。
type WebSocketStream struct {
	*websocket.Conn
	Reader io.Reader //当前WS消息中尚未消费的数据
}

func (c *WebSocketStream) Read(data []byte) (int, error) {
	for {
		if c.Reader == nil {
			kind, reader, err := c.NextReader()
			if err != nil {
				return 0, err
			}
			if kind != websocket.BinaryMessage {
				return 0, errors.New("expected binary WebSocket message")
			}
			c.Reader = reader
		}
		n, err := c.Reader.Read(data)
		if err == io.EOF {
			c.Reader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *WebSocketStream) Write(data []byte) (int, error) {
	if err := c.WriteMessage(websocket.BinaryMessage, data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (c *WebSocketStream) SetDeadline(deadline time.Time) error {
	if err := c.SetReadDeadline(deadline); err != nil {
		return err
	}
	return c.SetWriteDeadline(deadline)
}

// DialTarget 根据地址选择TCP或WS/WSS，复用相同的请求校验和计数逻辑。
func DialTarget(address string, timeout time.Duration) (net.Conn, error) {
	if strings.HasPrefix(address, "ws://") || strings.HasPrefix(address, "wss://") {
		dialer := websocket.Dialer{HandshakeTimeout: timeout}
		conn, response, err := dialer.Dial(address, nil)
		if err != nil {
			if response != nil && response.Body != nil {
				response.Body.Close()
			}
			return nil, err
		}
		conn.SetReadLimit(4096)
		return &WebSocketStream{Conn: conn}, nil
	}
	return net.DialTimeout("tcp", address, timeout)
}

func worker(ctx context.Context, address, mode string, interval, timeout, readDelay time.Duration, request []byte, s *counters, conns *sync.Map, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := DialTarget(address, timeout)
	if err != nil {
		s.dialFailed.Add(1)
		return
	}
	s.connected.Add(1)
	s.active.Add(1)
	conns.Store(conn, struct{}{})
	defer func() {
		conns.Delete(conn)
		conn.Close()
		s.active.Add(-1)
	}()

	if mode == "connect" {
		var one [1]byte
		for ctx.Err() == nil {
			//由主协程在测试结束时Close唤醒；WS读取超时后不可继续重试读取。
			_, err := conn.Read(one[:])
			if err != nil {
				if ctx.Err() == nil {
					s.failed.Add(1) // 服务端提前断开
				}
				return
			}
		}
		return
	}

	tickInterval := interval
	if tickInterval <= 0 {
		tickInterval = time.Nanosecond
	}
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		start := time.Now()
		conn.SetDeadline(start.Add(timeout))
		if err := writeAll(conn, request); err != nil {
			if ctx.Err() == nil {
				s.failed.Add(1)
			}
			return
		}
		s.sent.Add(1)
		if mode == "noread" {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			continue
		}
		if readDelay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(readDelay):
			}
		}
		if err := readReply(conn); err != nil {
			if ctx.Err() == nil {
				s.failed.Add(1)
			}
			return
		}
		s.observe(time.Since(start))
		s.received.Add(1)
		if interval == 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func main() {
	address := flag.String("addr", "127.0.0.1:10230", "TCP地址或ws://host:port/ws、wss://host:port/ws")
	payloadBytes := flag.Int("payload-bytes", 2, "protobuf请求数据字节数，最大4092；不改变服务端回包大小")
	mode := flag.String("mode", "echo", "connect=只保持连接；echo=校验回显；noread=发送但不读回包")
	count := flag.Int("connections", 100, "目标并发连接数")
	rate := flag.Int("ramp", 100, "每秒创建的连接数")
	duration := flag.Duration("duration", 30*time.Second, "全部连接发起后保持的时间")
	interval := flag.Duration("interval", time.Second, "每连接发包节拍；0表示不主动限速")
	timeout := flag.Duration("timeout", 5*time.Second, "拨号和单次请求超时")
	readDelay := flag.Duration("read-delay", 0, "delay before reading each reply; included in RTT")
	flag.Parse()
	if *count < 1 || *rate < 1 || *duration <= 0 || *interval < 0 || *readDelay < 0 || *timeout <= 0 || (*mode != "echo" && *mode != "connect" && *mode != "noread") {
		fmt.Fprintln(os.Stderr, "参数无效；使用 -h 查看说明")
		os.Exit(2)
	}
	request, err := packet(*payloadBytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("目标=%s 模式=%s 连接数=%d 建连速率=%d/s 持续=%s 请求数据=%dB\n", *address, *mode, *count, *rate, *duration, *payloadBytes)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var s counters
	var conns sync.Map
	var wg sync.WaitGroup
	spacing := time.Second / time.Duration(*rate)
	if spacing < time.Nanosecond {
		spacing = time.Nanosecond
	}
	start := time.Now()
	for i := 0; i < *count; i++ {
		wg.Add(1)
		go worker(ctx, *address, *mode, *interval, *timeout, *readDelay, request, &s, &conns, &wg)
		if i+1 < *count {
			time.Sleep(spacing)
		}
	}
	fmt.Printf("建连已发起，用时 %s；开始保持阶段\n", time.Since(start).Truncate(time.Millisecond))
	last := s.received.Load()
	lastReport := time.Now()
	ticker := time.NewTicker(time.Second)
	end := time.NewTimer(*duration)
	defer ticker.Stop()
	defer end.Stop()
	for running := true; running; {
		select {
		case <-ticker.C:
			s.print(time.Since(lastReport), &last)
			lastReport = time.Now()
		case <-end.C:
			running = false
		}
	}
	fmt.Println("最终统计（关闭连接前）：")
	s.print(time.Since(lastReport), &last)
	cancel()
	conns.Range(func(key, _ any) bool { key.(net.Conn).Close(); return true })
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("TOTAL elapsed=%s sent=%d validated=%d failed=%d dial_failed=%d outstanding=%d success_rps=%.2f p95=%s p99=%s\n", elapsed, s.sent.Load(), s.received.Load(), s.failed.Load(), s.dialFailed.Load(), s.sent.Load()-s.received.Load(), float64(s.received.Load())/elapsed.Seconds(), s.percentile(.95), s.percentile(.99))
}
