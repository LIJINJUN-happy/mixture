// 远程验收编排器：只访问指定业务/诊断端口，不修改或重启远端服务。
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type scenario struct {
	Name                          string
	Connections, Ramp, Payload    int
	Duration, Interval, ReadDelay string
	Mode                          string
}

type result struct {
	Scenario     scenario
	Start, End   time.Time
	Total, Error string
}

var client = &http.Client{Timeout: 4 * time.Second, Transport: &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 4}}

// fetch 保留原始JSON及本地时间，便于按同一进程、同一负载窗口计算增量。
func fetch(ctx context.Context, host string, port int) map[string]any {
	start := time.Now()
	record := map[string]any{"local_time": start, "port": port}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d/metrics", host, port), nil)
	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("HTTP %d", resp.StatusCode)
		} else {
			var data map[string]any
			err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&data)
			record["metrics"] = data
		}
	}
	if err != nil {
		record["error"] = err.Error()
	}
	record["elapsed_ms"] = float64(time.Since(start).Microseconds()) / 1000
	return record
}

func snapshot(ctx context.Context, host, name, dir string) error {
	data := make([]map[string]any, 5)
	for i := range data {
		data[i] = fetch(ctx, host, 6060+i)
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name+".json"), b, 0644)
}

func download(ctx context.Context, host string, port int, route, path string) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%d%s", host, port, route), nil)
	c := &http.Client{Timeout: 40 * time.Second, Transport: client.Transport}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, io.LimitReader(resp.Body, 32<<20))
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

// idleProbe 先完成合法请求，再测空闲连接读超时；不会发送非法包触发denylist。
func idleProbe(ctx context.Context, address, dir string) {
	start := time.Now()
	record := map[string]any{"start": start}
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err == nil {
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		_, err = conn.Write([]byte{0, 4, 0, 1, 8, 1})
		var reply [6]byte
		if err == nil {
			_, err = io.ReadFull(conn, reply[:])
		}
		if err == nil && (binary.BigEndian.Uint16(reply[:2]) != 4 || binary.BigEndian.Uint16(reply[2:4]) != 2 || reply[4] != 8 || reply[5] != 2) {
			err = fmt.Errorf("unexpected reply %x", reply)
		}
		if err == nil {
			idleStart := time.Now()
			conn.SetReadDeadline(idleStart.Add(205 * time.Second))
			var data [1]byte
			_, readErr := conn.Read(data[:])
			record["idle_seconds"] = time.Since(idleStart).Seconds()
			if readErr != nil {
				record["read_result"] = readErr.Error()
			}
		}
	}
	if err != nil {
		record["error"] = err.Error()
	}
	record["end"] = time.Now()
	b, _ := json.MarshalIndent(record, "", "  ")
	os.WriteFile(filepath.Join(dir, "idle-after-valid.json"), b, 0644)
	fmt.Printf("IDLE_PROBE %s\n", b)
}

// streamProbes 只使用合法包，覆盖TCP分片、合包和首包截止时间，不触发非法包封禁。
func streamProbes(address, dir string) {
	var records []map[string]any
	for _, name := range []string{"fragmented", "coalesced-100", "first-packet-before-deadline", "partial-packet-timeout"} {
		record := map[string]any{"name": name, "start": time.Now()}
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err == nil {
			conn.SetDeadline(time.Now().Add(15 * time.Second))
			packet := []byte{0, 4, 0, 1, 8, 1}
			switch name {
			case "fragmented", "first-packet-before-deadline":
				_, err = conn.Write(packet[:1])
				if name == "first-packet-before-deadline" {
					time.Sleep(8 * time.Second)
				} else {
					time.Sleep(10 * time.Millisecond)
				}
				if err == nil {
					_, err = conn.Write(packet[1:])
				}
			case "coalesced-100":
				var batch []byte
				for i := 0; i < 100; i++ {
					batch = append(batch, packet...)
				}
				_, err = conn.Write(batch)
			case "partial-packet-timeout":
				_, err = conn.Write(packet[:1])
				if err == nil {
					var data [1]byte
					_, err = conn.Read(data[:])
					record["close_seconds"] = time.Since(record["start"].(time.Time)).Seconds()
					if err == io.EOF {
						record["expected_close"] = true
						err = nil
					} else if err == nil {
						err = fmt.Errorf("unexpected reply to partial packet")
					}
				}
			}
			if err == nil && name != "partial-packet-timeout" {
				count := 1
				if name == "coalesced-100" {
					count = 100
				}
				for i := 0; i < count; i++ {
					var reply [6]byte
					_, err = io.ReadFull(conn, reply[:])
					if err != nil {
						break
					}
					if string(reply[:]) != string([]byte{0, 4, 0, 2, 8, 2}) {
						err = fmt.Errorf("unexpected reply %x", reply)
						break
					}
				}
				record["validated"] = err == nil
			}
			conn.Close()
		}
		if err != nil {
			record["error"] = err.Error()
		}
		record["end"] = time.Now()
		records = append(records, record)
		fmt.Printf("STREAM_PROBE %s err=%v\n", name, err)
	}
	b, _ := json.MarshalIndent(records, "", "  ")
	os.WriteFile(filepath.Join(dir, "stream-probes.json"), b, 0644)
}

// stallProbe 缩小本地接收窗口后只发送合法包，不读取回包；最多运行45秒且只占一条连接。
// 与默认socket缓冲的不读测试相比，更容易触发服务端写队列和写超时保护。
func stallProbe(address, dir string) {
	start := time.Now()
	record := map[string]any{"start": start, "receive_buffer_requested": 1024}
	raw, err := net.DialTimeout("tcp", address, 5*time.Second)
	packets := 0
	if err == nil {
		conn := raw.(*net.TCPConn)
		defer conn.Close()
		err = conn.SetReadBuffer(1024)
		if err == nil {
			batch := []byte{0, 4, 0, 1, 8, 1, 0, 4, 0, 1, 8, 1, 0, 4, 0, 1, 8, 1, 0, 4, 0, 1, 8, 1}
			ticker := time.NewTicker(2 * time.Millisecond)
			defer ticker.Stop()
			for time.Since(start) < 45*time.Second {
				conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				n, writeErr := conn.Write(batch)
				packets += n / 6
				if writeErr != nil {
					err = writeErr
					break
				}
				<-ticker.C
			}
		}
	}
	record["sent_packets"] = packets
	record["elapsed_seconds"] = time.Since(start).Seconds()
	if err != nil {
		record["error"] = err.Error()
	}
	record["end"] = time.Now()
	b, _ := json.MarshalIndent(record, "", "  ")
	os.WriteFile(filepath.Join(dir, "small-window-noread.json"), b, 0644)
	fmt.Printf("STALL_PROBE %s\n", b)
}

func run(ctx context.Context, load, address, host, dir string, s scenario) result {
	r := result{Scenario: s, Start: time.Now()}
	fmt.Printf("START %s %s\n", s.Name, r.Start.Format(time.RFC3339))
	snapshot(ctx, host, s.Name+"-before", dir)
	args := []string{"-addr", address, "-connections", strconv.Itoa(s.Connections), "-ramp", strconv.Itoa(s.Ramp), "-duration", s.Duration, "-interval", s.Interval, "-timeout", "5s"}
	if s.Payload != 0 {
		args = append(args, "-payload-bytes", strconv.Itoa(s.Payload))
	}
	if s.ReadDelay != "" {
		args = append(args, "-read-delay", s.ReadDelay)
	}
	if s.Mode != "" {
		args = append(args, "-mode", s.Mode)
	}
	cmd := exec.CommandContext(ctx, load, args...)
	// 原始逐秒输出独立存档，避免只保留TOTAL导致看不到掉线和稳态波动。
	f, err := os.Create(filepath.Join(dir, s.Name+".log"))
	if err != nil {
		r.Error = err.Error()
		return r
	}
	var out strings.Builder
	cmd.Stdout = io.MultiWriter(f, &out)
	cmd.Stderr = cmd.Stdout
	var profiles sync.WaitGroup
	if s.Name == "echo-100" {
		for _, port := range []int{6060, 6061} {
			profiles.Add(1)
			go func(port int) {
				defer profiles.Done()
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Second):
				}
				err := download(ctx, host, port, "/debug/pprof/profile?seconds=30", filepath.Join(dir, fmt.Sprintf("cpu-%d.pprof", port)))
				fmt.Printf("CPU_PROFILE port=%d err=%v\n", port, err)
			}(port)
		}
	}
	err = cmd.Run()
	f.Close()
	profiles.Wait()
	if err != nil {
		r.Error = err.Error()
	}
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "TOTAL ") {
			r.Total = line
		}
	}
	r.End = time.Now()
	snapshot(ctx, host, s.Name+"-after", dir)
	fmt.Printf("END %s %s error=%s\n", s.Name, r.Total, r.Error)
	return r
}

func main() {
	host := flag.String("host", "113.45.222.72", "diagnostic host (6060..6064)")
	address := flag.String("addr", "113.45.222.72:7891", "business TCP address")
	load := flag.String("load", "", "prebuilt concurrent load executable")
	dir := flag.String("out", "", "new artifacts directory")
	suite := flag.String("suite", "initial", "initial, recheck, edges, edges-low, stall, or soak")
	flag.Parse()
	if *load == "" || *dir == "" {
		panic("-load and -out are required")
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics, err := os.Create(filepath.Join(*dir, "metrics.jsonl"))
	if err != nil {
		panic(err)
	}
	var sampling sync.WaitGroup
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		defer metrics.Close()
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		enc := json.NewEncoder(metrics)
		for {
			for port := 6060; port <= 6064; port++ {
				enc.Encode(fetch(ctx, *host, port))
			}
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
	}()
	if err := snapshot(ctx, *host, "baseline", *dir); err != nil {
		panic(err)
	}
	if *suite == "edges" {
		streamProbes(*address, *dir)
	}
	var scenarios []scenario
	switch *suite {
	case "initial":
		for _, n := range []int{1, 10, 25, 50} {
			for repeat := 1; repeat <= 3; repeat++ {
				scenarios = append(scenarios, scenario{Name: fmt.Sprintf("echo-%d-r%d", n, repeat), Connections: n, Ramp: 50, Duration: "30s", Interval: "0"})
			}
		}
		scenarios = append(scenarios, scenario{Name: "echo-100", Connections: 100, Ramp: 50, Duration: "60s", Interval: "0"})
		for _, n := range []int{250, 500, 1000} {
			scenarios = append(scenarios, scenario{Name: fmt.Sprintf("online-%d", n), Connections: n, Ramp: 100, Duration: "45s", Interval: "1s"})
		}
	case "recheck":
		scenarios = []scenario{
			{Name: "recovery-before-recheck", Connections: 10, Ramp: 10, Duration: "20s", Interval: "0"},
			{Name: "echo-100-slow-ramp", Connections: 100, Ramp: 10, Duration: "45s", Interval: "0"},
		}
		for _, n := range []int{250, 500, 1000} {
			scenarios = append(scenarios, scenario{Name: fmt.Sprintf("online-%d", n), Connections: n, Ramp: 50, Duration: "45s", Interval: "1s"})
		}
	case "edges":
		scenarios = []scenario{
			{Name: "payload-1024", Connections: 20, Ramp: 20, Duration: "45s", Interval: "0", Payload: 1024},
			{Name: "payload-4092", Connections: 20, Ramp: 20, Duration: "45s", Interval: "0", Payload: 4092},
			{Name: "slowread", Connections: 20, Ramp: 20, Duration: "45s", Interval: "10ms", ReadDelay: "100ms"},
			{Name: "noread", Connections: 5, Ramp: 5, Duration: "60s", Interval: "1ms", Mode: "noread"},
			{Name: "first-packet-timeout", Connections: 20, Ramp: 20, Duration: "15s", Interval: "1s", Mode: "connect"},
			{Name: "recovery", Connections: 10, Ramp: 10, Duration: "30s", Interval: "0"},
		}
	case "soak":
		// 远端逐请求日志可能开启；持续阶段限速，避免把长时间稳定性测试变成日志磁盘压力测试。
		scenarios = []scenario{{Name: "soak-30min", Connections: 25, Ramp: 25, Duration: "30m", Interval: "100ms"}, {Name: "post-soak-recovery", Connections: 10, Ramp: 10, Duration: "30s", Interval: "0"}}
	case "edges-low":
		scenarios = []scenario{
			{Name: "payload-4092-paced", Connections: 20, Ramp: 5, Duration: "30s", Interval: "100ms", Payload: 4092},
			{Name: "slowread", Connections: 20, Ramp: 10, Duration: "45s", Interval: "10ms", ReadDelay: "100ms"},
			{Name: "noread", Connections: 5, Ramp: 5, Duration: "60s", Interval: "1ms", Mode: "noread"},
			{Name: "first-packet-timeout", Connections: 20, Ramp: 10, Duration: "15s", Interval: "1s", Mode: "connect"},
			{Name: "recovery", Connections: 10, Ramp: 10, Duration: "30s", Interval: "0"},
		}
	case "stall":
		stallProbe(*address, *dir)
		scenarios = []scenario{{Name: "post-stall-recovery", Connections: 10, Ramp: 10, Duration: "30s", Interval: "0"}}
	default:
		panic("unknown suite")
	}
	var idle sync.WaitGroup
	if *suite == "initial" {
		idle.Add(1)
		go func() { defer idle.Done(); idleProbe(ctx, *address, *dir) }()
	}
	var results []result
	failRE := regexp.MustCompile(`failed=(\d+) dial_failed=(\d+)`)
	for _, s := range scenarios {
		r := run(ctx, *load, *address, *host, *dir, s)
		results = append(results, r)
		b, _ := json.MarshalIndent(results, "", "  ")
		os.WriteFile(filepath.Join(*dir, "summary.json"), b, 0644)
		// 合法echo场景出现错误则暂停升压，避免在异常服务上继续加负载。
		m := failRE.FindStringSubmatch(r.Total)
		if r.Error != "" || r.Total == "" || (s.Mode == "" && len(m) == 3 && (m[1] != "0" || m[2] != "0")) {
			fmt.Printf("STOP: inspect errors before increasing load\n")
			break
		}
		// 留出连接回收和低频观测时间，不与下一档负载重叠。
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
	}
	idle.Wait()
	for _, port := range []int{6060, 6061} {
		for _, kind := range []string{"heap", "goroutine"} {
			err := download(ctx, *host, port, "/debug/pprof/"+kind, filepath.Join(*dir, fmt.Sprintf("%s-%d.pprof", kind, port)))
			fmt.Printf("PROFILE %s port=%d err=%v\n", kind, port, err)
		}
	}
	snapshot(ctx, *host, "final", *dir)
	cancel()
	sampling.Wait()
	fmt.Printf("DONE suite=%s artifacts=%s\n", *suite, *dir)
}
