// 直接测试 database 的缓存封装；用法及测试边界见 README.md。
package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mixture/src/common/printer"
	"mixture/src/database"
)

// endpoint 是测试专用入口。正常时转发真实节点，或提供最小协议模拟；
// 故障时仅影响本程序经过此入口建立的连接。
type endpoint struct {
	listener net.Listener
	backend  string
	fault    string
	mu       sync.Mutex
	down     bool
	conns    map[net.Conn]struct{}
	items    sync.Map
}

func newEndpoint(backend, fault string) (*endpoint, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	e := &endpoint{listener: l, backend: backend, fault: fault, conns: make(map[net.Conn]struct{})}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			e.mu.Lock()
			e.conns[c] = struct{}{}
			down := e.down
			e.mu.Unlock()
			go e.serve(c, down)
		}
	}()
	return e, nil
}

func (e *endpoint) serve(c net.Conn, down bool) {
	defer func() { c.Close(); e.mu.Lock(); delete(e.conns, c); e.mu.Unlock() }()
	if down {
		if e.fault == "timeout" {
			io.Copy(io.Discard, c)
		}
		return
	}
	if e.backend != "" {
		remote, err := net.DialTimeout("tcp", e.backend, 2*time.Second)
		if err != nil {
			return
		}
		defer remote.Close()
		done := make(chan struct{})
		go func() { io.Copy(remote, c); remote.Close(); close(done) }()
		io.Copy(c, remote)
		c.Close()
		<-done
		return
	}
	// 只实现本测试所用的文本协议；不模拟真实 Memcached 性能或淘汰策略。
	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			return
		}
		switch parts[0] {
		case "version":
			fmt.Fprint(c, "VERSION test-simulator\r\n")
		case "get", "gets":
			for _, key := range parts[1:] {
				if value, ok := e.items.Load(key); ok {
					data := value.([]byte)
					if parts[0] == "gets" {
						fmt.Fprintf(c, "VALUE %s 0 %d 1\r\n", key, len(data))
					} else {
						fmt.Fprintf(c, "VALUE %s 0 %d\r\n", key, len(data))
					}
					c.Write(data)
					fmt.Fprint(c, "\r\n")
				}
			}
			fmt.Fprint(c, "END\r\n")
		case "set":
			if len(parts) != 5 {
				return
			}
			n, err := strconv.Atoi(parts[4])
			if err != nil || n < 0 || n > 1024*1024 {
				return
			}
			data := make([]byte, n+2)
			if _, err := io.ReadFull(r, data); err != nil {
				return
			}
			e.items.Store(parts[1], data[:n])
			fmt.Fprint(c, "STORED\r\n")
		case "delete":
			if len(parts) != 2 {
				return
			}
			if _, ok := e.items.LoadAndDelete(parts[1]); ok {
				fmt.Fprint(c, "DELETED\r\n")
			} else {
				fmt.Fprint(c, "NOT_FOUND\r\n")
			}
		default:
			fmt.Fprint(c, "ERROR\r\n")
		}
	}
}

func (e *endpoint) breakConnections() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.down = true
	for c := range e.conns {
		c.Close()
	}
}

func (e *endpoint) close() { e.listener.Close(); e.breakConnections() }

// 使用临时 SQLite 初始化原有接口，避免改动项目 net_sql.db 或业务代码。
func initialize(address string) (string, error) {
	dir, err := os.MkdirTemp("", "mixture-memcache-test-")
	if err != nil {
		return "", err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return dir, err
	}
	if err := os.WriteFile(filepath.Join(dir, "system.txt"), []byte("slogLevel=4\n"), 0600); err != nil {
		return dir, err
	}
	if err := os.Chdir(dir); err != nil {
		return dir, err
	}
	defer os.Chdir(cwd)
	printer.InitPrinterMgr()
	printer.SetLoggerObj(printer.CreateLogger("memcache-test", dir+string(os.PathSeparator), false))
	db, err := sql.Open("sqlite3", filepath.Join(dir, "net_sql.db"))
	if err != nil {
		return dir, err
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE actorSerConfig (actorSerId INTEGER, memCacheAddress TEXT, memCachePort TEXT)"); err != nil {
		return dir, err
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return dir, err
	}
	if _, err = db.Exec("INSERT INTO actorSerConfig VALUES (1, ?, ?)", host, port); err != nil {
		return dir, err
	}
	database.MemCacheConnectClientInit("actor_server", "1")
	return dir, nil
}

type result struct {
	loads       atomic.Int64
	activeLoads atomic.Int64
	peakLoads   atomic.Int64
	failed      atomic.Int64
	durations   []time.Duration
	elapsed     time.Duration
}

func measure(key string, workers, requests int, delay time.Duration, payload []byte) *result {
	s := &result{durations: make([]time.Duration, requests)}
	var next atomic.Int64
	var wg, ready sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		wg.Add(1)
		ready.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			for {
				i := int(next.Add(1) - 1)
				if i >= requests {
					return
				}
				begin := time.Now()
				data, err := database.MemCacheGetOrLoad(key, func() ([]byte, error) {
					s.loads.Add(1)
					active := s.activeLoads.Add(1)
					defer s.activeLoads.Add(-1)
					for old := s.peakLoads.Load(); active > old; old = s.peakLoads.Load() {
						if s.peakLoads.CompareAndSwap(old, active) {
							break
						}
					}
					time.Sleep(delay) // 模拟数据库查询；不连接 MongoDB。
					return bytes.Clone(payload), nil
				})
				s.durations[i] = time.Since(begin)
				if err != nil || !bytes.Equal(data, payload) {
					s.failed.Add(1)
				}
			}
		}()
	}
	ready.Wait()
	begin := time.Now()
	close(start)
	wg.Wait()
	s.elapsed = time.Since(begin)
	sort.Slice(s.durations, func(i, j int) bool { return s.durations[i] < s.durations[j] })
	return s
}

func (s *result) print(name string) {
	n := len(s.durations)
	percentile := func(p int) time.Duration { return s.durations[(n*p+99)/100-1] }
	fmt.Printf("\n[%s] 请求=%d 业务错误/内容不符=%d 耗时=%s 吞吐=%.1f请求/秒\n", name, n, s.failed.Load(), s.elapsed.Round(time.Millisecond), float64(n)/s.elapsed.Seconds())
	fmt.Printf("回源次数=%d 回源比例=%.1f%% 回源并发峰值=%d p50=%s p95=%s p99=%s\n", s.loads.Load(), 100*float64(s.loads.Load())/float64(n), s.peakLoads.Load(), percentile(50), percentile(95), percentile(99))
}

func run() error {
	addr := flag.String("addr", "", "真实 Memcached/Proxy 地址；留空使用本地协议模拟端")
	workers := flag.Int("workers", 100, "并发工作协程数；热点场景每个协程发一个请求")
	requests := flag.Int("requests", 1000, "命中及故障场景各自的总请求数")
	delay := flag.Duration("loader-delay", 50*time.Millisecond, "模拟数据库每次查询耗时")
	fault := flag.String("fault", "timeout", "timeout=连接可建立但不响应；disconnect=断开连接")
	size := flag.Int("value-size", 256, "测试缓存值字节数")
	flag.Parse()
	if *workers < 1 || *workers > 10000 || *requests < *workers || *requests > 1000000 || *delay < 0 || *size < 1 || *size > 1024*1024 || (*fault != "timeout" && *fault != "disconnect") {
		return fmt.Errorf("参数无效：workers 1..10000，requests workers..1000000，loader-delay >= 0，value-size 1..1048576，fault timeout/disconnect")
	}
	e, err := newEndpoint(*addr, *fault)
	if err != nil {
		return err
	}
	defer e.close()
	dir, err := initialize(e.listener.Addr().String())
	if err != nil {
		return err
	}
	fmt.Println("测试日志及临时配置目录：", dir)
	if *addr == "" {
		fmt.Println("模式：本地协议模拟，只验证封装行为，数值不代表真实 Memcached 容量。")
	} else {
		fmt.Println("模式：真实缓存，地址：", *addr, "（经过本地故障注入入口）")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	prefix := "mixture:test:" + hex.EncodeToString(id[:]) + ":"
	fmt.Println("本次独立 key 前缀：", prefix)
	payload := bytes.Repeat([]byte{'x'}, *size)
	if _, err := database.MemCacheCmd("SET", prefix+"hit", payload); err != nil {
		return fmt.Errorf("预热失败: %w", err)
	}
	if value, err := database.MemCacheCmd("GET", prefix+"hit", nil); err != nil {
		return fmt.Errorf("预热后 GET 失败: %w", err)
	} else if value != string(payload) {
		return fmt.Errorf("预热后 GET 内容不符")
	}
	hit := measure(prefix+"hit", *workers, *requests, *delay, payload)
	hit.print("01 缓存命中")

	// DEL 只操作本次随机生成的 key，模拟失效瞬间；不等待固定的 15 分钟 TTL。
	if _, err := database.MemCacheCmd("SET", prefix+"hot", payload); err != nil {
		return err
	}
	if _, err := database.MemCacheCmd("DEL", prefix+"hot", nil); err != nil {
		return err
	}
	hot := measure(prefix+"hot", *workers, *workers, *delay, payload)
	hot.print("02 热点 key 失效，同批并发访问")
	if hot.loads.Load() > 1 {
		fmt.Println("观察：同一个 key 发生重复回源；当前封装没有合并这些并发加载。")
	}

	e.breakConnections()
	if _, err := database.MemCacheCmd("GET", prefix+"hit", nil); err == nil {
		return fmt.Errorf("故障注入未生效")
	} else {
		fmt.Println("\n故障注入检查：", err)
	}
	down := measure(prefix+"down", *workers, *requests, *delay, payload)
	down.print("03 缓存不可用，模拟数据库回源")
	fmt.Println("说明：业务成功可能来自回源，不能据此认定缓存可用；请求延迟不包含异步回写完成时间。")
	fmt.Println("测试 key 不执行全库清理，真实缓存中的测试数据按框架 15 分钟 TTL 过期。")
	if hit.failed.Load()+hot.failed.Load()+down.failed.Load() != 0 || hit.loads.Load() != 0 || hot.loads.Load() == 0 || down.loads.Load() != int64(*requests) {
		return fmt.Errorf("结果不符合当前封装预期，请查看统计及日志（真实节点驱逐也会导致命中场景回源）")
	}
	fmt.Println("PASS：三个场景的数据及故障注入检查通过；热点重复回源、故障回源放大属于观察结果，不代表已解决。")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}
