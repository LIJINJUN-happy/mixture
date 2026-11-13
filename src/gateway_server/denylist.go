package gateway_server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"time"

	"mixture/src/common/printer"
	"mixture/src/common/tools"
)

const (
	DenyListFileName        = "denylist.json"  // 封禁记录的 JSON 文件名，文件与 system.txt 放在同一目录。
	DenyListStrikeLimit     = 3                // 时间窗口内触发封禁所需的非法包次数。
	DenyListMaxTrackedIPs   = 100000           // 内存中最多同时跟踪的未封禁 IP 数量。
	DenyListStrikeWindow    = 10 * time.Minute // 累计非法包次数的时间窗口。
	DenyListBanDuration     = time.Hour        // 触发封禁后拒绝该 IP 的时长。
	DenyListFlushInterval   = time.Second      // 合并变更后尝试写盘的间隔。
	DenyListCleanupInterval = time.Minute      // 清理过期违规次数和封禁记录的间隔。
)

// 记录某个 IP 在当前时间窗口内首次违规时间和累计次数。
type DenyListStrike struct {
	first time.Time // 当前统计窗口内首次收到非法包的时间
	count int       // 当前统计窗口内收到的非法包次数
}

// 写入JSON的封禁记录，包含原因和到期时间。
type DenyListBan struct {
	Reason    string    `json:"reason"`    // 封禁原因，写入 JSON 供排查
	ExpiresAt time.Time `json:"expiresAt"` // 封禁到期时间；到期后允许重新连接
}

// 管理运行期违规次数、封禁状态及持久化通知。
type DenyListStore struct {
	mu      sync.Mutex                // 保护 strikes 和 bans 两张内存表
	writeMu sync.Mutex                // 串行化写盘，避免并发替换 JSON 文件
	path    string                    // denylist.json 的完整路径
	strikes map[string]DenyListStrike // IP 到当前窗口违规次数的映射，仅保存在内存中
	bans    map[string]DenyListBan    // IP 到封禁记录的映射，定期写入 JSON
	changed chan struct{}             // 合并多次变更，通知后台协程写盘
}

// 当前网关进程使用的 IP 封禁表，启动初始化后供接入和读协程共享。
var ClientDenyList *DenyListStore

// 在 system.txt 同目录加载封禁记录，并启动后台写盘协程。
func InitClientDenyList() error {
	systemPath, err := tools.ResolveFilePath("system.txt")
	if err != nil {
		return fmt.Errorf("resolve system.txt for denylist: %w", err)
	}
	systemPath, err = filepath.Abs(systemPath)
	if err != nil {
		return fmt.Errorf("resolve absolute system.txt path: %w", err)
	}
	store, err := LoadDenyList(filepath.Join(filepath.Dir(systemPath), DenyListFileName))
	if err != nil {
		return err
	}
	ClientDenyList = store
	go store.RunWriter()
	return nil
}

// 从指定路径加载未过期的封禁记录；文件不存在时返回空表。
func LoadDenyList(path string) (*DenyListStore, error) {
	store := &DenyListStore{
		path:    path,
		strikes: make(map[string]DenyListStrike),
		bans:    make(map[string]DenyListBan),
		changed: make(chan struct{}, 1),
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read denylist: %w", err)
	}
	if err := json.Unmarshal(data, &store.bans); err != nil {
		return nil, fmt.Errorf("parse denylist: %w", err)
	}
	if store.bans == nil {
		store.bans = make(map[string]DenyListBan)
	}
	now := time.Now()
	for ip, ban := range store.bans {
		if _, err := netip.ParseAddr(ip); err != nil || !now.Before(ban.ExpiresAt) {
			delete(store.bans, ip)
		}
	}
	return store, nil
}

// 从远端的 IP:端口地址提取并规范化 IP，兼容 IPv4 和 IPv6。
func DenyListIP(remoteAddr string) (string, bool) {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "", false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return "", false
	}
	return ip.Unmap().String(), true
}

// 判断远端 IP 是否仍在封禁期内，并清除查询时发现的过期记录。
func (d *DenyListStore) IsDenied(remoteAddr string) bool {
	ip, ok := DenyListIP(remoteAddr)
	if !ok {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	ban, found := d.bans[ip]
	if !found {
		return false
	}
	if time.Now().Before(ban.ExpiresAt) {
		return true
	}
	delete(d.bans, ip)
	d.MarkChanged()
	return false
}

// 记录一次非法包；达到窗口阈值时封禁该 IP 并返回 true。
func (d *DenyListStore) RecordInvalidPacket(remoteAddr string) bool {
	ip, ok := DenyListIP(remoteAddr)
	if !ok {
		return false
	}
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	if ban, found := d.bans[ip]; found && now.Before(ban.ExpiresAt) {
		return false
	}
	strike, found := d.strikes[ip]
	if !found && len(d.strikes) >= DenyListMaxTrackedIPs {
		return false
	}
	if !found || now.Sub(strike.first) >= DenyListStrikeWindow {
		strike = DenyListStrike{first: now}
	}
	strike.count++
	if strike.count < DenyListStrikeLimit {
		d.strikes[ip] = strike
		return false
	}
	delete(d.strikes, ip)
	d.bans[ip] = DenyListBan{Reason: "invalid_packet", ExpiresAt: now.Add(DenyListBanDuration)}
	d.MarkChanged()
	return true
}

// 通知后台协程写盘。调用方须持有 d.mu；通知通道会合并重复变更。
func (d *DenyListStore) MarkChanged() {
	select {
	case d.changed <- struct{}{}:
	default:
	}
}

// 定期合并写盘，并清理过期的违规次数和封禁记录。
func (d *DenyListStore) RunWriter() {
	flushTicker := time.NewTicker(DenyListFlushInterval)
	cleanupTicker := time.NewTicker(DenyListCleanupInterval)
	defer flushTicker.Stop()
	defer cleanupTicker.Stop()
	for {
		select {
		case <-flushTicker.C:
			select {
			case <-d.changed:
				if err := d.Flush(); err != nil {
					printer.TextError("Persist denylist failed", "err", err)
					d.MarkChanged()
				}
			default:
			}
		case <-cleanupTicker.C:
			d.CleanupExpired()
		}
	}
}

// 删除已过期的内存记录，并通知后台协程更新 JSON。
func (d *DenyListStore) CleanupExpired() {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	for ip, strike := range d.strikes {
		if now.Sub(strike.first) >= DenyListStrikeWindow {
			delete(d.strikes, ip)
		}
	}
	for ip, ban := range d.bans {
		if !now.Before(ban.ExpiresAt) {
			delete(d.bans, ip)
			d.MarkChanged()
		}
	}
}

// 将当前封禁表写入同目录临时文件，再替换目标 JSON 文件。
func (d *DenyListStore) Flush() error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.mu.Lock()
	data, err := json.MarshalIndent(d.bans, "", "  ")
	d.mu.Unlock()
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(d.path), ".denylist-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, d.path)
}
