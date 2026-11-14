package gateway_server

import (
	"database/sql"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"mixture/src/common/printer"
)

const (
	DenyListDBFileName      = "net_sql.db"     // 与网关配置共用的 SQLite 数据库文件。
	DenyListStrikeLimit     = 3                // 时间窗口内触发封禁所需的非法包次数。
	DenyListMaxTrackedIPs   = 100000           // 内存中最多同时跟踪的未封禁 IP 数量。
	DenyListStrikeWindow    = 10 * time.Minute // 累计非法包次数的时间窗口。
	DenyListBanDuration     = time.Hour        // 触发封禁后拒绝该 IP 的时长。
	DenyListFlushInterval   = time.Second      // 合并变更后尝试写入数据库的间隔。
	DenyListSyncInterval    = time.Second      // 从数据库同步其他网关封禁的间隔。
	DenyListCleanupInterval = time.Minute      // 清理过期违规次数和封禁记录的间隔。
	DenyListDBBusyTimeout   = 5 * time.Second  // 多进程同时写 SQLite 时等待锁的时长。
)

// 记录某个 IP 在当前时间窗口内首次违规时间和累计次数。
type DenyListStrike struct {
	first time.Time // 当前统计窗口内首次收到非法包的时间
	count int       // 当前统计窗口内收到的非法包次数
}

// 封禁记录，包含原因和到期时间。
type DenyListBan struct {
	Reason    string    // 封禁原因，写入 SQLite 供排查
	ExpiresAt time.Time // 封禁到期时间；到期后允许重新连接
}

// 管理运行期违规次数、封禁状态及 SQLite 持久化。
type DenyListStore struct {
	mu      sync.Mutex                // 保护 strikes、bans 和 pending 三张内存表
	writeMu sync.Mutex                // 串行化写入与同步，避免本进程的状态覆盖
	db      *sql.DB                   // 指向共享 net_sql.db 的持久连接
	strikes map[string]DenyListStrike // IP 到当前窗口违规次数的映射，仅保存在内存中
	bans    map[string]DenyListBan    // 当前网关用于快速判定的封禁快照
	pending map[string]DenyListBan    // 本进程新产生且尚未成功写入数据库的封禁
	changed chan struct{}             // 合并多次变更，通知后台协程写入数据库
}

// 当前网关进程使用的 IP 封禁表，启动初始化后供接入和读协程共享。
var ClientDenyList *DenyListStore

// 从 net_sql.db 加载封禁记录，并启动后台写入与同步协程。
func InitClientDenyList() error {
	store, err := LoadDenyList(DenyListDBFileName)
	if err != nil {
		return err
	}
	ClientDenyList = store
	go store.RunWriter()
	return nil
}

// 打开指定 SQLite 数据库，建表并加载未过期的封禁记录。
func LoadDenyList(path string) (*DenyListStore, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("%s?_busy_timeout=%d&_journal_mode=WAL", path, DenyListDBBusyTimeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("open denylist database: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS denylist (
		ip TEXT PRIMARY KEY NOT NULL,
		reason TEXT NOT NULL,
		expires_at INTEGER NOT NULL
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create denylist table: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS denylist_expires_at_idx ON denylist(expires_at)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create denylist index: %w", err)
	}
	store := &DenyListStore{
		db:      db,
		strikes: make(map[string]DenyListStrike),
		bans:    make(map[string]DenyListBan),
		pending: make(map[string]DenyListBan),
		changed: make(chan struct{}, 1),
	}
	if err := store.Refresh(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

// 关闭当前网关持有的 SQLite 连接。
func (d *DenyListStore) Close() error {
	return d.db.Close()
}

// 从共享数据库刷新封禁快照，同时保留本进程尚未写入的封禁。
func (d *DenyListStore) Refresh() error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	rows, err := d.db.Query(`SELECT ip, reason, expires_at FROM denylist WHERE expires_at > ?`, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("query denylist: %w", err)
	}
	defer rows.Close()
	bans := make(map[string]DenyListBan)
	for rows.Next() {
		var ip, reason string
		var expiresAt int64
		if err := rows.Scan(&ip, &reason, &expiresAt); err != nil {
			return fmt.Errorf("scan denylist: %w", err)
		}
		if _, err := netip.ParseAddr(ip); err == nil {
			bans[ip] = DenyListBan{Reason: reason, ExpiresAt: time.UnixMilli(expiresAt)}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate denylist: %w", err)
	}
	d.mu.Lock()
	for ip, ban := range d.pending {
		if current, found := bans[ip]; !found || ban.ExpiresAt.After(current.ExpiresAt) {
			bans[ip] = ban
		}
	}
	d.bans = bans
	d.mu.Unlock()
	return nil
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
	ban := DenyListBan{Reason: "invalid_packet", ExpiresAt: now.Add(DenyListBanDuration)}
	d.bans[ip] = ban
	d.pending[ip] = ban
	d.MarkChanged()
	return true
}

// 通知后台协程写入数据库。调用方须持有 d.mu；通知通道会合并重复变更。
func (d *DenyListStore) MarkChanged() {
	select {
	case d.changed <- struct{}{}:
	default:
	}
}

// 定期写入封禁、同步其他网关的封禁，并清理过期记录。
func (d *DenyListStore) RunWriter() {
	flushTicker := time.NewTicker(DenyListFlushInterval)
	syncTicker := time.NewTicker(DenyListSyncInterval)
	cleanupTicker := time.NewTicker(DenyListCleanupInterval)
	defer flushTicker.Stop()
	defer syncTicker.Stop()
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
		case <-syncTicker.C:
			if err := d.Refresh(); err != nil {
				printer.TextError("Sync denylist failed", "err", err)
			}
		case <-cleanupTicker.C:
			if err := d.CleanupExpired(); err != nil {
				printer.TextError("Cleanup denylist failed", "err", err)
			}
		}
	}
}

// 删除已过期的内存违规记录与数据库封禁记录。
func (d *DenyListStore) CleanupExpired() error {
	now := time.Now()
	d.mu.Lock()
	for ip, strike := range d.strikes {
		if now.Sub(strike.first) >= DenyListStrikeWindow {
			delete(d.strikes, ip)
		}
	}
	for ip, ban := range d.bans {
		if !now.Before(ban.ExpiresAt) {
			delete(d.bans, ip)
		}
	}
	d.mu.Unlock()
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if _, err := d.db.Exec(`DELETE FROM denylist WHERE expires_at <= ?`, now.UnixMilli()); err != nil {
		return fmt.Errorf("delete expired denylist rows: %w", err)
	}
	return nil
}

// 只写入本进程新增的封禁，避免覆盖其他网关写入的记录。
func (d *DenyListStore) Flush() error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.mu.Lock()
	pending := make(map[string]DenyListBan, len(d.pending))
	for ip, ban := range d.pending {
		pending[ip] = ban
	}
	d.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("begin denylist write: %w", err)
	}
	defer tx.Rollback()
	for ip, ban := range pending {
		_, err := tx.Exec(`INSERT INTO denylist(ip, reason, expires_at) VALUES (?, ?, ?)
			ON CONFLICT(ip) DO UPDATE SET reason=excluded.reason, expires_at=excluded.expires_at
			WHERE excluded.expires_at > denylist.expires_at`, ip, ban.Reason, ban.ExpiresAt.UnixMilli())
		if err != nil {
			return fmt.Errorf("write denylist IP %s: %w", ip, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit denylist: %w", err)
	}
	d.mu.Lock()
	for ip, ban := range pending {
		if current, found := d.pending[ip]; found && current == ban {
			delete(d.pending, ip)
		}
	}
	d.mu.Unlock()
	return nil
}
