package database

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"golang.org/x/sync/singleflight"
	"mixture/src/common/printer"
	"sync/atomic"
	"time"
)

/*缓存数据处理逻辑*/
var (
	cache               atomic.Pointer[memcache.Client]
	cacheDBAddr         string
	cacheDBPort         string
	cacheItemExpiration int32              = 60 * 15                //CacheItem 有效时长（单位：秒）
	cacheTimeOut                           = 500 * time.Millisecond //操作超时时长
	MemCacheLoadGroup   singleflight.Group                          //合并本进程内同一key的并发回源，避免热点失效时重复查询数据库
)

func MemCacheConnectClientInit(serName, processId string) {
	if serName != "robot_test" && serName != "gateway_server" {
		netDB, res := GetSQLiteConnDB()
		if res == nil {
			defer func() {
				if netDB.Close() != nil {
					panic("MemCaChe Init Error: netDB Close Fail")
				}
			}()
			cacheDBAddr, cacheDBPort = GetMemCacheAddressAndPort(serName, processId, netDB)
			cache.Store(CreateMenCacheElement())
		} else {
			panic("MemCaChe Init Error: netDB Connect Fail: " + res.Error())
		}
	}
}

func CreateMenCacheElement() *memcache.Client {
	localHost := cacheDBAddr + ":" + cacheDBPort
	client := memcache.New(localHost)
	client.Timeout = cacheTimeOut
	pingErr := client.Ping()
	if pingErr != nil {
		panic("MemCaChe CreateMenCacheElement Error: Ping Fail")
	}
	return client
}

// 缓存操作调用统一接口
// GET：命中返回 (value, nil)；未命中返回 ("", memcache.ErrCacheMiss)；其他错误返回 ("", err)
// SET/DEL/ADD/REP：成功返回 ("", nil)；失败返回 ("", err)
func MemCacheCmd(cmd, key string, val []byte) (string, error) {
	m := cache.Load() //获取连接池对象
	if m == nil {
		return "", errors.New("MemCacheCmd: client is nil")
	}

	switch cmd {
	case "SET":
		if err := m.Set(&memcache.Item{Key: key, Value: val, Expiration: cacheItemExpiration}); err != nil {
			return "", fmt.Errorf("MemCacheCmd SET %s: %w", key, err)
		}
		return "", nil
	case "GET":
		item, err := m.Get(key)
		if err == nil {
			return string(item.Value), nil
		}
		// ErrCacheMiss 也按错误返回，调用方用 errors.Is 判断
		return "", fmt.Errorf("MemCacheCmd GET %s: %w", key, err)
	case "DEL":
		if err := m.Delete(key); err != nil {
			return "", fmt.Errorf("MemCacheCmd DEL %s: %w", key, err)
		}
		return "", nil
	case "ADD":
		if err := m.Add(&memcache.Item{Key: key, Value: val, Expiration: cacheItemExpiration}); err != nil {
			return "", fmt.Errorf("MemCacheCmd ADD %s: %w", key, err)
		}
		return "", nil
	case "REP":
		if err := m.Replace(&memcache.Item{Key: key, Value: val, Expiration: cacheItemExpiration}); err != nil {
			return "", fmt.Errorf("MemCacheCmd REP %s: %w", key, err)
		}
		return "", nil
	default:
		return "", fmt.Errorf("MemCacheCmd: unknown cmd %q", cmd)
	}
}

// StartMemCacheHealthCheck 定期检查缓存连通性，故障时清理空闲连接并由原客户端按需重连。
func StartMemCacheHealthCheck(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		wasUnhealthy := false
		for range ticker.C {
			// 避免重复Load
			if client := cache.Load(); client != nil {
				if err := client.Ping(); err != nil {
					wasUnhealthy = true
					printer.TextWarn("MemCacheHealthCheck", "Ping failed; client will reconnect on demand", err.Error())
					// Close只关闭空闲连接；正在执行的请求仍使用原客户端，归还的连接也能继续复用。
					if closeErr := client.Close(); closeErr != nil {
						printer.TextWarn("MemCacheHealthCheck", "Close idle connections failed", closeErr.Error())
					}
				} else if wasUnhealthy {
					wasUnhealthy = false
					printer.TextInfo("MemCacheHealthCheck", "Connection recovered")
				}
			}
		}
	}()
}

// MemCacheBatchGet 批量获取缓存（减少网络往返）
// 返回：命中的key-value映射，未命中的key列表
func MemCacheBatchGet(keys []string) (map[string]string, []string) {
	m := cache.Load()
	if m == nil {
		return nil, keys // 全部miss
	}

	items, err := m.GetMulti(keys)
	if err != nil {
		printer.TextError("MemCacheBatchGet", "Error", err.Error())
		return nil, keys
	}

	result := make(map[string]string, len(items))
	missedKeys := make([]string, 0)

	for _, key := range keys {
		if item, exists := items[key]; exists {
			result[key] = string(item.Value)
		} else {
			missedKeys = append(missedKeys, key)
		}
	}

	return result, missedKeys
}

// MemCacheBatchSet 批量设置缓存
// 返回：设置失败的key列表
func MemCacheBatchSet(kvMap map[string][]byte) []string {
	m := cache.Load()
	if m == nil {
		keys := make([]string, 0, len(kvMap))
		for k := range kvMap {
			keys = append(keys, k)
		}
		return keys
	}

	failedKeys := make([]string, 0)
	for key, val := range kvMap {
		if err := m.Set(&memcache.Item{Key: key, Value: val, Expiration: cacheItemExpiration}); err != nil {
			failedKeys = append(failedKeys, key)
			printer.TextError("MemCacheBatchSet", "Key", key, "Error", err.Error())
		}
	}

	return failedKeys
}

// MemCacheGetOrLoad 先查缓存，未命中或缓存故障时合并本进程内同一key的并发加载。
// 同一key必须对应相同的数据及loader语义，loader不能递归调用本函数加载同一key。
// 回写完成后释放合并任务；回写失败仍返回加载结果，每个调用者获得独立的字节切片。
func MemCacheGetOrLoad(key string, loader func() ([]byte, error)) ([]byte, error) {
	// 先查缓存
	if val, err := MemCacheCmd("GET", key, nil); err == nil {
		return []byte(val), nil
	}

	data, err, _ := MemCacheLoadGroup.Do(key, func() (interface{}, error) {
		// 进入合并任务后再次检查，避免之前的任务已回写缓存却仍重复回源。
		if val, err := MemCacheCmd("GET", key, nil); err == nil {
			return []byte(val), nil
		}
		if loader == nil {
			return nil, errors.New("MemCacheGetOrLoad: loader is nil")
		}
		loaded, err := loader()
		if err != nil {
			return nil, err
		}
		loaded = bytes.Clone(loaded)
		// 在释放合并任务前回写，防止异步回写期间再次启动同key的加载。
		if _, setErr := MemCacheCmd("SET", key, loaded); setErr != nil {
			printer.TextWarn("MemCacheGetOrLoad", "SetCache failed", setErr.Error())
		}
		return loaded, nil
	})
	if err != nil {
		return nil, err
	}
	return bytes.Clone(data.([]byte)), nil
}
