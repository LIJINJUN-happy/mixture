package database

import (
	"errors"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"mixture/src/common/printer"
	"sync/atomic"
	"time"
)

/*缓存数据处理逻辑*/
var (
	cache               atomic.Pointer[memcache.Client]
	cacheDBAddr         string
	cacheDBPort         string
	cacheItemExpiration int32 = 60 * 15                //CacheItem 有效时长（单位：秒）
	cacheTimeOut              = 500 * time.Millisecond //操作超时时长
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

func StartMemCacheHealthCheck(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			// 避免重复Load
			if client := cache.Load(); client != nil {
				if err := client.Ping(); err != nil {
					printer.TextWarn("MemCacheHealthCheck", "Ping fail, reconnecting", err.Error())
					newClient := memcache.New(cacheDBAddr + ":" + cacheDBPort)
					newClient.Timeout = cacheTimeOut
					if pingErr := newClient.Ping(); pingErr == nil {
						cache.Store(newClient)
						printer.TextInfo("MemCacheHealthCheck", "Reconnect success")
					} else {
						printer.TextError("MemCacheHealthCheck", "Reconnect failed", pingErr.Error())
					}
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

// MemCacheGetOrLoad 缓存穿透保护：先查缓存，miss时执行loader并回写
// 使用场景：自动处理"先查缓存，miss再查DB"的逻辑
func MemCacheGetOrLoad(key string, loader func() ([]byte, error)) ([]byte, error) {
	// 先查缓存
	if val, err := MemCacheCmd("GET", key, nil); err == nil {
		return []byte(val), nil
	}

	// 缓存miss，执行loader
	data, err := loader()
	if err != nil {
		return nil, err
	}

	// 回写缓存（异步，不阻塞）
	go func(k string, d []byte) {
		if _, setErr := MemCacheCmd("SET", k, d); setErr != nil {
			printer.TextWarn("MemCacheGetOrLoad", "SetCache failed", setErr.Error())
		}
	}(key, data)

	return data, nil
}
