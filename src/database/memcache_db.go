package database

import (
	"github.com/bradfitz/gomemcache/memcache"
	"time"
)

/*缓存数据处理逻辑*/
var cache *memcache.Client
var cacheDBAddr string
var cacheDBPort string
var cacheItemExpiration int32 = 60 * 15 //CacheItem 有效时长（单位：秒）
var cacheTimeOut = 15 * time.Second     //操作超时时长

func MemCacheConnectClientInit(serName, processId string) {
	if serName != "robot_test" && serName != "gateway_server" {
		res, netDB := GetSQLiteConnDB()
		if res == nil {
			defer func() {
				if netDB.Close() != nil {
					panic("MemCaChe Init Error: netDB Close Fail")
				}
			}()
			cacheDBAddr, cacheDBPort = GetMemCacheAddressAndPort(serName, processId, netDB)
			cache = CreateMenCacheElement()
		} else {
			panic("MemCaChe Init Error: netDB Connect Fail: " + res.Error())
		}
	}
}

func CreateMenCacheElement() *memcache.Client {
	localHost := cacheDBAddr + ":" + cacheDBPort
	client := memcache.New(localHost)
	client.Timeout = cacheTimeOut
	if pingErr := client.Ping(); pingErr == nil {
		return client
	} else {
		panic("MemCaChe CreateMenCacheElement Error: Ping Fail")
	}
	return nil
}

func GetMemCacheClient() *memcache.Client {
	return cache
}

// 缓存操作调用统一接口
func MemCacheCmd(cmd, key string, val []byte) (bool, string) {
	errString := "Get NUll MemCache Client" //错误描述
	m := GetMemCacheClient()                //获取连接池对象

	if m != nil {
		//操作：
		switch cmd {
		case "SET":
			res := m.Set(&memcache.Item{Key: key, Value: val, Expiration: cacheItemExpiration})
			if res == nil {
				return true, ""
			}
			errString = res.Error()
		case "GET":
			if item, err := m.Get(key); err == nil {
				return true, string(item.Value)
			} else {
				errString = err.Error()
			}
		case "DEL":
			res := m.Delete(key)
			if res == nil {
				return true, ""
			} else {
				errString = res.Error()
			}
		case "ADD":
			res := m.Add(&memcache.Item{Key: key, Value: val, Expiration: cacheItemExpiration})
			if res == nil {
				return true, ""
			} else {
				errString = res.Error()
			}
		case "REP":
			res := m.Replace(&memcache.Item{Key: key, Value: val, Expiration: cacheItemExpiration})
			if res == nil {
				return true, ""
			} else {
				errString = res.Error()
			}
		default:
			errString = "Can Not Find Any Cmd = '" + cmd + "'"
			break
		}
	}
	return false, errString
}
