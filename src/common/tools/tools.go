package tools

import (
	"bufio"
	"math/rand"
	"os"
	"strings"
	"time"
)

func ReadFileByName(fileName string) (*bufio.Scanner, *os.File) {
	f, e := os.Open(fileName)
	if e != nil {
		return nil, nil
	}
	ns := bufio.NewScanner(f)
	return ns, f
}

func GetSysValueByKey(key string) string {
	ns, f := ReadFileByName("system.txt")
	if (ns != nil) && (f != nil) {
		defer func(sysFile *os.File) {
			if r := sysFile.Close(); r != nil {
				panic(r)
			}
		}(f)

		for ns.Scan() {
			line := ns.Text()
			//fmt.Println(line)
			if strings.HasPrefix(line, key) {
				idx := strings.Index(line, key+"=")
				if idx == -1 {
					return ""
				} else {
					idx = strings.Index(line, "=")
					return strings.Clone(line[idx+1:])
				}
			}
		}
	}
	return ""
}

func GetRandNum(rMin, rMax int64) int64 {
	return rand.Int63n(rMax) + rMin
}

// 获取当天零时整点时间戳
func GetZeroTime() time.Time {
	now := time.Now()    // 当前时间
	lc := now.Location() //保持本地时区
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, lc)
	//fmt.Println(midnight) // 年-月-日 00:00:00 +0800 CST
	return midnight
}

// 获取到第二天的时间间隔
func GetNextDaySecond() time.Duration {
	now := time.Now()

	// 计算明天零点的时间
	tomorrow := now.AddDate(0, 0, 1)
	tomorrowMidnight := time.Date(tomorrow.Year(), tomorrow.Month(), tomorrow.Day(), 0, 0, 0, 0, now.Location())

	// 计算从当前时间到明天零点的时间间隔
	durationToMidnight := tomorrowMidnight.Sub(now)
	return durationToMidnight
}

// 计算两个时间戳的相隔天数
func CheckDurDayWithTime(timestamp1, timestamp2 int64) int {
	time1 := time.Unix(timestamp1, 0)
	time2 := time.Unix(timestamp2, 0)

	// 计算两个时间之间的天数差
	duration := time2.Sub(time1)
	days := int(duration.Hours() / 24)
	return days
}

// 随机UID
func CreateUUid() int64 {
	timeRand := time.Now().UnixNano()
	seedRand := rand.Int63()
	diff := int64(0)
	if timeRand > seedRand {
		diff = timeRand - seedRand
	} else if seedRand > timeRand {
		diff = seedRand - timeRand
	} else {
		diff = seedRand
	}
	return diff
}

// 检测这个时间戳是否属于今日
func CheckIsToday(timestamp int64) bool {
	if timestamp == 0 {
		return false
	}
	t := time.Unix(timestamp, 0)                 // 将 int64 类型的时间戳转换为 time.Time
	today := time.Now().Truncate(24 * time.Hour) // 获取今天的日期
	timestampDate := t.Truncate(24 * time.Hour)  // 获取时间戳对应的日期

	// 判断是否是今天
	if timestampDate.Equal(today) {
		return true
	}
	return false
}