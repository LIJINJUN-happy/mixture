package robot

import (
	"container/list"
	"mixture/src/common/tools"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 机器人模块配置结构体（读取robot_config内容进行写入）
type config struct {
	protoType               string
	serAddress              string
	way                     int
	RobotNum                int  //机器人数量
	CreateRobotNumOneSec    int  //每秒创建机器人数量
	RobotSendMsgIntervalSec int  //每个机器人发送协议的间隔
	RobotSendMsgIsLimit     bool //机器人发送协议是否有次数限制
	RobotSendMsgTimes       int  //每个机器人发送协议的次数（RobotSendMsgIsLimit是true才生效该字段）
}

// TestRobot 机器人结构体
type TestRobot struct {
	uid        int       //机器人唯一标识
	name       string    //机器人名字
	curSendNum int       //机器人当前发送协议从次数
	createTime time.Time //创建时间点
}

var RBList = list.New()     // 机器人链表
var ConfigObj = new(config) //配置对象
var Wg sync.WaitGroup       //机器人协程同步

// Run 开始执行机器逻辑流程
func Run() {
	initConfig()
	if ConfigObj.protoType == "json" {
		createJSRot()
	} else if ConfigObj.protoType == "pb" {
		createPBRot()
	} else {
		panic("No such proto type")
	}

	//所有机器人退出才结束整个主进程
	time.Sleep(10 * time.Second)
	Wg.Wait()
}

func initConfig() {
	//初始化链表
	RBList.Init()

	//打开txt文件
	fileLoadPath := "./robot/robot_config.txt"
	ns, f := tools.ReadFileByName(fileLoadPath)
	if (ns != nil) && (f != nil) {
		defer func(ff *os.File) {
			if r := ff.Close(); r != nil {
				panic(r)
			}
		}(f)
	} else {
		panic(fileLoadPath)
		return
	}

	//txt中查找字段值赋值给configObj内的对应字段
	matchTimes := 0
	searchByKey := func(lineString, key string) (bool, string) {
		if strings.HasPrefix(lineString, key) {
			idx := strings.Index(lineString, key+"=")
			if idx != -1 {
				idx = strings.Index(lineString, "=")
				valString := strings.Clone(lineString[idx+1:])
				return true, valString
			}
		}
		return false, ""
	}
	for ns.Scan() {
		line := ns.Text()
		//fmt.Println(line)

		//检测该行内容是否有匹配字段
		res := false
		valString := ""
		if res, valString = searchByKey(line, "RobotNum"); res {
			ConfigObj.RobotNum, _ = strconv.Atoi(valString)
			matchTimes += 1
			continue
		}

		if res, valString = searchByKey(line, "CreateRobotNumOneSec"); res {
			ConfigObj.CreateRobotNumOneSec, _ = strconv.Atoi(valString)
			matchTimes += 1
			continue
		}

		if res, valString = searchByKey(line, "RobotSendMsgIntervalSec"); res {
			ConfigObj.RobotSendMsgIntervalSec, _ = strconv.Atoi(valString)
			matchTimes += 1
			continue
		}

		if res, valString = searchByKey(line, "RobotSendMsgIsLimit"); res {
			ConfigObj.RobotSendMsgIsLimit = valString == "true"
			matchTimes += 1
			continue
		}

		if res, valString = searchByKey(line, "RobotSendMsgTimes"); res {
			ConfigObj.RobotSendMsgTimes, _ = strconv.Atoi(valString)
			matchTimes += 1
			continue
		}

		if res, valString = searchByKey(line, "RobotConnAddress"); res {
			ConfigObj.serAddress = valString
			matchTimes += 1
			continue
		}

		if res, valString = searchByKey(line, "way"); res {
			ConfigObj.way, _ = strconv.Atoi(valString)
			matchTimes += 1
			continue
		}

		if res, valString = searchByKey(line, "RobotProtoType"); res {
			ConfigObj.protoType = valString
			matchTimes += 1
			continue
		}
	}

	//字段匹配次数（config结构参数数量和txt内是否一致,一致则加载完成）
	structArgsNum := reflect.TypeOf(*ConfigObj).NumField()
	if matchTimes != structArgsNum {
		panic("Robot File's Args Num Match Fail, matchTimes = " + strconv.Itoa(matchTimes) + ", reflect.TypeOf(configObj).NumField() = " + strconv.Itoa(structArgsNum))
	}
}
