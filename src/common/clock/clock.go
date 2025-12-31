package clock

//时间定时器工具定义文件包
import (
	"mixture/src/common/printer"
	"time"
)

// 计时器的循环检测方法类型
const OnceTimesSec uint8 = 1    //一次性倒计时
const LoopingWithTime uint8 = 2 //每多少秒执行一次

const UnStart uint8 = 0
const Running uint8 = 1
const Stopping uint8 = 2

type TickerElement struct {
	tickType    uint8         //计时器类型
	status      uint8         //状态（未启动 or 停止 or 运行 or 结束）
	Ticker      *time.Ticker  //计时器(循环执行)
	Timer       *time.Timer   //计时器(单次执行)
	beginTime   time.Time     //开始时间
	createTime  time.Time     //创建时间
	intervalSec time.Duration //间隔时间
	name        string        //定时器命名
}

// 开始计时器开启
func TimerCreate(timerName string, tickType uint8, interval time.Duration) *TickerElement {
	t := new(TickerElement)
	t.Timer = nil
	t.Ticker = nil
	t.tickType = tickType
	t.name = timerName
	t.status = UnStart
	t.createTime = time.Now()
	t.intervalSec = interval
	return t
}

func (t *TickerElement) TimerStart() (bool, <-chan time.Time) {
	if t.status == Running {
		printer.TextWarn("ClockStart Fail", "Status", "Running")
		return false, nil
	} else {
		t.status = Running
		t.beginTime = time.Now()
	}

	//根据类型返回创建的定时器类型（循环还是单次）
	switch t.tickType {
	case OnceTimesSec:
		t.Timer = time.NewTimer(t.intervalSec)
		return true, t.Timer.C
	case LoopingWithTime:
		t.Ticker = time.NewTicker(t.intervalSec)
		return true, t.Ticker.C
	default:
		printer.TextError("ClockStart TickType Error", "TickType", t.tickType)
	}
	return false, nil
}

func (t *TickerElement) TimerStop() {
	if t.status != Running {
		printer.TextError("ClockStop  Error", "Clock Status", t.status)
		return
	} else {
		t.status = Stopping
	}

	//timer Stop
	if t.tickType == OnceTimesSec {
		t.Timer.Stop()
		return
	} else {
		//ticker Stop
		t.Ticker.Stop()
	}
}

// 传nil则按当前的倒计时重新开始，参数不是nil则是设置新的倒计时时间间隔
func (t *TickerElement) Recount(newTimeSec any) {
	if newTimeSec != nil {
		t.intervalSec = newTimeSec.(time.Duration) * time.Second
	}
	if t.tickType == OnceTimesSec {
		t.Timer.Reset(t.intervalSec)
	} else {
		t.Ticker.Reset(t.intervalSec)
	}
	t.status = Running
}

func (t *TickerElement) TimerStatus() uint8 {
	return t.status
}

// 返回计时器累计执行时间(秒)
func (t *TickerElement) TimerRunDurationSec() time.Duration {
	nt := time.Now()
	diffSecond := t.beginTime.Sub(nt)
	return diffSecond
}
