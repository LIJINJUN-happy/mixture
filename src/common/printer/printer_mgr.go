package printer

import (
	"mixture/src/common/tools"
	"sync"
	"time"
)

// 日志管理器对象指针
var PrintMgrPtr *printerMgr = nil

// 日志管理器结构
type printerMgr struct {
	logMapPtr   *sync.Map //日志容器的对象指针
	curZeroDate time.Time //当前日志天数分页标识
}

func InitPrinterMgr() {
	PrintMgrPtr = new(printerMgr)
	PrintMgrPtr.logMapPtr = new(sync.Map) //（key:日志名称,val:日志对象指针）
	PrintMgrPtr.curZeroDate = tools.GetZeroTime()
	go func(p *printerMgr) {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			p.CheckIsNeedCreateNewDate()
		}
	}(PrintMgrPtr)
}

func (p *printerMgr) Get(logName string) *LoggerObj {
	if l, ok := p.logMapPtr.Load(logName); ok {
		return l.(*LoggerObj)
	}
	return nil
}

func (p *printerMgr) Set(logName string, l *LoggerObj) {
	p.logMapPtr.Store(logName, l)
}

func (p *printerMgr) Del(logName string) {
	p.logMapPtr.Delete(logName)
}

// 检测是否跨天分页日志
func (p *printerMgr) CheckIsNeedCreateNewDate() {
	if zeroTime := tools.GetZeroTime(); zeroTime != p.curZeroDate {
		p.curZeroDate = zeroTime
		p.logMapPtr.Range(func(k, v interface{}) bool {
			logName := k.(string)
			l := v.(*LoggerObj)

			//假如是系统文件直接无视（只分页模块日志,系统日志不做分页）
			if !l.systemLog {
				if CreateLogger(logName, l.loggerPath, false) != nil {
					//替换新日志对象后,关闭旧的os.File对象
					if errClose := l.osFile.Close(); errClose != nil {
						if systemLogObj != nil {
							PanicStack(errClose)
						}
					}
				}
			}
			return true
		})
	}
}
