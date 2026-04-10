package printer

import (
	"fmt"
	"log/slog"
	"mixture/src/common/tools"
	"os"
	"runtime"
	"strconv"
	"strings"
)

var systemLogObj *LoggerObj = nil

type LoggerObj struct {
	serName    string               //日志对应的服务名
	loggerPath string               //日志路径
	loggerName string               //生成的日志名字
	systemLog  bool                 //是否为系统日志对象（是的话覆盖os的输出）
	osFile     *os.File             //输出对象
	opts       *slog.HandlerOptions //设置
	textLogger *slog.Logger         //日志对象(文本格式)
	jsonLogger *slog.Logger         //日志对象(json格式)
}

func CreateLogger(logName, loggerPath string, sys bool) *LoggerObj {
	if loggerPath == "" || logName == "" {
		return nil
	}
	loggerObj := new(LoggerObj)
	if loggerObj.InitLogger(logName, loggerPath, sys) {
		PrintMgrPtr.Del(logName)
		PrintMgrPtr.Set(logName, loggerObj)
		return loggerObj
	}
	return nil
}

func (l *LoggerObj) InitLogger(logName, loggerPath string, sys bool) bool {
	l.serName = logName
	l.loggerPath = loggerPath

	strBuilder := strings.Builder{}
	strBuilder.Grow(len(logName) + 20 + len(loggerPath))
	strBuilder.WriteString(logName)
	strBuilder.WriteString(".log")
	//非系统日志要区分日期页
	if !sys {
		year, month, day := PrintMgrPtr.curZeroDate.Date()
		strBuilder.WriteString("_")
		strBuilder.WriteString(strconv.Itoa(year))
		strBuilder.WriteString("-")
		strBuilder.WriteString(strconv.Itoa(int(month)))
		strBuilder.WriteString("-")
		strBuilder.WriteString(strconv.Itoa(day))
	}
	l.loggerName = strBuilder.String()

	//先尝试open,不存在的话再os.Create创建,并设定输出路径
	strBuilder.Reset()
	strBuilder.WriteString(l.loggerPath)
	strBuilder.WriteString(l.loggerName)
	completeDirPath := strBuilder.String()
	f, logError := os.OpenFile(completeDirPath, os.O_APPEND|os.O_WRONLY, 0644)
	if logError != nil {
		f, logError = os.Create(completeDirPath)
		if logError != nil {
			panic("Cant Find The Logger Path ,The Logger Path Value Is: " + completeDirPath)
			return false
		}
	}

	//假如是系统日志需要覆盖掉标准输出对象
	if sys {
		os.Stdout = f
		os.Stderr = f
		l.systemLog = true
	} else {
		l.systemLog = false
	}

	//根据配置获取log等级设置
	slogLevel := tools.GetSysValueByKey("slogLevel")
	trueLv, res := strconv.Atoi(slogLevel)
	if res != nil {
		panic("slogLevel Error: " + res.Error())
		return false
	}
	l.opts = new(slog.HandlerOptions)
	l.opts.Level = slog.Level(trueLv)

	l.osFile = f
	l.textLogger = slog.New(slog.NewTextHandler(f, l.opts))
	l.jsonLogger = slog.New(slog.NewJSONHandler(f, l.opts))
	return true
}

func SetLoggerObj(log *LoggerObj) {
	systemLogObj = log
}

func TextDebug(msg string, args ...any) {
	systemLogObj.textLogger.Debug(msg, args...)
}

func TextInfo(msg string, args ...any) {
	systemLogObj.textLogger.Info(msg, args...)
}

func TextWarn(msg string, args ...any) {
	systemLogObj.textLogger.Warn(msg, args...)
}

func TextError(msg string, args ...any) {
	systemLogObj.textLogger.Error(msg, args...)
}

func JsonDebug(msg string, args ...any) {
	systemLogObj.jsonLogger.Debug(msg, args...)
}

func JsonInfo(msg string, args ...any) {
	systemLogObj.jsonLogger.Info(msg, args...)
}

func JsonWarn(msg string, args ...any) {
	systemLogObj.jsonLogger.Warn(msg, args...)
}

func JsonError(msg string, args ...any) {
	systemLogObj.jsonLogger.Error(msg, args...)
}

func PanicStack(e any) {
	panicBuff := make([]byte, 2048)
	n := runtime.Stack(panicBuff[:], false)
	str := strings.Replace(string(panicBuff[:n]), "\n", "  ", -1)
	str = strings.Replace(str, "\t", "  ", -1)

	var errMsg string
	if err, ok := e.(error); ok {
		errMsg = err.Error()
	} else {
		errMsg = fmt.Sprintf("%v", e)
	}
	TextError(errMsg, "Panic Stack", str)
}

// 记录日志数据调用（一般是模块日志记录，用于后续查询日志排查问题）
func TrickLog(modName string, msg string, args ...any) {
	defer func() {
		if r := recover(); r != nil {
			PanicStack(r)
		}
	}()
	if l := PrintMgrPtr.Get(modName); l != nil {
		l.jsonLogger.Info(msg, args...)
	}
}
