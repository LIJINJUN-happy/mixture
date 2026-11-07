package server_common

import (
	"errors"
	"mixture/src/common/printer"
	"mixture/src/gateway_server"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// 服务进程内的模块Map容器定义(每个进程服务用于存储各自的不同模块)
var (
	ModCallerTimeOut     = 10 * time.Second                  //最大模块调用超时时长
	ServerModMgr         = new(sync.Map)                     //服务进程模块容器（key:模块名,val:模块对象指针）
	ProtoName2ModMap     = make(map[string]chan interface{}) //协议模块映射表（key:协议名,val:模块通道chan）
	InterfaceName2ModMap = make(map[string]chan interface{}) //外部接口模块映射表（key:外部接口名,val:模块通道chan）
)

// 服务模块
type ServerModule struct {
	modName          string                                                     //模块名称
	closeChan        chan os.Signal                                             //服务器关闭通知模块通道(信号通道)
	receiptChan      chan interface{}                                           //模块内接收数据的通道（发送给对应模块的数据都从这里接收）
	LoggerObj        *printer.LoggerObj                                         //模块日志指针对象(模块日志区分)
	ModInit          func() error                                               //启动模块初始化处理函数
	ServerDown       func() error                                               //关闭服务器模块功能处理函数
	HandleFuncMap    map[string]func(data []byte, gateId int, sessionID uint32) //协议以及模块处理逻辑映射表
	InterfaceFuncMap map[string]func(ags ...any) interface{}                    //外部接口以及模块处理逻辑映射表
}

// 协议通信包对象池
var ProtoPackInfoPool = sync.Pool{
	New: func() interface{} {
		return new(ProtoPackInfo)
	},
}

// 模块通信包对象池
var ModulePackInfoPool = sync.Pool{
	New: func() interface{} {
		return new(ModulePackInfo)
	},
}

// 传输给模块通道的协议数据包（来自别的进程分发以及客户端传输）
type ProtoPackInfo struct {
	ProName   string  //协议名字
	Data      *[]byte //协议数据内容
	GateId    int     //来着哪个网关进程
	SessionID uint32  //玩家标识
}

// 传输给模块通道的模块数据包（来自同的进程下不同模块的调用数据传输）
type ModulePackInfo struct {
	InterfaceName string           //目标函数注册接口名
	ReturnChan    chan interface{} //同步返回通信口（设置为空就是异步调用,否则是同步调用）
	ArgsList      []any            //参数列表
}

func CreateMod(modName string, chanSize int, modInitFunc, serverDownFunc func() error, wg *sync.WaitGroup) *ServerModule {
	newMod := new(ServerModule)

	//设置初始信息
	newMod.modName = modName
	newMod.ModInit = modInitFunc
	newMod.ServerDown = serverDownFunc
	newMod.HandleFuncMap = make(map[string]func(data []byte, gateId int, sessionID uint32))
	newMod.InterfaceFuncMap = make(map[string]func(ags ...any) interface{})
	newMod.receiptChan = make(chan interface{}, chanSize)
	newMod.closeChan = make(chan os.Signal, 1)
	signal.Notify(newMod.closeChan, syscall.SIGINT, syscall.SIGTERM) // 注册需要监听的信号

	//初始化模块
	kickRes := newMod.ModInit()
	if kickRes != nil {
		printer.PanicStack(kickRes)
		return nil
	}

	//初始化日志对象
	if l := printer.CreateLogger(modName, "log/moduleLog/", false); l != nil {
		newMod.LoggerObj = l
	} else {
		err := errors.New("CreateLogger Logger Failed, ModName: " + modName)
		printer.PanicStack(err)
		return nil
	}

	//把模块名和模块通道做关联,新数据通过通道进入模块内部处理
	//printer.TextInfo("LoadMod", "SerName", ServicePrt.ServerName, "SerId", ServicePrt.ServerId, "ModName", newMod.modName)
	ServerModMgr.Store(newMod.modName, newMod)
	wg.Add(1)
	go newMod.LoadMod(wg)

	return newMod
}

// 加载模块(每一个模块都是一个Go协程)
func (mod *ServerModule) LoadMod(wg *sync.WaitGroup) {
	modTimer := time.NewTimer(time.Hour)
	defer func() {
		modTimer.Stop()
		if err := recover(); err != nil {
			printer.TextError("LoadMod", "ServerName", ServicePrt.ServerName, "ServerId", ServicePrt.ServerId, "modName", mod.modName, "Error", err)
		}
		wg.Done()
	}()

	for {
		select {
		case <-mod.closeChan: //监听服务器关闭通知
			if downRes := mod.ServerDown(); downRes != nil {
				printer.PanicStack(downRes)
			}
			return
		case receiptData := <-mod.receiptChan: //监听模块接收数据通知
			if protoPack, transProtoPack := receiptData.(*ProtoPackInfo); transProtoPack {
				mod.HandleProtoPack(protoPack)
			} else if modulePack, transModulePack := receiptData.(*ModulePackInfo); transModulePack {
				mod.HandleModulePack(modulePack)
			} else {
				printer.TextError("LoadMod receiptChan Pack Error", "receiptData", receiptData)
				return
			}
		case <-modTimer.C:
			modTimer.Stop()
			modTimer.Reset(time.Hour)
		}
	}
}

// 处理协议数据包（来源：进程或者客户端）
func (mod *ServerModule) HandleProtoPack(protoPack *ProtoPackInfo) {
	defer func() {
		if err := recover(); err != nil {
			printer.TextError("HandleProtoPack", "ServerName", ServicePrt.ServerName, "ServerId", ServicePrt.ServerId, "modName", mod.modName, "Error", err)
		}

		//执行完毕后把协议数据包内的Data放回内存池（需在ProtoPackInfoPool前处理）
		if protoPack.Data != nil {
			bufPtr := protoPack.Data
			*bufPtr = (*bufPtr)[:cap(*bufPtr)]
			ReceivePacketBufPool.Put(bufPtr)
			protoPack.Data = nil
		}

		// 协议数据包放回对象池，减少每次都new新包的开销
		ProtoPackInfoPool.Put(protoPack)
	}()
	if callFunc, res := mod.HandleFuncMap[protoPack.ProName]; res {
		dataIdxFrom := gateway_server.ProtoNumLen + gateway_server.SessionIdLen
		callFunc((*protoPack.Data)[dataIdxFrom:], protoPack.GateId, protoPack.SessionID)
	} else {
		printer.TextError("ModCall", "ServerName", ServicePrt.ServerName, "ServerId", ServicePrt.ServerId, "MissCallProto", protoPack.ProName)
	}
}

// 处理接口数据包（来源：同进程下不同模块）
func (mod *ServerModule) HandleModulePack(modulePack *ModulePackInfo) {
	defer func() {
		if err := recover(); err != nil {
			printer.TextError("HandleModulePack", "ServerName", ServicePrt.ServerName, "ServerId", ServicePrt.ServerId, "modName", mod.modName, "Error", err)
		}

		// 清理引用后再放入池中，防止内存泄漏
		modulePack.ArgsList = nil
		modulePack.ReturnChan = nil
		ModulePackInfoPool.Put(modulePack)
	}()

	//函数参数检测（调用者必须是函数对象）
	interfaceName := modulePack.InterfaceName
	fn, resFindFunc := mod.InterfaceFuncMap[interfaceName]
	if !resFindFunc {
		err := errors.New("HandleModulePack Error Search InterfaceFuncMap With InterfaceName : " + interfaceName)
		printer.PanicStack(err)
		return
	}

	//判断是否需要同步返回结果
	results := fn(modulePack.ArgsList...)
	if modulePack.ReturnChan == nil {
		return
	}
	modulePack.ReturnChan <- results
}

// 注册协议对应的函数调用
func (mod *ServerModule) RegisterProtoHandleCaller(proName string, protoHandleFunction func(data []byte, gateId int, sessionID uint32)) {
	ProtoName2ModMap[proName] = mod.receiptChan
	mod.HandleFuncMap[proName] = protoHandleFunction
}

// 注册同进程模块接口对应的函数调用
func (mod *ServerModule) RegisterInterfaceHandleCaller(interfaceName string, interfaceHandleFunction func(ags ...any) interface{}) {
	InterfaceName2ModMap[interfaceName] = mod.receiptChan
	mod.InterfaceFuncMap[interfaceName] = interfaceHandleFunction
}

// 打包模块调用数据“同步”到对应的模块通道并等待返回结果
func SynchronousPack2Module(modName string, interfaceFuncName string, args []any) interface{} {
	if sm, ok := ServerModMgr.Load(modName); ok {
		if mod, res := sm.(*ServerModule); res {
			timer := time.NewTimer(ModCallerTimeOut) // 入队和等待结果共用一个超时预算。
			defer timer.Stop()
			returnChan := make(chan interface{}, 1)

			//创建模块包对象并发送到模块通道
			info := ModulePackInfoPool.Get().(*ModulePackInfo)
			info.InterfaceName = interfaceFuncName
			info.ArgsList = args
			info.ReturnChan = returnChan
			select {
			case mod.receiptChan <- info: // 所有权已交给模块，此后不能再访问或回收info
			case <-timer.C:
				info.ArgsList = nil
				info.ReturnChan = nil
				ModulePackInfoPool.Put(info)
				printer.TextError("SynchronousPack2Module_EnqueueTimeOut", "ModName", modName, "CallFunc", interfaceFuncName)
				return nil
			}

			//开始等待同步调用的数据返回
			select {
			case retData := <-returnChan:
				return retData
			case <-timer.C:
				printer.TextError("SynchronousPack2Module_TimeOut", "ModName", modName, "CallFunc", interfaceFuncName, "CallArgs", args)
				return nil
			}
		}
	} else {
		printer.TextError("SynchronousPack2Module_modNameError", "ModName", modName)
	}
	return nil
}

// 打包模块“异步”调用数据到对应的模块通道
func AsynchronousPack2Module(modName string, interfaceFuncName string, args []any) {
	if sm, ok := ServerModMgr.Load(modName); ok {
		if mod, res := sm.(*ServerModule); res {
			info := ModulePackInfoPool.Get().(*ModulePackInfo)
			info.InterfaceName = interfaceFuncName
			info.ArgsList = args
			info.ReturnChan = nil
			mod.receiptChan <- info
		}
	} else {
		printer.TextError("AsynchronousPack2Module_modNameError", "ModName", modName)
	}
}
