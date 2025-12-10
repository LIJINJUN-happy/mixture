package settings

import "sync/atomic"

// 所有配置的全局名称以及其对应的结构体原子指针定义
var ConstCfg atomic.Pointer[constcfg]

// 加载配置统一接口
func DoLoad(loadAll bool, loadAssignPath string) {
	if path := "./settings/const_cfg.json"; loadAll || (loadAssignPath == path) {
		LoadWay(loadAll, path, ConstCfg.Store)
	}
}
