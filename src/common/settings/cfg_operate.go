package settings

import (
	"encoding/json"
	"fmt"
	"mixture/src/common/printer"
	"os"
)

// (参数1:否全加载),(参数2:加载指定路径下的配置)（可以选择启动全加载以及热更指定文件）（参数3:需要执行的函数对象）
func LoadWay[T any](loadAll bool, loadAssignPath string, store func(*T)) {
	if loadAll {
		LoadConfig(loadAssignPath, store)
	} else {
		if reLoadErr := ReloadConfig(loadAssignPath, store); reLoadErr != nil {
			printer.PanicStack(reLoadErr)
			return
		}
	}
}

// 泛型加载配置并存储
func LoadConfig[T any](path string, store func(*T)) {
	cfg := new(T)
	data, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("Failed To Read Config File: %s, Error: %v", path, err))
		return
	}
	if errUnmarshal := json.Unmarshal(data, cfg); errUnmarshal != nil {
		panic(fmt.Sprintf("Failed To Unmarshal Config File: %s, Error: %v", path, errUnmarshal))
		return
	}
	store(cfg)
	//fmt.Println("LoadConfig Success：" + path)
}

// 泛型重载配置
func ReloadConfig[T any](path string, store func(*T)) error {
	cfg := new(T)
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read config file: %s, error: %v", path, err)
	}
	if errUnmarshal := json.Unmarshal(data, cfg); errUnmarshal != nil {
		return fmt.Errorf("failed to unmarshal config file: %s, error: %v", path, errUnmarshal)
	}
	store(cfg)
	printer.TextInfo("ReloadConfig Success", "Path", path)
	return nil
}
