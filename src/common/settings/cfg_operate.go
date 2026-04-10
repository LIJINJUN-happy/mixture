package settings

import (
	"encoding/json"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"mixture/src/common/printer"
	"os"
	"path/filepath"
	"time"
)

// (参数1:否全加载),(参数2:加载指定路径下的配置)（可以选择启动全加载以及热更指定文件）（参数3:需要执行的函数对象）
func LoadWay[T any](loadAll bool, loadAssignPath string, store func(*T)) {
	if loadAll {
		LoadConfig(loadAssignPath, store)
	} else if reLoadErr := ReloadConfig(loadAssignPath, store); reLoadErr != nil {
		printer.PanicStack(reLoadErr)
		return
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

// StartFileWatcher 监听 settings/ 目录，文件变更后自动热更配置
func StartFileWatcher() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		printer.TextError("FileWatcher", "init failed", err.Error())
		return
	}

	go func() {
		defer watcher.Close()

		debounce := time.NewTimer(0)
		<-debounce.C // 排空初始触发，避免启动时误触发

		var lastFile string // 记录最后触发的文件路径
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) && filepath.Ext(event.Name) == ".json" {
					lastFile = event.Name
					debounce.Reset(500 * time.Millisecond)
				}
			case <-debounce.C:
				if lastFile != "" {
					printer.TextInfo("HotReload", "Config file changed, reloading file", lastFile)
					DoLoad(false, lastFile)
					lastFile = ""
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				printer.TextError("FileWatcher", "error", err.Error())
			}
		}
	}()

	if err = watcher.Add("./settings"); err != nil {
		printer.TextError("FileWatcher", "add path failed", err.Error())
	}
}
