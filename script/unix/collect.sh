#!/bin/bash
dn=$(dirname $0)
cd ${dn} ; cd .. ; cd ..
#ls

if [ ! -d "/pprof/" ];then
	mkdir -p  pprof
	mkdir -p  pprof/actor_server
	mkdir -p  pprof/modularized_server
	mkdir -p  pprof/interactive_server
	mkdir -p  pprof/gateway_server
	mkdir -p  pprof/web_server
fi
cd pprof

analysis(){
	#PProf的地址端口
	local Host="$1"
	local Port="$2"
	local serverName="$3"
	
	#进去对应服务的目录
	cd $serverName

	# CPU性能分析（采集30秒）
	curl -s http://$Host:$Port/debug/pprof/profile?seconds=30 > profile.prof
	go tool pprof -text profile.prof > profile.txt
	rm -rf profile.prof

	# 堆内存分析
	curl -s http://$Host:$Port/debug/pprof/heap > heap.prof
	go tool pprof -text heap.prof > heap.txt
	rm -rf heap.prof

	# 内存分配分析
	curl -s http://$Host:$Port/debug/pprof/allocs > allocs.prof
	go tool pprof -text allocs.prof > allocs.txt
	rm -rf allocs.prof

	# Goroutine分析
	curl -s http://$Host:$Port/debug/pprof/goroutine?debug=1 > goroutineDebug1.txt
	curl -s http://$Host:$Port/debug/pprof/goroutine?debug=2 > goroutineDebug2.txt

	# 阻塞操作
	curl -s http://$Host:$Port/debug/pprof/block > block.prof
	go tool pprof -text block.prof > block.txt
	rm -rf block.prof

	# 互斥锁竞争
	curl -s http://$Host:$Port/debug/pprof/mutex > mutex.prof
	go tool pprof -text mutex.prof > mutex.txt
	rm -rf mutex.prof

	# 线程创建分析
	curl -s http://$Host:$Port/debug/pprof/threadcreate > threadcreate.prof
	go tool pprof -text threadcreate.prof > threadcreate.txt
	rm -rf threadcreate.prof
	
	#生成完毕后返回上一层
	cd ..
}

#调用函数执行分析逻辑以及生成对应的报告
analysis localhost 36060 gateway_server
analysis localhost 36061 actor_server
analysis localhost 36062 modularized_server
analysis localhost 36063 interactive_server
analysis localhost 36064 web_server
