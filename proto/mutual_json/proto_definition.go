package mutual_json

// 测试客户端发送给服务器
type C2STest struct {
	Name string `json:"name"`
}

// 测试服务器协议返回给客户端
type S2CTestRet struct {
	Name string `json:"name"`
}

// 网关广播给客户端的协议。
type S2CBroadcast struct {
	Data []byte `json:"data"`
}
