package interior_json

type GateIdSyncInfo struct {
	SerName string `json:"ser_name"`
	GateId  int32  `json:"gate_id"`
}

// 服务进程发送给网关的广播指令（JSON内部协议）。
type S2SGateBroadcast struct {
	SessionIDs string `json:"session_ids"`
	Data       []byte `json:"data"`
}

// 内部转发到modularized
type S2SHotFixModularizedSerSettings struct {
	HotFixAll    bool   `json:"HotFixAll"`
	SettingsName string `json:"SettingsInfo"`
}

// 内部转发到interactive
type S2SHotFixInteractiveSerSettings struct {
	HotFixAll    bool   `json:"HotFixAll"`
	SettingsName string `json:"SettingsInfo"`
}

// 内部转发到Web
type S2SHotFixWebSerSettings struct {
	HotFixAll    bool   `json:"HotFixAll"`
	SettingsName string `json:"SettingsInfo"`
}
