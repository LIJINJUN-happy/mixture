package settings

// 所有配置结构定义都在这里

// const_cfg配置定义
type constcfg struct {
	Const struct {
		HeroInitList      []int `json:"HeroInitList"`
		EquipmentInitList []int `json:"EquipmentInitList"`
	} `json:"Const"`
	Currency map[string]int `json:"Currency"`
}
