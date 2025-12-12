package gateway_server

import (
	"bytes"
	"encoding/binary"
)

// 流数据打包处理流程
func DoPack(byteList []byte, data []byte, packWay uint8) int {
	byteList = byteList[0:0]
	switch packWay {
	case 1:
		return DoPackByReadHeadLen(byteList, data)
	case 2:
		return DoPackBySign(byteList, data)
	case 3:
		return DoPackByAppointLen(byteList, data)
	default:
	}
	return 0
}

// DoPackByReadHeadLen 按包头+包体数据的格式打包
func DoPackByReadHeadLen(byteList []byte, data []byte) int {
	dataLen := len(data)
	allLen := dataLen + HeadLen
	byteList = append(byteList[:HeadLen], data[0:]...)        //先存放数据用append扩大长度（预留HeadLen存放包头）
	binary.BigEndian.PutUint16(byteList[0:], uint16(dataLen)) //大端存放包头字节数据
	return allLen
}

// DoPackBySign 用切割符号作为标志打包
func DoPackBySign(byteList []byte, data []byte) int {
	byteList = append(byteList, data[0:]...)
	byteList = append(byteList, splitSign)
	return len(byteList)
}

// DoPackByAppointLen 定长方式打包（需要和接受方一致,空白处需要补全）
func DoPackByAppointLen(byteList []byte, data []byte) int {
	byteList = append(byteList, data[0:]...)
	limitSize, byteListSize := int(appointLen), len(byteList)
	if byteListSize > limitSize {
		return 0
	} else {
		//补充空白处
		diff := limitSize - byteListSize
		if diff > 0 {
			byteList = append(byteList, bytes.Repeat([]byte("\x00"), diff)...)
			//printer.TextDebug("doPackByAppointLen", "Len byteList", len(byteList))
		}
	}
	return limitSize
}
