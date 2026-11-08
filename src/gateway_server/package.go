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
	if len(data) == 0 || len(data) > int(MaxPacketBodySize) || bytes.IndexByte(data, splitSign) >= 0 || cap(byteList) < len(data)+1 {
		return 0
	}
	byteList = byteList[:0]
	byteList = append(byteList, data[0:]...)
	byteList = append(byteList, splitSign)
	return len(byteList)
}

// DoPackByAppointLen 固定包长556字节，前2字节为大端包体长度，包体最多554字节，其余补零。
func DoPackByAppointLen(byteList []byte, data []byte) int {
	if len(data) == 0 || len(data) > int(appointLen)-HeadLen || cap(byteList) < int(appointLen) {
		return 0
	}
	byteList = byteList[:int(appointLen)]
	copy(byteList[HeadLen:], data)
	binary.BigEndian.PutUint16(byteList[:HeadLen], uint16(len(data)))
	for i := HeadLen + len(data); i < len(byteList); i++ {
		byteList[i] = 0
	}
	return len(byteList)
}
