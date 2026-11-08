package gateway_server

import (
	"bytes"
	"encoding/binary"
)

// 处理网关服务进程连接对象发送过来的协议，返回 false 表示收到非法包，调用方应断开连接
func DealBufferData(newConn *NewConnection, readNum int, l *CutIndexList, sliceBuffer []byte) bool {
	if !DoSplit(newConn.StreamBuffer, newConn.ReadBuf, readNum, l, sliceBuffer) {
		return false
	}
	DoDistribute(l, sliceBuffer[0:], newConn.connSession)
	return true
}

// 三种读取字节流切割方式： 1.根据字节长度来读取数据   2.根据标记来切割字节流  3定长读取，每个包都是固定长度
func DoSplit(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *CutIndexList, sliceBuffer []byte) bool {
	switch j.readWay {
	case 1:
		return DoSplitByReadHeadLen(j, byteSlice, readNum, list, sliceBuffer, 0)
	case 2:
		return DoSplitBySign(j, byteSlice, readNum, list, sliceBuffer, 0)
	case 3:
		return DoSplitByAppointLen(j, byteSlice, readNum, list, sliceBuffer, 0)
	default:
	}
	return false
}

// 模式1.根据字节长度来读取数据(注意切割的时候有可能是包头被切割了,需要判断下包头字节问题,除非包头是1字节)
// 返回 false 表示长度、参数或输出容量非法；调用方必须丢弃本批结果并断开连接。
func DoSplitByReadHeadLen(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *CutIndexList, sliceBuffer []byte, beginIdx int) bool {
	// Read缓冲区可能大于实际数据，所有分支（包括递归）只处理有效字节。
	if readNum < 0 || readNum > len(byteSlice) || beginIdx < 0 || beginIdx > len(sliceBuffer) {
		return false
	}
	if len(j.headReadArray) > HeadLen || j.curReadLen > j.totalNeedReadLen || len(j.byteData) != int(j.curReadLen) {
		return false
	}
	byteSlice = byteSlice[:readNum]
	if readNum == 0 {
		return true
	}

	if j.totalNeedReadLen == 0 {
		diffLen := HeadLen - len(j.headReadArray) //包头还缺多少字节，而不是总需要多少字节
		if readNum >= diffLen {
			j.headReadArray = append(j.headReadArray, byteSlice[:diffLen]...)
			j.totalNeedReadLen = binary.BigEndian.Uint16(j.headReadArray[:HeadLen])

			// 客户端至少包含协议号，内部连接还必须包含sessionId。
			// 错误由调用方记录并断连。
			if j.totalNeedReadLen < j.minPacketBodySize || j.totalNeedReadLen > j.maxPacketBodySize {
				j.totalNeedReadLen, j.curReadLen = 0, 0
				j.byteData = j.byteData[0:0]
				j.headReadArray = j.headReadArray[0:0]
				return false
			}

			byteSlice = byteSlice[diffLen:]
			readNum -= diffLen
		} else {
			//不足够读取整个头包长度（可能有1字节被分成下次Read的数据中）
			j.headReadArray = append(j.headReadArray, byteSlice...)
			return true
		}
	}
	packageNeedLen := j.totalNeedReadLen - j.curReadLen //该完整包剩余所需读取字节数
	//printer.TextDebug("doSplitByReadHeadLen", "包头对应的值", j.totalNeedReadLen, ", 该完整包剩余所需字节数为", packageNeedLen, ", 切片内剩余字节数为", readNum)

	if readNum < int(packageNeedLen) {
		j.byteData = append(j.byteData, byteSlice...) //剩余的不足够拼成一个完整包（先直接追加进内存等下次Read执行再继续拼接后半段）
		j.curReadLen += uint16(readNum)
	} else if readNum >= int(packageNeedLen) {
		// copy空间不足会静默截断，Push满后会丢包，必须在消费完整包之前检查。
		if int(j.totalNeedReadLen) > len(sliceBuffer)-beginIdx || list.Len() >= MaxPacketsPerRead {
			return false
		}
		j.byteData = append(j.byteData, byteSlice[:packageNeedLen]...)
		//printer.TextDebug("newString = " + string(j.byteData))
		retNum := copy(sliceBuffer[beginIdx:], j.byteData[0:])
		beginIdx += retNum
		if retNum >= 1 {
			list.Push(beginIdx)
		}

		//重置readStreamBufferObject结构对象成员值
		j.totalNeedReadLen, j.curReadLen = 0, 0
		j.byteData = j.byteData[0:0]
		j.headReadArray = j.headReadArray[0:0]
		byteSlice = byteSlice[packageNeedLen:]

		if readNum != int(packageNeedLen) {
			return DoSplitByReadHeadLen(j, byteSlice, readNum-int(packageNeedLen), list, sliceBuffer, beginIdx) //剩余的数据大于等于所需的(先拼接好传到发送通信到等待传输去Actor服务进程，然后用递归继续执行剩余的数据拼接新的数据)
		}
	}
	return true
}

// 模式2：非法包返回false，由调用方断连；超长包不能清空后继续解析残余数据。
func DoSplitBySign(j *ReadStreamBufferObject, input []byte, readNum int, list *CutIndexList, output []byte, begin int) bool {
	if readNum < 0 || readNum > len(input) || begin < 0 || begin > len(output) {
		return false
	}
	input = input[:readNum]
	for len(input) > 0 {
		end := bytes.IndexByte(input, splitSign)
		count := end
		if end < 0 {
			count = len(input)
		}
		size := len(j.byteData) + count
		if size > int(j.maxPacketBodySize) {
			return false
		}
		if end >= 0 && (size < int(j.minPacketBodySize) || size > len(output)-begin || list.Len() >= MaxPacketsPerRead) {
			return false
		}
		j.byteData = append(j.byteData, input[:count]...)
		if end < 0 {
			return true
		}
		begin += copy(output[begin:], j.byteData)
		list.Push(begin)
		j.byteData = j.byteData[:0]
		input = input[end+1:]
	}
	return true
}

// 模式3：固定包长556字节，格式为2字节大端包体长度、有效包体、补零。
// 按有效长度提取数据，保留包体末尾真实的零字节。
func DoSplitByAppointLen(j *ReadStreamBufferObject, input []byte, readNum int, list *CutIndexList, output []byte, begin int) bool {
	if readNum < 0 || readNum > len(input) || begin < 0 || begin > len(output) || len(j.byteData) >= int(appointLen) {
		return false
	}
	input = input[:readNum]
	for len(input) > 0 {
		count := int(appointLen) - len(j.byteData)
		if count > len(input) {
			count = len(input)
		}
		j.byteData = append(j.byteData, input[:count]...)
		input = input[count:]
		if len(j.byteData) >= HeadLen {
			size := int(binary.BigEndian.Uint16(j.byteData[:HeadLen]))
			if size < int(j.minPacketBodySize) || size > int(j.maxPacketBodySize) || size > int(appointLen)-HeadLen {
				return false
			}
			if len(j.byteData) == int(appointLen) {
				if size > len(output)-begin || list.Len() >= MaxPacketsPerRead {
					return false
				}
				begin += copy(output[begin:], j.byteData[HeadLen:HeadLen+size])
				list.Push(begin)
				j.byteData = j.byteData[:0]
			}
		}
	}
	return true
}
