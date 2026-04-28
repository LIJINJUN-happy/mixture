package gateway_server

import (
	"bytes"
	"encoding/binary"
	"mixture/src/common/printer"
)

// 处理服务进程连接对象发送过来的协议，返回 false 表示收到非法包，调用方应断开连接
func DealBufferData(newConn *NewConnection, readNum int, l *CutIndexList, sliceBuffer []byte) bool {
	if !DoSplit(newConn.StreamBuffer, newConn.ReadBuf, readNum, l, sliceBuffer) {
		return false
	}
	DoDistribute(l, sliceBuffer[0:], newConn.connSession)
	return true
}

// DoSplit /* 三种读取字节流切割方式： 1.根据字节长度来读取数据   2.根据标记来切割字节流  3定长读取，每个包都是固定长度*/
func DoSplit(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *CutIndexList, sliceBuffer []byte) bool {
	switch j.readWay {
	case 1:
		return DoSplitByReadHeadLen(j, byteSlice, readNum, list, sliceBuffer, 0)
	case 2:
		DoSplitBySign(j, byteSlice, readNum, list, sliceBuffer, 0)
	case 3:
		DoSplitByAppointLen(j, byteSlice, readNum, list, sliceBuffer, 0)
	default:
	}
	return true
}

// 1.根据字节长度来读取数据(注意切割的时候有可能是包头被切割了,需要判断下包头字节问题,除非包头是1字节)
// 返回 false 表示包头声明的长度超过允许上限，调用方应断开连接
func DoSplitByReadHeadLen(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *CutIndexList, sliceBuffer []byte, beginIdx int) bool {
	if j.totalNeedReadLen == 0 {
		if len(byteSlice) >= HeadLen {
			//需要判断包头是否被切割了（不仅仅只考虑包体拼接）
			diffLen := HeadLen - len(j.headReadArray) //头包剩余所需长度
			j.headReadArray = append(j.headReadArray, byteSlice[:diffLen]...)
			j.totalNeedReadLen = binary.BigEndian.Uint16(j.headReadArray[:HeadLen])

			// 包长校验：超过上限视为非法包，重置状态并通知断连
			if j.totalNeedReadLen > MaxPacketBodySize {
				printer.TextWarn("DoSplitByReadHeadLen", "Oversized packet declared", j.totalNeedReadLen, "limit", MaxPacketBodySize)
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

// 2.根据标记来切割字节流(这里用splitSign常量表示,该方法缺点是前端发送的数据中带有该切割符就会让该方法切割错误)
func DoSplitBySign(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *CutIndexList, sliceBuffer []byte, beginIdx int) {
	byteSlice = byteSlice[:readNum] //实际数据
	//printer.TextDebug("doSplitBySign", "要处理的数据大小", readNum)
	//printer.TextDebug("doSplitBySign", "要处理的数据", string(byteSlice))
	for {
		index := bytes.Index(byteSlice, []byte{splitSign})
		//没找到就直接追加上去然后break
		if index == -1 {
			j.byteData = append(j.byteData, byteSlice...)
			if uint16(len(j.byteData)) > MaxPacketBodySize {
				printer.TextWarn("DoSplitBySign", "Oversized packet accumulated", len(j.byteData), "limit", MaxPacketBodySize)
				j.byteData = j.byteData[0:0]
			}
			break
		} else {
			//在字符串中有切割符号
			j.byteData = append(j.byteData, byteSlice[:index]...)
			lenData := len(j.byteData)
			if uint16(lenData) > MaxPacketBodySize {
				printer.TextWarn("DoSplitBySign", "Oversized packet accumulated", len(j.byteData), "limit", MaxPacketBodySize)
			} else if lenData > 0 {
				//printer.TextDebug("doSplitBySign","The String", j.byteData)
				retNum := copy(sliceBuffer[beginIdx:], j.byteData[0:])
				beginIdx += retNum
				if retNum >= 1 {
					list.Push(beginIdx)
				}
			}
			j.byteData = j.byteData[0:0]
			readNum -= index //剩余多少字节没处理

			byteSlice = byteSlice[index:]
			//printer.TextDebug("剩余未处理字节大小" + strconv.Itoa(readNum))
			//printer.TextDebug("剩余内容： " + string(byteSlice))
			if (readNum == 1) || (readNum == 0) {
				break //只剩下切割符了或者没有字符串了（直接无视）
			}
			//到了这里必定是两个以上的数据长度（包含一个切割符，且切割符在第一位）
			DoSplitBySign(j, byteSlice[1:], readNum-1, list, sliceBuffer, beginIdx)
			return
		}
	}
}

// 3.根据定长取字节流数据(缺点是假如数据量少就会浪费流量资源,空白部分用'\0'补充)
func DoSplitByAppointLen(j *ReadStreamBufferObject, byteSlice []byte, readNum int, list *CutIndexList, sliceBuffer []byte, beginIdx int) {
	byteSlice = byteSlice[:readNum]
	diffLen := int(appointLen - j.curReadLen) //剩余形成完整数据包要读的数据大小
	if diffLen <= readNum {
		j.byteData = append(j.byteData, byteSlice[:diffLen]...)
		j.byteData = bytes.TrimRight(j.byteData, "\x00") //去除空白部分
		byteSlice = byteSlice[diffLen:]
		//printer.TextDebug("doSplitByAppointLen", "newStringData", string(j.byteData), "newStringData Len", len(string(j.byteData)))
		retNum := copy(sliceBuffer[beginIdx:], j.byteData[0:])
		beginIdx += retNum
		if retNum >= 1 {
			list.Push(beginIdx)
		}
		j.byteData = j.byteData[0:0]
		j.curReadLen = 0
		readNum -= diffLen

		//读取完需要判断下还有没有可数据,有就递归继续处理
		if readNum > 0 {
			DoSplitByAppointLen(j, byteSlice, readNum, list, sliceBuffer, beginIdx)
		}
	} else {
		j.byteData = append(j.byteData, byteSlice...)
		j.curReadLen += uint16(readNum)
	}
}
