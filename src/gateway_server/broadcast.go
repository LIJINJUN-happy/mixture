package gateway_server

import (
	"encoding/binary"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"mixture/proto/interior_json"
	"mixture/proto/interior_pb/ipb"
	"mixture/proto/mutual_json"
	"mixture/proto/mutual_pb/pb"
	"mixture/src/common/printer"
	"strconv"
	"strings"
)

const (
	InternalBroadcastProtocolName = "S2SGateBroadcast" // 服务进程发给网关的内部广播协议，不会直接发送给客户端。
	ClientBroadcastProtocolName   = "S2CBroadcast"     // 客户端最终收到的广播协议。
)

// 记录一次网关本地广播的投递结果。
type BroadcastResult struct {
	Targets int // 找到的目标连接数
	Sent    int // 成功放入客户端写队列的数量
	Dropped int // 连接已关闭、写队列已满或session不存在的数量
	Invalid int // session字符串中无法转换的项目数量
}

// 将同一业务包投递给当前网关内的目标客户端。
// sessionIDs为空时遍历全部在线连接，否则解析逗号分隔的sessionID；每个客户端独占一块池化缓冲区。
func BroadcastToGateClients(sessionIDs string, protoNum uint16, payload []byte) BroadcastResult {
	result := BroadcastResult{}
	packetLen := SessionIdLen + ProtoNumLen + len(payload)
	if packetLen > int(MaxPacketBodySize) {
		result.Dropped = 1
		return result
	}

	send := func(sessionID uint32, connection *NewConnection) {
		result.Targets++
		bufPtr := DistributeDataPool.Get().(*[]byte)
		packet := (*bufPtr)[:packetLen]
		binary.BigEndian.PutUint32(packet[:SessionIdLen], sessionID)
		binary.BigEndian.PutUint16(packet[SessionIdLen:SessionIdLen+ProtoNumLen], protoNum)
		copy(packet[SessionIdLen+ProtoNumLen:], payload)
		*bufPtr = packet
		if connection.TrySend(bufPtr) {
			result.Sent++
			return
		}
		*bufPtr = (*bufPtr)[:cap(*bufPtr)]
		DistributeDataPool.Put(bufPtr)
		result.Dropped++
	}

	if sessionIDs == "" {
		GateClientMap.Range(func(key, value any) bool {
			sessionID, idOK := key.(uint32)
			session, sessionOK := value.(*clientSession)
			if idOK && sessionOK {
				send(sessionID, session.connection)
			}
			return true
		})
		return result
	}

	seen := make(map[uint32]struct{})
	for _, textID := range strings.Split(sessionIDs, ",") {
		value, err := strconv.ParseUint(strings.TrimSpace(textID), 10, 32)
		if err != nil || value == 0 {
			result.Invalid++
			continue
		}
		sessionID := uint32(value)
		if _, duplicate := seen[sessionID]; duplicate {
			continue
		}
		seen[sessionID] = struct{}{}
		client, ok := GateClientMap.Load(sessionID)
		if !ok {
			result.Dropped++
			continue
		}
		session, ok := client.(*clientSession)
		if !ok {
			result.Dropped++
			continue
		}
		send(sessionID, session.connection)
	}
	return result
}

// 解析内部广播协议并投递给当前网关内的目标客户端。
func HandleGateBroadcast(data []byte) {
	var sessionIDs string
	var broadcastData, clientPayload []byte
	switch ProtocolDistributeInfo.ProtocolType {
	case "protobuf":
		message := new(ipb.S2SGateBroadcast)
		if err := proto.Unmarshal(data, message); err != nil {
			printer.TextWarn("GateBroadcast Protobuf Unmarshal Failed", "err", err)
			return
		}
		sessionIDs = message.SessionIds
		broadcastData = message.Data
		var err error
		clientPayload, err = proto.Marshal(&pb.S2CBroadcast{Data: broadcastData})
		if err != nil {
			printer.TextWarn("GateBroadcast Client Protobuf Marshal Failed", "err", err)
			return
		}
	case "json":
		message := new(interior_json.S2SGateBroadcast)
		if err := json.Unmarshal(data, message); err != nil {
			printer.TextWarn("GateBroadcast JSON Unmarshal Failed", "err", err)
			return
		}
		sessionIDs = message.SessionIDs
		broadcastData = message.Data
		var err error
		clientPayload, err = json.Marshal(&mutual_json.S2CBroadcast{Data: broadcastData})
		if err != nil {
			printer.TextWarn("GateBroadcast Client JSON Marshal Failed", "err", err)
			return
		}
	default:
		printer.TextWarn("GateBroadcast Unsupported Protocol Type", "protocolType", ProtocolDistributeInfo.ProtocolType)
		return
	}

	targetProtoNum, ok := ProtocolDistributeInfo.ProtoNameMap[ClientBroadcastProtocolName]
	if !ok || ProtocolDistributeInfo.ProtoDistributeArray[targetProtoNum] != "gateway_server" {
		printer.TextWarn("GateBroadcast Client Protocol Missing", "protoName", ClientBroadcastProtocolName)
		return
	}
	result := BroadcastToGateClients(sessionIDs, targetProtoNum, clientPayload)
	if result.Dropped > 0 || result.Invalid > 0 {
		printer.TextWarn("GateBroadcast Partial Delivery", "protoNum", targetProtoNum, "targets", result.Targets, "sent", result.Sent, "dropped", result.Dropped, "invalid", result.Invalid)
	}
}
