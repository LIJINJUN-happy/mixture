package test

import (
	"bytes"
	"encoding/binary"
	gate "mixture/src/gateway_server"
	"testing"
)

func makePacket(body []byte) []byte {
	packet := make([]byte, gate.HeadLen+len(body))
	binary.BigEndian.PutUint16(packet[:gate.HeadLen], uint16(len(body)))
	copy(packet[gate.HeadLen:], body)
	return packet
}

func newStream() *gate.ReadStreamBufferObject {
	stream := new(gate.ReadStreamBufferObject)
	stream.Init(1)
	return stream
}

// splitChunks 模拟TCP固定读缓冲：未读区域填充旧数据，readNum表示本次有效长度。
func splitChunks(t *testing.T, chunks ...[]byte) ([][]byte, bool) {
	t.Helper()
	stream := newStream()
	readBuffer := make([]byte, gate.ConnReadBufSize)
	out := make([]byte, int(gate.MaxPacketBodySize)+gate.ConnReadBufSize)
	var list gate.CutIndexList
	var packets [][]byte

	for _, chunk := range chunks {
		if len(chunk) > len(readBuffer) {
			t.Fatalf("test chunk length %d exceeds TCP read buffer", len(chunk))
		}
		for i := range readBuffer {
			readBuffer[i] = 0xEE
		}
		copy(readBuffer, chunk)
		list.Reset()
		if !gate.DoSplit(stream, readBuffer, len(chunk), &list, out) {
			return packets, false
		}

		begin := 0
		for i := 0; i < list.Len(); i++ {
			end := list.Get(i)
			if end <= begin || end > len(out) {
				t.Fatalf("invalid cut boundary %d..%d", begin, end)
			}
			packets = append(packets, bytes.Clone(out[begin:end]))
			begin = end
		}
	}
	return packets, true
}

func requirePackets(t *testing.T, got [][]byte, want ...[]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("packet count=%d, want=%d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("packet %d mismatch: got length=%d want length=%d", i, len(got[i]), len(want[i]))
		}
	}
}

func requireProtocolBodyLength(t *testing.T, body []byte, min, max int, wantValid bool) {
	t.Helper()
	valid := len(body) >= min && len(body) <= max
	if valid != wantValid {
		t.Fatalf("body length=%d, valid=%v, want valid=%v", len(body), valid, wantValid)
	}
}

func TestDoSplitByReadHeadLen17Scenarios(t *testing.T) {
	body1 := []byte{0, 1, 'a', 'b'}
	body2 := []byte{0, 2, 'c'}
	p1 := makePacket(body1)
	p2 := makePacket(body2)

	t.Run("01_完整单包", func(t *testing.T) {
		got, ok := splitChunks(t, p1)
		if !ok {
			t.Fatal("split returned false")
		}
		requirePackets(t, got, body1)
	})

	t.Run("02_包头按1加1拆分", func(t *testing.T) {
		got, ok := splitChunks(t, p1[:1], p1[1:2], p1[2:])
		if !ok {
			t.Fatal("split returned false")
		}
		requirePackets(t, got, body1)
	})

	t.Run("03_包体跨多次Read", func(t *testing.T) {
		got, ok := splitChunks(t, p1[:3], p1[3:4], p1[4:])
		if !ok {
			t.Fatal("split returned false")
		}
		requirePackets(t, got, body1)
	})

	t.Run("04_两个完整包粘在一起", func(t *testing.T) {
		got, ok := splitChunks(t, append(bytes.Clone(p1), p2...))
		if !ok {
			t.Fatal("split returned false")
		}
		requirePackets(t, got, body1, body2)
	})

	t.Run("05_前一个包半包后接剩余包体和下一包", func(t *testing.T) {
		joined := append(bytes.Clone(p1), p2...)
		got, ok := splitChunks(t, joined[:3], joined[3:])
		if !ok {
			t.Fatal("split returned false")
		}
		requirePackets(t, got, body1, body2)
	})

	t.Run("06_固定4096字节TCP缓冲但readNum等于1", func(t *testing.T) {
		stream := newStream()
		readBuffer := bytes.Repeat([]byte{0xEE}, gate.ConnReadBufSize)
		readBuffer[0] = p1[0]
		var list gate.CutIndexList
		out := make([]byte, int(gate.MaxPacketBodySize)+gate.ConnReadBufSize)
		if !gate.DoSplit(stream, readBuffer, 1, &list, out) {
			t.Fatal("header first byte was rejected")
		}
		if list.Len() != 0 {
			t.Fatal("partial header was published")
		}
		copy(readBuffer, p1[1:])
		list.Reset()
		if !gate.DoSplit(stream, readBuffer, len(p1)-1, &list, out) {
			t.Fatal("remaining packet was rejected")
		}
		if list.Len() != 1 || !bytes.Equal(out[:list.Get(0)], body1) {
			t.Fatal("packet was polluted by unread buffer bytes")
		}
	})

	t.Run("07_4096字节大残包完成后紧跟4093字节包", func(t *testing.T) {
		bodyA := bytes.Repeat([]byte{0xA1}, int(gate.MaxPacketBodySize))
		bodyB := bytes.Repeat([]byte{0xB2}, 4093)
		packetA := makePacket(bodyA)
		packetB := makePacket(bodyB)
		lastChunk := append(bytes.Clone(packetA[len(packetA)-1:]), packetB...)
		got, ok := splitChunks(t, packetA[:3002], packetA[3002:len(packetA)-1], lastChunk)
		if !ok {
			t.Fatal("large packet sequence was rejected")
		}
		requirePackets(t, got, bodyA, bodyB)
	})

	t.Run("08_客户端最小长度2", func(t *testing.T) {
		body := []byte{0, 1}
		got, ok := splitChunks(t, makePacket(body))
		if !ok {
			t.Fatal("minimum client body was rejected")
		}
		requirePackets(t, got, body)
		requireProtocolBodyLength(t, got[0], gate.ProtoNumLen, int(gate.MaxPacketBodySize), true)
	})

	t.Run("09_客户端长度1被协议层拒绝", func(t *testing.T) {
		body := []byte{1}
		got, ok := splitChunks(t, makePacket(body))
		if !ok {
			t.Fatal("generic framing rejected the one-byte body")
		}
		requirePackets(t, got, body)
		requireProtocolBodyLength(t, got[0], gate.ProtoNumLen, int(gate.MaxPacketBodySize), false)
	})

	t.Run("10_客户端最大长度4092", func(t *testing.T) {
		body := bytes.Repeat([]byte{0xA0}, int(gate.MaxPacketBodySize)-gate.SessionIdLen)
		body[0], body[1] = 0, 1
		got, ok := splitChunks(t, makePacket(body))
		if !ok {
			t.Fatal("maximum framing length was rejected")
		}
		requirePackets(t, got, body)
		requireProtocolBodyLength(t, got[0], gate.ProtoNumLen, int(gate.MaxPacketBodySize)-gate.SessionIdLen, true)
	})

	t.Run("11_客户端长度4093被协议层拒绝", func(t *testing.T) {
		body := bytes.Repeat([]byte{0xA1}, int(gate.MaxPacketBodySize)-gate.SessionIdLen+1)
		got, ok := splitChunks(t, makePacket(body))
		if !ok {
			t.Fatal("generic framing rejected the body before protocol validation")
		}
		requirePackets(t, got, body)
		requireProtocolBodyLength(t, got[0], gate.ProtoNumLen, int(gate.MaxPacketBodySize)-gate.SessionIdLen, false)
	})

	t.Run("12_内部最小长度6", func(t *testing.T) {
		body := []byte{0, 0, 0, 1, 0, 2}
		got, ok := splitChunks(t, makePacket(body))
		if !ok {
			t.Fatal("minimum internal body framing was rejected")
		}
		requirePackets(t, got, body)
		requireProtocolBodyLength(t, got[0], gate.SessionIdLen+gate.ProtoNumLen, int(gate.MaxPacketBodySize), true)
	})

	t.Run("13_内部长度5被协议层拒绝", func(t *testing.T) {
		body := []byte{0, 0, 0, 1, 0}
		got, ok := splitChunks(t, makePacket(body))
		if !ok {
			t.Fatal("generic framing rejected the five-byte body")
		}
		requirePackets(t, got, body)
		requireProtocolBodyLength(t, got[0], gate.SessionIdLen+gate.ProtoNumLen, int(gate.MaxPacketBodySize), false)
	})

	t.Run("14_内部最大长度4096", func(t *testing.T) {
		body := bytes.Repeat([]byte{0xB0}, int(gate.MaxPacketBodySize))
		packet := makePacket(body)
		got, ok := splitChunks(t, packet[:gate.ConnReadBufSize], packet[gate.ConnReadBufSize:])
		if !ok {
			t.Fatal("maximum internal body was rejected")
		}
		requirePackets(t, got, body)
		requireProtocolBodyLength(t, got[0], gate.SessionIdLen+gate.ProtoNumLen, int(gate.MaxPacketBodySize), true)
	})

	t.Run("15_readNum超过输入切片长度", func(t *testing.T) {
		var list gate.CutIndexList
		if gate.DoSplit(newStream(), []byte{0, 2}, 3, &list, make([]byte, 32)) {
			t.Fatal("invalid readNum was accepted")
		}
	})

	t.Run("16_输出缓冲不足", func(t *testing.T) {
		var list gate.CutIndexList
		packet := makePacket([]byte{0, 1, 'x'})
		if gate.DoSplit(newStream(), packet, len(packet), &list, make([]byte, 2)) {
			t.Fatal("truncated output was accepted")
		}
	})

	t.Run("17_单批超过1024个包", func(t *testing.T) {
		packet := makePacket([]byte{0, 1})
		input := bytes.Repeat(packet, gate.MaxPacketsPerRead+1)
		var list gate.CutIndexList
		if gate.DoSplit(newStream(), input, len(input), &list, make([]byte, 8192)) {
			t.Fatal("batch over MaxPacketsPerRead was accepted")
		}
		if list.Len() != gate.MaxPacketsPerRead {
			t.Fatalf("published packet count=%d, want=%d", list.Len(), gate.MaxPacketsPerRead)
		}
	})
}
