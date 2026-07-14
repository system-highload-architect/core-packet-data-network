package pregen

import (
	"time"

	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/zeroalloc"
)

// Packets содержит предварительно сериализованные пакеты. Индекс = ID-1.
var Packets [][]byte

// Init генерирует пакеты в глобальную переменную.
// total - количество пакетов, maxDataSize - максимальный размер данных.
func Init(total int, maxDataSize int) {
	Packets = make([][]byte, total)
	buf := make([]byte, 0, maxDataSize+64)
	dataBuf := make([]byte, maxDataSize)

	for id := uint64(1); id <= uint64(total); id++ {
		dataLen := int(id) % maxDataSize
		if dataLen < 1 {
			dataLen = 1
		}
		zeroalloc.FillRandomBytes(dataBuf[:dataLen])

		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(), // фиктивное время
			Data:      dataBuf[:dataLen],
		}
		data, _ := pkt.SerializeTo(buf)
		Packets[id-1] = make([]byte, len(data))
		copy(Packets[id-1], data)
		buf = data[:0]
	}
}
