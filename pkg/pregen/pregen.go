package pregen

import (
	"crypto/sha256"
	"time"

	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/zeroalloc"
)

// Packets содержит предварительно сериализованные пакеты. Индекс = ID-1.
var Packets [][]byte

// Глобальный буфер арены для удержания единого куска памяти
var memoryArena []byte

// Init генерирует пакеты в глобальную переменную по паттерну Арены Памяти (Zero Allocations в цикле)
func Init(total int, maxDataSize int) {
	Packets = make([][]byte, total)

	// Рассчитываем точный размер одного пакета: 8(ID) + 8(Time) + 4(Len) + dataLen + 32(Hash) + 2(ExtraLen) + 0(Extra)
	// В среднем размер равен фиксированной части (54 байта) + переменный размер данных
	const fixedPacketHeaderSize = 8 + 8 + 4 + 32 + 2

	// Шаг 1: Считаем суммарный объем памяти, который нужен под ВСЕ пакеты
	totalArenaSize := 0
	for id := uint64(1); id <= uint64(total); id++ {
		dataLen := int(id) % maxDataSize
		if dataLen < 1 {
			dataLen = 1
		}
		totalArenaSize += fixedPacketHeaderSize + dataLen
	}

	// Шаг 2: Делаем ОДНУ единственную аллокацию под весь пул пакетов
	memoryArena = make([]byte, totalArenaSize)
	arenaOffset := 0

	// Локальные буферы для генерации (вынесены из цикла)
	dataBuf := make([]byte, maxDataSize)
	serializeBuf := make([]byte, 0, maxDataSize+fixedPacketHeaderSize)

	for id := uint64(1); id <= uint64(total); id++ {
		dataLen := int(id) % maxDataSize
		if dataLen < 1 {
			dataLen = 1
		}
		zeroalloc.FillRandomBytes(dataBuf[:dataLen])

		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(),
			Data:      dataBuf[:dataLen],
		}
		// Предрассчитываем хэш сразу фиксированным массивом
		pkt.Checksum = sha256.Sum256(pkt.Data)

		serializeBuf = serializeBuf[:0]
		data, _ := pkt.SerializeTo(serializeBuf)

		// Шаг 3: Нарезаем слайс прямо из Арены памяти (Ноль аллокаций!)
		nextOffset := arenaOffset + len(data)
		packetSlice := memoryArena[arenaOffset:nextOffset]
		copy(packetSlice, data)

		Packets[id-1] = packetSlice
		arenaOffset = nextOffset
	}
}
