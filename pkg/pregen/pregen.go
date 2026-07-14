package pregen

import (
	"crypto/sha256"
	"time"

	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/zeroalloc"
)

// Packets содержит предварительно сериализованные пакеты. Индекс = ID-1.
var Packets [][]byte

// RU: Глобальный буфер арены для удержания единого куска памяти
// EN: Global arena buffer to retain a single continuous memory block
var memoryArena []byte

// Init генерирует пакеты в глобальную переменную по паттерну Арены Памяти (Zero Allocations в цикле)
// Init generates packets into a global variable using the Memory Arena pattern (Zero Allocations in loop)
func Init(total int, maxDataSize int) {
	Packets = make([][]byte, total)

	// RU: Рассчитываем точный размер одного пакета: 8(ID) + 8(Time) + 4(Len) + dataLen + 32(Hash) + 2(ExtraLen) + 0(Extra)
	// RU: В среднем размер равен фиксированной части (54 байта) + переменный размер данных
	// EN: Calculate precise packet size constraints to pre-allocate continuous memory arena
	const fixedPacketHeaderSize = 8 + 8 + 4 + 32 + 2

	// RU: Шаг 1: Считаем суммарный объем памяти, который нужен под ВСЕ пакеты
	// EN: Step 1: Pre-calculate the exact cumulative memory size needed for ALL packets
	totalArenaSize := 0
	for id := uint64(1); id <= uint64(total); id++ {
		dataLen := int(id) % maxDataSize
		if dataLen < 1 {
			dataLen = 1
		}
		totalArenaSize += fixedPacketHeaderSize + dataLen
	}

	// RU: Шаг 2: Делаем ОДНУ единственную аллокацию под весь пул пакетов
	// EN: Step 2: Perform ONE single allocation for the entire packet pool
	memoryArena = make([]byte, totalArenaSize)
	arenaOffset := 0

	// RU: Локальные буферы для генерации (вынесены из цикла)
	// EN: Local generation buffers (moved outside the hot loop)
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
		// RU: Предрассчитываем хэш сразу фиксированным массивом
		// EN: Pre-compute hash natively as a fixed array block
		pkt.Checksum = sha256.Sum256(pkt.Data)

		serializeBuf = serializeBuf[:0]
		data, _ := pkt.SerializeTo(serializeBuf)

		// RU: Шаг 3: Нарезаем слайс прямо из Арены памяти (Ноль аллокаций!)
		// EN: Step 3: Slice directly out of the Memory Arena (Zero allocations!)
		nextOffset := arenaOffset + len(data)
		packetSlice := memoryArena[arenaOffset:nextOffset]
		copy(packetSlice, data)

		Packets[id-1] = packetSlice
		arenaOffset = nextOffset
	}
}
