package packet

import "time"

type Packet struct {
	ID        uint64
	Timestamp time.Time
	Data      []byte
	Checksum  [32]byte // RU: Теперь это жесткий массив фиксированной длины, ноль аллокаций | EN: Fixed array, zero allocations
	Extra     []byte
}
