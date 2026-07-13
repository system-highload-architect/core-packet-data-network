package packet

import "time"

// Packet представляет пакет данных.
type Packet struct {
	ID        uint64
	Timestamp time.Time
	Data      []byte
	Checksum  []byte // 32 байта, SHA-256
	Extra     []byte // дополнительные данные (опционально)
}
