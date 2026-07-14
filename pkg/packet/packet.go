package packet

import "time"

type Packet struct {
	ID        uint64
	Timestamp time.Time
	Data      []byte
	Checksum  [32]byte
	Extra     []byte
}
