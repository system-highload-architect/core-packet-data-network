package packet

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"
)

// Serialize упаковывает пакет в бинарный формат.
// Формат: [ID(8)][Timestamp(8)][DataLen(4)][Data][Checksum(32)][ExtraLen(2)][Extra]
func (p *Packet) Serialize() ([]byte, error) {
	dataLen := len(p.Data)
	extraLen := len(p.Extra)
	if dataLen > 0xFFFFFFFF {
		return nil, errors.New("packet: data too large")
	}
	if extraLen > 0xFFFF {
		return nil, errors.New("packet: extra data too large")
	}
	totalLen := 8 + 8 + 4 + dataLen + 32 + 2 + extraLen
	buf := make([]byte, totalLen)
	offset := 0

	// ID
	binary.BigEndian.PutUint64(buf[offset:offset+8], p.ID)
	offset += 8

	// Timestamp (Unix nano)
	unixNano := p.Timestamp.UnixNano()
	binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(unixNano))
	offset += 8

	// DataLen
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(dataLen))
	offset += 4

	// Data
	if dataLen > 0 {
		copy(buf[offset:offset+dataLen], p.Data)
		offset += dataLen
	}

	// Checksum
	if len(p.Checksum) != 32 {
		hash := sha256.Sum256(p.Data)
		p.Checksum = hash[:]
	}
	copy(buf[offset:offset+32], p.Checksum)
	offset += 32

	// ExtraLen
	binary.BigEndian.PutUint16(buf[offset:offset+2], uint16(extraLen))
	offset += 2

	// Extra
	if extraLen > 0 {
		copy(buf[offset:offset+extraLen], p.Extra)
	}

	return buf, nil
}

// Deserialize распаковывает бинарные данные в пакет.
func (p *Packet) Deserialize(data []byte) error {
	if len(data) < 8+8+4+32+2 {
		return errors.New("packet: data too short")
	}
	offset := 0

	p.ID = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	unixNano := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	p.Timestamp = time.Unix(0, unixNano)
	offset += 8

	dataLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if int(dataLen) > len(data)-offset-32-2 {
		return errors.New("packet: data length exceeds packet length")
	}
	if int(dataLen) > 0 {
		p.Data = make([]byte, dataLen)
		copy(p.Data, data[offset:offset+int(dataLen)])
	} else {
		p.Data = nil
	}
	offset += int(dataLen)

	if len(data) < offset+32 {
		return errors.New("packet: missing checksum")
	}
	p.Checksum = make([]byte, 32)
	copy(p.Checksum, data[offset:offset+32])
	offset += 32

	if len(data) < offset+2 {
		return errors.New("packet: missing extra length")
	}
	extraLen := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	if int(extraLen) > len(data)-offset {
		return errors.New("packet: extra data length exceeds packet length")
	}
	if extraLen > 0 {
		p.Extra = make([]byte, extraLen)
		copy(p.Extra, data[offset:offset+int(extraLen)])
	} else {
		p.Extra = nil
	}

	// Проверка контрольной суммы
	if !p.VerifyChecksum() {
		return errors.New("packet: checksum mismatch")
	}

	return nil
}

// VerifyChecksum проверяет, что контрольная сумма соответствует данным.
func (p *Packet) VerifyChecksum() bool {
	if len(p.Checksum) != 32 {
		return false
	}
	hash := sha256.Sum256(p.Data)
	return equal(hash[:], p.Checksum)
}

// equal сравнивает два среза байт без лишних аллокаций.
func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// SerializeTo пытается сериализовать пакет в предоставленный буфер buf.
// Если cap(buf) достаточно, возвращает buf[:totalLen].
// Иначе аллоцирует новый буфер и возвращает его.
func (p *Packet) SerializeTo(buf []byte) ([]byte, error) {
	dataLen := len(p.Data)
	extraLen := len(p.Extra)
	if dataLen > 0xFFFFFFFF {
		return nil, errors.New("packet: data too large")
	}
	if extraLen > 0xFFFF {
		return nil, errors.New("packet: extra data too large")
	}
	totalLen := 8 + 8 + 4 + dataLen + 32 + 2 + extraLen

	var data []byte
	if cap(buf) >= totalLen {
		data = buf[:totalLen]
	} else {
		data = make([]byte, totalLen)
	}

	offset := 0
	binary.BigEndian.PutUint64(data[offset:offset+8], p.ID)
	offset += 8

	unixNano := p.Timestamp.UnixNano()
	binary.BigEndian.PutUint64(data[offset:offset+8], uint64(unixNano))
	offset += 8

	binary.BigEndian.PutUint32(data[offset:offset+4], uint32(dataLen))
	offset += 4

	if dataLen > 0 {
		copy(data[offset:offset+dataLen], p.Data)
		offset += dataLen
	}

	if len(p.Checksum) != 32 {
		hash := sha256.Sum256(p.Data)
		p.Checksum = hash[:]
	}
	copy(data[offset:offset+32], p.Checksum)
	offset += 32

	binary.BigEndian.PutUint16(data[offset:offset+2], uint16(extraLen))
	offset += 2

	if extraLen > 0 {
		copy(data[offset:offset+extraLen], p.Extra)
	}

	return data, nil
}
