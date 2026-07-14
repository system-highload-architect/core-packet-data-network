package packet

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"
)

// Serialize упаковывает пакет в бинарный формат.
func (p *Packet) Serialize() ([]byte, error) {
	return p.SerializeTo(nil)
}

// SerializeTo упаковывает пакет в предоставленный буфер без аллокаций, если емкости достаточно.
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

	// Считаем хэш, если он не был предрассчитан
	zeroChecksum := [32]byte{}
	if p.Checksum == zeroChecksum {
		p.Checksum = sha256.Sum256(p.Data)
	}
	copy(data[offset:offset+32], p.Checksum[:])
	offset += 32

	binary.BigEndian.PutUint16(data[offset:offset+2], uint16(extraLen))
	offset += 2

	if extraLen > 0 {
		copy(data[offset:offset+extraLen], p.Extra)
	}

	return data, nil
}

// Deserialize распаковывает данные, ссылаясь на исходную память без выделения новой (Zero-Copy)
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

	// Вместо p.Data = make([]byte) просто берем срез (ссылку на исходный буфер)
	if int(dataLen) > 0 {
		p.Data = data[offset : offset+int(dataLen)]
	} else {
		p.Data = nil
	}
	offset += int(dataLen)

	if len(data) < offset+32 {
		return errors.New("packet: missing checksum")
	}

	// Копируем фиксированные 32 байта прямо в массив структуры
	copy(p.Checksum[:], data[offset:offset+32])
	offset += 32

	if len(data) < offset+2 {
		return errors.New("packet: missing extra length")
	}
	extraLen := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	if int(extraLen) > len(data)-offset {
		return errors.New("packet: extra data length exceeds packet length")
	}

	// Zero-Copy для поля Extra
	if extraLen > 0 {
		p.Extra = data[offset : offset+int(extraLen)]
	} else {
		p.Extra = nil
	}

	// Финальная проверка целостности пакета
	if !p.VerifyChecksum() {
		return errors.New("packet: checksum mismatch")
	}

	return nil
}

// VerifyChecksum проверяет контрольную сумму без аллокаций
func (p *Packet) VerifyChecksum() bool {
	hash := sha256.Sum256(p.Data)
	return hash == p.Checksum
}
