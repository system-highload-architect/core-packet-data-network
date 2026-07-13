package zeroalloc

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// GenerateRandomData создаёт слайс байт заданной длины со случайными данными.
func GenerateRandomData(length int) []byte {
	data := make([]byte, length)
	if _, err := rand.Read(data); err != nil {
		// fallback на псевдослучайные
		for i := range data {
			data[i] = byte(i % 256)
		}
	}
	return data
}

// GenerateRandomUint64 возвращает случайное 64-битное число.
func GenerateRandomUint64() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(b[:])
}
