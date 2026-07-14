package packet

import (
	"crypto/sha256"
)

// ComputeChecksum вычисляет SHA-256 от данных и возвращает фиксированный массив.
// Это исключает аллокации в куче (Escape Analysis) и копирование памяти.
// ComputeChecksum calculates SHA-256 and returns a fixed-size array byte.
// This prevents heap allocations and memory escaping to the heap.
func ComputeChecksum(data []byte) [32]byte {
	return sha256.Sum256(data)
}

// ComputeChecksumSlice оставлен для обратной совместимости, если где-то нужен срез.
// ComputeChecksumSlice preserved for backward compatibility where slice is required.
func ComputeChecksumSlice(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}
