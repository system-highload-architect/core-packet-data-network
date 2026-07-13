package packet

import (
	"crypto/sha256"
)

// ComputeChecksum вычисляет SHA-256 от данных.
func ComputeChecksum(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}
