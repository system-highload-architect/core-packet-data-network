package packet

import (
	"testing"
	"time"
)

func TestPacket_SerializeDeserialize(t *testing.T) {
	orig := Packet{
		ID:        12345,
		Timestamp: time.Now(),
		Data:      []byte("hello world"),
		Extra:     []byte("extra"),
	}
	// Принудительно вычисляем контрольную сумму
	orig.Checksum = ComputeChecksum(orig.Data)

	data, err := orig.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	var restored Packet
	if err := restored.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if restored.ID != orig.ID {
		t.Errorf("ID mismatch: got %d, want %d", restored.ID, orig.ID)
	}
	if !restored.Timestamp.Equal(orig.Timestamp) {
		t.Errorf("Timestamp mismatch: got %v, want %v", restored.Timestamp, orig.Timestamp)
	}
	if string(restored.Data) != string(orig.Data) {
		t.Errorf("Data mismatch: got %s, want %s", restored.Data, orig.Data)
	}
	if string(restored.Extra) != string(orig.Extra) {
		t.Errorf("Extra mismatch: got %s, want %s", restored.Extra, orig.Extra)
	}
	if !restored.VerifyChecksum() {
		t.Error("Checksum verification failed")
	}
}

func TestPacket_ChecksumMismatch(t *testing.T) {
	p := Packet{
		ID:        1,
		Timestamp: time.Now(),
		Data:      []byte("test data"),
	}
	p.Checksum = []byte("not a valid hash") // не 32 байта
	if p.VerifyChecksum() {
		t.Error("VerifyChecksum should return false for invalid checksum length")
	}
	// Устанавливаем неправильную контрольную сумму
	p.Checksum = make([]byte, 32)
	// Контрольная сумма не соответствует данным
	validHash := ComputeChecksum(p.Data)
	// Инвертируем первый байт, чтобы сделать её неверной
	invalidHash := make([]byte, 32)
	copy(invalidHash, validHash)
	invalidHash[0] ^= 0xFF
	p.Checksum = invalidHash
	if p.VerifyChecksum() {
		t.Error("VerifyChecksum should detect mismatch")
	}
}

func TestPacket_EmptyData(t *testing.T) {
	orig := Packet{
		ID:        999,
		Timestamp: time.Now(),
		Data:      []byte{},
		Extra:     []byte{},
	}
	orig.Checksum = ComputeChecksum(orig.Data)

	data, err := orig.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	var restored Packet
	if err := restored.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if !restored.VerifyChecksum() {
		t.Error("Checksum verification failed for empty data")
	}
	if len(restored.Data) != 0 {
		t.Errorf("Data should be empty, got length %d", len(restored.Data))
	}
}
