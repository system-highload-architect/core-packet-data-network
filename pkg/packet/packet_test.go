package packet

import (
	"crypto/sha256"
	"testing"
	"time"
)

// RU: Тест сериализации и десериализации пакета с проверкой точности данных
// EN: Test case covering end-to-end packet data serialization and deserialization
func TestPacket_SerializeDeserialize(t *testing.T) {
	orig := Packet{
		ID:        12345,
		Timestamp: time.Now(),
		Data:      []byte("hello world"),
		Extra:     []byte("extra"),
	}
	// RU: Рассчитываем контрольную сумму напрямую в структуру массива
	// EN: Compute and assign message checksum directly into the fixed-size structure array
	orig.Checksum = sha256.Sum256(orig.Data)

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
	// RU: Округление до наносекунд для точного сравнения времени в разных ОС
	// EN: Bound timestamps to nanosecond constraints ensuring accurate platform assertions
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

// RU: Тест валидации поврежденных данных и несовпадения контрольных сумм
// EN: Test covering verification handling of corrupted data payloads and hash mismatches
func TestPacket_ChecksumMismatch(t *testing.T) {
	p := Packet{
		ID:        1,
		Timestamp: time.Now(),
		Data:      []byte("test data"),
	}

	// RU: Инициализируем заведомо неверную контрольную сумму
	// EN: Intentionally initialize an invalid mock checksum array context
	p.Checksum = sha256.Sum256(p.Data)
	p.Checksum[0] ^= 0xFF // RU: Инвертируем байт, имитируя искажение в сети | EN: Invert byte mimicking network corruption

	if p.VerifyChecksum() {
		t.Error("VerifyChecksum should detect mismatch on altered data bytes")
	}
}

// RU: Тест корректной обработки граничных условий с пустыми полями
// EN: Test confirming accurate processing of dynamic boundaries containing empty payloads
func TestPacket_EmptyData(t *testing.T) {
	orig := Packet{
		ID:        999,
		Timestamp: time.Now(),
		Data:      []byte{},
		Extra:     []byte{},
	}
	orig.Checksum = sha256.Sum256(orig.Data)

	data, err := orig.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	var restored Packet
	if err := restored.Deserialize(data); err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}
	if !restored.VerifyChecksum() {
		t.Error("Checksum verification failed for empty data structures")
	}
	if len(restored.Data) != 0 {
		t.Errorf("Data should be empty, got length %d", len(restored.Data))
	}
}
