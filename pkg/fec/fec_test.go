package fec

import (
	"bytes"
	"testing"
)

func TestXorEncoder(t *testing.T) {
	dataShards := 4
	fec := NewXorEncoder(dataShards)

	originalData := []byte("hello world, this is a test message for FEC")
	originalLen := len(originalData)

	shardSize := (originalLen + dataShards - 1) / dataShards
	shards := make([][]byte, dataShards)
	for i := 0; i < dataShards; i++ {
		start := i * shardSize
		end := start + shardSize
		if end > originalLen {
			end = originalLen
		}
		shard := make([]byte, shardSize)
		copy(shard, originalData[start:end])
		shards[i] = shard
	}

	encoded, err := fec.Encode(shards)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if len(encoded) != dataShards+1 {
		t.Errorf("expected %d shards, got %d", dataShards+1, len(encoded))
	}

	// Симулируем потерю одного шарда (индекс 1)
	lost := make([][]byte, len(encoded))
	for i, sh := range encoded {
		if i == 1 {
			lost[i] = nil
		} else {
			lost[i] = sh
		}
	}

	recovered, err := fec.Recover(lost)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Проверяем, что все шарды восстановлены
	for i, sh := range recovered {
		if sh == nil || len(sh) == 0 {
			t.Errorf("shard %d is missing after recovery", i)
		}
	}

	// Объединяем восстановленные шарды данных
	var recoveredData []byte
	for i := 0; i < dataShards; i++ {
		recoveredData = append(recoveredData, recovered[i]...)
	}
	if len(recoveredData) > originalLen {
		recoveredData = recoveredData[:originalLen]
	}

	if !bytes.Equal(recoveredData, originalData) {
		t.Errorf("recovered data does not match original:\n got: '%s'\n expected: '%s'", string(recoveredData), string(originalData))
	}
}
