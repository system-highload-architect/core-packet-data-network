package fec

import "errors"

// XorEncoder реализует FEC через XOR (один избыточный шард).
type XorEncoder struct {
	dataShards int
}

// NewXorEncoder создаёт XOR-кодер.
func NewXorEncoder(dataShards int) *XorEncoder {
	return &XorEncoder{dataShards: dataShards}
}

// Encode вычисляет XOR всех шардов и добавляет его как parity.
func (x *XorEncoder) Encode(shards [][]byte) ([][]byte, error) {
	if len(shards) != x.dataShards {
		return nil, errors.New("fec: invalid number of data shards")
	}
	if x.dataShards == 0 {
		return nil, errors.New("fec: no data shards")
	}
	// Проверяем, что все шарды не nil и одинаковой длины
	var size int
	for i, sh := range shards {
		if sh == nil {
			return nil, errors.New("fec: data shard is nil")
		}
		if i == 0 {
			size = len(sh)
		} else if len(sh) != size {
			return nil, errors.New("fec: data shards have different sizes")
		}
	}
	if size == 0 {
		return nil, errors.New("fec: empty shards")
	}
	// Вычисляем parity
	parity := make([]byte, size)
	for i := 0; i < size; i++ {
		var val byte
		for _, sh := range shards {
			val ^= sh[i]
		}
		parity[i] = val
	}
	// Возвращаем все шарды (данные + parity)
	result := make([][]byte, x.dataShards+1)
	for i := 0; i < x.dataShards; i++ {
		result[i] = shards[i]
	}
	result[x.dataShards] = parity
	return result, nil
}

// Recover восстанавливает один потерянный шард (если потерян ровно один).
func (x *XorEncoder) Recover(shards [][]byte) ([][]byte, error) {
	if len(shards) != x.dataShards+1 {
		return nil, errors.New("fec: invalid number of shards")
	}
	missingIndex := -1
	var size int
	for i, sh := range shards {
		if sh == nil || len(sh) == 0 {
			if missingIndex != -1 {
				return nil, errors.New("fec: multiple shards missing, XOR cannot recover")
			}
			missingIndex = i
		} else {
			if size == 0 {
				size = len(sh)
			} else if len(sh) != size {
				return nil, errors.New("fec: shards have different sizes")
			}
		}
	}
	if missingIndex == -1 {
		return shards, nil // ничего не потеряно
	}
	if size == 0 {
		return nil, errors.New("fec: all shards are missing")
	}
	// Восстанавливаем потерянный шард XOR'ом всех остальных
	recovered := make([]byte, size)
	for i := 0; i < size; i++ {
		var val byte
		for j, sh := range shards {
			if j == missingIndex || sh == nil {
				continue
			}
			val ^= sh[i]
		}
		recovered[i] = val
	}
	// Создаём результат
	result := make([][]byte, len(shards))
	for i, sh := range shards {
		if sh != nil && len(sh) > 0 {
			result[i] = sh
		} else {
			result[i] = recovered
		}
	}
	return result, nil
}

// DataShards возвращает количество шардов данных.
func (x *XorEncoder) DataShards() int {
	return x.dataShards
}

// TotalShards возвращает общее количество шардов.
func (x *XorEncoder) TotalShards() int {
	return x.dataShards + 1
}
