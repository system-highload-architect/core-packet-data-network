package fec

import (
	"errors"

	"github.com/klauspost/reedsolomon"
)

type ReedSolomon struct {
	enc reedsolomon.Encoder
	ds  int
	ps  int
}

func NewReedSolomon(dataShards, parityShards int) (*ReedSolomon, error) {
	if dataShards <= 0 || parityShards <= 0 {
		return nil, errors.New("fec: shards count must be positive")
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	return &ReedSolomon{
		enc: enc,
		ds:  dataShards,
		ps:  parityShards,
	}, nil
}

func (r *ReedSolomon) Encode(shards [][]byte) ([][]byte, error) {
	if len(shards) != r.ds {
		return nil, errors.New("fec: invalid number of data shards")
	}
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
	total := r.ds + r.ps
	fullShards := make([][]byte, total)
	for i := 0; i < r.ds; i++ {
		fullShards[i] = shards[i]
	}
	for i := r.ds; i < total; i++ {
		fullShards[i] = make([]byte, size)
	}
	if err := r.enc.Encode(fullShards); err != nil {
		return nil, err
	}
	return fullShards, nil
}

func (r *ReedSolomon) Recover(shards [][]byte) ([][]byte, error) {
	if len(shards) != r.ds+r.ps {
		return nil, errors.New("fec: invalid number of shards")
	}
	var size int
	for _, sh := range shards {
		if sh == nil {
			continue
		}
		if size == 0 {
			size = len(sh)
		} else if len(sh) != size {
			return nil, errors.New("fec: shards have different sizes")
		}
	}
	if size == 0 {
		return nil, errors.New("fec: all shards are missing")
	}
	work := make([][]byte, len(shards))
	for i, sh := range shards {
		if sh == nil {
			work[i] = make([]byte, size)
		} else {
			work[i] = sh
		}
	}
	if err := r.enc.Reconstruct(work); err != nil {
		return nil, err
	}
	// Проверяем, что все шарды восстановлены
	for _, sh := range work {
		if sh == nil || len(sh) == 0 {
			return nil, errors.New("fec: reconstruction failed")
		}
	}
	return work, nil
}

func (r *ReedSolomon) DataShards() int {
	return r.ds
}

func (r *ReedSolomon) TotalShards() int {
	return r.ds + r.ps
}
