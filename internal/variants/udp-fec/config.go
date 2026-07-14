package udpfec

import "time"

type Config struct {
	ServerAddr    string
	ClientAddr    string
	TotalPackets  uint64
	MaxPacketSize int
	DataShards    int // количество шардов данных (parity = 1)
	MaxRetries    int
	RetryTimeout  time.Duration
	BenchMode     bool
	PregenPackets [][]byte
	Workers       int // RU: Добавляем поле для контроля потоков | EN: Add field for concurrency control
}

func DefaultConfig() *Config {
	return &Config{
		ServerAddr:    "127.0.0.1:7000",
		ClientAddr:    "127.0.0.1:0",
		TotalPackets:  10000,
		MaxPacketSize: 1400,
		DataShards:    4,
		MaxRetries:    3,
		RetryTimeout:  100 * time.Millisecond,
		Workers:       10, // RU: Строго 10 потоков по умолчанию по ТЗ | EN: Exactly 10 threads by default per task requirements
	}
}
