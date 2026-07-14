package udp

import "time"

type Config struct {
	ServerAddr    string
	ClientAddr    string
	TotalPackets  uint64
	MaxPacketSize int
	MaxRetries    int
	RetryTimeout  time.Duration
	BenchMode     bool
	PregenPackets [][]byte      // если не nil, используется вместо генерации
	SendInterval  time.Duration // интервал между пакетами в генераторе (0 = без задержки)
}

func DefaultConfig() *Config {
	return &Config{
		ServerAddr:    "127.0.0.1:6000",
		ClientAddr:    "127.0.0.1:0",
		TotalPackets:  10000,
		MaxPacketSize: 1400,
		MaxRetries:    3,
		RetryTimeout:  100 * time.Millisecond,
	}
}
