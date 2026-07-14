package udpfec

import "time"

type Config struct {
	ServerAddr    string
	ClientAddr    string
	TotalPackets  uint64
	MaxPacketSize int
	DataShards    int // например, 4
	MaxRetries    int
	RetryTimeout  time.Duration
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
	}
}
