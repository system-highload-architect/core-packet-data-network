package sctp

type Config struct {
	ServerAddr    string
	ClientAddr    string
	TotalPackets  uint64
	MaxPacketSize int
	BenchMode     bool
	PregenPackets [][]byte
}

func DefaultConfig() *Config {
	return &Config{
		ServerAddr:    "127.0.0.1:9000",
		ClientAddr:    "127.0.0.1:0",
		TotalPackets:  10000,
		MaxPacketSize: 1400,
	}
}
