package sctp

type Config struct {
	ListenAddr    string
	RemoteAddr    string
	TotalPackets  uint64
	MaxPacketSize int
	BenchMode     bool
	PregenPackets [][]byte
}

func DefaultConfig() *Config {
	return &Config{
		ListenAddr:    "127.0.0.1:5000",
		RemoteAddr:    "127.0.0.1:5000",
		TotalPackets:  10000,
		MaxPacketSize: 1400,
	}
}
