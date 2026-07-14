package quic

type Config struct {
	ServerAddr    string
	ClientAddr    string
	CertFile      string
	KeyFile       string
	TotalPackets  uint64
	MaxPacketSize int
	BenchMode     bool
	PregenPackets [][]byte
	Workers       int
}

func DefaultConfig() *Config {
	return &Config{
		ServerAddr:    "127.0.0.1:8000",
		ClientAddr:    "127.0.0.1:0",
		CertFile:      "certs/cert.pem",
		KeyFile:       "certs/key.pem",
		TotalPackets:  10000,
		MaxPacketSize: 1400,
		Workers:       10,
	}
}
