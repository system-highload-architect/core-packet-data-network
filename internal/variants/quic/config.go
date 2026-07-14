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
	Workers       int // RU: Добавляем поле для контроля потоков | EN: Add field for concurrency control
}

func DefaultConfig() *Config {
	return &Config{
		ServerAddr:    "127.0.0.1:8000",
		ClientAddr:    "127.0.0.1:0",
		CertFile:      "certs/cert.pem",
		KeyFile:       "certs/key.pem",
		TotalPackets:  10000,
		MaxPacketSize: 1400,
		Workers:       10, // RU: Строго 10 потоков по умолчанию по ТЗ | EN: Exactly 10 threads by default per task requirements
	}
}
