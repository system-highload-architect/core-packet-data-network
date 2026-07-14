package sctp

import "time"

// Config общая конфигурация для SCTP клиента и сервера.
type Config struct {
	ListenAddr    string        // адрес сервера (для сервера)
	RemoteAddr    string        // адрес сервера (для клиента)
	TotalPackets  uint64        // количество пакетов (клиент)
	MaxPacketSize int           // максимальный размер данных в пакете
	MaxRetries    int           // максимум повторных отправок (клиент)
	RetryTimeout  time.Duration // таймаут повторной отправки (клиент)
}

func DefaultConfig() *Config {
	return &Config{
		ListenAddr:    "127.0.0.1:5000",
		RemoteAddr:    "127.0.0.1:5000",
		TotalPackets:  10000,
		MaxPacketSize: 1400,
		MaxRetries:    3,
		RetryTimeout:  100 * time.Millisecond,
	}
}
