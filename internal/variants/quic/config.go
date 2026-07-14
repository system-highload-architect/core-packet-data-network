package quic

import (
	"crypto/tls"
	"time"
)

type ClientConfig struct {
	ServerAddr    string
	TLSConfig     *tls.Config
	TotalPackets  uint64
	MaxPacketSize int
	MaxRetries    int
	RetryTimeout  time.Duration
	BenchMode     bool
	PregenPackets [][]byte
}

type ServerConfig struct {
	ListenAddr string
	TLSConfig  *tls.Config
	BenchMode  bool
}
