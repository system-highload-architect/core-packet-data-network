package main

import (
	"flag"
	"os"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/quic"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:8000", "QUIC server target address")
	totalPackets := flag.Uint64("packets", 10000, "total packets to transmit via QUIC")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing TLS-secured QUIC client application")

	cfg := quic.DefaultConfig()
	cfg.ServerAddr = *serverAddr
	cfg.TotalPackets = *totalPackets
	cfg.BenchMode = false

	client, err := quic.NewClient(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create QUIC client instance", "error", err)
		os.Exit(1)
	}

	if err := client.Run(); err != nil {
		log.Error("QUIC client execution failed", "error", err)
		os.Exit(1)
	}

	client.Shutdown()
	log.Info("QUIC client session terminated successfully")
}
