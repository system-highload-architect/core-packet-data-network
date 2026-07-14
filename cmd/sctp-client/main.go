package main

import (
	"flag"
	"os"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/sctp"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:9000", "SCTP server target address")
	totalPackets := flag.Uint64("packets", 10000, "total packets to transmit via SCTP")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing high-performance SCTP client application")

	cfg := sctp.DefaultConfig()
	cfg.ServerAddr = *serverAddr
	cfg.TotalPackets = *totalPackets
	cfg.BenchMode = false

	client, err := sctp.NewClient(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create SCTP client instance", "error", err)
		os.Exit(1)
	}

	if err := client.Run(); err != nil {
		log.Error("SCTP client session runtime failed", "error", err)
		os.Exit(1)
	}

	client.Shutdown()
	log.Info("SCTP client session completed successfully")
}
