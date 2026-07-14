package main

import (
	"context"
	"flag"
	"os"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/udp"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:6000", "UDP server target address")
	totalPackets := flag.Uint64("packets", 10000, "total packets to transmit")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing UDP client application")

	cfg := udp.DefaultConfig()
	cfg.ServerAddr = *serverAddr
	cfg.TotalPackets = *totalPackets
	cfg.BenchMode = false

	client, err := udp.NewClient(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create UDP client instance", "error", err)
		os.Exit(1)
	}

	if err := client.Run(); err != nil {
		log.Error("UDP client runtime execution failed", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = client.Shutdown(ctx)
	log.Info("UDP client execution completed successfully")
}
