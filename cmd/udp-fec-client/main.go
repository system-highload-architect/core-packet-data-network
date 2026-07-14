package main

import (
	"context"
	"flag"
	"os"
	"time"

	"core-packet-data-network/internal/common/logger"
	udpfec "core-packet-data-network/internal/variants/udp-fec"
)

func main() {
	serverAddr := flag.String("server", "127.0.0.1:7000", "UDP+FEC server target address")
	totalPackets := flag.Uint64("packets", 10000, "total packets to transmit")
	shards := flag.Int("shards", 4, "number of data shards for FEC")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing UDP+FEC client application")

	cfg := udpfec.DefaultConfig()
	cfg.ServerAddr = *serverAddr
	cfg.TotalPackets = *totalPackets
	cfg.DataShards = *shards
	cfg.BenchMode = false

	client, err := udpfec.NewClient(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create UDP+FEC client instance", "error", err)
		os.Exit(1)
	}

	if err := client.Run(); err != nil {
		log.Error("UDP+FEC client runtime execution failed", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_ = client.Shutdown(ctx)
	log.Info("UDP+FEC client execution completed successfully")
}
