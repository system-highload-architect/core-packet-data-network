package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"core-packet-data-network/internal/common/logger"
	udpfec "core-packet-data-network/internal/variants/udp-fec" // Импортируем наш пакет udpfec
)

func main() {
	// Парсим адрес из консоли
	addr := flag.String("addr", "127.0.0.1:7000", "UDP+FEC server listen address")
	shards := flag.Int("shards", 4, "number of data shards for FEC")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing UDP+FEC server application")

	cfg := udpfec.DefaultConfig()
	cfg.ServerAddr = *addr
	cfg.DataShards = *shards
	cfg.BenchMode = false // Включаем полноценный вывод и обработку

	server, err := udpfec.NewServer(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create UDP+FEC server instance", "error", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Run(); err != nil {
			log.Error("UDP+FEC server runtime error", "error", err)
		}
	}()

	<-sigCh
	log.Info("interruption signal received, initiating graceful shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error("error during UDP+FEC server shutdown", "error", err)
		os.Exit(1)
	}

	log.Info("UDP+FEC server stopped cleanly")
}
