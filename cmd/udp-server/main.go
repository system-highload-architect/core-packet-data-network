package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/udp"
)

func main() {
	// Парсим флаги командной строки
	addr := flag.String("addr", "127.0.0.1:6000", "UDP server listen address")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing UDP server application")

	cfg := udp.DefaultConfig()
	cfg.ServerAddr = *addr
	cfg.BenchMode = false // В консольном режиме честно выводим данные

	server, err := udp.NewServer(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create UDP server instance", "error", err)
		os.Exit(1)
	}

	// Канал для перехвата системных сигналов прерывания (Graceful Shutdown)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Run(); err != nil {
			log.Error("UDP server runtime error", "error", err)
		}
	}()

	<-sigCh
	log.Info("interruption signal received, initiating graceful shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error("error during UDP server shutdown", "error", err)
		os.Exit(1)
	}

	log.Info("UDP server stopped cleanly")
}
