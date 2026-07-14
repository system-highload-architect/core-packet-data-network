package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/sctp" // Импортируем наш пакет sctp
)

func main() {
	// Парсим адрес прослушивания из консоли
	addr := flag.String("addr", "127.0.0.1:9000", "SCTP server listen address")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing high-performance SCTP server application")

	cfg := sctp.DefaultConfig()
	cfg.ServerAddr = *addr
	cfg.BenchMode = false // Включаем полноценный вывод результатов в поток

	server, err := sctp.NewServer(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create SCTP server instance", "error", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Run(); err != nil {
			log.Error("SCTP server runtime execution error", "error", err)
		}
	}()

	<-sigCh
	log.Info("interruption signal captured, stopping SCTP server pipeline")

	server.Shutdown()
	log.Info("SCTP server shutdown completed cleanly")
}
