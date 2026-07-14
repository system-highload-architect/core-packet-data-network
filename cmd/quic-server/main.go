package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/quic"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8000", "QUIC server listen address")
	certFile := flag.String("cert", "certs/cert.pem", "path to TLS certificate")
	keyFile := flag.String("key", "certs/key.pem", "path to TLS private key")
	flag.Parse()

	log := logger.New(logger.WithLevel(logger.LevelInfo), logger.WithOutput(os.Stdout))
	log.Info("initializing TLS-secured QUIC server application")

	cfg := quic.DefaultConfig()
	cfg.ServerAddr = *addr
	cfg.CertFile = *certFile
	cfg.KeyFile = *keyFile
	cfg.BenchMode = false

	server, err := quic.NewServer(cfg, log, os.Stdout)
	if err != nil {
		log.Error("failed to create QUIC server instance", "error", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Run(); err != nil {
			log.Error("QUIC server runtime error", "error", err)
		}
	}()

	<-sigCh
	log.Info("interruption signal received, stopping QUIC listener")

	server.Shutdown()
	log.Info("QUIC server stopped cleanly")
}
