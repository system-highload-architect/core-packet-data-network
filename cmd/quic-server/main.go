package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/quic"
)

func main() {
	var (
		listenAddr = flag.String("addr", "127.0.0.1:4242", "listen address")
		tlsCert    = flag.String("cert", "certs/cert.pem", "TLS certificate")
		tlsKey     = flag.String("key", "certs/key.pem", "TLS key")
		debug      = flag.Bool("debug", false, "enable debug logging")
	)
	flag.Parse()

	logLevel := logger.LevelInfo
	if *debug {
		logLevel = logger.LevelDebug
	}
	log := logger.New(logger.WithLevel(logLevel))

	cfg := &quic.ServerConfig{
		ListenAddr: *listenAddr,
		TLSCert:    *tlsCert,
		TLSKey:     *tlsKey,
	}

	server, err := quic.NewServer(cfg, log)
	if err != nil {
		log.Fatal("failed to create server: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := server.Run(); err != nil {
			log.Error("server run error: %v", err)
		}
		stop()
	}()

	<-ctx.Done()
	if err := server.Shutdown(context.Background()); err != nil {
		log.Error("shutdown error: %v", err)
	}
	log.Info("server finished")
}
