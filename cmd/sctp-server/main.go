package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/common/metrics"
	"core-packet-data-network/internal/variants/sctp"
)

func main() {
	var (
		listenAddr = flag.String("addr", "127.0.0.1:5000", "listen address")
		debug      = flag.Bool("debug", false, "debug logging")
	)
	flag.Parse()

	logLevel := logger.LevelInfo
	if *debug {
		logLevel = logger.LevelDebug
	}
	log := logger.New(logger.WithLevel(logLevel))

	cfg := &sctp.Config{
		ListenAddr: *listenAddr,
	}
	// метрики можно добавить при желании
	metr := metrics.NewRegistry()

	server, err := sctp.NewServer(cfg, log, metr)
	if err != nil {
		log.Fatal("server create: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := server.Run(); err != nil {
			log.Error("run error: %v", err)
		}
		stop()
	}()

	<-ctx.Done()
	if err := server.Shutdown(context.Background()); err != nil {
		log.Error("shutdown error: %v", err)
	}
	log.Info("server finished")
}
