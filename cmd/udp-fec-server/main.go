package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"

	"core-packet-data-network/internal/common/logger"
	udpfec "core-packet-data-network/internal/variants/udp-fec"
)

func main() {
	var (
		listenAddr = flag.String("addr", "127.0.0.1:7000", "listen address")
		dataShards = flag.Int("data-shards", 4, "FEC data shards count")
		debug      = flag.Bool("debug", false, "debug logging")
	)
	flag.Parse()

	logLevel := logger.LevelInfo
	if *debug {
		logLevel = logger.LevelDebug
	}
	log := logger.New(logger.WithLevel(logLevel))

	cfg := &udpfec.Config{
		ServerAddr: *listenAddr,
		DataShards: *dataShards,
	}

	server, err := udpfec.NewServer(cfg, log)
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
