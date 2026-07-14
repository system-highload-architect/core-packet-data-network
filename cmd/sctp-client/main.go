package main

import (
	"context"
	"flag"
	"os/signal"
	"syscall"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/variants/sctp"
)

func main() {
	var (
		remoteAddr = flag.String("addr", "127.0.0.1:5000", "server address")
		total      = flag.Uint64("n", 10000, "total packets")
		maxSize    = flag.Int("max-size", 1400, "max data size")
		maxRetries = flag.Int("retries", 3, "max retransmissions")
		retryTO    = flag.Duration("retry-timeout", 100_000_000, "retry timeout")
		debug      = flag.Bool("debug", false, "debug logging")
	)
	flag.Parse()

	logLevel := logger.LevelInfo
	if *debug {
		logLevel = logger.LevelDebug
	}
	log := logger.New(logger.WithLevel(logLevel))

	cfg := &sctp.Config{
		RemoteAddr:    *remoteAddr,
		TotalPackets:  *total,
		MaxPacketSize: *maxSize,
		MaxRetries:    *maxRetries,
		RetryTimeout:  *retryTO,
	}

	client, err := sctp.NewClient(cfg, log)
	if err != nil {
		log.Fatal("client create: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := client.Run(); err != nil {
			log.Error("run error: %v", err)
		}
		stop()
	}()

	<-ctx.Done()
	if err := client.Shutdown(context.Background()); err != nil {
		log.Error("shutdown error: %v", err)
	}
	log.Info("client finished")
}
