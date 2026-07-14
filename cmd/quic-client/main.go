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
		serverAddr   = flag.String("addr", "127.0.0.1:4242", "server address")
		total        = flag.Uint64("n", 10000, "total packets to send")
		maxSize      = flag.Int("max-size", 1400, "max packet data size")
		maxRetries   = flag.Int("retries", 3, "max retransmissions")
		retryTimeout = flag.Duration("retry-timeout", 100_000_000, "retry timeout") // 100ms
		tlsCert      = flag.String("cert", "certs/cert.pem", "TLS certificate")
		tlsKey       = flag.String("key", "certs/key.pem", "TLS key")
		debug        = flag.Bool("debug", false, "enable debug logging")
	)
	flag.Parse()

	logLevel := logger.LevelInfo
	if *debug {
		logLevel = logger.LevelDebug
	}
	log := logger.New(logger.WithLevel(logLevel))

	cfg := &quic.ClientConfig{
		ServerAddr:    *serverAddr,
		TotalPackets:  *total,
		MaxPacketSize: *maxSize,
		MaxRetries:    *maxRetries,
		RetryTimeout:  *retryTimeout,
		TLSCert:       *tlsCert,
		TLSKey:        *tlsKey,
	}

	client, err := quic.NewClient(cfg, log)
	if err != nil {
		log.Fatal("failed to create client: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := client.Run(); err != nil {
			log.Error("client run error: %v", err)
		}
		stop()
	}()

	<-ctx.Done()
	if err := client.Shutdown(context.Background()); err != nil {
		log.Error("shutdown error: %v", err)
	}
	log.Info("client finished")
}
