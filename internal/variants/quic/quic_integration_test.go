package quic

import (
	"context"
	"io"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/pregen"
)

func TestQuicIntegration(t *testing.T) {
	log := logger.New(logger.WithLevel(logger.LevelError), logger.WithOutput(io.Discard))
	out := io.Discard

	total := 1000 // маленькое количество для стабильности
	maxSize := 64
	if len(pregen.Packets) < total {
		pregen.Init(total, maxSize)
	}

	serverTLS := generateTLSConfig()
	clientTLS := serverTLS.Clone()
	clientTLS.InsecureSkipVerify = true
	clientTLS.Certificates = nil

	srvCfg := &ServerConfig{
		ListenAddr: "127.0.0.1:0",
		TLSConfig:  serverTLS,
		BenchMode:  true,
	}
	cliCfg := &ClientConfig{
		TotalPackets:  uint64(total),
		MaxPacketSize: maxSize,
		BenchMode:     true,
		PregenPackets: pregen.Packets[:total],
		TLSConfig:     clientTLS,
	}

	server, err := NewServer(srvCfg, log, out)
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	go server.Run()
	defer server.Shutdown(context.Background())

	time.Sleep(1 * time.Second) // достаточно для рукопожатия
	cliCfg.ServerAddr = server.listener.Addr().String()

	client, err := NewClient(cliCfg, log, out)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer client.Shutdown(context.Background())

	start := time.Now()
	if err := client.Run(); err != nil {
		t.Fatalf("client run: %v", err)
	}
	elapsed := time.Since(start)

	sent := client.sentCount.Load()
	acked := client.ackCount.Load()
	lost := uint64(total) - acked
	lossRate := float64(lost) / float64(total) * 100

	t.Logf("QUIC integration test: sent=%d acked=%d lost=%d loss=%.2f%% elapsed=%v",
		sent, acked, lost, lossRate, elapsed)

	if lossRate > 3.0 {
		t.Errorf("loss rate too high: %.2f%%", lossRate)
	}
}
