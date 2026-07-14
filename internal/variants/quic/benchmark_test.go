package quic

import (
	"fmt"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
)

func BenchmarkQUICThroughput(b *testing.B) {
	log := logger.New(logger.WithLevel(logger.LevelWarn))
	total := uint64(100_000)

	// Серверный TLS (с сертификатом)
	serverTLS := generateTLSConfig()
	// Клиентский TLS: без сертификата, доверяет серверу
	clientTLS := serverTLS.Clone()
	clientTLS.InsecureSkipVerify = true
	clientTLS.Certificates = nil

	srvCfg := &ServerConfig{
		ListenAddr: "127.0.0.1:0",
		TLSConfig:  serverTLS,
	}
	cliCfg := &ClientConfig{
		TotalPackets:  total,
		MaxPacketSize: 1400,
		MaxRetries:    2,
		RetryTimeout:  50 * time.Millisecond,
		TLSConfig:     clientTLS,
	}

	server, err := NewServer(srvCfg, log)
	if err != nil {
		b.Fatalf("server: %v", err)
	}
	go server.Run()
	defer server.Shutdown(nil)

	time.Sleep(500 * time.Millisecond)
	cliCfg.ServerAddr = server.listener.Addr().String()

	client, err := NewClient(cliCfg, log)
	if err != nil {
		b.Fatalf("client: %v", err)
	}

	b.ResetTimer()
	start := time.Now()
	if err := client.Run(); err != nil {
		b.Fatalf("client run: %v", err)
	}
	elapsed := time.Since(start)
	b.StopTimer()

	rps := float64(total) / elapsed.Seconds()
	sent := client.sentCount.Load()
	acked := client.ackCount.Load()
	lost := client.lostCount.Load()
	lossRate := float64(lost) / float64(total) * 100

	b.ReportMetric(rps, "rps")
	b.ReportMetric(float64(sent), "sent")
	b.ReportMetric(float64(acked), "acked")
	b.ReportMetric(float64(lost), "lost")

	if lossRate > 3.0 {
		b.Errorf("loss rate too high: %.2f%%", lossRate)
	}
	fmt.Printf("QUIC RPS: %.0f, loss: %.2f%%\n", rps, lossRate)
}
