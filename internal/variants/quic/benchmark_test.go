package quic

import (
	"fmt"
	"io"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/pregen"
)

func BenchmarkQUICThroughput(b *testing.B) {
	log := logger.New(logger.WithLevel(logger.LevelError), logger.WithOutput(io.Discard))
	out := io.Discard

	maxSize := 64
	const maxPregenRequired = 2_000_000
	if len(pregen.Packets) < maxPregenRequired {
		pregen.Init(maxPregenRequired, maxSize)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		total := 100_000
		if b.N > total {
			total = b.N
		}

		srvCfg := &Config{
			ServerAddr:    "127.0.0.1:0",
			CertFile:      "../../../certs/cert.pem", // Относительный путь от папки теста до корня certs/
			KeyFile:       "../../../certs/key.pem",
			MaxPacketSize: maxSize,
			BenchMode:     true,
		}
		cliCfg := &Config{
			ServerAddr:    "",
			ClientAddr:    "127.0.0.1:0",
			TotalPackets:  uint64(total),
			MaxPacketSize: maxSize,
			BenchMode:     true,
			PregenPackets: pregen.Packets[:total],
		}

		server, err := NewServer(srvCfg, log, out)
		if err != nil {
			b.Fatalf("quic server benchmark init failure: %v", err)
		}
		go server.Run()

		time.Sleep(10 * time.Millisecond)
		cliCfg.ServerAddr = server.listener.Addr().String()

		client, err := NewClient(cliCfg, log, out)
		if err != nil {
			server.Shutdown()
			b.Fatalf("quic client benchmark init failure: %v", err)
		}

		b.StartTimer()
		start := time.Now()
		if err := client.Run(); err != nil {
			client.Shutdown()
			server.Shutdown()
			b.Fatalf("quic client benchmark execution failed: %v", err)
		}
		elapsed := time.Since(start)
		b.StopTimer()

		rps := float64(total) / elapsed.Seconds()
		sent := client.sentCount.Load()
		acked := client.ackCount.Load()

		b.ReportMetric(rps, "rps")
		b.ReportMetric(float64(sent), "sent_pkts")
		b.ReportMetric(float64(acked), "acked_pkts")

		client.Shutdown()
		server.Shutdown()

		if i == b.N-1 {
			fmt.Printf("\n[QUIC DATAGRAM BENCHMARK] Total: %d | RPS: %.0f\n", total, rps)
		}
	}
}
