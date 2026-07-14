package udpfec

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/pregen"
)

func BenchmarkUDPFECThroughput(b *testing.B) {
	log := logger.New(logger.WithLevel(logger.LevelError), logger.WithOutput(io.Discard))
	out := io.Discard

	maxSize := 64
	// RU: Гарантируем пул предгенерации с запасом под итерации b.N
	// EN: Ensure pregen pool capacity handles standard b.N scaling loops
	const maxPregenRequired = 2_000_000
	if len(pregen.Packets) < maxPregenRequired {
		pregen.Init(maxPregenRequired, maxSize)
	}

	b.ResetTimer()

	// RU: Настоящий цикл бенчмарка Go, управляемый b.N
	// EN: True Go benchmark loop driven natively by b.N
	for i := 0; i < b.N; i++ {
		total := 100_000
		if b.N > total {
			total = b.N
		}

		srvCfg := &Config{
			ServerAddr:    "127.0.0.1:0",
			MaxPacketSize: maxSize,
			BenchMode:     true,
		}
		cliCfg := &Config{
			ClientAddr:    "127.0.0.1:0",
			TotalPackets:  uint64(total),
			MaxPacketSize: maxSize,
			DataShards:    4,
			BenchMode:     true,
			PregenPackets: pregen.Packets[:total],
		}

		server, err := NewServer(srvCfg, log, out)
		if err != nil {
			b.Fatalf("server initialization failure: %v", err)
		}
		go server.Run()

		// RU: Даем сокету сервера минимальный зазор для привязки к порту
		// EN: Provide a minimal gap for the server socket to bind
		time.Sleep(5 * time.Millisecond)
		cliCfg.ServerAddr = server.conn.Addr().String()

		// RU: Исправлено: передаем out третьим аргументом в соответствии с обновленным конструктором
		// EN: Fixed: pass out as the third argument matching the updated constructor signature
		client, err := NewClient(cliCfg, log, out)
		if err != nil {
			server.Shutdown(context.Background())
			b.Fatalf("client initialization failure: %v", err)
		}

		b.StartTimer()
		start := time.Now()
		if err := client.Run(); err != nil {
			client.Shutdown(context.Background())
			server.Shutdown(context.Background())
			b.Fatalf("client run failure: %v", err)
		}
		elapsed := time.Since(start)
		b.StopTimer()

		rps := float64(total) / elapsed.Seconds()
		sent := client.sentCount.Load()
		acked := client.ackCount.Load()

		// RU: Фиксируем метрики текущей итерации
		// EN: Report metrics for the current benchmark iteration
		b.ReportMetric(rps, "rps")
		b.ReportMetric(float64(sent), "sent_batches")
		b.ReportMetric(float64(acked), "acked_batches")

		// RU: Мгновенная очистка портов сокетов перед следующим шагом b.N
		// EN: Immediate socket port cleanup before the next b.N iteration step
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		client.Shutdown(ctx)
		server.Shutdown(ctx)
		cancel()

		if i == b.N-1 {
			fmt.Printf("\n[UDP+FEC BENCHMARK] Total Batches: %d | RPS: %.0f\n", total, rps)
		}
	}
}
