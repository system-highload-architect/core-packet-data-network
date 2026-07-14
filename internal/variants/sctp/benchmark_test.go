package sctp

import (
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/pregen"
)

func BenchmarkSCTPThroughput(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("SCTP benchmark is only supported on native Linux environments. Skipping on non-Linux OS.")
	}

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
			ServerAddr:    "127.0.0.1:0", // Динамический порт
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
			b.Fatalf("sctp server benchmark init failure: %v", err)
		}
		go server.Run()

		// RU: Даем ядру базовый зазор на запуск слушателя
		// EN: Give kernel standard gap to spin up the listener thread
		time.Sleep(20 * time.Millisecond)
		cliCfg.ServerAddr = server.listener.Addr().String()

		// RU: Внедряем паттерн Retry Loop для надежного подключения клиента
		// EN: Inject Retry Loop pattern to guarantee robust client connection context
		var client *Client
		var clientErr error
		maxAttempts := 5

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			client, clientErr = NewClient(cliCfg, log, out)
			if clientErr == nil {
				break // Успешно подключились!
			}

			// RU: Если это последняя попытка и всё равно ошибка — падаем
			// EN: If this is the final attempt and still failing — abort
			if attempt == maxAttempts {
				server.Shutdown()
				b.Fatalf("sctp client benchmark init failure after %d retries: %v", maxAttempts, clientErr)
			}

			// RU: Делаем экспоненциальную паузу перед следующей попыткой
			// EN: Apply exponential backoff delay before next retry sequence
			time.Sleep(time.Duration(attempt*20) * time.Millisecond)
		}

		b.StartTimer()
		start := time.Now()

		if err := client.Run(); err != nil {
			client.Shutdown()
			server.Shutdown()
			b.Fatalf("sctp client benchmark execution failed: %v", err)
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
			fmt.Printf("\n[SCTP HIGH-PERFORMANCE BENCHMARK] Total: %d | RPS: %.0f\n", total, rps)
		}
	}
}
