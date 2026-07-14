package udp

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/pregen"
)

func BenchmarkUDPThroughput(b *testing.B) {
	// Предупреждение о WSL2
	if os.Getenv("WSL_DISTRO_NAME") != "" {
		b.Log("WSL2 detected: UDP performance may be limited. For best results, run on native Linux or move project to ~/ inside WSL2.")
	}

	log := logger.New(logger.WithLevel(logger.LevelError), logger.WithOutput(io.Discard))
	out := io.Discard

	// Инициализируем пул предгенерации с запасом под максимальный b.N
	maxSize := 64

	// Чтобы b.N корректно управлял тестом, мы выносим инициализацию за рамки итерационного цикла
	const maxPregenRequired = 5_000_000
	if len(pregen.Packets) < maxPregenRequired {
		pregen.Init(maxPregenRequired, maxSize)
	}

	b.ResetTimer()

	// Настоящий цикл бенчмарка Go, управляемый b.N
	for i := 0; i < b.N; i++ {
		// Определяем объем пачки пакетов под конкретную итерацию (минимум 10 000 по ТЗ)
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
			BenchMode:     true,
			PregenPackets: pregen.Packets[:total],
			Workers:       20,
		}
		server, err := NewServer(srvCfg, log, out)
		if err != nil {
			b.Fatalf("server initialization failure: %v", err)
		}

		go server.Run()

		// Даем сокету сервера гарантированно забиндить порт
		time.Sleep(5 * time.Millisecond)
		cliCfg.ServerAddr = server.conn.Addr().String()

		client, err := NewClient(cliCfg, log, out)
		if err != nil {
			server.Shutdown(context.Background())
			b.Fatalf("client initialization failure: %v", err)
		}

		// Запускаем замер времени конкретно для отправки/получения
		b.StartTimer()
		start := time.Now()

		if err := client.Run(); err != nil {
			client.Shutdown(context.Background())
			server.Shutdown(context.Background())
			b.Fatalf("client execution failure: %v", err)
		}

		elapsed := time.Since(start)
		b.StopTimer()

		rps := float64(total) / elapsed.Seconds()
		sent := client.sentCount.Load()
		acked := client.ackCount.Load()
		lost := uint64(total) - acked
		lossRate := float64(lost) / float64(total) * 100

		// Записываем кастомные метрики в результат текущей итерации
		b.ReportMetric(rps, "rps")
		b.ReportMetric(float64(sent), "sent_pkts")
		b.ReportMetric(float64(acked), "acked_pkts")
		b.ReportMetric(lossRate, "loss_pct")

		// Мгновенный Graceful Shutdown для очистки портов перед следующим шагом b.N
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		client.Shutdown(ctx)
		server.Shutdown(ctx)
		cancel()

		if lossRate > 3.0 {
			b.Errorf("loss rate boundary breached: %.2f%% (lost=%d/%d)", lossRate, lost, total)
		}

		// Выводим промежуточный результат для контроля на Windows/Linux
		if i == b.N-1 {
			fmt.Printf("\n[UDP BENCHMARK EXECUTION] Total: %d | RPS: %.0f | Loss: %.2f%%\n", total, rps, lossRate)
		}
	}
}
