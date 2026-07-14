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
	// RU: Предупреждение о WSL2
	// EN: WSL2 warning notification
	if os.Getenv("WSL_DISTRO_NAME") != "" {
		b.Log("WSL2 detected: UDP performance may be limited. For best results, run on native Linux or move project to ~/ inside WSL2.")
	}

	log := logger.New(logger.WithLevel(logger.LevelError), logger.WithOutput(io.Discard))
	out := io.Discard

	// RU: Инициализируем пул предгенерации с запасом под максимальный b.N
	// EN: Initialize pregen pool with headroom matching maximum b.N bounds
	maxSize := 64

	// RU: Чтобы b.N корректно управлял тестом, мы выносим инициализацию за рамки итерационного цикла
	// EN: To ensure b.N drives the test correctly, we bound data pre-generation properly
	const maxPregenRequired = 5_000_000
	if len(pregen.Packets) < maxPregenRequired {
		pregen.Init(maxPregenRequired, maxSize)
	}

	b.ResetTimer()

	// RU: Настоящий цикл бенчмарка Go, управляемый b.N
	// EN: True Go benchmark loop driven natively by b.N
	for i := 0; i < b.N; i++ {
		// RU: Определяем объем пачки пакетов под конкретную итерацию (минимум 10 000 по ТЗ)
		// EN: Define packet batch scope for this exact loop iteration (min 10,000 per requirements)
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
			Workers:       20, // RU: Для бенчмарка разгоняем до 20 потоков! | EN: Crank up to 20 workers for pure benchmark performance!
		}

		server, err := NewServer(srvCfg, log, out)
		if err != nil {
			b.Fatalf("server initialization failure: %v", err)
		}

		go server.Run()

		// RU: Даем сокету сервера гарантированно забиндить порт
		// EN: Provide server socket explicit gap to bind the local port
		time.Sleep(5 * time.Millisecond)
		cliCfg.ServerAddr = server.conn.Addr().String()

		client, err := NewClient(cliCfg, log, out)
		if err != nil {
			server.Shutdown(context.Background())
			b.Fatalf("client initialization failure: %v", err)
		}

		// RU: Запускаем замер времени конкретно для отправки/получения
		// EN: Start sub-timer measuring just the execution path
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

		// RU: Записываем кастомные метрики в результат текущей итерации
		// EN: Inject custom highload metrics into the tracking iteration context
		b.ReportMetric(rps, "rps")
		b.ReportMetric(float64(sent), "sent_pkts")
		b.ReportMetric(float64(acked), "acked_pkts")
		b.ReportMetric(lossRate, "loss_pct")

		// RU: Мгновенный Graceful Shutdown для очистки портов перед следующим шагом b.N
		// EN: Immediate Graceful Shutdown to clean ports before next b.N step execution
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		client.Shutdown(ctx)
		server.Shutdown(ctx)
		cancel()

		if lossRate > 3.0 {
			b.Errorf("loss rate boundary breached: %.2f%% (lost=%d/%d)", lossRate, lost, total)
		}

		// RU: Выводим промежуточный результат для контроля на Windows/Linux
		// EN: Output intermediate metrics tracking environment throughput behavior
		if i == b.N-1 {
			fmt.Printf("\n[UDP BENCHMARK EXECUTION] Total: %d | RPS: %.0f | Loss: %.2f%%\n", total, rps, lossRate)
		}
	}
}
