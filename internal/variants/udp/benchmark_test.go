package udp

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/pregen"
)

func BenchmarkUDPThroughput(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("Skipping UDP benchmark on Windows: high rate UDP is limited by winsock. Run on Linux/WSL2 for 100k+ RPS.")
	}

	log := logger.New(logger.WithLevel(logger.LevelError), logger.WithOutput(io.Discard))
	out := io.Discard

	total := 100_000
	maxSize := 64

	if len(pregen.Packets) < total {
		pregen.Init(total, maxSize)
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
		SendInterval:  0, // в Linux не нужна задержка
	}

	server, err := NewServer(srvCfg, log, out)
	if err != nil {
		b.Fatalf("server: %v", err)
	}
	go server.Run()
	defer server.Shutdown(context.Background())

	time.Sleep(200 * time.Millisecond)
	cliCfg.ServerAddr = server.conn.Addr().String()

	client, err := NewClient(cliCfg, log, out)
	if err != nil {
		b.Fatalf("client: %v", err)
	}
	defer client.Shutdown(context.Background())

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
	lost := uint64(total) - acked
	lossRate := float64(lost) / float64(total) * 100

	b.ReportMetric(rps, "rps")
	b.ReportMetric(float64(sent), "sent")
	b.ReportMetric(float64(acked), "acked")
	b.ReportMetric(float64(lost), "lost")

	if lossRate > 3.0 {
		b.Errorf("loss rate too high: %.2f%% (lost=%d)", lossRate, lost)
	}
	fmt.Printf("UDP RPS: %.0f, loss: %.2f%%\n", rps, lossRate)
}
