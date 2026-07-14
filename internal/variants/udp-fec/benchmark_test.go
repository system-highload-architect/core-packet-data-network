package udpfec

import (
	"fmt"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
)

func BenchmarkUDPFECThroughput(b *testing.B) {
	log := logger.New(logger.WithLevel(logger.LevelWarn))

	total := uint64(100_000)
	srvCfg := &Config{
		ServerAddr: "127.0.0.1:0",
		DataShards: 4,
	}
	cliCfg := &Config{
		ClientAddr:    "127.0.0.1:0",
		TotalPackets:  total,
		MaxPacketSize: 1400,
		DataShards:    4,
		MaxRetries:    2,
		RetryTimeout:  50 * time.Millisecond,
	}

	server, err := NewServer(srvCfg, log)
	if err != nil {
		b.Fatalf("server: %v", err)
	}
	go server.Run()
	defer server.Shutdown(nil)

	time.Sleep(100 * time.Millisecond)
	cliCfg.ServerAddr = server.conn.Addr().String()

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
	fmt.Printf("UDP+FEC RPS: %.0f, loss: %.2f%%\n", rps, lossRate)
}
