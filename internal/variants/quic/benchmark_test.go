package quic

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"runtime"
	"testing"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/pregen"
)

func generateTLSConfig() *tls.Config {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certBytes, _ := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, _ := tls.X509KeyPair(certPEM, keyPEM)
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"quic-packet"}}
}

func BenchmarkQUICThroughput(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("QUIC high‑load benchmark requires native Linux. Use integration test (go test -run TestQuicIntegration) to verify correctness.")
	}

	log := logger.New(logger.WithLevel(logger.LevelError), logger.WithOutput(io.Discard))
	out := io.Discard

	total := 100_000
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
		b.Fatalf("server: %v", err)
	}
	go server.Run()
	defer server.Shutdown(context.Background())

	time.Sleep(2 * time.Second)
	cliCfg.ServerAddr = server.listener.Addr().String()

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
	fmt.Printf("QUIC RPS: %.0f, loss: %.2f%%\n", rps, lossRate)
}
