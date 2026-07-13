package network

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"
)

// generateTLSConfig создаёт самоподписанный TLS-сертификат для QUIC.
func generateTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ServerName:   "localhost",
		NextProtos:   []string{"quic-test"},
	}, nil
}

func TestUDPConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	server, err := NewUDPConn("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create UDP server: %v", err)
	}
	defer server.Close()

	go func() {
		msg, err := server.Receive(ctx)
		if err != nil {
			t.Errorf("server receive error: %v", err)
			return
		}
		if msg == nil || string(msg.Data) != "hello" {
			t.Errorf("expected 'hello', got '%s'", string(msg.Data))
		}
	}()

	clientAddr, err := net.ResolveUDPAddr("udp", server.Addr().String())
	if err != nil {
		t.Fatalf("resolve server addr: %v", err)
	}
	client, err := NewUDPConn("127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create UDP client: %v", err)
	}
	defer client.Close()

	if err := client.Send(ctx, []byte("hello"), clientAddr); err != nil {
		t.Fatalf("client send error: %v", err)
	}
}

func TestSCTPConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener, err := NewSCTPServer("127.0.0.1:0")
	if err != nil {
		t.Skipf("SCTP not supported or error: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Errorf("accept error: %v", err)
			return
		}
		defer conn.Close()
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			t.Errorf("read error: %v", err)
			return
		}
		if string(buf[:n]) != "hello" {
			t.Errorf("expected 'hello', got '%s'", string(buf[:n]))
		}
		if _, err := conn.Write([]byte("ack")); err != nil {
			t.Errorf("write error: %v", err)
		}
	}()

	time.Sleep(10 * time.Millisecond)

	client, err := NewSCTPConn(listener.Addr().String())
	if err != nil {
		t.Skipf("SCTP client error: %v", err)
	}
	defer client.Close()

	if err := client.Send(ctx, []byte("hello"), client.Addr()); err != nil {
		t.Fatalf("client send error: %v", err)
	}
}

func TestQUICConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tlsConfig, err := generateTLSConfig()
	if err != nil {
		t.Skipf("TLS generation failed: %v", err)
	}

	listener, err := NewQUICListener("127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Skipf("QUIC listener error: %v", err)
	}
	defer listener.Close()

	errCh := make(chan error, 1)

	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			errCh <- fmt.Errorf("accept failed: %w", err)
			return
		}
		defer conn.CloseWithError(0, "done")

		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			errCh <- fmt.Errorf("accept stream failed: %w", err)
			return
		}
		// Не закрываем поток здесь

		buf := make([]byte, 1024)
		n, err := stream.Read(buf)
		if err != nil {
			errCh <- fmt.Errorf("read failed: %w", err)
			return
		}
		if string(buf[:n]) != "hello" {
			errCh <- fmt.Errorf("expected 'hello', got '%s'", string(buf[:n]))
			return
		}
		_, err = stream.Write([]byte("ack"))
		errCh <- err
		// Не закрываем поток
	}()

	clientConfig := tlsConfig.Clone()
	clientConfig.InsecureSkipVerify = true
	client, err := NewQUICConnClient(listener.Addr().String(), clientConfig)
	if err != nil {
		t.Skipf("QUIC client error: %v", err)
	}
	defer client.Close()

	stream, err := client.conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatalf("open stream error: %v", err)
	}
	defer stream.Close()

	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatalf("write error: %v", err)
	}

	// Читаем ответ
	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		t.Fatalf("read response error: %v", err)
	}
	if string(buf[:n]) != "ack" {
		t.Errorf("expected 'ack', got '%s'", string(buf[:n]))
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("server error: %v", err)
		}
	case <-ctx.Done():
		t.Error("timeout waiting for server response")
	}
}
