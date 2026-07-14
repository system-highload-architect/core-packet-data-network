package quic

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/lru"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
)

type ServerConfig struct {
	ListenAddr  string
	TLSCert     string
	TLSKey      string
	TLSConfig   *tls.Config // приоритетнее загрузки из файлов
	WorkerCount int
}

type Server struct {
	config     *ServerConfig
	listener   *network.QUICListener
	log        *logger.Logger
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
}

func NewServer(cfg *ServerConfig, log *logger.Logger) (*Server, error) {
	var tlsConfig *tls.Config
	if cfg.TLSConfig != nil {
		tlsConfig = cfg.TLSConfig
	} else {
		tlsCert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load TLS cert: %w", err)
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			NextProtos:   []string{"quic-packet"},
		}
	}

	listener, err := network.NewQUICListener(cfg.ListenAddr, tlsConfig)
	if err != nil {
		return nil, err
	}

	s := &Server{
		config:     cfg,
		listener:   listener,
		log:        log,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      lru.NewCache[uint64, struct{}](30 * time.Second),
		stopCh:     make(chan struct{}),
	}

	s.shutdowner = shutdown.New()
	s.shutdowner.Register("quic listener", shutdown.CloserFunc(func(ctx context.Context) error {
		return listener.Close(ctx)
	}), shutdown.PriorityHigh)

	return s, nil
}

func (s *Server) Run() error {
	s.log.Info("QUIC server listening", "addr", s.listener.Addr())
	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			conn, err := s.listener.Accept(context.Background())
			if err != nil {
				s.log.Error("accept error", "error", err)
				continue
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *Server) handleConnection(conn *network.QUICConn) {
	s.log.Info("new QUIC connection", "remote", conn.RemoteAddr())
	for {
		data, err := conn.ReceiveDatagram(context.Background())
		if err != nil {
			s.log.Error("receive error", "error", err)
			return
		}
		s.processDatagram(data, conn)
	}
}

func (s *Server) processDatagram(data []byte, conn *network.QUICConn) {
	var pkt packet.Packet
	if err := pkt.Deserialize(data); err != nil {
		return
	}

	recvTime := time.Now()
	s.recvCount.Add(1)

	if _, ok := s.dedup.Get(pkt.ID); ok {
		return
	}
	s.dedup.Set(pkt.ID, struct{}{})

	resultStr := fmt.Sprintf("ID=%d Formed=%v Received=%v Checksum=", pkt.ID,
		pkt.Timestamp.Format(time.RFC3339Nano), recvTime.Format(time.RFC3339Nano))
	checksumOK := pkt.VerifyChecksum()
	if checksumOK {
		resultStr += "OK"
	} else {
		resultStr += "FAIL"
		s.badChecksum.Add(1)
	}

	for _, r := range s.orderedBuf.Insert(pkt.ID, resultStr) {
		fmt.Println(r)
	}

	ackBuf := make([]byte, 9)
	binary.BigEndian.PutUint64(ackBuf[0:8], pkt.ID)
	if checksumOK {
		ackBuf[8] = 1
	} else {
		ackBuf[8] = 0
	}
	if err := conn.SendDatagram(ackBuf); err != nil {
		s.log.Error("ack send error", "id", pkt.ID, "error", err)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	s.log.Info("server shutting down",
		"received", s.recvCount.Load(),
		"bad_checksum", s.badChecksum.Load(),
	)
	return s.shutdowner.Shutdown(ctx)
}
