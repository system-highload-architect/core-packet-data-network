package sctp

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/internal/common/metrics"
	"core-packet-data-network/pkg/lru"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
)

// Server SCTP-сервер с поддержкой highload.
type Server struct {
	config     *Config
	listener   *network.SCTPListener
	log        *logger.Logger
	orderedBuf *order.OrderedBuffer[string] // упорядоченный вывод
	dedup      *lru.Cache[uint64, struct{}] // дедупликация
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
}

// NewServer создаёт сервер.
func NewServer(cfg *Config, log *logger.Logger, metr *metrics.Registry) (*Server, error) {
	listener, err := network.NewSCTPServer(cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("create SCTP listener: %w", err)
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
	s.shutdowner.Register("sctp listener", shutdown.CloserFunc(func(ctx context.Context) error {
		return listener.Close(ctx)
	}), shutdown.PriorityHigh)

	return s, nil
}

// Run запускает сервер.
func (s *Server) Run() error {
	s.log.Info("SCTP server listening", "addr", s.listener.Addr())
	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				s.log.Error("accept error: %v", err)
				continue
			}
			go s.handleConnection(conn)
		}
	}
}

func (s *Server) handleConnection(conn *network.SCTPConn) {
	s.log.Info("new SCTP connection", "remote", conn.RemoteAddr())
	for {
		msg, err := conn.Receive(context.Background())
		if err != nil {
			s.log.Error("receive error", "error", err)
			return
		}
		s.processMessage(msg.Data, conn)
	}
}

func (s *Server) processMessage(data []byte, conn *network.SCTPConn) {
	var pkt packet.Packet
	if err := pkt.Deserialize(data); err != nil {
		return // повреждённый пакет, ACK не отправляем
	}

	recvTime := time.Now()
	s.recvCount.Add(1)

	// Проверка дубликата
	if _, ok := s.dedup.Get(pkt.ID); ok {
		return
	}
	s.dedup.Set(pkt.ID, struct{}{})

	// Формирование строки вывода
	resultStr := fmt.Sprintf("ID=%d Formed=%v Received=%v Checksum=", pkt.ID,
		pkt.Timestamp.Format(time.RFC3339Nano), recvTime.Format(time.RFC3339Nano))
	checksumOK := pkt.VerifyChecksum()
	if checksumOK {
		resultStr += "OK"
	} else {
		resultStr += "FAIL"
		s.badChecksum.Add(1)
	}

	// Упорядоченный вывод
	for _, r := range s.orderedBuf.Insert(pkt.ID, resultStr) {
		fmt.Println(r)
	}

	// Отправка бинарного ACK
	ackBuf := make([]byte, 9)
	binary.BigEndian.PutUint64(ackBuf[0:8], pkt.ID)
	if checksumOK {
		ackBuf[8] = 1
	} else {
		ackBuf[8] = 0
	}
	if err := conn.Send(context.Background(), ackBuf, nil); err != nil {
		s.log.Error("ack send error", "id", pkt.ID, "error", err)
	}
}

// Shutdown мягко завершает сервер.
func (s *Server) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	s.log.Info("server shutting down",
		"received", s.recvCount.Load(),
		"bad_checksum", s.badChecksum.Load(),
	)
	return s.shutdowner.Shutdown(ctx)
}
