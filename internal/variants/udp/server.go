package udp

import (
	"context"
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

type Server struct {
	config     *Config
	conn       *network.UDPConn
	log        *logger.Logger
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
}

func NewServer(cfg *Config, log *logger.Logger) (*Server, error) {
	conn, err := network.NewUDPConn(cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("udp server: %w", err)
	}

	s := &Server{
		config:     cfg,
		conn:       conn,
		log:        log,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      lru.NewCache[uint64, struct{}](30 * time.Second),
		stopCh:     make(chan struct{}),
	}

	s.shutdowner = shutdown.New()
	s.shutdowner.Register("udp server conn", conn, shutdown.PriorityHigh)

	return s, nil
}

func (s *Server) Run() error {
	s.log.Info("UDP server listening", "addr", s.config.ServerAddr)
	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			msg, err := s.conn.Receive(context.Background())
			if err != nil {
				s.log.Error("receive error", "error", err)
				continue
			}
			go s.processMessage(msg)
		}
	}
}

func (s *Server) processMessage(msg *network.Message) {
	var pkt packet.Packet
	if err := pkt.Deserialize(msg.Data); err != nil {
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

	if err := s.conn.Send(context.Background(), ackBuf, msg.Addr); err != nil {
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
