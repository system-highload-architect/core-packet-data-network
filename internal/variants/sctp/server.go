package sctp

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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
	listener   *network.SCTPListener
	log        *logger.Logger
	out        io.Writer
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
}

func NewServer(cfg *Config, log *logger.Logger, out io.Writer) (*Server, error) {
	if out == nil {
		out = io.Discard
	}
	listener, err := network.NewSCTPServer(cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("sctp listener: %w", err)
	}

	s := &Server{
		config:     cfg,
		listener:   listener,
		log:        log,
		out:        out,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      lru.NewCache[uint64, struct{}](30 * time.Second),
		stopCh:     make(chan struct{}),
	}

	s.shutdowner = shutdown.New()
	s.shutdowner.Register("sctp listener", listener, shutdown.PriorityHigh)
	return s, nil
}

func (s *Server) Run() error {
	s.log.Info("SCTP server listening", "addr", s.listener.Addr())
	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				s.log.Error("accept error", "error", err)
				continue
			}
			go s.handleConn(conn)
		}
	}
}

func (s *Server) handleConn(conn *network.SCTPConn) {
	defer conn.Close(context.Background())
	s.log.Info("new connection", "remote", conn.RemoteAddr())

	for {
		msg, err := conn.Receive(context.Background())
		if err != nil {
			return
		}
		s.processMessage(msg.Data, conn)
	}
}

func (s *Server) processMessage(data []byte, conn *network.SCTPConn) {
	var pkt packet.Packet
	if err := pkt.Deserialize(data); err != nil {
		return
	}
	recvTime := time.Now()
	s.recvCount.Add(1)

	checksumOK := pkt.VerifyChecksum()
	if !checksumOK {
		s.badChecksum.Add(1)
	}

	if !s.config.BenchMode {
		if _, ok := s.dedup.Get(pkt.ID); ok {
			return
		}
		s.dedup.Set(pkt.ID, struct{}{})

		resultStr := fmt.Sprintf("ID=%d Formed=%v Received=%v Checksum=", pkt.ID,
			pkt.Timestamp.Format(time.RFC3339Nano), recvTime.Format(time.RFC3339Nano))
		if checksumOK {
			resultStr += "OK"
		} else {
			resultStr += "FAIL"
		}
		for _, r := range s.orderedBuf.Insert(pkt.ID, resultStr) {
			fmt.Fprintln(s.out, r)
		}
	}

	ack := [9]byte{}
	binary.BigEndian.PutUint64(ack[0:8], pkt.ID)
	if checksumOK {
		ack[8] = 1
	}
	if err := conn.Send(context.Background(), ack[:], nil); err != nil {
		s.log.Error("ack send error", "id", pkt.ID, "error", err)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	s.log.Info("server shutting down", "received", s.recvCount.Load(), "bad_checksum", s.badChecksum.Load())
	return s.shutdowner.Shutdown(ctx)
}
