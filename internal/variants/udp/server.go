package udp

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
)

type Server struct {
	config     *Config
	conn       *network.UDPConn
	log        *logger.Logger
	out        io.Writer
	orderedBuf *order.OrderedBuffer[string]
	dedup      map[uint64]struct{}
	dedupMu    sync.Mutex
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
}

func NewServer(cfg *Config, log *logger.Logger, out io.Writer) (*Server, error) {
	if out == nil {
		out = os.Stdout
	}
	conn, err := network.NewUDPConn(cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("udp server: %w", err)
	}

	s := &Server{
		config:     cfg,
		conn:       conn,
		log:        log,
		out:        out,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      make(map[uint64]struct{}),
		stopCh:     make(chan struct{}),
	}

	s.shutdowner = shutdown.New()
	s.shutdowner.Register("udp server conn", conn, shutdown.PriorityHigh)
	return s, nil
}

func (s *Server) Run() error {
	s.log.Info("UDP server listening", "addr", s.conn.Addr())
	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			msg, err := s.conn.Receive(context.Background())
			if err != nil {
				if isClosedNetworkError(err) {
					return nil
				}
				s.log.Error("receive error", "error", err)
				continue
			}
			s.processMessage(msg.Data, msg.Addr)
		}
	}
}

func (s *Server) processMessage(data []byte, addr net.Addr) {
	var pkt packet.Packet
	if err := pkt.Deserialize(data); err != nil {
		return
	}

	recvTime := time.Now()
	s.recvCount.Add(1)

	if !s.config.BenchMode {
		s.dedupMu.Lock()
		if _, ok := s.dedup[pkt.ID]; ok {
			s.dedupMu.Unlock()
			return
		}
		s.dedup[pkt.ID] = struct{}{}
		s.dedupMu.Unlock()

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
			fmt.Fprintln(s.out, r)
		}
	} else {
		if !pkt.VerifyChecksum() {
			s.badChecksum.Add(1)
		}
	}

	ackBuf := make([]byte, 9)
	binary.BigEndian.PutUint64(ackBuf[0:8], pkt.ID)
	if pkt.VerifyChecksum() {
		ackBuf[8] = 1
	} else {
		ackBuf[8] = 0
	}
	if err := s.conn.Send(context.Background(), ackBuf, addr); err != nil {
		if !isClosedNetworkError(err) {
			s.log.Error("ack send error", "id", pkt.ID, "error", err)
		}
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
