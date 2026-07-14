package sctp

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/lru"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
)

type Server struct {
	config     *Config
	listener   *network.SCTPListener
	log        *logger.Logger
	out        io.Writer
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	jobs       chan sctpJobMessage
	outputCh   chan string
	stopCh     chan struct{}

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
	bufPool     sync.Pool
}

type sctpJobMessage struct {
	BufPtr *[]byte
	Length int
	Conn   *network.SCTPConn
}

func NewServer(cfg *Config, log *logger.Logger, out io.Writer) (*Server, error) {
	if out == nil {
		out = io.Discard
	}

	listener, err := network.NewSCTPServer(cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("sctp listener generation failed: %w", err)
	}

	s := &Server{
		config:     cfg,
		listener:   listener,
		log:        log,
		out:        out,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      lru.NewCache[uint64, struct{}](30 * time.Second),
		jobs:       make(chan sctpJobMessage, 100_000),
		outputCh:   make(chan string, 100_000),
		stopCh:     make(chan struct{}),
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, cfg.MaxPacketSize+256)
				return &b
			},
		},
	}

	for i := 0; i < 30; i++ {
		go s.worker()
	}
	go s.outputPipeline()

	return s, nil
}

func (s *Server) worker() {
	for msg := range s.jobs {
		data := (*msg.BufPtr)[:msg.Length]
		s.processMessage(data, msg.Conn)
		s.bufPool.Put(msg.BufPtr)
	}
}

func (s *Server) outputPipeline() {
	for res := range s.outputCh {
		fmt.Fprintln(s.out, res)
	}
}

func (s *Server) Run() error {
	s.log.Info("SCTP server listener active", "addr", s.listener.Addr())

	conn, err := s.listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()

	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			bufPtr := s.bufPool.Get().(*[]byte)
			buf := *bufPtr

			n, _, err := conn.ReceiveTo(buf)
			if err != nil {
				s.bufPool.Put(bufPtr)
				return nil
			}

			select {
			case s.jobs <- sctpJobMessage{BufPtr: bufPtr, Length: n, Conn: conn}:
			default:
				s.bufPool.Put(bufPtr)
			}
		}
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

		resultStr := fmt.Sprintf("ID=%d Formed=%v Received=%v Checksum(SCTP)=", pkt.ID,
			pkt.Timestamp.Format(time.RFC3339Nano), recvTime.Format(time.RFC3339Nano))
		if checksumOK {
			resultStr += "OK"
		} else {
			resultStr += "FAIL"
		}

		for _, r := range s.orderedBuf.Insert(pkt.ID, resultStr) {
			s.outputCh <- r
		}
	}

	// RU: Фиксированный массив на стеке из 9 байт, ноль аллокаций в куче
	// EN: Fixed stack-allocated array of 9 bytes, zero heap allocations
	var ackBuf [9]byte
	binary.BigEndian.PutUint64(ackBuf[0:8], pkt.ID)
	if checksumOK {
		ackBuf[8] = 1
	} else {
		ackBuf[8] = 0
	}

	// RU: Отправляем ACK обратно клиенту через SCTP-соединение
	// EN: Stream the ACK back to the client via the SCTP connection
	_ = conn.Send(context.Background(), ackBuf[:], nil)
}

func (s *Server) Shutdown() {
	close(s.stopCh)
	close(s.jobs)
	close(s.outputCh)
	_ = s.listener.Close()
}
