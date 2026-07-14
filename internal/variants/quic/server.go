package quic

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
	"core-packet-data-network/pkg/shutdown"
)

type Server struct {
	config     *ServerConfig
	listener   *network.QUICListener
	log        *logger.Logger
	out        io.Writer
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}

	jobs   chan []byte
	ackCh  chan [9]byte
	wg     sync.WaitGroup // воркеры
	connWg sync.WaitGroup // обработчики соединений

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
}

func NewServer(cfg *ServerConfig, log *logger.Logger, out io.Writer) (*Server, error) {
	if out == nil {
		out = io.Discard
	}
	listener, err := network.NewQUICListener(cfg.ListenAddr, cfg.TLSConfig)
	if err != nil {
		return nil, fmt.Errorf("quic listener: %w", err)
	}

	s := &Server{
		config:     cfg,
		listener:   listener,
		log:        log,
		out:        out,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      lru.NewCache[uint64, struct{}](30 * time.Second),
		jobs:       make(chan []byte, 10000),
		ackCh:      make(chan [9]byte, 10000),
		stopCh:     make(chan struct{}),
	}

	for i := 0; i < 20; i++ {
		s.wg.Add(1)
		go s.worker()
	}

	s.shutdowner = shutdown.New()
	s.shutdowner.Register("quic listener", shutdown.CloserFunc(func(ctx context.Context) error {
		return listener.Close(ctx)
	}), shutdown.PriorityHigh)
	return s, nil
}

func (s *Server) worker() {
	defer s.wg.Done()
	for data := range s.jobs {
		var pkt packet.Packet
		if err := pkt.Deserialize(data); err != nil {
			continue
		}
		recvTime := time.Now()
		s.recvCount.Add(1)

		checksumOK := pkt.VerifyChecksum()
		if !checksumOK {
			s.badChecksum.Add(1)
		}

		if !s.config.BenchMode {
			if _, ok := s.dedup.Get(pkt.ID); ok {
				continue
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

		var ack [9]byte
		binary.BigEndian.PutUint64(ack[0:8], pkt.ID)
		if checksumOK {
			ack[8] = 1
		}
		s.ackCh <- ack
	}
}

func (s *Server) Run() error {
	s.log.Info("QUIC server listening", "addr", s.listener.Addr())
	for {
		select {
		case <-s.stopCh:
			// Ждём завершения всех обработчиков соединений
			s.connWg.Wait()
			// Закрываем jobs, чтобы воркеры завершились
			close(s.jobs)
			s.wg.Wait()
			// Теперь можно закрыть ackCh (ackWriter'ы уже завершились)
			close(s.ackCh)
			return nil
		default:
			conn, err := s.listener.Accept(context.Background())
			if err != nil {
				s.log.Error("accept error", "error", err)
				continue
			}
			s.connWg.Add(1)
			go s.handleConn(conn)
		}
	}
}

func (s *Server) handleConn(conn *network.QUICConn) {
	defer s.connWg.Done()
	defer conn.Close(context.Background())

	dataStream, err := conn.AcceptStream(context.Background())
	if err != nil {
		s.log.Error("accept data stream error", "error", err)
		return
	}
	defer dataStream.Close()

	ackStream, err := conn.AcceptStream(context.Background())
	if err != nil {
		s.log.Error("accept ack stream error", "error", err)
		return
	}
	defer ackStream.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		s.dataReader(dataStream)
	}()
	go func() {
		defer wg.Done()
		s.ackWriter(ackStream)
	}()

	wg.Wait()
}

func (s *Server) dataReader(stream io.Reader) {
	lenBuf := make([]byte, 2)
	for {
		_, err := io.ReadFull(stream, lenBuf)
		if err != nil {
			return
		}
		length := binary.BigEndian.Uint16(lenBuf)
		data := make([]byte, length)
		_, err = io.ReadFull(stream, data)
		if err != nil {
			return
		}
		select {
		case s.jobs <- data:
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) ackWriter(stream io.Writer) {
	for ack := range s.ackCh {
		if _, err := stream.Write(ack[:]); err != nil {
			s.log.Error("ack write error", "error", err)
			return
		}
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	s.log.Info("server shutting down", "received", s.recvCount.Load(), "bad_checksum", s.badChecksum.Load())
	return s.shutdowner.Shutdown(ctx)
}
