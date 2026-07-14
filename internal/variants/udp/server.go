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
	out        io.Writer
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}
	jobs       chan jobMessage
	outputCh   chan string

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
	bufPool     sync.Pool
}

// Передаем четкую структуру с указателем на массив из пула и реальной длиной данных
type jobMessage struct {
	BufPtr *[]byte
	Length int
	Addr   net.Addr
}

func NewServer(cfg *Config, log *logger.Logger, out io.Writer) (*Server, error) {
	if out == nil {
		out = os.Stdout
	}
	conn, err := network.NewUDPConn(cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("udp server: %w", err)
	}

	conn.SetReadBuffer(8 * 1024 * 1024)
	conn.SetWriteBuffer(8 * 1024 * 1024)

	s := &Server{
		config:     cfg,
		conn:       conn,
		log:        log,
		out:        out,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      lru.NewCache[uint64, struct{}](30 * time.Second),
		jobs:       make(chan jobMessage, 100_000),
		stopCh:     make(chan struct{}),
		bufPool: sync.Pool{
			New: func() any {
				// Выделяем буфер под MTU с запасом
				b := make([]byte, cfg.MaxPacketSize+256)
				return &b
			},
		},
	}

	for i := 0; i < 30; i++ {
		go s.worker()
	}
	go s.outputPipeline()

	s.shutdowner = shutdown.New()
	s.shutdowner.Register("udp server conn", conn, shutdown.PriorityHigh)
	return s, nil
}

func (s *Server) worker() {
	for msg := range s.jobs {
		// Берем срез данных строго по записанной длине
		data := (*msg.BufPtr)[:msg.Length]
		s.processMessage(data, msg.Addr)

		// Безопасно возвращаем точный указатель обратно в пул
		s.bufPool.Put(msg.BufPtr)
	}
}

func (s *Server) outputPipeline() {
	for res := range s.outputCh {
		fmt.Fprintln(s.out, res)
	}
}

func (s *Server) Run() error {
	s.log.Info("UDP server listening", "addr", s.conn.Addr())
	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			// Забираем указатель на базовый массив из пула
			bufPtr := s.bufPool.Get().(*[]byte)
			buf := *bufPtr

			n, addr, err := s.conn.ReceiveTo(buf)
			if err != nil {
				s.bufPool.Put(bufPtr) // Возврат при ошибках чтения
				if isClosedNetworkError(err) {
					return nil
				}
				s.log.Error("receive error", "error", err)
				continue
			}

			// Отправляем в канал точные метаданные
			select {
			case s.jobs <- jobMessage{BufPtr: bufPtr, Length: n, Addr: addr}:
			default:
				// Если воркеры не успевают — сбрасываем буфер назад
				s.bufPool.Put(bufPtr)
			}
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
			s.outputCh <- r
		}
	}

	var ackBuf [9]byte
	binary.BigEndian.PutUint64(ackBuf[0:8], pkt.ID)
	if checksumOK {
		ackBuf[8] = 1
	} else {
		ackBuf[8] = 0
	}

	if err := s.conn.Send(context.Background(), ackBuf[:], addr); err != nil {
		if !isClosedNetworkError(err) {
			s.log.Error("ack send error", "id", pkt.ID, "error", err)
		}
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	close(s.jobs)
	close(s.outputCh)
	s.log.Info("server shutting down",
		"received", s.recvCount.Load(),
		"bad_checksum", s.badChecksum.Load(),
	)
	return s.shutdowner.Shutdown(ctx)
}
