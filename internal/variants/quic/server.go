package quic

import (
	"context"
	"crypto/tls"
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
	listener   *network.QUICListener
	log        *logger.Logger
	out        io.Writer
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	jobs       chan quicJobMessage
	outputCh   chan string
	stopCh     chan struct{}

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
	bufPool     sync.Pool
}

type quicJobMessage struct {
	BufPtr *[]byte
	Length int
	Conn   *network.QUICConn
}

func NewServer(cfg *Config, log *logger.Logger, out io.Writer) (*Server, error) {
	if out == nil {
		out = io.Discard
	}

	// Загрузка TLS сертификатов для QUIC
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load QUIC TLS certs: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"core-packet-data-quic"},
	}

	listener, err := network.NewQUICListener(cfg.ServerAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("quic server listener error: %w", err)
	}

	s := &Server{
		config:     cfg,
		listener:   listener,
		log:        log,
		out:        out,
		orderedBuf: order.NewOrderedBuffer[string](1),
		dedup:      lru.NewCache[uint64, struct{}](30 * time.Second),
		jobs:       make(chan quicJobMessage, 100_000),
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
	s.log.Info("QUIC server active stream pipeline deployed", "addr", s.listener.Addr())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := s.listener.Accept(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			// Принимаем стрим последовательно, без спавна тысяч горутин
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				return nil
			}

			bufPtr := s.bufPool.Get().(*[]byte)
			buf := *bufPtr

			// Вычитываем стрим прямо здесь. Потоки QUIC работают быстро, сисколл не заблокирует сокет
			offset := 0
			for {
				n, err := stream.Read(buf[offset:])
				offset += n
				if err != nil { // io.EOF
					break
				}
			}
			_ = stream.Close()

			if offset > 0 {
				select {
				case s.jobs <- quicJobMessage{BufPtr: bufPtr, Length: offset, Conn: conn}:
				default:
					s.bufPool.Put(bufPtr)
				}
			} else {
				s.bufPool.Put(bufPtr)
			}
		}
	}
}

func (s *Server) processMessage(data []byte, conn *network.QUICConn) {
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

		resultStr := fmt.Sprintf("ID=%d Formed=%v Received=%v Checksum(QUIC)=", pkt.ID,
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

	// Отправка ACK датаграммы обратно
	var ackBuf [9]byte
	binary.BigEndian.PutUint64(ackBuf[0:8], pkt.ID)
	if checksumOK {
		ackBuf[8] = 1
	} else {
		ackBuf[8] = 0
	}

	_ = conn.Send(context.Background(), ackBuf[:], nil)
}

func (s *Server) Shutdown() {
	close(s.stopCh)
	close(s.jobs)
	close(s.outputCh)
	_ = s.listener.Close(context.Background())
}
