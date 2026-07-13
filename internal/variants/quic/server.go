package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/lru"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
	"core-packet-data-network/pkg/workerpool"

	"github.com/quic-go/quic-go"
)

type Server struct {
	config      *Config
	listener    *network.QUICListener
	workerPool  *workerpool.Pool
	orderedBuf  *order.OrderedBuffer[packet.Packet]
	layerCache  *lru.LayerCache[uint64, packet.Packet]
	shutdowner  *shutdown.Shutdowner
	log         *logger.Logger
	stopCh      chan struct{}
	running     atomic.Bool
	packetCount atomic.Uint64
}

type Config struct {
	ListenAddr  string
	TLSCert     string
	TLSKey      string
	WorkerCount int
	QueueSize   int
	LayerTTL    []time.Duration
	MaxAttempts int
}

// QUICStream — интерфейс для потока, совместимый с quic.Stream
type QUICStream interface {
	Read([]byte) (int, error)
	Write([]byte) (int, error)
	Close() error
}

func NewServer(cfg *Config, log *logger.Logger) (*Server, error) {
	tlsCert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("load TLS cert: %w", err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"quic-packet"},
	}

	listener, err := network.NewQUICListener(cfg.ListenAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("create QUIC listener: %w", err)
	}

	var layerConfigs []lru.LayerConfig
	for _, ttl := range cfg.LayerTTL {
		layerConfigs = append(layerConfigs, lru.LayerConfig{TTL: ttl, MaxAttempt: cfg.MaxAttempts})
	}
	backoff := lru.NewExponentialBackoff()
	layerCache := lru.NewLayerCache[uint64, packet.Packet](layerConfigs, backoff)

	orderedBuf := order.NewOrderedBuffer[packet.Packet](1)
	pool := workerpool.New(cfg.WorkerCount, cfg.QueueSize)
	shutdowner := shutdown.New()

	s := &Server{
		config:     cfg,
		listener:   listener,
		workerPool: pool,
		orderedBuf: orderedBuf,
		layerCache: layerCache,
		shutdowner: shutdowner,
		log:        log,
		stopCh:     make(chan struct{}),
	}

	// Регистрируем для graceful shutdown
	shutdowner.Register("QUIC listener", shutdown.CloserFunc(func(ctx context.Context) error {
		return listener.Close(ctx)
	}), shutdown.PriorityHigh)
	shutdowner.Register("QUIC worker pool", shutdown.CloserFunc(func(ctx context.Context) error {
		return pool.Close(ctx)
	}), shutdown.PriorityMedium)

	go s.acceptLoop()
	return s, nil
}

func (s *Server) acceptLoop() {
	s.running.Store(true)
	defer s.running.Store(false)

	ctx := context.Background()
	for {
		select {
		case <-s.stopCh:
			return
		default:
			conn, err := s.listener.Accept(ctx)
			if err != nil {
				s.log.Error("accept error", "error", err)
				continue
			}
			go s.handleConnection(ctx, conn)
		}
	}
}

func (s *Server) handleConnection(ctx context.Context, conn *network.QUICConn) {
	defer conn.Close(ctx)
	s.log.Info("new connection", "remote", conn.RemoteAddr())

	for {
		select {
		case <-s.stopCh:
			return
		default:
			stream, err := conn.AcceptStream(context.Background())
			if err != nil {
				s.log.Error("accept stream error", "error", err)
				return
			}
			s.workerPool.Submit(workerpool.TaskFunc(func(ctx context.Context) error {
				defer stream.Close()
				return s.handleStream(stream)
			}))
		}
	}
}

func (s *Server) handleStream(stream *quic.Stream) error {
	// Читаем все данные из потока
	var buf []byte
	tmp := make([]byte, 1024)
	for {
		n, err := stream.Read(tmp)
		if err != nil {
			break
		}
		buf = append(buf, tmp[:n]...)
	}

	var pkt packet.Packet
	if err := pkt.Deserialize(buf); err != nil {
		s.log.Error("deserialize error", "error", err)
		return nil
	}

	s.packetCount.Add(1)
	s.log.Debug("received packet", "id", pkt.ID)

	if !pkt.VerifyChecksum() {
		s.log.Error("checksum mismatch", "id", pkt.ID)
		s.sendAck(stream, pkt.ID, false)
		return nil
	}

	results := s.orderedBuf.Insert(pkt.ID, pkt)
	for _, p := range results {
		s.log.Info("ordered packet", "id", p.ID)
	}

	s.sendAck(stream, pkt.ID, true)
	s.layerCache.Delete(pkt.ID)

	return nil
}

func (s *Server) sendAck(stream *quic.Stream, id uint64, ok bool) {
	msg := fmt.Sprintf("ACK:%d:%v\n", id, ok)
	_, err := stream.Write([]byte(msg))
	if err != nil {
		s.log.Error("send ack error", "error", err)
	}
}

func (s *Server) Run() error {
	s.log.Info("QUIC server listening", "addr", s.listener.Addr())
	<-s.stopCh
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("shutting down...")
	close(s.stopCh)
	return s.shutdowner.Shutdown(ctx)
}
