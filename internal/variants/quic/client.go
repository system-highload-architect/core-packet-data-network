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
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
	"core-packet-data-network/pkg/workerpool"
)

type Client struct {
	config      *ClientConfig
	conn        *network.QUICConn
	workerPool  *workerpool.Pool
	layerCache  *lru.LayerCache[uint64, packet.Packet]
	shutdowner  *shutdown.Shutdowner
	log         *logger.Logger
	stopCh      chan struct{}
	running     atomic.Bool
	packetCount atomic.Uint64
}

type ClientConfig struct {
	ServerAddr   string
	TLSCert      string
	TLSKey       string
	WorkerCount  int
	QueueSize    int
	LayerTTL     []time.Duration
	MaxAttempts  int
	TotalPackets uint64
}

func NewClient(cfg *ClientConfig, log *logger.Logger) (*Client, error) {
	// TLS
	tlsCert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("load TLS cert: %w", err)
	}
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{tlsCert},
		NextProtos:         []string{"quic-packet"},
		InsecureSkipVerify: true, // для тестов
	}

	conn, err := network.NewQUICConnClient(cfg.ServerAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("connect to server: %w", err)
	}

	// Слои для backoff
	var layerConfigs []lru.LayerConfig
	for _, ttl := range cfg.LayerTTL {
		layerConfigs = append(layerConfigs, lru.LayerConfig{TTL: ttl, MaxAttempt: cfg.MaxAttempts})
	}
	backoff := lru.NewExponentialBackoff()
	layerCache := lru.NewLayerCache[uint64, packet.Packet](layerConfigs, backoff)

	pool := workerpool.New(cfg.WorkerCount, cfg.QueueSize)
	shutdowner := shutdown.New()

	c := &Client{
		config:     cfg,
		conn:       conn,
		workerPool: pool,
		layerCache: layerCache,
		shutdowner: shutdowner,
		log:        log,
		stopCh:     make(chan struct{}),
	}

	shutdowner.Register("QUIC client connection", conn, shutdown.PriorityHigh)
	shutdowner.Register("QUIC client pool", pool, shutdown.PriorityMedium)

	go c.sendLoop()
	return c, nil
}

func (c *Client) sendLoop() {
	c.running.Store(true)
	defer c.running.Store(false)

	ctx := context.Background()
	var id uint64 = 1
	for id <= c.config.TotalPackets {
		select {
		case <-c.stopCh:
			return
		default:
			pkt := c.generatePacket(id)
			c.layerCache.Set(id, pkt)

			if err := c.sendPacket(ctx, pkt); err != nil {
				c.log.Error("send packet id=%d error: %v", id, err)
			} else {
				c.packetCount.Add(1)
				c.log.Debug("sent packet id=%d", id)
			}
			id++
			time.Sleep(10 * time.Millisecond) // эмуляция нагрузки
		}
	}

	// Ждём подтверждения всех пакетов (заглушка)
	c.log.Info("all packets sent, waiting for ACKs...")
	time.Sleep(5 * time.Second)
}

func (c *Client) generatePacket(id uint64) packet.Packet {
	data := []byte(fmt.Sprintf("packet data %d", id))
	checksum := packet.ComputeChecksum(data)
	return packet.Packet{
		ID:        id,
		Timestamp: time.Now(),
		Data:      data,
		Checksum:  checksum,
	}
}

func (c *Client) sendPacket(ctx context.Context, pkt packet.Packet) error {
	data, err := pkt.Serialize()
	if err != nil {
		return err
	}
	return c.conn.Send(ctx, data, c.conn.RemoteAddr())
}

func (c *Client) Run() error {
	c.log.Info("QUIC client started, sending %d packets", c.config.TotalPackets)
	<-c.stopCh
	return nil
}

func (c *Client) Shutdown(ctx context.Context) error {
	c.log.Info("shutting down client...")
	close(c.stopCh)
	return c.shutdowner.Shutdown(ctx)
}
