package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
	"core-packet-data-network/pkg/zeroalloc"
)

type ClientConfig struct {
	ServerAddr    string
	TLSCert       string
	TLSKey        string
	TLSConfig     *tls.Config // если не nil, используется вместо загрузки из файлов
	TotalPackets  uint64
	MaxPacketSize int
	MaxRetries    int
	RetryTimeout  time.Duration
}

type Client struct {
	config        *ClientConfig
	conn          *network.QUICConn
	log           *logger.Logger
	orderedOutput *order.OrderedBuffer[string]
	pending       map[uint64]*pendingPacket
	pendingMu     sync.Mutex

	sentCount atomic.Uint64
	ackCount  atomic.Uint64
	lostCount atomic.Uint64

	workerWg   sync.WaitGroup
	stopCh     chan struct{}
	shutdowner *shutdown.Shutdowner
}

type pendingPacket struct {
	pkt      packet.Packet
	attempts int
	lastSent time.Time
}

func NewClient(cfg *ClientConfig, log *logger.Logger) (*Client, error) {
	var tlsConfig *tls.Config
	if cfg.TLSConfig != nil {
		tlsConfig = cfg.TLSConfig
	} else {
		tlsCert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load TLS cert: %w", err)
		}
		tlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{tlsCert},
			NextProtos:         []string{"quic-packet"},
			InsecureSkipVerify: true,
		}
	}

	conn, err := network.NewQUICConnClient(cfg.ServerAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	c := &Client{
		config:        cfg,
		conn:          conn,
		log:           log,
		orderedOutput: order.NewOrderedBuffer[string](1),
		pending:       make(map[uint64]*pendingPacket),
		stopCh:        make(chan struct{}),
	}

	c.shutdowner = shutdown.New()
	c.shutdowner.Register("quic client conn", conn, shutdown.PriorityHigh)
	return c, nil
}

func (c *Client) Run() error {
	c.log.Info("QUIC client starting", "server", c.config.ServerAddr)
	go c.ackReceiver()

	for i := 0; i < 10; i++ {
		c.workerWg.Add(1)
		go c.generator(i)
	}
	go c.retransmitter()

	c.workerWg.Wait()
	c.log.Info("All packets sent, waiting for pending ACKs...")

	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.pendingMu.Lock()
		pendingLen := len(c.pending)
		c.pendingMu.Unlock()
		if pendingLen == 0 {
			break
		}
		select {
		case <-ticker.C:
		case <-deadline:
			c.log.Warn("timeout waiting for ACKs")
			break
		}
	}

	c.log.Info("client metrics",
		"sent", c.sentCount.Load(),
		"acked", c.ackCount.Load(),
		"lost", c.lostCount.Load(),
		"loss_rate", float64(c.lostCount.Load())/float64(c.sentCount.Load())*100,
	)
	return nil
}

func (c *Client) generator(idx int) {
	defer c.workerWg.Done()
	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += 10 {
		select {
		case <-c.stopCh:
			return
		default:
		}

		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(),
			Data:      zeroalloc.GenerateRandomData(int(id) + int(id)%2),
		}
		if len(pkt.Data) > c.config.MaxPacketSize {
			pkt.Data = pkt.Data[:c.config.MaxPacketSize]
		}

		data, err := pkt.Serialize()
		if err != nil {
			c.log.Error("serialize error", "id", id)
			continue
		}

		if err := c.conn.SendDatagram(data); err != nil {
			c.log.Error("send error", "id", id, "error", err)
			continue
		}
		c.sentCount.Add(1)

		c.pendingMu.Lock()
		c.pending[id] = &pendingPacket{
			pkt:      pkt,
			attempts: 1,
			lastSent: time.Now(),
		}
		c.pendingMu.Unlock()
	}
}

func (c *Client) ackReceiver() {
	for {
		data, err := c.conn.ReceiveDatagram(context.Background())
		if err != nil {
			c.log.Error("ack receiver error", "error", err)
			return
		}
		if len(data) < 9 {
			continue
		}
		id := uint64(data[0])<<56 | uint64(data[1])<<48 | uint64(data[2])<<40 | uint64(data[3])<<32 |
			uint64(data[4])<<24 | uint64(data[5])<<16 | uint64(data[6])<<8 | uint64(data[7])
		success := data[8] == 1

		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()

		c.ackCount.Add(1)

		var result string
		if success {
			result = fmt.Sprintf("ID=%d OK", id)
		} else {
			result = fmt.Sprintf("ID=%d FAIL (checksum mismatch)", id)
		}
		for _, r := range c.orderedOutput.Insert(id, result) {
			fmt.Println(r)
		}
	}
}

func (c *Client) retransmitter() {
	ticker := time.NewTicker(c.config.RetryTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.pendingMu.Lock()
			for id, pp := range c.pending {
				if time.Since(pp.lastSent) > c.config.RetryTimeout {
					if pp.attempts >= c.config.MaxRetries {
						c.lostCount.Add(1)
						delete(c.pending, id)
						result := fmt.Sprintf("ID=%d LOST (max retries)", id)
						for _, r := range c.orderedOutput.Insert(id, result) {
							fmt.Println(r)
						}
						continue
					}
					data, err := pp.pkt.Serialize()
					if err != nil {
						c.log.Error("resend serialize error", "id", id)
						continue
					}
					if err := c.conn.SendDatagram(data); err != nil {
						c.log.Error("resend error", "id", id, "error", err)
						continue
					}
					pp.attempts++
					pp.lastSent = time.Now()
				}
			}
			c.pendingMu.Unlock()
		}
	}
}

func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
