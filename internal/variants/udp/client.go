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
	"core-packet-data-network/pkg/zeroalloc"
)

type Client struct {
	config        *Config
	conn          *network.UDPConn
	serverAddr    *net.UDPAddr
	log           *logger.Logger
	out           io.Writer
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
	data     []byte
	checksum []byte
	attempts int
	lastSent time.Time
}

func NewClient(cfg *Config, log *logger.Logger, out io.Writer) (*Client, error) {
	if out == nil {
		out = os.Stdout
	}
	conn, err := network.NewUDPConn(cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("udp client: %w", err)
	}

	serverAddr, err := net.ResolveUDPAddr("udp", cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve server addr: %w", err)
	}

	c := &Client{
		config:        cfg,
		conn:          conn,
		serverAddr:    serverAddr,
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		pending:       make(map[uint64]*pendingPacket),
		stopCh:        make(chan struct{}),
	}

	c.shutdowner = shutdown.New()
	c.shutdowner.Register("udp client conn", conn, shutdown.PriorityHigh)
	return c, nil
}

func (c *Client) Run() error {
	c.log.Info("UDP client starting", "server", c.config.ServerAddr)
	go c.ackReceiver()

	workers := 40
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
	}
	if !c.config.BenchMode {
		go c.retransmitter()
	}

	c.workerWg.Wait()
	if c.config.BenchMode {
		deadline := time.After(10 * time.Second)
		for c.ackCount.Load() < c.config.TotalPackets {
			select {
			case <-deadline:
				c.log.Warn("timeout waiting for ACKs")
				goto exit
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}
	} else {
		deadline := time.After(30 * time.Second)
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
				goto exit
			}
		}
	}
exit:
	c.log.Info("client metrics",
		"sent", c.sentCount.Load(),
		"acked", c.ackCount.Load(),
		"lost", c.lostCount.Load(),
	)
	return nil
}

func (c *Client) generator(idx, step int) {
	defer c.workerWg.Done()

	// Если задан интервал, создаём тикер для ограничения темпа
	var ticker *time.Ticker
	if c.config.SendInterval > 0 {
		ticker = time.NewTicker(c.config.SendInterval)
		defer ticker.Stop()
	}

	if c.config.PregenPackets != nil {
		for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
			select {
			case <-c.stopCh:
				return
			default:
			}
			if ticker != nil {
				<-ticker.C // ждём разрешённого интервала
			}
			data := c.config.PregenPackets[id-1]
			if err := c.conn.Send(context.Background(), data, c.serverAddr); err != nil {
				if isClosedNetworkError(err) {
					return
				}
				continue
			}
			c.sentCount.Add(1)
		}
		return
	}

	// Обычная генерация (для production)
	buf := make([]byte, 0, c.config.MaxPacketSize+64)
	dataBuf := make([]byte, c.config.MaxPacketSize)

	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
		select {
		case <-c.stopCh:
			return
		default:
		}
		if ticker != nil {
			<-ticker.C
		}

		dataLen := int(id) % c.config.MaxPacketSize
		if dataLen < 1 {
			dataLen = 1
		}
		zeroalloc.FillRandomBytes(dataBuf[:dataLen])

		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(),
			Data:      dataBuf[:dataLen],
		}
		data, err := pkt.SerializeTo(buf)
		if err != nil {
			continue
		}
		buf = data[:0]

		if err := c.conn.Send(context.Background(), data, c.serverAddr); err != nil {
			if isClosedNetworkError(err) {
				return
			}
			continue
		}
		c.sentCount.Add(1)

		if !c.config.BenchMode {
			pendingData := make([]byte, len(pkt.Data))
			copy(pendingData, pkt.Data)
			checksum := make([]byte, len(pkt.Checksum))
			copy(checksum, pkt.Checksum)

			c.pendingMu.Lock()
			c.pending[id] = &pendingPacket{
				data:     pendingData,
				checksum: checksum,
				attempts: 1,
				lastSent: time.Now(),
			}
			c.pendingMu.Unlock()
		}
	}
}

func (c *Client) ackReceiver() {
	for {
		msg, err := c.conn.Receive(context.Background())
		if err != nil {
			if isClosedNetworkError(err) {
				return
			}
			return
		}
		if len(msg.Data) < 9 {
			continue
		}
		id := binary.BigEndian.Uint64(msg.Data[0:8])
		success := msg.Data[8] == 1

		c.ackCount.Add(1)

		if !c.config.BenchMode {
			c.pendingMu.Lock()
			delete(c.pending, id)
			c.pendingMu.Unlock()

			var result string
			if success {
				result = fmt.Sprintf("ID=%d OK", id)
			} else {
				result = fmt.Sprintf("ID=%d FAIL", id)
			}
			for _, r := range c.orderedOutput.Insert(id, result) {
				fmt.Fprintln(c.out, r)
			}
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
						result := fmt.Sprintf("ID=%d LOST", id)
						for _, r := range c.orderedOutput.Insert(id, result) {
							fmt.Fprintln(c.out, r)
						}
						continue
					}
					pkt := packet.Packet{
						ID:        id,
						Timestamp: time.Now(),
						Data:      pp.data,
						Checksum:  pp.checksum,
					}
					data, err := pkt.Serialize()
					if err != nil {
						continue
					}
					if err := c.conn.Send(context.Background(), data, c.serverAddr); err != nil {
						if isClosedNetworkError(err) {
							return
						}
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

func isClosedNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if opErr, ok := err.(*net.OpError); ok {
		return opErr.Err.Error() == "use of closed network connection"
	}
	return false
}
