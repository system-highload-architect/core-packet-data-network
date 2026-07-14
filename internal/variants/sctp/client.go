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
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
	"core-packet-data-network/pkg/zeroalloc"
)

type Client struct {
	config        *Config
	conn          *network.SCTPConn
	log           *logger.Logger
	out           io.Writer
	orderedOutput *order.OrderedBuffer[string]

	sentCount atomic.Uint64
	ackCount  atomic.Uint64
	lostCount atomic.Uint64

	stopCh     chan struct{}
	workerWg   sync.WaitGroup
	shutdowner *shutdown.Shutdowner
}

func NewClient(cfg *Config, log *logger.Logger, out io.Writer) (*Client, error) {
	if out == nil {
		out = io.Discard
	}
	conn, err := network.NewSCTPConn(cfg.RemoteAddr)
	if err != nil {
		return nil, fmt.Errorf("sctp client: %w", err)
	}

	c := &Client{
		config:        cfg,
		conn:          conn,
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		stopCh:        make(chan struct{}),
	}

	c.shutdowner = shutdown.New()
	c.shutdowner.Register("sctp client conn", conn, shutdown.PriorityHigh)
	return c, nil
}

func (c *Client) Run() error {
	c.log.Info("SCTP client starting", "server", c.config.RemoteAddr)

	// Запускаем горутину-читатель ACK
	go c.ackReader()

	// Генераторы (20 штук)
	workers := 20
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
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

	if c.config.PregenPackets != nil {
		for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
			select {
			case <-c.stopCh:
				return
			default:
			}
			data := c.config.PregenPackets[id-1]
			if err := c.conn.Send(context.Background(), data, nil); err != nil {
				c.log.Error("send error", "id", id, "error", err)
				continue
			}
			c.sentCount.Add(1)
		}
		return
	}

	buf := make([]byte, 0, c.config.MaxPacketSize+64)
	dataBuf := make([]byte, c.config.MaxPacketSize)
	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
		select {
		case <-c.stopCh:
			return
		default:
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
		if err := c.conn.Send(context.Background(), data, nil); err != nil {
			c.log.Error("send error", "id", id, "error", err)
			continue
		}
		c.sentCount.Add(1)
	}
}

func (c *Client) ackReader() {
	for {
		msg, err := c.conn.Receive(context.Background())
		if err != nil {
			c.log.Error("ack read error", "error", err)
			return
		}
		if len(msg.Data) < 9 {
			continue
		}
		id := binary.BigEndian.Uint64(msg.Data[0:8])
		success := msg.Data[8] == 1
		c.ackCount.Add(1)

		if !c.config.BenchMode {
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

func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
