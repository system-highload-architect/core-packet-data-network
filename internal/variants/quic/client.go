package quic

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/zeroalloc"
)

type Client struct {
	config        *Config
	conn          *network.QUICConn
	log           *logger.Logger
	out           io.Writer
	orderedOutput *order.OrderedBuffer[string]
	outputCh      chan string // RU: Канал для неблокирующего вывода | EN: Non-blocking output pipeline channel

	sentCount atomic.Uint64
	ackCount  atomic.Uint64

	workerWg sync.WaitGroup
	stopCh   chan struct{}
	sendMu   sync.Mutex
}

func NewClient(cfg *Config, log *logger.Logger, out io.Writer) (*Client, error) {
	if out == nil {
		out = os.Stdout
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"core-packet-data-quic"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := network.NewQUICConnClient(ctx, cfg.ServerAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("quic client initialization failed: %w", err)
	}

	return &Client{
		config:        cfg,
		conn:          conn,
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		outputCh:      make(chan string, 100_000), // Буфер с запасом под все пакеты
		stopCh:        make(chan struct{}),
	}, nil
}

func (c *Client) Run() error {
	c.log.Info("QUIC client starting connection down to", "server", c.config.ServerAddr)

	go c.ackReceiver()
	go c.outputPipeline() // RU: Запуск фонового печатника экрана | EN: Start background print worker

	workers := c.config.Workers
	if workers <= 0 {
		workers = 10
	}
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
	}

	c.workerWg.Wait()

	// RU: Увеличиваем дедлайн ожидания в консоли, чтобы дать медленному терминалу пропечатать строки
	// EN: Extend deadline bounds in console mode providing slow terminal gaps to flush strings
	deadline := time.After(20 * time.Second)
	for c.ackCount.Load() < c.config.TotalPackets {
		select {
		case <-deadline:
			c.log.Warn("timeout waiting for QUIC ACKs")
			goto exit
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

exit:
	c.log.Info("QUIC client metrics completed",
		"sent", c.sentCount.Load(),
		"acked", c.ackCount.Load(),
	)
	return nil
}

func (c *Client) generator(idx, step int) {
	defer c.workerWg.Done()

	if c.config.PregenPackets != nil {
		ctx := context.Background()
		for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
			select {
			case <-c.stopCh:
				return
			default:
			}
			data := c.config.PregenPackets[id-1]
			stream, err := c.conn.OpenStreamSync(ctx)
			if err != nil {
				return
			}
			_, err = stream.Write(data)
			_ = stream.Close()
			if err != nil {
				return
			}
			c.sentCount.Add(1)
		}
		return
	}

	serializeBuf := make([]byte, 0, c.config.MaxPacketSize+256)
	rawBuf := make([]byte, c.config.MaxPacketSize+256)
	ctx := context.Background()

	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
		select {
		case <-c.stopCh:
			return
		default:
		}

		minLen := int(id)
		maxLen := 2 * int(id)
		dataLen := minLen
		if maxLen > minLen {
			dataLen = minLen + (int(id) % (maxLen - minLen + 1))
		}
		if dataLen > c.config.MaxPacketSize {
			dataLen = c.config.MaxPacketSize
		}

		payload := rawBuf[:dataLen]
		zeroalloc.FillRandomBytes(payload)

		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(),
			Data:      payload,
		}

		serializeBuf = serializeBuf[:0]
		data, err := pkt.SerializeTo(serializeBuf)
		if err != nil {
			continue
		}

		c.sendMu.Lock()
		stream, err := c.conn.OpenStreamSync(ctx)
		c.sendMu.Unlock()
		if err != nil {
			return
		}

		_, err = stream.Write(data)
		_ = stream.Close()
		if err != nil {
			return
		}
		c.sentCount.Add(1)

		time.Sleep(20 * time.Microsecond) // Плавный pacing для стабильности
	}
}

func (c *Client) ackReceiver() {
	var ackBuf [32]byte
	for {
		n, _, err := c.conn.ReceiveTo(ackBuf[:])
		if err != nil {
			return
		}
		if n < 9 {
			continue
		}

		id := binary.BigEndian.Uint64(ackBuf[0:8])
		success := ackBuf[8] == 1
		c.ackCount.Add(1)

		if !c.config.BenchMode {
			var result string
			if success {
				result = fmt.Sprintf("ID=%d OK (QUIC)", id)
			} else {
				result = fmt.Sprintf("ID=%d FAIL (QUIC)", id)
			}
			// RU: Вместо прямой печати шлем строки в неблокирующий канал
			// EN: Instead of inline printing route strings into non-blocking channel
			for _, r := range c.orderedOutput.Insert(id, result) {
				select {
				case c.outputCh <- r:
				default:
				}
			}
		}
	}
}

func (c *Client) outputPipeline() {
	for res := range c.outputCh {
		fmt.Fprintln(c.out, res)
	}
}

func (c *Client) Shutdown() {
	close(c.stopCh)
	close(c.outputCh)
	c.workerWg.Wait()
	_ = c.conn.Close()
}
