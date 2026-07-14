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
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
	"core-packet-data-network/pkg/zeroalloc"
)

type Client struct {
	config        *ClientConfig
	conn          *network.QUICConn
	log           *logger.Logger
	out           io.Writer
	orderedOutput *order.OrderedBuffer[string]

	sendCh    chan []byte
	sentCount atomic.Uint64
	ackCount  atomic.Uint64
	lostCount atomic.Uint64

	ctx        context.Context
	cancel     context.CancelFunc
	workerWg   sync.WaitGroup
	shutdowner *shutdown.Shutdowner
}

func NewClient(cfg *ClientConfig, log *logger.Logger, out io.Writer) (*Client, error) {
	if out == nil {
		out = io.Discard
	}
	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer connCancel()

	conn, err := network.NewQUICConnClient(connCtx, cfg.ServerAddr, cfg.TLSConfig)
	if err != nil {
		return nil, fmt.Errorf("quic client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		config:        cfg,
		conn:          conn,
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		sendCh:        make(chan []byte, 1000),
		ctx:           ctx,
		cancel:        cancel,
	}

	c.shutdowner = shutdown.New()
	c.shutdowner.Register("quic client conn", conn, shutdown.PriorityHigh)
	return c, nil
}

func (c *Client) Run() error {
	c.log.Info("QUIC client starting", "server", c.config.ServerAddr)

	dataStream, err := c.conn.OpenStreamSync(context.Background())
	if err != nil {
		return fmt.Errorf("open data stream: %w", err)
	}
	defer dataStream.Close()

	ackStream, err := c.conn.OpenStreamSync(context.Background())
	if err != nil {
		return fmt.Errorf("open ack stream: %w", err)
	}
	defer ackStream.Close()

	c.workerWg.Add(1)
	go c.dataWriter(dataStream)

	c.workerWg.Add(1)
	go c.ackReader(ackStream)

	workers := 20
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
	}

	// Ждём, пока все пакеты будут отправлены
	for c.sentCount.Load() < c.config.TotalPackets {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(c.sendCh) // закрываем канал отправки, dataWriter завершится

	// Ждём все ACK или таймаут
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
		// Production – тоже ждём все ACK
		for c.ackCount.Load() < c.config.TotalPackets {
			time.Sleep(5 * time.Millisecond)
		}
	}
exit:
	c.cancel()        // завершаем ackReader
	c.workerWg.Wait() // ожидаем dataWriter и ackReader

	if c.config.BenchMode {
		c.log.Info("client metrics",
			"sent", c.sentCount.Load(),
			"acked", c.ackCount.Load(),
			"lost", c.lostCount.Load(),
		)
	} else {
		// Вывод упорядоченных результатов был выполнен в ackReader
	}
	return nil
}

func (c *Client) dataWriter(stream io.Writer) {
	defer c.workerWg.Done()
	lenBuf := make([]byte, 2)
	for data := range c.sendCh {
		binary.BigEndian.PutUint16(lenBuf, uint16(len(data)))
		if _, err := stream.Write(lenBuf); err != nil {
			c.log.Error("write len error: %v", err)
			return
		}
		if _, err := stream.Write(data); err != nil {
			c.log.Error("write data error: %v", err)
			return
		}
	}
}

func (c *Client) ackReader(stream io.Reader) {
	defer c.workerWg.Done()
	var ack [9]byte
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		_, err := io.ReadFull(stream, ack[:])
		if err != nil {
			if err != io.EOF {
				c.log.Error("ack read error: %v", err)
			}
			return
		}
		id := binary.BigEndian.Uint64(ack[0:8])
		success := ack[8] == 1
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

func (c *Client) generator(idx, step int) {
	defer c.workerWg.Done()

	if c.config.PregenPackets != nil {
		for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			data := c.config.PregenPackets[id-1]
			c.sendCh <- data
			c.sentCount.Add(1)
		}
		return
	}

	buf := make([]byte, 0, c.config.MaxPacketSize+64)
	dataBuf := make([]byte, c.config.MaxPacketSize)
	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
		select {
		case <-c.ctx.Done():
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
		c.sendCh <- data
		c.sentCount.Add(1)
	}
}

func (c *Client) Shutdown(ctx context.Context) error {
	c.cancel()
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
