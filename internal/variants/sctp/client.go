package sctp

import (
	"context"
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
	conn          *network.SCTPConn
	log           *logger.Logger
	out           io.Writer
	orderedOutput *order.OrderedBuffer[string]

	sentCount atomic.Uint64
	ackCount  atomic.Uint64

	workerWg sync.WaitGroup
	stopCh   chan struct{}
}

func NewClient(cfg *Config, log *logger.Logger, out io.Writer) (*Client, error) {
	if out == nil {
		out = os.Stdout
	}

	// Устанавливаем единое SCTP соединение с сервером
	conn, err := network.NewSCTPConn(cfg.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("sctp client connection failed: %w", err)
	}

	return &Client{
		config:        cfg,
		conn:          conn,
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		stopCh:        make(chan struct{}),
	}, nil
}

func (c *Client) Run() error {
	c.log.Info("SCTP client session established", "server", c.config.ServerAddr)
	go c.ackReceiver()

	workers := 10 // Требование ТЗ по параллельным потокам
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
	}

	c.workerWg.Wait()

	// Ожидание финальных ACK подтверждений от сервера
	deadline := time.After(10 * time.Second)
	for c.ackCount.Load() < c.config.TotalPackets {
		select {
		case <-deadline:
			c.log.Warn("timeout waiting for SCTP ACKs")
			goto exit
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

exit:
	c.log.Info("SCTP client session completed",
		"sent", c.sentCount.Load(),
		"acked", c.ackCount.Load(),
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
				return
			}
			c.sentCount.Add(1)
		}
		return
	}

	serializeBuf := make([]byte, 0, c.config.MaxPacketSize+128)
	rawBuf := make([]byte, c.config.MaxPacketSize)

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

		if err := c.conn.Send(context.Background(), data, nil); err != nil {
			return
		}
		c.sentCount.Add(1)
	}
}
func (c *Client) ackReceiver() {
	// Жесткий фиксированный массив на стеке горутины. Ноль аллокаций в куче!
	var ackBuf [16]byte

	for {
		// Передаем срез фиксированного стекового массива. Данные пишутся in-place.
		n, _, err := c.conn.ReceiveTo(ackBuf[:])
		if err != nil {
			return
		}
		// Защита: ACK-пакет должен содержать как минимум 8 байт ID + 1 байт статус
		if n < 9 {
			continue
		}

		// Декодируем ID пакета из первых 8 байт
		id := binary.BigEndian.Uint64(ackBuf[0:8])

		// Исправлено: читаем статус успеха строго из 8-го байта пришедших данных
		success := ackBuf[8] == 1

		c.ackCount.Add(1)

		if !c.config.BenchMode {
			var result string
			if success {
				result = fmt.Sprintf("ID=%d OK (SCTP)", id)
			} else {
				result = fmt.Sprintf("ID=%d FAIL (SCTP)", id)
			}

			// Потоковый вывод строго по порядку номеров через кольцевой буфер
			for _, r := range c.orderedOutput.Insert(id, result) {
				fmt.Fprintln(c.out, r)
			}
		}
	}
}

func (c *Client) Shutdown() {
	close(c.stopCh)
	c.workerWg.Wait()
	_ = c.conn.Close(context.Background())
}
