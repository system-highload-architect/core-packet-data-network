package udpfec

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
	"core-packet-data-network/pkg/fec"
	"core-packet-data-network/pkg/lru" // Импортируем наш готовый LayerCache
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/shutdown"
	"core-packet-data-network/pkg/zeroalloc"
)

type fecShard struct {
	PacketID uint64
	ShardID  uint8
	Data     []byte
}

func (s *fecShard) SerializeTo(buf []byte) []byte {
	totalLen := 8 + 1 + len(s.Data)
	var out []byte
	if cap(buf) >= totalLen {
		out = buf[:totalLen]
	} else {
		out = make([]byte, totalLen)
	}
	binary.BigEndian.PutUint64(out[0:8], s.PacketID)
	out[8] = s.ShardID
	copy(out[9:], s.Data)
	return out
}

type Client struct {
	config        *Config
	conn          *network.UDPConn
	serverAddr    *net.UDPAddr
	fecEncoder    *fec.XorEncoder
	log           *logger.Logger
	out           io.Writer
	orderedOutput *order.OrderedBuffer[string]

	// Подключаем многослойный кэш вместо сырой мапы
	layerCache *lru.LayerCache[uint64, *pendingFEC]
	pendingMu  sync.Mutex

	sentCount atomic.Uint64
	ackCount  atomic.Uint64
	lostCount atomic.Uint64

	workerWg   sync.WaitGroup
	stopCh     chan struct{}
	shutdowner *shutdown.Shutdowner
	bufPool    sync.Pool
}

type pendingFEC struct {
	shards [][]byte
}

func NewClient(cfg *Config, log *logger.Logger, out io.Writer) (*Client, error) {
	if out == nil {
		out = os.Stdout
	}
	conn, err := network.NewUDPConn(cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("udp client: %w", err)
	}

	conn.SetReadBuffer(8 * 1024 * 1024)
	conn.SetWriteBuffer(8 * 1024 * 1024)

	serverAddr, err := net.ResolveUDPAddr("udp", cfg.ServerAddr)
	if err != nil {
		return nil, err
	}

	// Настраиваем 3 экспоненциальных слоя ожидания для шардов
	layerConfigs := []lru.LayerConfig{
		{TTL: cfg.RetryTimeout},     // Слой 0: 100мс
		{TTL: cfg.RetryTimeout * 2}, // Слой 1: 200мс
		{TTL: cfg.RetryTimeout * 4}, // Слой 2: 400мс
	}

	c := &Client{
		config:        cfg,
		conn:          conn,
		serverAddr:    serverAddr,
		fecEncoder:    fec.NewXorEncoder(cfg.DataShards),
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		layerCache:    lru.NewLayerCache[uint64, *pendingFEC](layerConfigs, lru.NewExponentialBackoff()),
		stopCh:        make(chan struct{}),
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, cfg.MaxPacketSize+256)
				return &b
			},
		},
	}
	c.shutdowner = shutdown.New()
	c.shutdowner.Register("udp-fec client conn", conn, shutdown.PriorityHigh)
	return c, nil
}

func (c *Client) Run() error {
	c.log.Info("UDP+FEC client starting")
	go c.ackReceiver()

	workers := c.config.Workers
	if workers <= 0 {
		workers = 10
	}
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
	}

	if !c.config.BenchMode {
		go c.retransmitterDaemon()
	}

	c.workerWg.Wait()

	if c.config.BenchMode {
		time.Sleep(50 * time.Millisecond)
	} else {
		// Даем слоям кэша полностью очиститься
		deadline := time.After(15 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			if c.layerCache.Len() == 0 {
				break
			}
			select {
			case <-ticker.C:
			case <-deadline:
				c.log.Warn("timeout waiting for FEC layers to deplete")
				return nil
			}
		}
	}

	c.log.Info("UDP+FEC client metrics completed",
		"sent", c.sentCount.Load(),
		"acked", c.ackCount.Load(),
		"lost", c.lostCount.Load(),
	)
	return nil
}

func (c *Client) generator(idx, step int) {
	defer c.workerWg.Done()

	rawPayloadBuf := make([]byte, c.config.MaxPacketSize)
	serializeBuf := make([]byte, c.config.MaxPacketSize+256)
	maxShardSize := c.config.MaxPacketSize + 256

	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
		select {
		case <-c.stopCh:
			return
		default:
		}

		var payload []byte
		if c.config.PregenPackets != nil && int(id-1) < len(c.config.PregenPackets) {
			payload = c.config.PregenPackets[id-1]
		} else {
			minLen := int(id)
			maxLen := 2 * int(id)
			dataLen := minLen
			if maxLen > minLen {
				dataLen = minLen + (int(id) % (maxLen - minLen + 1))
			}
			if dataLen > c.config.MaxPacketSize {
				dataLen = c.config.MaxPacketSize
			}
			payload = rawPayloadBuf[:dataLen]
			zeroalloc.FillRandomBytes(payload)
		}

		dataLen := len(payload)
		shardSize := (dataLen + c.config.DataShards - 1) / c.config.DataShards

		// Безопасное выделение матрицы под шарды
		shards := make([][]byte, c.config.DataShards+1)
		for i := 0; i < c.config.DataShards+1; i++ {
			shards[i] = make([]byte, shardSize, maxShardSize)
		}

		for i := 0; i < c.config.DataShards; i++ {
			start := i * shardSize
			end := start + shardSize
			if end > dataLen {
				end = dataLen
			}
			if start < dataLen {
				copy(shards[i], payload[start:end])
			}
		}

		encoded, err := c.fecEncoder.Encode(shards[:c.config.DataShards])
		if err != nil {
			continue
		}

		for i, sh := range encoded {
			s := fecShard{PacketID: id, ShardID: uint8(i), Data: sh}
			data := s.SerializeTo(serializeBuf)
			_ = c.conn.Send(context.Background(), data, c.serverAddr)
		}
		c.sentCount.Add(1)

		if !c.config.BenchMode {
			c.layerCache.Set(id, &pendingFEC{
				shards: encoded,
			})
		}
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
			c.layerCache.Delete(id) // Удаляем со всех слоев при успехе

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

func (c *Client) retransmitterDaemon() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}

		var alive bool

		// Шаг 1: Ищем УЖЕ просроченные по TTL элементы среди слоев
		id, pp, layerIdx, foundExpired := c.layerCache.PeekExpiredScan()

		if foundExpired {
			// Продвигаем элемент на следующий слой Backoff
			pp, alive = c.layerCache.PromoteLayer(id, layerIdx)

			if !alive {
				c.lostCount.Add(1)
				result := fmt.Sprintf("ID=%d LOST", id)
				for _, r := range c.orderedOutput.Insert(id, result) {
					fmt.Fprintln(c.out, r)
				}
				continue
			}

			// Пакет жив! Повторно отправляем все его сохраненные FEC-шарды
			bufPtr := c.bufPool.Get().(*[]byte)
			buf := *bufPtr

			for i, sh := range pp.shards {
				s := fecShard{PacketID: id, ShardID: uint8(i), Data: sh}
				data := s.SerializeTo(buf)
				_ = c.conn.Send(context.Background(), data, c.serverAddr)
			}
			c.bufPool.Put(bufPtr)
			continue
		}

		// Шаг 2: Просроченных нет. Засыпаем ровно до ближайшего дедлайна.
		earliestDeadline, hasActiveDeadlines := c.layerCache.GetEarliestDeadline()

		if !hasActiveDeadlines {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		now := time.Now()
		if earliestDeadline.After(now) {
			sleepDuration := earliestDeadline.Sub(now)
			time.Sleep(sleepDuration)
		}
	}
}

func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
