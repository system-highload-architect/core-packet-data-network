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
	"core-packet-data-network/pkg/lru"
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

	// Наш многослойный кэш дедлайнов вместо сырой мапы
	layerCache *lru.LayerCache[uint64, *pendingPacket]
	pendingMu  sync.Mutex

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
		return nil, fmt.Errorf("resolve server addr: %w", err)
	}

	// Конфигурируем 3 слоя экспоненциального Backoff ожидания
	layerConfigs := []lru.LayerConfig{
		{TTL: cfg.RetryTimeout},     // Слой 0: 100мс (Быстрый)
		{TTL: cfg.RetryTimeout * 2}, // Слой 1: 200мс (Средний)
		{TTL: cfg.RetryTimeout * 4}, // Слой 2: 400мс (Экстренный)
	}

	c := &Client{
		config:        cfg,
		conn:          conn,
		serverAddr:    serverAddr,
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		layerCache:    lru.NewLayerCache[uint64, *pendingPacket](layerConfigs, lru.NewExponentialBackoff()),
		stopCh:        make(chan struct{}),
	}

	c.shutdowner = shutdown.New()
	c.shutdowner.Register("udp client conn", conn, shutdown.PriorityHigh)
	return c, nil
}

func (c *Client) Run() error {
	c.log.Info("UDP client starting", "server", c.config.ServerAddr)
	go c.ackReceiver()

	workers := c.config.Workers
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
	}

	if !c.config.BenchMode {
		go c.retransmitterDaemon()
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
		// Даем слоям кэша честно выгрести все хвосты перед выходом
		deadline := time.After(15 * time.Second)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			// Исправлено: вызываем инкапсулированный метод Len() вместо прямого доступа к полю
			if c.layerCache.Len() == 0 {
				break
			}
			select {
			case <-ticker.C:
			case <-deadline:
				c.log.Warn("timeout waiting for retransmit layers to deplete")
				goto exit
			}
		}
	}

exit:
	c.log.Info("client metrics completed",
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
			_ = c.conn.Send(context.Background(), data, c.serverAddr)
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

		_ = c.conn.Send(context.Background(), data, c.serverAddr)
		c.sentCount.Add(1)

		if !c.config.BenchMode {
			pendingData := make([]byte, len(pkt.Data))
			copy(pendingData, pkt.Data)

			// Создаем указатель на структуру и сразу передаем в кэш — переменная использована!
			c.layerCache.Set(id, &pendingPacket{
				data:     pendingData,
				checksum: pkt.Checksum[:],
			})
		}
	}
}

func (c *Client) ackReceiver() {
	// Фиксированный массив на стеке для приема ACK-пакетов
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
			// Прилетел ACK — полностью стираем пакет из всех слоев! Доставлено!
			c.layerCache.Delete(id)

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
				// Все попытки исчерпаны — это честный LOST
				c.lostCount.Add(1)
				result := fmt.Sprintf("ID=%d LOST", id)
				for _, r := range c.orderedOutput.Insert(id, result) {
					fmt.Fprintln(c.out, r)
				}
				continue
			}

			// Пакет жив! Перевыпускаем в сеть с обновленным штампом времени
			pkt := packet.Packet{
				ID:        id,
				Timestamp: time.Now(),
				Data:      pp.data,
			}
			copy(pkt.Checksum[:], pp.checksum)

			data, err := pkt.Serialize()
			if err == nil {
				_ = c.conn.Send(context.Background(), data, c.serverAddr)
			}
			continue // Выгребаем просроченную очередь без засыпания
		}

		// Шаг 2: Просроченных нет. Вычисляем точное время сна до ближайшего дедлайна
		earliestDeadline, hasActiveDeadlines := c.layerCache.GetEarliestDeadline()

		if !hasActiveDeadlines {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		now := time.Now()
		if earliestDeadline.After(now) {
			sleepDuration := earliestDeadline.Sub(now)
			time.Sleep(sleepDuration) // Спим в точности до миллисекунды истечения срока ближайшего пакета
		}
	}
}

func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
