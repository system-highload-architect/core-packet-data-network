package sctp

import (
	"context"
	"encoding/binary"
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

// Client SCTP-клиент с параллельной генерацией и подтверждениями.
type Client struct {
	config        *Config
	conn          *network.SCTPConn
	log           *logger.Logger
	orderedOutput *order.OrderedBuffer[string] // упорядоченный вывод
	pending       map[uint64]*pendingPacket    // ожидающие подтверждения
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

// NewClient создаёт клиент.
func NewClient(cfg *Config, log *logger.Logger) (*Client, error) {
	conn, err := network.NewSCTPConn(cfg.RemoteAddr)
	if err != nil {
		return nil, fmt.Errorf("sctp connect: %w", err)
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
	c.shutdowner.Register("sctp conn", conn, shutdown.PriorityHigh)
	return c, nil
}

// Run запускает клиент.
func (c *Client) Run() error {
	c.log.Info("SCTP client starting", "server", c.config.RemoteAddr)

	// Приём ACK в отдельной горутине
	go c.ackReceiver()

	// Запуск 10 генераторов
	for i := 0; i < 10; i++ {
		c.workerWg.Add(1)
		go c.generator(i)
	}

	// Ретрансмиттер
	go c.retransmitter()

	// Ждём завершения отправки всех пакетов
	c.workerWg.Wait()
	c.log.Info("All packets sent, waiting for pending ACKs...")

	// Ожидаем, пока все ACK не будут получены или таймаут
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

// generator генерирует и отправляет пакеты (шардирование ID).
func (c *Client) generator(idx int) {
	defer c.workerWg.Done()
	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += 10 {
		select {
		case <-c.stopCh:
			return
		default:
		}

		// Формирование пакета
		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(),
			Data:      zeroalloc.GenerateRandomData(int(id) + int(id)%2), // длина [id, 2*id]
			Checksum:  nil,                                               // вычисляется при Serialize
		}
		// Обрезаем данные до MaxPacketSize
		if len(pkt.Data) > c.config.MaxPacketSize {
			pkt.Data = pkt.Data[:c.config.MaxPacketSize]
		}

		// Сериализация
		data, err := pkt.Serialize()
		if err != nil {
			c.log.Error("serialize error", "id", id)
			continue
		}

		// Отправка через SCTP (адрес не нужен, соединение установлено)
		if err := c.conn.Send(context.Background(), data, nil); err != nil {
			c.log.Error("send error", "id", id, "error", err)
			continue
		}
		c.sentCount.Add(1)

		// Добавляем в список ожидающих ACK
		c.pendingMu.Lock()
		c.pending[id] = &pendingPacket{
			pkt:      pkt,
			attempts: 1,
			lastSent: time.Now(),
		}
		c.pendingMu.Unlock()
	}
}

// ackReceiver принимает бинарные ACK и обрабатывает.
func (c *Client) ackReceiver() {
	for {
		msg, err := c.conn.Receive(context.Background())
		if err != nil {
			c.log.Error("ack receiver error", "error", err)
			return
		}
		if len(msg.Data) < 9 {
			continue
		}
		// Формат: 8 байт ID + 1 байт статус (0 - ошибка, 1 - успех)
		id := binary.BigEndian.Uint64(msg.Data[0:8])
		success := msg.Data[8] == 1

		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()

		c.ackCount.Add(1)

		// Формируем строку результата и выводим упорядоченно
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

// retransmitter повторно отправляет не подтверждённые пакеты.
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
						// Превышено число попыток
						c.lostCount.Add(1)
						delete(c.pending, id)
						result := fmt.Sprintf("ID=%d LOST (max retries)", id)
						for _, r := range c.orderedOutput.Insert(id, result) {
							fmt.Println(r)
						}
						continue
					}
					// Повторная отправка
					data, err := pp.pkt.Serialize()
					if err != nil {
						c.log.Error("resend serialize error", "id", id)
						continue
					}
					if err := c.conn.Send(context.Background(), data, nil); err != nil {
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

// Shutdown останавливает клиент.
func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
