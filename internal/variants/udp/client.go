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

// Client инкапсулирует логику UDP-клиента
// Client encapsulates the UDP client runtime logic
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

// pendingPacket хранит метаданные пакета для повторной отправки
// pendingPacket maintains packet metadata required for retransmissions
type pendingPacket struct {
	data     []byte
	checksum [32]byte // RU: Меняем []byte на [32]byte | EN: Change []byte to [32]byte
	attempts int
	lastSent time.Time
}

// NewClient инициализирует и настраивает сетевые буферы клиента
// NewClient initializes and configures local client network buffers
func NewClient(cfg *Config, log *logger.Logger, out io.Writer) (*Client, error) {
	if out == nil {
		out = os.Stdout
	}
	conn, err := network.NewUDPConn(cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("udp client: %w", err)
	}

	// RU: Выделяем 8 МБ под системные буферы сокета для Highload
	// EN: Allocate 8 MB for system socket buffers to handle Highload workloads
	conn.SetReadBuffer(8 * 1024 * 1024)
	conn.SetWriteBuffer(8 * 1024 * 1024)

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

// Run запускает воркеры отправки и диспетчеры подтверждений
// Run starts transmission workers and acknowledgement dispatchers
func (c *Client) Run() error {
	c.log.Info("UDP client starting", "server", c.config.ServerAddr)
	go c.ackReceiver()

	workers := 20
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

// generator параллельно генерирует и шлет пакеты без аллокаций в куче
// generator concurrently constructs and sends packets with zero heap allocations
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

	// RU: Аллокация переиспользуемых буферов строго внутри горутины, но вне цикла отправки
	// EN: Allocate reusable byte arrays explicitly inside the goroutine scope but outside the hot loop
	serializeBuf := make([]byte, 0, c.config.MaxPacketSize+128)
	rawBuf := make([]byte, c.config.MaxPacketSize)

	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
		select {
		case <-c.stopCh:
			return
		default:
		}

		// RU: Расчет размера данных строго по ТЗ: в пределах <номер пакета> - <2 * номер пакета>
		// EN: Payload boundary constraints calculated according to specification: <ID> up to <2 * ID>
		minLen := int(id)
		maxLen := 2 * int(id)
		dataLen := minLen
		if maxLen > minLen {
			dataLen = minLen + (int(id) % (maxLen - minLen + 1))
		}

		// RU: Ограничиваем размер рамками MTU конфигурации
		// EN: Tighten data slice size to prevent exceeding standard MTU bounds
		if dataLen > c.config.MaxPacketSize {
			dataLen = c.config.MaxPacketSize
		}

		// RU: Безопасное заполнение без лишних аллокаций
		// EN: Secure data population avoiding extra memory overhead
		payload := rawBuf[:dataLen]
		zeroalloc.FillRandomBytes(payload)

		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(),
			Data:      payload,
		}

		// RU: Сбрасываем длину буфера сериализации, сохраняя его cap
		// EN: Reset serialization buffer slice boundaries while preserving capacity
		serializeBuf = serializeBuf[:0]
		data, err := pkt.SerializeTo(serializeBuf)
		if err != nil {
			continue
		}

		if err := c.conn.Send(context.Background(), data, c.serverAddr); err != nil {
			if isClosedNetworkError(err) {
				return
			}
			continue
		}
		c.sentCount.Add(1)

		if !c.config.BenchMode {
			// RU: Для данных по-прежнему делаем копию, а массив [32]byte копируется автоматически по значению!
			// EN: We still copy the dynamic data slice, but the [32]byte array is copied automatically by value!
			pendingData := make([]byte, len(pkt.Data))
			copy(pendingData, pkt.Data)

			c.pendingMu.Lock()
			c.pending[id] = &pendingPacket{
				data:     pendingData,
				checksum: pkt.Checksum, // RU: Просто присваиваем, ноль аллокаций | EN: Plain assignment, zero allocations
				attempts: 1,
				lastSent: time.Now(),
			}
			c.pendingMu.Unlock()
		}
	}
}

// ackReceiver принимает подтверждения и отправляет их в упорядоченный пайплайн вывода
// ackReceiver ingests incoming ACKs and channels them down to the ordered ring buffer
func (c *Client) ackReceiver() {
	// RU: Фиксированный массив на стеке для приема ACK-пакетов
	// EN: Fixed stack array allocation for incoming ACK structures
	var ackBuf [32]byte

	for {
		n, _, err := c.conn.ReceiveTo(ackBuf[:])
		if err != nil {
			if isClosedNetworkError(err) {
				return
			}
			return
		}
		if n < 9 {
			continue
		}
		id := binary.BigEndian.Uint64(ackBuf[0:8])
		success := ackBuf[8] == 1 // RU: Индексация по фиксированному локальному массиву | EN: Evaluate bounds matching fixed array indices

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

// retransmitter отслеживает таймауты и осуществляет повторную отправку без блокировки мапы сетевыми вызовами
// retransmitter tracks packet deadlines and fires re-sends avoiding blocking the storage map with network calls
func (c *Client) retransmitter() {
	ticker := time.NewTicker(c.config.RetryTimeout)
	defer ticker.Stop()

	type retryTask struct {
		id   uint64
		data []byte
	}

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			var tasks []retryTask
			now := time.Now()

			c.pendingMu.Lock()
			for id, pp := range c.pending {
				if now.Sub(pp.lastSent) > c.config.RetryTimeout {
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
						Timestamp: now,
						Data:      pp.data,
						Checksum:  pp.checksum, // RU: Прямое присвоение массива [32]byte | EN: Direct [32]byte array assignment
					}

					data, err := pkt.Serialize()
					if err != nil {
						continue
					}

					tasks = append(tasks, retryTask{id: id, data: data})
					pp.attempts++
					pp.lastSent = now
				}
			}
			c.pendingMu.Unlock()

			// RU: Отправка пакетов по сети выполняется ПОСЛЕ снятия мьютекса c.pendingMu
			// EN: Network package distribution executes explicitly AFTER releasing the c.pendingMu mutex lock
			for _, task := range tasks {
				if err := c.conn.Send(context.Background(), task.data, c.serverAddr); err != nil {
					if isClosedNetworkError(err) {
						return
					}
				}
			}
		}
	}
}

// Shutdown осуществляет корректное завершение работы клиента
// Shutdown coordinates graceful termination routines across client background routines
func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
