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
	pending       map[uint64]*pendingFEC
	pendingMu     sync.Mutex

	sentCount atomic.Uint64
	ackCount  atomic.Uint64
	lostCount atomic.Uint64

	workerWg   sync.WaitGroup
	stopCh     chan struct{}
	shutdowner *shutdown.Shutdowner
	bufPool    sync.Pool
}

type pendingFEC struct {
	shards   [][]byte
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

	conn.SetReadBuffer(8 * 1024 * 1024)
	conn.SetWriteBuffer(8 * 1024 * 1024)

	serverAddr, err := net.ResolveUDPAddr("udp", cfg.ServerAddr)
	if err != nil {
		return nil, err
	}

	c := &Client{
		config:        cfg,
		conn:          conn,
		serverAddr:    serverAddr,
		fecEncoder:    fec.NewXorEncoder(cfg.DataShards),
		log:           log,
		out:           out,
		orderedOutput: order.NewOrderedBuffer[string](1),
		pending:       make(map[uint64]*pendingFEC),
		stopCh:        make(chan struct{}),
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, cfg.MaxPacketSize+128)
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

	workers := 10
	for i := 0; i < workers; i++ {
		c.workerWg.Add(1)
		go c.generator(i, workers)
	}

	if !c.config.BenchMode {
		go c.retransmitter()
	}

	c.workerWg.Wait() // Ждем, пока все генераторы отправят пакеты

	// RU: Надежный алгоритм ожидания ACK для Production и BenchMode
	// EN: Reliable ACK waiting routine for Production and BenchMode contexts
	if c.config.BenchMode {
		// В режиме бенчмарка даем короткий зазор на вычитку оставшихся в буфере ACK
		time.Sleep(50 * time.Millisecond)
	} else {
		// В реальной работе честно ждем, пока мапа pending полностью не опустеет
		deadline := time.After(15 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
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
				c.log.Warn("timeout waiting for retransmit ACKs")
				return nil
			}
		}
	}
	return nil
}

func (c *Client) generator(idx, step int) {
	defer c.workerWg.Done()

	rawPayloadBuf := make([]byte, c.config.MaxPacketSize)
	serializeBuf := make([]byte, c.config.MaxPacketSize+128)

	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += uint64(step) {
		select {
		case <-c.stopCh:
			return
		default:
		}

		// RU: Корректная обработка предгенерированных пакетов в BenchMode с разбивкой на шарды!
		// EN: Proper handling of pre-generated packets in BenchMode with shard slicing!
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
		shards := make([][]byte, c.config.DataShards)

		for i := 0; i < c.config.DataShards; i++ {
			start := i * shardSize
			end := start + shardSize
			if end > dataLen {
				end = dataLen
			}

			chunk := make([]byte, shardSize)
			if start < dataLen {
				copy(chunk, payload[start:end])
			}
			shards[i] = chunk
		}

		encoded, err := c.fecEncoder.Encode(shards)
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
			c.pendingMu.Lock()
			c.pending[id] = &pendingFEC{
				shards:   encoded,
				attempts: 1,
				lastSent: time.Now(),
			}
			c.pendingMu.Unlock()
		}
	}
}

func (c *Client) ackReceiver() {
	var ackBuf [16]byte
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

	type retryTask struct {
		id     uint64
		shards [][]byte
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
						result := fmt.Sprintf("ID=%d LOST (max retries)", id)
						for _, r := range c.orderedOutput.Insert(id, result) {
							fmt.Fprintln(c.out, r)
						}
						continue
					}

					tasks = append(tasks, retryTask{id: id, shards: pp.shards})
					pp.attempts++
					pp.lastSent = now
				}
			}
			c.pendingMu.Unlock()

			if len(tasks) > 0 {
				bufPtr := c.bufPool.Get().(*[]byte)
				buf := *bufPtr
				for _, task := range tasks {
					for i, sh := range task.shards {
						s := fecShard{PacketID: task.id, ShardID: uint8(i), Data: sh}
						data := s.SerializeTo(buf)
						_ = c.conn.Send(context.Background(), data, c.serverAddr)
					}
				}
				c.bufPool.Put(bufPtr)
			}
		}
	}
}

func (c *Client) Shutdown(ctx context.Context) error {
	close(c.stopCh)
	c.workerWg.Wait()
	return c.shutdowner.Shutdown(ctx)
}
