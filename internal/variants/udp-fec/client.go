package udpfec

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/fec"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
	"core-packet-data-network/pkg/zeroalloc"
)

// FECShard расширяет packet.Packet для передачи шарда.
type fecShard struct {
	PacketID uint64
	ShardID  uint8 // 0..dataShards, dataShards = parity
	Data     []byte
}

func (s *fecShard) Serialize() []byte {
	buf := make([]byte, 8+1+len(s.Data))
	binary.BigEndian.PutUint64(buf[0:8], s.PacketID)
	buf[8] = s.ShardID
	copy(buf[9:], s.Data)
	return buf
}

func deserializeShard(data []byte) (*fecShard, error) {
	if len(data) < 9 {
		return nil, fmt.Errorf("shard too short")
	}
	return &fecShard{
		PacketID: binary.BigEndian.Uint64(data[0:8]),
		ShardID:  data[8],
		Data:     data[9:],
	}, nil
}

type Client struct {
	config        *Config
	conn          *network.UDPConn
	serverAddr    *net.UDPAddr
	fecEncoder    *fec.XorEncoder
	log           *logger.Logger
	orderedOutput *order.OrderedBuffer[string]
	pending       map[uint64]*pendingFEC
	pendingMu     sync.Mutex

	sentCount atomic.Uint64
	ackCount  atomic.Uint64
	lostCount atomic.Uint64

	workerWg   sync.WaitGroup
	stopCh     chan struct{}
	shutdowner *shutdown.Shutdowner
}

type pendingFEC struct {
	pkt      packet.Packet
	shards   [][]byte // закодированные шарды
	attempts int
	lastSent time.Time
}

func NewClient(cfg *Config, log *logger.Logger) (*Client, error) {
	conn, err := network.NewUDPConn(cfg.ClientAddr)
	if err != nil {
		return nil, fmt.Errorf("udp client: %w", err)
	}
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
		orderedOutput: order.NewOrderedBuffer[string](1),
		pending:       make(map[uint64]*pendingFEC),
		stopCh:        make(chan struct{}),
	}
	c.shutdowner = shutdown.New()
	c.shutdowner.Register("udp-fec client conn", conn, shutdown.PriorityHigh)
	return c, nil
}

func (c *Client) Run() error {
	c.log.Info("UDP+FEC client starting")
	go c.ackReceiver()
	for i := 0; i < 10; i++ {
		c.workerWg.Add(1)
		go c.generator(i)
	}
	go c.retransmitter()
	c.workerWg.Wait()
	// wait for acks...
	time.Sleep(5 * time.Second) // упрощённо, можно доработать
	return nil
}

func (c *Client) generator(idx int) {
	defer c.workerWg.Done()
	for id := uint64(1) + uint64(idx); id <= c.config.TotalPackets; id += 10 {
		select {
		case <-c.stopCh:
			return
		default:
		}
		pkt := packet.Packet{
			ID:        id,
			Timestamp: time.Now(),
			Data:      zeroalloc.GenerateRandomData(int(id) + int(id)%2),
		}
		if len(pkt.Data) > c.config.MaxPacketSize {
			pkt.Data = pkt.Data[:c.config.MaxPacketSize]
		}
		// разбиваем данные на шарды
		shardSize := (len(pkt.Data) + c.config.DataShards - 1) / c.config.DataShards
		shards := make([][]byte, c.config.DataShards)
		for i := 0; i < c.config.DataShards; i++ {
			start := i * shardSize
			end := start + shardSize
			if end > len(pkt.Data) {
				end = len(pkt.Data)
			}
			shard := make([]byte, shardSize)
			copy(shard, pkt.Data[start:end])
			shards[i] = shard
		}
		encoded, err := c.fecEncoder.Encode(shards)
		if err != nil {
			c.log.Error("fec encode error", "id", id)
			continue
		}

		// отправляем все шарды
		for i, sh := range encoded {
			s := fecShard{PacketID: id, ShardID: uint8(i), Data: sh}
			data := s.Serialize()
			if err := c.conn.Send(context.Background(), data, c.serverAddr); err != nil {
				c.log.Error("send shard error", "id", id, "shard", i, "error", err)
				continue
			}
		}
		c.sentCount.Add(1)

		c.pendingMu.Lock()
		c.pending[id] = &pendingFEC{
			pkt:      pkt,
			shards:   encoded,
			attempts: 1,
			lastSent: time.Now(),
		}
		c.pendingMu.Unlock()
	}
}

// ackReceiver принимает ACK (точно такой же формат: 8 байт ID + 1 байт статус)
func (c *Client) ackReceiver() {
	for {
		msg, err := c.conn.Receive(context.Background())
		if err != nil {
			return
		}
		if len(msg.Data) < 9 {
			continue
		}
		id := binary.BigEndian.Uint64(msg.Data[0:8])
		success := msg.Data[8] == 1
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
		c.ackCount.Add(1)
		var result string
		if success {
			result = fmt.Sprintf("ID=%d OK", id)
		} else {
			result = fmt.Sprintf("ID=%d FAIL", id)
		}
		for _, r := range c.orderedOutput.Insert(id, result) {
			fmt.Println(r)
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
						result := fmt.Sprintf("ID=%d LOST (max retries)", id)
						for _, r := range c.orderedOutput.Insert(id, result) {
							fmt.Println(r)
						}
						continue
					}
					// повторно отправляем все шарды
					for i, sh := range pp.shards {
						s := fecShard{PacketID: id, ShardID: uint8(i), Data: sh}
						data := s.Serialize()
						if err := c.conn.Send(context.Background(), data, c.serverAddr); err != nil {
							c.log.Error("resend shard error", "id", id, "shard", i)
							continue
						}
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
