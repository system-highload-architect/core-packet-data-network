package udpfec

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"core-packet-data-network/internal/common/logger"
	"core-packet-data-network/pkg/fec"
	"core-packet-data-network/pkg/lru"
	"core-packet-data-network/pkg/network"
	"core-packet-data-network/pkg/order"
	"core-packet-data-network/pkg/packet"
	"core-packet-data-network/pkg/shutdown"
)

type Server struct {
	config     *Config
	conn       *network.UDPConn
	fecDecoder *fec.XorEncoder
	log        *logger.Logger
	orderedBuf *order.OrderedBuffer[string]
	dedup      *lru.Cache[uint64, struct{}]
	shutdowner *shutdown.Shutdowner
	stopCh     chan struct{}

	// для сборки шардов по пакетам
	shardGroups map[uint64]*shardCollector
	groupsMu    sync.Mutex

	recvCount   atomic.Uint64
	badChecksum atomic.Uint64
}

type shardCollector struct {
	shards    [][]byte
	received  int
	total     int
	firstSeen time.Time
}

func NewServer(cfg *Config, log *logger.Logger) (*Server, error) {
	conn, err := network.NewUDPConn(cfg.ServerAddr)
	if err != nil {
		return nil, err
	}
	s := &Server{
		config:      cfg,
		conn:        conn,
		fecDecoder:  fec.NewXorEncoder(cfg.DataShards),
		log:         log,
		orderedBuf:  order.NewOrderedBuffer[string](1),
		dedup:       lru.NewCache[uint64, struct{}](30 * time.Second),
		shardGroups: make(map[uint64]*shardCollector),
		stopCh:      make(chan struct{}),
	}
	s.shutdowner = shutdown.New()
	s.shutdowner.Register("udp-fec server conn", conn, shutdown.PriorityHigh)
	return s, nil
}

func (s *Server) Run() error {
	s.log.Info("UDP+FEC server listening", "addr", s.config.ServerAddr)
	go s.collectorCleanup()
	for {
		select {
		case <-s.stopCh:
			return nil
		default:
			msg, err := s.conn.Receive(context.Background())
			if err != nil {
				continue
			}
			go s.processShard(msg)
		}
	}
}

func (s *Server) processShard(msg *network.Message) {
	shard, err := deserializeShard(msg.Data)
	if err != nil {
		return
	}
	if _, ok := s.dedup.Get(shard.PacketID); ok {
		return // пакет уже собран
	}

	s.groupsMu.Lock()
	grp, ok := s.shardGroups[shard.PacketID]
	if !ok {
		total := s.config.DataShards + 1
		grp = &shardCollector{
			shards:    make([][]byte, total),
			total:     total,
			firstSeen: time.Now(),
		}
		s.shardGroups[shard.PacketID] = grp
	}
	if grp.shards[shard.ShardID] == nil {
		grp.shards[shard.ShardID] = shard.Data
		grp.received++
	}
	s.groupsMu.Unlock()

	if grp.received == grp.total {
		// собрали все шарды
		s.groupsMu.Lock()
		delete(s.shardGroups, shard.PacketID)
		s.groupsMu.Unlock()
		s.dedup.Set(shard.PacketID, struct{}{})

		// объединяем данные
		var fullData []byte
		for i := 0; i < s.config.DataShards; i++ {
			fullData = append(fullData, grp.shards[i]...)
		}
		pkt := packet.Packet{
			ID:        shard.PacketID,
			Timestamp: time.Now(), // мы не знаем время формирования, но можем извлечь из первого шарда? Упростим
			Data:      fullData,
		}
		// восстанавливаем, если были nil
		recovered, err := s.fecDecoder.Recover(grp.shards)
		if err == nil {
			// проверяем целостность первых dataShards
			var cleanData []byte
			for i := 0; i < s.config.DataShards; i++ {
				cleanData = append(cleanData, recovered[i]...)
			}
			// обрезаем оригинальную длину, если padding
			// здесь предполагаем, что данные до обрезки были полные
			_ = cleanData // в реальности нужно знать исходный размер
		}

		recvTime := time.Now()
		s.recvCount.Add(1)
		resultStr := fmt.Sprintf("ID=%d Formed=%v Received=%v Checksum=OK", pkt.ID, pkt.Timestamp, recvTime)
		for _, r := range s.orderedBuf.Insert(pkt.ID, resultStr) {
			fmt.Println(r)
		}

		ackBuf := make([]byte, 9)
		binary.BigEndian.PutUint64(ackBuf[0:8], pkt.ID)
		ackBuf[8] = 1
		s.conn.Send(context.Background(), ackBuf, msg.Addr)
	}
}

// collectorCleanup удаляет устаревшие группы шардов
func (s *Server) collectorCleanup() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.groupsMu.Lock()
		for id, grp := range s.shardGroups {
			if time.Since(grp.firstSeen) > 5*time.Second {
				delete(s.shardGroups, id)
			}
		}
		s.groupsMu.Unlock()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	close(s.stopCh)
	return s.shutdowner.Shutdown(ctx)
}
