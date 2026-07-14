package network

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

type QUICConn struct {
	conn *quic.Conn
	addr net.Addr
}

func NewQUICConnClient(ctx context.Context, serverAddr string, tlsConfig *tls.Config) (*QUICConn, error) {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, err
	}
	quicConf := &quic.Config{
		EnableDatagrams:       true,
		MaxIncomingStreams:    1000,
		MaxIncomingUniStreams: 1000,
		KeepAlivePeriod:       5 * time.Second,
		MaxIdleTimeout:        30 * time.Second,
	}

	// DialAddr возвращает quic.Conn в текущей версии библиотеки
	conn, err := quic.DialAddr(ctx, addr.String(), tlsConfig, quicConf)
	if err != nil {
		return nil, err
	}
	return &QUICConn{
		conn: conn,
		addr: conn.LocalAddr(),
	}, nil
}

// Send реализует интерфейс Sender через датаграммы (для максимальной скорости)
func (q *QUICConn) Send(ctx context.Context, data []byte, addr net.Addr) error {
	return q.conn.SendDatagram(data)
}

// Receive реализует интерфейс Receiver (старый вариант с аллокациями)
func (q *QUICConn) Receive(ctx context.Context) (*Message, error) {
	msg, err := q.conn.ReceiveDatagram(ctx)
	if err != nil {
		return nil, err
	}
	return &Message{Addr: q.conn.RemoteAddr(), Data: msg}, nil
}

// ReceiveTo высокопроизводительное чтение датаграмм без аллокаций
func (q *QUICConn) ReceiveTo(buf []byte) (int, net.Addr, error) {
	// quic-go принимает датаграмму во внутренний буфер
	msg, err := q.conn.ReceiveDatagram(context.Background())
	if err != nil {
		return 0, nil, err
	}
	// Копируем данные напрямую в наш переиспользуемый буфер (Zero Allocations в куче)
	n := copy(buf, msg)
	return n, q.conn.RemoteAddr(), nil
}

// OpenStreamSync открывает поток синхронно.
func (q *QUICConn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	return q.conn.OpenStreamSync(ctx)
}

// AcceptStream принимает новый поток.
func (q *QUICConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	return q.conn.AcceptStream(ctx)
}

// Close закрывает QUIC-соединение (соответствует интерфейсу Closer)
func (q *QUICConn) Close(ctx context.Context) error {
	_ = ctx
	return q.conn.CloseWithError(0, "closed")
}

func (q *QUICConn) Addr() net.Addr {
	return q.addr
}

func (q *QUICConn) RemoteAddr() net.Addr {
	return q.conn.RemoteAddr()
}
