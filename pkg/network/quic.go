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

func NewQUICConnClient(serverAddr string, tlsConfig *tls.Config) (*QUICConn, error) {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, err
	}
	// Увеличиваем буферы для максимальной производительности
	quicConf := &quic.Config{
		EnableDatagrams:       true,
		MaxIncomingStreams:    0, // только датаграммы
		MaxIncomingUniStreams: 0,
		KeepAlivePeriod:       5 * time.Second,
		MaxIdleTimeout:        30 * time.Second,
	}
	conn, err := quic.DialAddr(context.Background(), addr.String(), tlsConfig, quicConf)
	if err != nil {
		return nil, err
	}
	return &QUICConn{
		conn: conn,
		addr: conn.LocalAddr(),
	}, nil
}

// SendDatagram отправляет датаграмму.
func (q *QUICConn) SendDatagram(payload []byte) error {
	return q.conn.SendDatagram(payload)
}

// ReceiveDatagram принимает датаграмму.
func (q *QUICConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	msg, err := q.conn.ReceiveDatagram(ctx)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

// Close закрывает QUIC-соединение.
func (q *QUICConn) Close(ctx context.Context) error {
	return q.conn.CloseWithError(0, "closed")
}

func (q *QUICConn) Addr() net.Addr {
	return q.addr
}

// RemoteAddr возвращает удалённый адрес.
func (q *QUICConn) RemoteAddr() net.Addr {
	return q.conn.RemoteAddr()
}
