package network

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/quic-go/quic-go"
)

// QUICConn обёртка над quic.Connection.
type QUICConn struct {
	conn *quic.Conn
	addr net.Addr
}

// NewQUICConnClient создаёт QUIC-клиентское соединение.
func NewQUICConnClient(serverAddr string, tlsConfig *tls.Config) (*QUICConn, error) {
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, err
	}
	conn, err := quic.DialAddr(context.Background(), addr.String(), tlsConfig, nil)
	if err != nil {
		return nil, err
	}
	return &QUICConn{
		conn: conn,
		addr: conn.LocalAddr(),
	}, nil
}

// Send отправляет данные по QUIC (создаёт новый поток).
func (q *QUICConn) Send(ctx context.Context, data []byte, addr net.Addr) error {
	stream, err := q.conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	_, err = stream.Write(data)
	return err
}

// Receive принимает данные из QUIC-потока.
func (q *QUICConn) Receive(ctx context.Context) (*Message, error) {
	stream, err := q.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	buf := make([]byte, 65535)
	n, err := stream.Read(buf)
	if err != nil {
		return nil, err
	}
	return &Message{
		Addr: q.conn.RemoteAddr(),
		Data: buf[:n],
	}, nil
}

// Close закрывает QUIC-соединение.
func (q *QUICConn) Close(ctx context.Context) error {
	return q.conn.CloseWithError(0, "closed")
}

// CloseWithError закрывает с ошибкой.
func (q *QUICConn) CloseWithError(code quic.ApplicationErrorCode, reason string) error {
	return q.conn.CloseWithError(code, reason)
}

// Addr возвращает локальный адрес.
func (q *QUICConn) Addr() net.Addr {
	return q.addr
}

// RemoteAddr возвращает удалённый адрес.
func (q *QUICConn) RemoteAddr() net.Addr {
	return q.conn.RemoteAddr()
}

// AcceptStream принимает новый поток (для сервера).
func (q *QUICConn) AcceptStream(ctx context.Context) (*quic.Stream, error) {
	return q.conn.AcceptStream(ctx)
}

// OpenStreamSync открывает поток синхронно (для клиента).
func (q *QUICConn) OpenStreamSync(ctx context.Context) (*quic.Stream, error) {
	return q.conn.OpenStreamSync(ctx)
}
