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

// NewQUICListener создаёт QUIC-сервер (слушатель).
func NewQUICListener(localAddr string, tlsConfig *tls.Config) (*quic.Listener, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}
	return quic.ListenAddr(addr.String(), tlsConfig, nil)
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
	// Читаем данные из потока
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
func (q *QUICConn) Close() error {
	return q.conn.CloseWithError(0, "closed")
}

// Addr возвращает локальный адрес.
func (q *QUICConn) Addr() net.Addr {
	return q.addr
}
