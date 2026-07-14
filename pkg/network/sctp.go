package network

import (
	"context"
	"net"

	"github.com/ishidawataru/sctp"
)

// SCTPConn обёртка над sctp.SCTPConn.
type SCTPConn struct {
	conn *sctp.SCTPConn
}

// RemoteAddr возвращает удалённый адрес.
func (s *SCTPConn) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// LocalAddr возвращает локальный адрес.
func (s *SCTPConn) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// Send отправляет данные.
func (s *SCTPConn) Send(ctx context.Context, data []byte, addr net.Addr) error {
	_, err := s.conn.Write(data)
	return err
}

// Receive читает данные.
func (s *SCTPConn) Receive(ctx context.Context) (*Message, error) {
	buf := make([]byte, 65535)
	n, err := s.conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return &Message{
		Addr: s.conn.RemoteAddr(),
		Data: buf[:n],
	}, nil
}

// Close закрывает соединение (для shutdown.Closer).
func (s *SCTPConn) Close(ctx context.Context) error {
	return s.conn.Close()
}

// NewSCTPConn создаёт активное SCTP-соединение (клиент).
func NewSCTPConn(remoteAddr string) (*SCTPConn, error) {
	addr, err := sctp.ResolveSCTPAddr("sctp", remoteAddr)
	if err != nil {
		return nil, err
	}
	conn, err := sctp.DialSCTP("sctp", nil, addr)
	if err != nil {
		return nil, err
	}
	return &SCTPConn{conn: conn}, nil
}
