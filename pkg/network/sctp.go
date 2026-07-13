package network

import (
	"context"
	"net"

	"github.com/ishidawataru/sctp"
)

// SCTPConn обёртка над sctp.SCTPConn (активное соединение).
type SCTPConn struct {
	conn *sctp.SCTPConn
	addr net.Addr
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
	return &SCTPConn{
		conn: conn,
		addr: conn.LocalAddr(),
	}, nil
}

// NewSCTPServer создаёт SCTP-слушатель (сервер).
// Возвращает функцию Accept, которая будет возвращать активные соединения.
func NewSCTPServer(localAddr string) (*sctp.SCTPListener, error) {
	addr, err := sctp.ResolveSCTPAddr("sctp", localAddr)
	if err != nil {
		return nil, err
	}
	return sctp.ListenSCTP("sctp", addr)
}

// Send отправляет данные по SCTP.
func (s *SCTPConn) Send(ctx context.Context, data []byte, addr net.Addr) error {
	// SCTPConn.Write принимает []byte и отправляет на удалённый адрес,
	// который уже задан при создании соединения.
	_, err := s.conn.Write(data)
	return err
}

// Receive читает один SCTP-пакет.
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

// Close закрывает соединение.
func (s *SCTPConn) Close() error {
	return s.conn.Close()
}

// Addr возвращает локальный адрес.
func (s *SCTPConn) Addr() net.Addr {
	return s.addr
}
