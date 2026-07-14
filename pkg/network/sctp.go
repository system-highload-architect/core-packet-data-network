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

// RemoteAddr возвращает удалённый адрес.
func (s *SCTPConn) RemoteAddr() net.Addr {
	return s.conn.RemoteAddr()
}

// LocalAddr возвращает локальный адрес.
func (s *SCTPConn) LocalAddr() net.Addr {
	return s.conn.LocalAddr()
}

// Addr возвращает локальный адрес для удовлетворения интерфейсу Transport.
func (s *SCTPConn) Addr() net.Addr {
	return s.conn.LocalAddr()
}

// Send отправляет данные.
func (s *SCTPConn) Send(ctx context.Context, data []byte, addr net.Addr) error {
	_, err := s.conn.Write(data)
	return err
}

// Receive читает данные (оставлен для совместимости).
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

// ReceiveTo реализует ВЫСОКОПРОИЗВОДИТЕЛЬНОЕ чтение в готовый буфер без аллокаций.
// ReceiveTo implements high-performance zero-allocation read into a pre-allocated buffer.
func (s *SCTPConn) ReceiveTo(buf []byte) (int, net.Addr, error) {
	n, err := s.conn.Read(buf)
	if err != nil {
		return 0, nil, err
	}
	return n, s.conn.RemoteAddr(), nil
}

// Close закрывает соединение (приведено к единой сигнатуре Closer без контекста).
// Close terminates sctp connection session (aligned with standard context-free Closer).
func (s *SCTPConn) Close() error {
	return s.conn.Close()
}
