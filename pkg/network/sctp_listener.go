package network

import (
	"context"
	"net"

	"github.com/ishidawataru/sctp"
)

// SCTPListener обёртка над sctp.SCTPListener.
type SCTPListener struct {
	listener *sctp.SCTPListener
	addr     net.Addr
}

// NewSCTPServer создаёт SCTP-слушатель.
func NewSCTPServer(localAddr string) (*SCTPListener, error) {
	addr, err := sctp.ResolveSCTPAddr("sctp", localAddr)
	if err != nil {
		return nil, err
	}
	listener, err := sctp.ListenSCTP("sctp", addr)
	if err != nil {
		return nil, err
	}
	return &SCTPListener{
		listener: listener,
		addr:     addr,
	}, nil
}

// Accept принимает новое SCTP-соединение.
func (ln *SCTPListener) Accept() (*SCTPConn, error) {
	conn, err := ln.listener.Accept()
	if err != nil {
		return nil, err
	}
	// Приводим к *sctp.SCTPConn
	if sctpConn, ok := conn.(*sctp.SCTPConn); ok {
		return &SCTPConn{conn: sctpConn}, nil
	}
	return nil, nil // или ошибка, если не удалось привести
}

// Close закрывает слушатель.
func (ln *SCTPListener) Close(ctx context.Context) error {
	return ln.listener.Close()
}

// Addr возвращает локальный адрес.
func (ln *SCTPListener) Addr() net.Addr {
	return ln.addr
}
