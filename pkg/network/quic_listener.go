package network

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/quic-go/quic-go"
)

// QUICListener — обёртка над quic.Listener для сервера.
type QUICListener struct {
	listener *quic.Listener
	addr     net.Addr
}

// NewQUICListener создаёт QUIC-сервер (слушатель).
func NewQUICListener(localAddr string, tlsConfig *tls.Config) (*QUICListener, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}
	listener, err := quic.ListenAddr(addr.String(), tlsConfig, nil)
	if err != nil {
		return nil, err
	}
	return &QUICListener{
		listener: listener,
		addr:     addr,
	}, nil
}

// Accept принимает новое QUIC-соединение.
func (l *QUICListener) Accept(ctx context.Context) (*QUICConn, error) {
	conn, err := l.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return &QUICConn{
		conn: conn,
		addr: conn.LocalAddr(),
	}, nil
}

// Close закрывает слушатель.
func (l *QUICListener) Close(ctx context.Context) error {
	return l.listener.Close()
}

// Addr возвращает адрес слушателя.
func (l *QUICListener) Addr() net.Addr {
	return l.addr
}
