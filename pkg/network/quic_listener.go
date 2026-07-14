package network

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

type QUICListener struct {
	listener *quic.Listener
	addr     net.Addr
}

func NewQUICListener(localAddr string, tlsConfig *tls.Config) (*QUICListener, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}
	quicConf := &quic.Config{
		EnableDatagrams: true,
		KeepAlivePeriod: 5 * time.Second,
		MaxIdleTimeout:  30 * time.Second,
	}
	listener, err := quic.ListenAddr(addr.String(), tlsConfig, quicConf)
	if err != nil {
		return nil, err
	}
	return &QUICListener{
		listener: listener,
		addr:     addr,
	}, nil
}

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

func (l *QUICListener) Close(ctx context.Context) error {
	return l.listener.Close()
}

func (l *QUICListener) Addr() net.Addr {
	return l.addr
}
