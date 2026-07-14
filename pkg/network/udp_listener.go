package network

import (
	"context"
	"net"
)

type UDPListener struct {
	conn *net.UDPConn
	addr net.Addr
}

func NewUDPListener(localAddr string) (*UDPListener, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &UDPListener{conn: conn, addr: addr}, nil
}

func (l *UDPListener) Accept() (*UDPConn, error) {
	return &UDPConn{conn: l.conn}, nil
}

func (l *UDPListener) Close(ctx context.Context) error {
	return l.conn.Close()
}

func (l *UDPListener) Addr() net.Addr {
	return l.addr
}
