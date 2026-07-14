package network

import (
	"context"
	"net"
)

type UDPConn struct {
	conn *net.UDPConn
}

func NewUDPConn(localAddr string) (*UDPConn, error) {
	addr, err := net.ResolveUDPAddr("udp", localAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	return &UDPConn{conn: conn}, nil
}

func (u *UDPConn) Send(ctx context.Context, data []byte, addr net.Addr) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addr.String())
	if err != nil {
		return err
	}
	_, err = u.conn.WriteToUDP(data, udpAddr)
	return err
}

func (u *UDPConn) Receive(ctx context.Context) (*Message, error) {
	buf := make([]byte, 65535)
	n, addr, err := u.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	return &Message{Addr: addr, Data: buf[:n]}, nil
}

// Close реализует интерфейс shutdown.Closer (контекст игнорируется).
func (u *UDPConn) Close(ctx context.Context) error {
	return u.conn.Close()
}

func (u *UDPConn) Addr() net.Addr {
	return u.conn.LocalAddr()
}
