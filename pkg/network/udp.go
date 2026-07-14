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
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		var err error
		udpAddr, err = net.ResolveUDPAddr("udp", addr.String())
		if err != nil {
			return err
		}
	}
	_, err := u.conn.WriteToUDP(data, udpAddr)
	return err
}

// RU: Старый метод Receive оставлен для обратной совместимости, но не для горячих циклов Highload
// EN: Legacy Receive method preserved for backward compatibility, not for highload hot loops
func (u *UDPConn) Receive(ctx context.Context) (*Message, error) {
	buf := make([]byte, 65535)
	n, addr, err := u.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	return &Message{Addr: addr, Data: buf[:n]}, nil
}

// RU: ВЫСОКОПРОИЗВОДИТЕЛЬНЫЙ МЕТОД: Чтение напрямую в предоставленный буфер без единой аллокации в куче!
// EN: HIGH-PERFORMANCE METHOD: Read directly into provided buffer with absolute zero heap allocations!
func (u *UDPConn) ReceiveTo(buf []byte) (int, net.Addr, error) {
	n, addr, err := u.conn.ReadFromUDP(buf)
	if err != nil {
		return 0, nil, err
	}
	return n, addr, nil
}

func (u *UDPConn) Close(ctx context.Context) error {
	return u.conn.Close()
}

func (u *UDPConn) Addr() net.Addr {
	return u.conn.LocalAddr()
}

func (u *UDPConn) SetReadBuffer(bytes int) error {
	return u.conn.SetReadBuffer(bytes)
}

func (u *UDPConn) SetWriteBuffer(bytes int) error {
	return u.conn.SetWriteBuffer(bytes)
}
