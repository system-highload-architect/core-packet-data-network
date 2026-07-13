package network

import (
	"context"
	"net"
)

// Message представляет полученное сообщение.
type Message struct {
	Addr net.Addr
	Data []byte
}

// Sender отправляет данные.
type Sender interface {
	Send(ctx context.Context, data []byte, addr net.Addr) error
}

// Receiver принимает данные.
type Receiver interface {
	Receive(ctx context.Context) (*Message, error)
}

// Closer закрывает соединение.
type Closer interface {
	Close() error
}

// Transport объединяет все интерфейсы.
type Transport interface {
	Sender
	Receiver
	Closer
	Addr() net.Addr
}
