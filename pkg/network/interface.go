package network

import (
	"context"
	"net"
)

type Message struct {
	Addr net.Addr
	Data []byte
}

type Sender interface {
	Send(ctx context.Context, data []byte, addr net.Addr) error
}

type Receiver interface {
	Receive(ctx context.Context) (*Message, error)
	// RU: Добавляем метод для чтения в готовый буфер (Zero Allocations)
	// EN: Add a method for reading into a pre-allocated buffer (Zero Allocations)
	ReceiveTo(buf []byte) (int, net.Addr, error)
}

type Closer interface {
	Close() error
}

type Transport interface {
	Sender
	// RU: Встраиваем обновленный Receiver
	// EN: Embed the updated Receiver interface
	Receiver
	Closer
	Addr() net.Addr
}
