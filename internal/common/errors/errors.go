package errors

import "errors"

// Общие ошибки.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalidInput  = errors.New("invalid input")
	ErrUnauthorized  = errors.New("unauthorized")
	ErrInternal      = errors.New("internal error")
	ErrTimeout       = errors.New("timeout")
	ErrBusy          = errors.New("resource busy")
	ErrUnavailable   = errors.New("resource unavailable")
	ErrPoolClosed    = errors.New("pool closed")
)
