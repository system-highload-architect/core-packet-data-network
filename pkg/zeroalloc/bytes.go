package zeroalloc

import (
	"strconv"
)

// AppendInt добавляет целое число к срезу байт без лишних аллокаций.
func AppendInt(dst []byte, n int64) []byte {
	return strconv.AppendInt(dst, n, 10)
}

// AppendUint добавляет беззнаковое целое.
func AppendUint(dst []byte, n uint64) []byte {
	return strconv.AppendUint(dst, n, 10)
}
