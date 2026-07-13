package zeroalloc

import "strconv"

// AppendInt appends the string representation of an int to a byte slice.
// This avoids allocations compared to fmt.Append or strconv.AppendInt
// that allocate a new string.
func AppendInt(b []byte, i int) []byte {
	return strconv.AppendInt(b, int64(i), 10)
}

// AppendUint appends the string representation of a uint64 to a byte slice.
func AppendUint(b []byte, i uint64) []byte {
	return strconv.AppendUint(b, i, 10)
}
