package fec

// Encoder определяет интерфейс для FEC-кодирования.
type Encoder interface {
	// Encode принимает слайс шардов данных и добавляет избыточные шарды.
	Encode(shards [][]byte) ([][]byte, error)

	// Recover восстанавливает недостающие шарды (nil или пустые).
	Recover(shards [][]byte) ([][]byte, error)

	// DataShards возвращает количество шардов данных.
	DataShards() int

	// TotalShards возвращает общее количество шардов (данные + избыточные).
	TotalShards() int
}
