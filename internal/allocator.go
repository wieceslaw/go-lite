package internal

type (
	ChunkId   uint64
	ChunkSize uint64

	ChunkHandle interface {
		Read()
		Write()
		Sync()
		Close()
	}
)

type ChunkAllocator interface {
	GetChunk(id ChunkId) (ChunkHandle, error)
	AllocChunk(size ChunkSize) (ChunkHandle, error)
	FreeChunk(id ChunkId) error
}
