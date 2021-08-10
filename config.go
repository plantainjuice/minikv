package minikv

type Config struct {
	MaxMemstoreSize   int
	FlushMaxRetries   int
	DataDir           string
	MaxDiskFiles      int
	MaxThreadPoolSize int

	LevelDBMaxHeight int
}

var DefaultConfig = Config{
	MaxMemstoreSize:   16 * 1024 * 1024,
	FlushMaxRetries:   10,
	DataDir:           "minikv",
	MaxDiskFiles:      10,
	MaxThreadPoolSize: 5,
	LevelDBMaxHeight: 12,
}
