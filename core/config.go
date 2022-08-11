package core

type Config struct {
	MaxMemstoreSize   int
	FlushMaxRetries   int
	DataDir           string
	MaxDiskFiles      int
	MaxThreadPoolSize int

	SkipListMaxHeight int
}

var DefaultConfig = Config{
	MaxMemstoreSize:   16 * 1024 * 1024,
	FlushMaxRetries:   10,
	DataDir:           "minikv",
	MaxDiskFiles:      10,
	MaxThreadPoolSize: 5,
	SkipListMaxHeight: 12,
}
