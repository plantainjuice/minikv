package core

type Config struct {
	MaxMemstoreSize int
	FlushMaxRetries int
	DataDir         string
}

var DefaultConfig = Config{
	MaxMemstoreSize: 16 * 1024 * 1024,
	FlushMaxRetries: 10,
	DataDir:         "minikv",
}
