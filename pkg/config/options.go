package config

// Option — функциональная опция для Load.
type Option func(*options)

type options struct {
	path      string
	envPrefix string
}

func defaultOptions() *options {
	return &options{
		path:      "config.yaml",
		envPrefix: "APP_",
	}
}

// WithPath задаёт путь к YAML-файлу.
func WithPath(path string) Option {
	return func(o *options) {
		o.path = path
	}
}

// WithEnvPrefix задаёт префикс для переменных окружения.
// Например, "APP_" будет искать APP_PORT, APP_DB_HOST и т.д.
func WithEnvPrefix(prefix string) Option {
	return func(o *options) {
		o.envPrefix = prefix
	}
}
