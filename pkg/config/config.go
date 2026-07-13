package config

import (
	"fmt"
	"os"
	"reflect"
)

// Load читает YAML-файл и применяет переменные окружения.
// cfg должен быть ненулевым указателем на структуру, поля которой размечены тегами `yaml` и опционально `env`.
// Переменные окружения имеют приоритет над значениями из файла.
//
// Пример структуры:
//
//	type Config struct {
//	    Port int    `yaml:"port" env:"PORT"`
//	    DB   struct {
//	        Host string `yaml:"host" env:"DB_HOST"`
//	    } `yaml:"db"`
//	}
//
// По умолчанию файл ищется в пути "config.yaml". Изменить можно через WithPath.
// Префикс для env-переменных задаётся через WithEnvPrefix, по умолчанию "APP_".
func Load(cfg interface{}, opts ...Option) error {
	v := reflect.ValueOf(cfg)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("config: cfg must be a non-nil pointer to a struct, got %T", cfg)
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("config: cfg must be a pointer to a struct, got pointer to %v", v.Kind())
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	// Читаем файл (если он есть; если отсутствует, это не фатально, если только не задан явный путь)
	if err := readYAML(o.path, cfg); err != nil {
		// Если путь не стандартный и файл не найден — ошибка
		if o.path != "config.yaml" || !os.IsNotExist(err) {
			return fmt.Errorf("config: loading YAML %q: %w", o.path, err)
		}
		// Иначе просто не загружаем
	}

	// Применяем переменные окружения
	return overrideFromEnv(v, o.envPrefix, "")
}
