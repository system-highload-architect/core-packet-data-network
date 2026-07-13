package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// overrideFromEnv рекурсивно обходит структуру v и для каждого поля с тегом `env`
// пытается найти переменную окружения по имени, собранному из префикса и пути.
// parentPath используется для вложенных структур (например, "DB").
func overrideFromEnv(v reflect.Value, prefix, parentPath string) error {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldVal := v.Field(i)

		// Если поле — структура (не указатель), рекурсивно заходим в неё
		if field.Type.Kind() == reflect.Struct {
			subPath := joinPath(parentPath, field.Tag.Get("yaml"))
			if err := overrideFromEnv(fieldVal, prefix, subPath); err != nil {
				return err
			}
			continue
		}

		// Обрабатываем поле, только если есть тег `env`
		envTag := field.Tag.Get("env")
		if envTag == "" {
			continue
		}

		// Строим полное имя переменной: PREFIX + PARENT_PATH + ENV_TAG
		envName := strings.ToUpper(joinPath(parentPath, envTag))
		if prefix != "" {
			envName = prefix + envName
		}

		valStr, ok := os.LookupEnv(envName)
		if !ok {
			continue
		}

		// Устанавливаем значение поля из строки
		if err := setField(fieldVal, valStr); err != nil {
			return fmt.Errorf("config: env %s: %w", envName, err)
		}
	}
	return nil
}

// joinPath соединяет части пути через подчёркивание.
func joinPath(parts ...string) string {
	var nonEmpty []string
	for _, p := range parts {
		if p != "" {
			nonEmpty = append(nonEmpty, p)
		}
	}
	return strings.Join(nonEmpty, "_")
}

// setField парсит строку в значение поля (поддерживаются int, uint, float, bool, string).
func setField(field reflect.Value, val string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(val)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(val, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(val, 10, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(val, field.Type().Bits())
		if err != nil {
			return err
		}
		field.SetFloat(f)
	case reflect.Bool:
		b, err := strconv.ParseBool(val)
		if err != nil {
			return err
		}
		field.SetBool(b)
	default:
		return fmt.Errorf("unsupported type %s", field.Kind())
	}
	return nil
}

// readYAML декодирует YAML-файл в cfg.
func readYAML(path string, cfg interface{}) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, cfg)
}
