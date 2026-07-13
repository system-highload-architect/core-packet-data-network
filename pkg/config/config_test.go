package config

import (
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	// Создаём временный YAML-файл
	yamlContent := []byte(`
port: 8080
db:
  host: localhost
`)
	if err := os.WriteFile("test_config.yaml", yamlContent, 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}
	defer os.Remove("test_config.yaml")

	type Config struct {
		Port int `yaml:"port" env:"PORT"`
		DB   struct {
			Host string `yaml:"host" env:"DB_HOST"`
		} `yaml:"db"`
	}

	var cfg Config
	if err := Load(&cfg, WithPath("test_config.yaml")); err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}
	if cfg.DB.Host != "localhost" {
		t.Errorf("expected db.host localhost, got %s", cfg.DB.Host)
	}
}
