package config

import "os"

// Config holds runtime configuration for keplerd.
type Config struct {
	HTTPAddr       string
	StoreBackend   string // "postgres" | "memory"
	DatabaseURL    string
	SandboxRuntime string
	LogLevel       string
}

// LoadFromEnv loads configuration from environment variables with sensible local defaults.
func LoadFromEnv() Config {
	return Config{
		HTTPAddr:       envOrDefault("KEPLER_HTTP_ADDR", "127.0.0.1:8080"),
		StoreBackend:   envOrDefault("KEPLER_STORE", "postgres"),
		DatabaseURL:    envOrDefault("KEPLER_DATABASE_URL", "postgres://kepler:kepler_dev_password@127.0.0.1:5432/kepler?sslmode=disable"),
		SandboxRuntime: envOrDefault("KEPLER_SANDBOX_RUNTIME", "runc"),
		LogLevel:       envOrDefault("KEPLER_LOG_LEVEL", "info"),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
