package config

import (
	"log/slog"
	"os"
	"strconv"
)

type Config struct {
	RedisAddr		string
	RedisPassword	string
	RedisDB			int

	ProducerPort	string

	DashboardPort	string
	DashboardUser	string
	DashboardPassword	string

	ConsumerWorkers	int
	ConsumerName	string

	LogLevel	string
}


func Load() *Config {
	return &Config{
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:     getEnv("REDIS_PASSWORD", ""),
		RedisDB:           getEnvInt("REDIS_DB", 0),
		ProducerPort:      getEnv("PRODUCER_PORT", "8081"),
		DashboardPort:     getEnv("DASHBOARD_PORT", "8080"),
		DashboardUser:     getEnv("DASHBOARD_USER", "admin"),
		DashboardPassword: getEnv("DASHBOARD_PASSWORD", "admin"),
		ConsumerWorkers:   getEnvInt("CONSUMER_WORKERS", 5),
		ConsumerName:      getEnv("CONSUMER_NAME", "worker-1"),
		LogLevel:          getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid int env var, using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}