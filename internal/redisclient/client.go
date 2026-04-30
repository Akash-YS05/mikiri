package redisclient

import (
	"context"
	"fmt"
	"time"

	"mikiri/internal/config"

	"github.com/redis/go-redis/v9"
)

func New(cfg *config.Config) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.RedisPassword,
		DB:           cfg.RedisDB,
		PoolSize:     10,			//controls maximum no of open sockets
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err!=nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	} 
	return rdb, nil
}