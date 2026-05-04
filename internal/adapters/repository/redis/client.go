package redis

import (
	"context"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type Options struct {
	Addr      string
	Password  string
	DB        int
	KeyPrefix string
	PoolSize  int
}

type Store struct {
	client    *goredis.Client
	keyPrefix string
}

func NewStore(options Options) *Store {
	if options.Addr == "" {
		options.Addr = "localhost:6379"
	}
	if options.KeyPrefix == "" {
		options.KeyPrefix = "pgr"
	}
	if options.PoolSize <= 0 {
		options.PoolSize = 128
	}

	client := goredis.NewClient(&goredis.Options{
		Addr:         options.Addr,
		Password:     options.Password,
		DB:           options.DB,
		PoolSize:     options.PoolSize,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	return &Store{
		client:    client,
		keyPrefix: strings.Trim(options.KeyPrefix, ":"),
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Key(parts ...string) string {
	clean := make([]string, 0, len(parts)+1)
	if s.keyPrefix != "" {
		clean = append(clean, s.keyPrefix)
	}
	for _, part := range parts {
		part = strings.Trim(part, ":")
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, ":")
}
