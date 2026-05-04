package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type RuntimeConfig struct {
	Addr                             string      `mapstructure:"addr"`
	ConfigPath                       string      `mapstructure:"config_path"`
	ConfigReloadIntervalSeconds      int         `mapstructure:"config_reload_interval_seconds"`
	StateBackend                     string      `mapstructure:"state_backend"`
	GatewayStateCacheTTLMilliseconds int         `mapstructure:"gateway_state_cache_ttl_ms"`
	RoutingCacheTTLMilliseconds      int         `mapstructure:"routing_cache_ttl_ms"`
	Redis                            RedisConfig `mapstructure:"redis"`
}

type RedisConfig struct {
	Addr      string `mapstructure:"addr"`
	Password  string `mapstructure:"password"`
	DB        int    `mapstructure:"db"`
	KeyPrefix string `mapstructure:"key_prefix"`
	PoolSize  int    `mapstructure:"pool_size"`
}

func LoadRuntimeConfig() (RuntimeConfig, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	v.SetDefault("addr", ":8080")
	v.SetDefault("config_path", "./configs/gateways.yaml")
	v.SetDefault("config_reload_interval_seconds", 2)
	v.SetDefault("state_backend", "memory")
	v.SetDefault("gateway_state_cache_ttl_ms", 500)
	v.SetDefault("routing_cache_ttl_ms", 100)
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.key_prefix", "pgr")
	v.SetDefault("redis.pool_size", 128)

	var cfg RuntimeConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return RuntimeConfig{}, fmt.Errorf("load runtime config: %w", err)
	}
	cfg.StateBackend = strings.ToLower(strings.TrimSpace(cfg.StateBackend))
	if err := cfg.Validate(); err != nil {
		return RuntimeConfig{}, err
	}
	return cfg, nil
}

func (c RuntimeConfig) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("addr is required")
	}
	if c.ConfigPath == "" {
		return fmt.Errorf("config_path is required")
	}
	if c.ConfigReloadIntervalSeconds <= 0 {
		return fmt.Errorf("config_reload_interval_seconds must be positive")
	}
	if c.StateBackend != "memory" && c.StateBackend != "redis" {
		return fmt.Errorf("state_backend must be memory or redis")
	}
	if c.GatewayStateCacheTTLMilliseconds <= 0 {
		return fmt.Errorf("gateway_state_cache_ttl_ms must be positive")
	}
	if c.RoutingCacheTTLMilliseconds <= 0 {
		return fmt.Errorf("routing_cache_ttl_ms must be positive")
	}
	if c.Redis.DB < 0 {
		return fmt.Errorf("redis_db must be zero or positive")
	}
	if c.Redis.PoolSize <= 0 {
		return fmt.Errorf("redis_pool_size must be positive")
	}
	return nil
}

func (c RuntimeConfig) ConfigReloadInterval() time.Duration {
	return time.Duration(c.ConfigReloadIntervalSeconds) * time.Second
}

func (c RuntimeConfig) GatewayStateCacheTTL() time.Duration {
	return time.Duration(c.GatewayStateCacheTTLMilliseconds) * time.Millisecond
}

func (c RuntimeConfig) RoutingCacheTTL() time.Duration {
	return time.Duration(c.RoutingCacheTTLMilliseconds) * time.Millisecond
}
