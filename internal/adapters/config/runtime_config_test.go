package config

import (
	"strings"
	"testing"
	"time"
)

var runtimeEnvKeys = []string{
	"ADDR",
	"CONFIG_PATH",
	"CONFIG_RELOAD_INTERVAL_SECONDS",
	"STATE_BACKEND",
	"GATEWAY_STATE_CACHE_TTL_MS",
	"ROUTING_CACHE_TTL_MS",
	"REDIS_ADDR",
	"REDIS_PASSWORD",
	"REDIS_DB",
	"REDIS_KEY_PREFIX",
	"REDIS_POOL_SIZE",
}

func clearRuntimeEnv(t *testing.T) {
	t.Helper()
	for _, key := range runtimeEnvKeys {
		t.Setenv(key, "")
	}
}

func TestLoadRuntimeConfigUsesDefaults(t *testing.T) {
	clearRuntimeEnv(t)

	cfg, err := LoadRuntimeConfig()
	if err != nil {
		t.Fatalf("expected defaults to be valid: %v", err)
	}

	if cfg.Addr != ":8080" {
		t.Fatalf("expected default addr, got %q", cfg.Addr)
	}
	if cfg.StateBackend != "memory" {
		t.Fatalf("expected memory backend, got %q", cfg.StateBackend)
	}
	if cfg.ConfigReloadInterval() != 2*time.Second {
		t.Fatalf("expected 2s reload interval, got %s", cfg.ConfigReloadInterval())
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Fatalf("expected default redis addr, got %q", cfg.Redis.Addr)
	}
}

func TestLoadRuntimeConfigReadsEnvironment(t *testing.T) {
	clearRuntimeEnv(t)
	t.Setenv("ADDR", ":9090")
	t.Setenv("CONFIG_PATH", "/tmp/gateways.yaml")
	t.Setenv("CONFIG_RELOAD_INTERVAL_SECONDS", "5")
	t.Setenv("STATE_BACKEND", "REDIS")
	t.Setenv("GATEWAY_STATE_CACHE_TTL_MS", "250")
	t.Setenv("ROUTING_CACHE_TTL_MS", "50")
	t.Setenv("REDIS_ADDR", "redis:6379")
	t.Setenv("REDIS_PASSWORD", "secret")
	t.Setenv("REDIS_DB", "2")
	t.Setenv("REDIS_KEY_PREFIX", "router")
	t.Setenv("REDIS_POOL_SIZE", "64")

	cfg, err := LoadRuntimeConfig()
	if err != nil {
		t.Fatalf("expected env config to be valid: %v", err)
	}

	if cfg.Addr != ":9090" {
		t.Fatalf("expected env addr, got %q", cfg.Addr)
	}
	if cfg.ConfigPath != "/tmp/gateways.yaml" {
		t.Fatalf("expected env config path, got %q", cfg.ConfigPath)
	}
	if cfg.StateBackend != "redis" {
		t.Fatalf("expected normalized redis backend, got %q", cfg.StateBackend)
	}
	if cfg.GatewayStateCacheTTL() != 250*time.Millisecond {
		t.Fatalf("expected 250ms gateway cache ttl, got %s", cfg.GatewayStateCacheTTL())
	}
	if cfg.RoutingCacheTTL() != 50*time.Millisecond {
		t.Fatalf("expected 50ms routing cache ttl, got %s", cfg.RoutingCacheTTL())
	}
	if cfg.Redis.Addr != "redis:6379" || cfg.Redis.Password != "secret" || cfg.Redis.DB != 2 || cfg.Redis.KeyPrefix != "router" || cfg.Redis.PoolSize != 64 {
		t.Fatalf("unexpected redis config: %+v", cfg.Redis)
	}
}

func TestLoadRuntimeConfigRejectsInvalidBackend(t *testing.T) {
	clearRuntimeEnv(t)
	t.Setenv("STATE_BACKEND", "postgres")

	_, err := LoadRuntimeConfig()
	if err == nil || !strings.Contains(err.Error(), "state_backend") {
		t.Fatalf("expected state_backend validation error, got %v", err)
	}
}
