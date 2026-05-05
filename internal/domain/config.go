package domain

import (
	"errors"
	"fmt"
)

type AppConfig struct {
	Routing  RoutingConfig   `json:"routing" yaml:"routing"`
	Gateways []GatewayConfig `json:"gateways" yaml:"gateways"`
}

type RoutingConfig struct {
	HealthWindowSeconds         int     `json:"health_window_seconds" yaml:"health_window_seconds"`
	UnhealthyCooldownSeconds    int     `json:"unhealthy_cooldown_seconds" yaml:"unhealthy_cooldown_seconds"`
	MinCallbackCount            int     `json:"min_callback_count" yaml:"min_callback_count"`
	SuccessRateThreshold        float64 `json:"success_rate_threshold" yaml:"success_rate_threshold"`
	HalfOpenProbeCount          int     `json:"half_open_probe_count" yaml:"half_open_probe_count"`
	HalfOpenProbeTimeoutSeconds int     `json:"half_open_probe_timeout_seconds" yaml:"half_open_probe_timeout_seconds"`
}

type GatewayConfig struct {
	Name    string `json:"name" yaml:"name"`
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Weight  int    `json:"weight" yaml:"weight"`
}

func DefaultRoutingConfig() RoutingConfig {
	return RoutingConfig{
		HealthWindowSeconds:         15 * 60,
		UnhealthyCooldownSeconds:    30 * 60,
		MinCallbackCount:            10,
		SuccessRateThreshold:        0.90,
		HalfOpenProbeCount:          1,
		HalfOpenProbeTimeoutSeconds: 60,
	}
}

func (c AppConfig) WithDefaults() AppConfig {
	defaults := DefaultRoutingConfig()
	if c.Routing.HealthWindowSeconds <= 0 {
		c.Routing.HealthWindowSeconds = defaults.HealthWindowSeconds
	}
	if c.Routing.UnhealthyCooldownSeconds <= 0 {
		c.Routing.UnhealthyCooldownSeconds = defaults.UnhealthyCooldownSeconds
	}
	if c.Routing.MinCallbackCount <= 0 {
		c.Routing.MinCallbackCount = defaults.MinCallbackCount
	}
	if c.Routing.SuccessRateThreshold <= 0 {
		c.Routing.SuccessRateThreshold = defaults.SuccessRateThreshold
	}
	if c.Routing.SuccessRateThreshold > 1 {
		c.Routing.SuccessRateThreshold = c.Routing.SuccessRateThreshold / 100
	}
	if c.Routing.HalfOpenProbeCount <= 0 {
		c.Routing.HalfOpenProbeCount = defaults.HalfOpenProbeCount
	}
	if c.Routing.HalfOpenProbeTimeoutSeconds <= 0 {
		c.Routing.HalfOpenProbeTimeoutSeconds = defaults.HalfOpenProbeTimeoutSeconds
	}
	return c
}

func (c AppConfig) Validate() error {
	if len(c.Gateways) == 0 {
		return errors.New("at least one gateway must be configured")
	}

	names := make(map[string]struct{}, len(c.Gateways))
	enabledCount := 0
	for _, gateway := range c.Gateways {
		if gateway.Name == "" {
			return errors.New("gateway name is required")
		}
		if _, exists := names[gateway.Name]; exists {
			return fmt.Errorf("duplicate gateway %q", gateway.Name)
		}
		names[gateway.Name] = struct{}{}
		if gateway.Enabled {
			enabledCount++
			if gateway.Weight <= 0 {
				return fmt.Errorf("enabled gateway %q must have positive weight", gateway.Name)
			}
		}
	}

	if enabledCount == 0 {
		return errors.New("at least one gateway must be enabled")
	}
	if c.Routing.SuccessRateThreshold <= 0 || c.Routing.SuccessRateThreshold > 1 {
		return errors.New("success_rate_threshold must be in the range (0, 1]")
	}
	return nil
}
