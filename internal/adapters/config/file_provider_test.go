package config

import "testing"

func TestParseSimpleYAML(t *testing.T) {
	content := []byte(`
routing:
  health_window_seconds: 60
  unhealthy_cooldown_seconds: 120
  min_callback_count: 3
  success_rate_threshold: 0.75
  half_open_probe_count: 2

gateways:
  - name: razorpay
    enabled: true
    weight: 50
  - name: payu
    enabled: false
    weight: 30
`)

	cfg, err := Parse(content, ".yaml")
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config should be valid: %v", err)
	}

	if cfg.Routing.HealthWindowSeconds != 60 {
		t.Fatalf("health window = %d, want 60", cfg.Routing.HealthWindowSeconds)
	}
	if cfg.Routing.SuccessRateThreshold != 0.75 {
		t.Fatalf("threshold = %f, want 0.75", cfg.Routing.SuccessRateThreshold)
	}
	if len(cfg.Gateways) != 2 {
		t.Fatalf("gateways = %d, want 2", len(cfg.Gateways))
	}
	if cfg.Gateways[0].Name != "razorpay" || cfg.Gateways[0].Weight != 50 {
		t.Fatalf("unexpected first gateway: %+v", cfg.Gateways[0])
	}
}
