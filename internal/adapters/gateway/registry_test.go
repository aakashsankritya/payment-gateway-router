package gateway

import (
	"testing"

	"payment-gateway-router/internal/domain"
)

func TestNewRegistryDefaultCatalog(t *testing.T) {
	registry := NewRegistry()
	want := []string{"cashfree", "juspay", "payu", "razorpay"}
	got := registry.Names()
	if len(got) != len(want) {
		t.Fatalf("Names() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Names() = %v, want %v", got, want)
		}
	}
}

func TestRegistryRegisterIsImmutable(t *testing.T) {
	base := NewRegistry()
	extended := base.Register(Registration{
		Name:    "custom",
		Client:  RazorpayClient{},
		Decoder: RazorpayCallbackDecoder{},
	})

	if base.Has("custom") {
		t.Fatal("base registry should not include custom after Register")
	}
	if !extended.Has("custom") {
		t.Fatal("extended registry should include custom")
	}
}

func TestRegistryValidateConfig(t *testing.T) {
	registry := NewRegistry()
	err := registry.ValidateConfig(domain.AppConfig{
		Gateways: []domain.GatewayConfig{{Name: "juspay", Enabled: true, Weight: 1}},
	})
	if err != nil {
		t.Fatalf("ValidateConfig error: %v", err)
	}

	err = registry.ValidateConfig(domain.AppConfig{
		Gateways: []domain.GatewayConfig{{Name: "unknown", Enabled: true, Weight: 1}},
	})
	if err == nil {
		t.Fatal("expected ValidateConfig error for unknown gateway")
	}
}
