package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"payment-gateway-router/internal/adapters/gateway/mock"
	"payment-gateway-router/internal/adapters/repository/memory"
	"payment-gateway-router/internal/domain"
)

type staticConfigProvider struct {
	cfg domain.AppConfig
}

func (p staticConfigProvider) Current(ctx context.Context) domain.AppConfig {
	return p.cfg
}

type mutableClock struct {
	now time.Time
}

func (c *mutableClock) Now() time.Time {
	return c.now
}

func (c *mutableClock) Advance(duration time.Duration) {
	c.now = c.now.Add(duration)
}

type sequenceIDGenerator struct {
	next int
}

func (g *sequenceIDGenerator) NewID(prefix string) string {
	g.next++
	return prefix + "_test_" + strconv.Itoa(g.next)
}

func newTestServices(cfg domain.AppConfig, clock *mutableClock) (*TransactionService, *HealthService) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider := staticConfigProvider{cfg: cfg.WithDefaults()}
	healthRepo := memory.NewGatewayHealthRepository()
	health := NewHealthService(healthRepo, provider, clock, logger)
	router := NewRoutingService(provider, health, clock, logger)
	transactions := NewTransactionService(
		memory.NewTransactionRepository(),
		router,
		health,
		mock.Registry{},
		&sequenceIDGenerator{},
		clock,
		logger,
	)
	return transactions, health
}

func TestAtomicWeightedSelectionDistributesByWeight(t *testing.T) {
	router := &RoutingService{}
	candidates := []domain.GatewayConfig{
		{Name: "razorpay", Enabled: true, Weight: 5},
		{Name: "payu", Enabled: true, Weight: 3},
		{Name: "cashfree", Enabled: true, Weight: 2},
	}

	counts := map[string]int{}
	for i := 0; i < 10; i++ {
		selected, ok := router.selectWeighted(candidates)
		if !ok {
			t.Fatal("expected gateway selection")
		}
		counts[selected.Name]++
	}

	if counts["razorpay"] != 5 || counts["payu"] != 3 || counts["cashfree"] != 2 {
		t.Fatalf("unexpected distribution: %+v", counts)
	}
}

func TestInitiateCreatesMultipleAttemptsForSameOrder(t *testing.T) {
	ctx := context.Background()
	clock := &mutableClock{now: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)}
	transactions, _ := newTestServices(singleGatewayConfig(10, 1, 0.90, 60), clock)

	first, err := transactions.Initiate(ctx, InitiateTransactionInput{OrderID: "ORD123", Amount: 499})
	if err != nil {
		t.Fatalf("first initiate failed: %v", err)
	}
	second, err := transactions.Initiate(ctx, InitiateTransactionInput{OrderID: "ORD123", Amount: 499})
	if err != nil {
		t.Fatalf("second initiate failed: %v", err)
	}

	if first.AttemptNo != 1 {
		t.Fatalf("first attempt = %d, want 1", first.AttemptNo)
	}
	if second.AttemptNo != 2 {
		t.Fatalf("second attempt = %d, want 2", second.AttemptNo)
	}
	if first.ID == second.ID {
		t.Fatal("transaction IDs should be unique")
	}
}

func TestDuplicateCallbackIsIdempotentAndDoesNotDoubleCount(t *testing.T) {
	ctx := context.Background()
	clock := &mutableClock{now: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)}
	transactions, health := newTestServices(singleGatewayConfig(10, 1, 0.90, 60), clock)

	transaction, err := transactions.Initiate(ctx, InitiateTransactionInput{OrderID: "ORD123", Amount: 499})
	if err != nil {
		t.Fatalf("initiate failed: %v", err)
	}

	first, err := transactions.Callback(ctx, CallbackInput{
		TransactionID: transaction.ID,
		OrderID:       transaction.OrderID,
		Gateway:       transaction.Gateway,
		Status:        domain.TransactionStatusSuccess,
	})
	if err != nil {
		t.Fatalf("callback failed: %v", err)
	}
	if first.Idempotent {
		t.Fatal("first callback should not be idempotent")
	}

	duplicate, err := transactions.Callback(ctx, CallbackInput{
		TransactionID: transaction.ID,
		OrderID:       transaction.OrderID,
		Gateway:       transaction.Gateway,
		Status:        domain.TransactionStatusSuccess,
	})
	if err != nil {
		t.Fatalf("duplicate callback failed: %v", err)
	}
	if !duplicate.Idempotent {
		t.Fatal("duplicate callback should be idempotent")
	}

	stats, err := health.Stats(ctx, transaction.Gateway)
	if err != nil {
		t.Fatalf("stats failed: %v", err)
	}
	if stats.Total != 1 {
		t.Fatalf("stats total = %d, want 1", stats.Total)
	}
}

func TestGatewayMarkedUnhealthyAfterThreshold(t *testing.T) {
	ctx := context.Background()
	clock := &mutableClock{now: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)}
	transactions, health := newTestServices(singleGatewayConfig(2, 1, 0.90, 60), clock)

	for i := 0; i < 2; i++ {
		transaction, err := transactions.Initiate(ctx, InitiateTransactionInput{OrderID: "ORD123", Amount: 499})
		if err != nil {
			t.Fatalf("initiate failed: %v", err)
		}
		_, err = transactions.Callback(ctx, CallbackInput{
			TransactionID: transaction.ID,
			OrderID:       transaction.OrderID,
			Gateway:       transaction.Gateway,
			Status:        domain.TransactionStatusFailure,
			Reason:        "declined",
		})
		if err != nil {
			t.Fatalf("callback failed: %v", err)
		}
	}

	state, err := health.State(ctx, "razorpay")
	if err != nil {
		t.Fatalf("state failed: %v", err)
	}
	if state.State != domain.GatewayStateUnhealthy {
		t.Fatalf("state = %s, want unhealthy", state.State)
	}
}

func TestHalfOpenProbeFlow(t *testing.T) {
	ctx := context.Background()
	clock := &mutableClock{now: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)}
	transactions, health := newTestServices(singleGatewayConfig(1, 1, 0.90, 60), clock)

	failed, err := transactions.Initiate(ctx, InitiateTransactionInput{OrderID: "ORD123", Amount: 499})
	if err != nil {
		t.Fatalf("initiate failed: %v", err)
	}
	if _, err := transactions.Callback(ctx, CallbackInput{
		TransactionID: failed.ID,
		OrderID:       failed.OrderID,
		Gateway:       failed.Gateway,
		Status:        domain.TransactionStatusFailure,
	}); err != nil {
		t.Fatalf("failure callback failed: %v", err)
	}

	clock.Advance(61 * time.Second)
	probe, err := transactions.Initiate(ctx, InitiateTransactionInput{OrderID: "ORD124", Amount: 500})
	if err != nil {
		t.Fatalf("probe initiate failed: %v", err)
	}

	state, err := health.State(ctx, "razorpay")
	if err != nil {
		t.Fatalf("state failed: %v", err)
	}
	if state.State != domain.GatewayStateHalfOpen || state.HalfOpenInFlight != 1 {
		t.Fatalf("state = %+v, want half_open with 1 in-flight probe", state)
	}

	_, err = transactions.Initiate(ctx, InitiateTransactionInput{OrderID: "ORD125", Amount: 501})
	if !errors.Is(err, domain.ErrNoAvailableGateway) {
		t.Fatalf("second probe error = %v, want no available gateway", err)
	}

	if _, err := transactions.Callback(ctx, CallbackInput{
		TransactionID: probe.ID,
		OrderID:       probe.OrderID,
		Gateway:       probe.Gateway,
		Status:        domain.TransactionStatusSuccess,
	}); err != nil {
		t.Fatalf("probe callback failed: %v", err)
	}

	state, err = health.State(ctx, "razorpay")
	if err != nil {
		t.Fatalf("state failed: %v", err)
	}
	if state.State != domain.GatewayStateHealthy {
		t.Fatalf("state = %s, want healthy", state.State)
	}
}

func singleGatewayConfig(minCallbacks int, probeCount int, threshold float64, cooldownSeconds int) domain.AppConfig {
	return domain.AppConfig{
		Routing: domain.RoutingConfig{
			HealthWindowSeconds:      900,
			UnhealthyCooldownSeconds: cooldownSeconds,
			MinCallbackCount:         minCallbacks,
			SuccessRateThreshold:     threshold,
			HalfOpenProbeCount:       probeCount,
		},
		Gateways: []domain.GatewayConfig{
			{Name: "razorpay", Enabled: true, Weight: 100},
		},
	}
}
