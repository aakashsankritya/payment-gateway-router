package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"payment-gateway-router/internal/domain"
)

type GatewayHealthRepository struct {
	mu     sync.RWMutex
	states map[string]domain.GatewayRuntimeState
	events []domain.GatewayEvent
}

func NewGatewayHealthRepository() *GatewayHealthRepository {
	return &GatewayHealthRepository{
		states: make(map[string]domain.GatewayRuntimeState),
		events: make([]domain.GatewayEvent, 0),
	}
}

func (r *GatewayHealthRepository) Ensure(ctx context.Context, gateway string, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.states[gateway]; exists {
		return nil
	}
	r.states[gateway] = domain.GatewayRuntimeState{
		Gateway:   gateway,
		State:     domain.GatewayStateHealthy,
		UpdatedAt: now,
	}
	return nil
}

func (r *GatewayHealthRepository) Get(ctx context.Context, gateway string) (domain.GatewayRuntimeState, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	state, exists := r.states[gateway]
	if !exists {
		return domain.GatewayRuntimeState{}, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
	}
	return state, nil
}

func (r *GatewayHealthRepository) Save(ctx context.Context, state domain.GatewayRuntimeState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.states[state.Gateway] = state
	return nil
}

func (r *GatewayHealthRepository) PruneExpiredHalfOpenProbes(ctx context.Context, gateway string, now time.Time) (domain.GatewayRuntimeState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.states[gateway]
	if !exists {
		return domain.GatewayRuntimeState{}, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
	}
	if state.State != domain.GatewayStateHalfOpen {
		return state, nil
	}
	state = pruneExpiredHalfOpenProbes(state, now)
	state.UpdatedAt = now
	r.states[gateway] = state
	return state, nil
}

func (r *GatewayHealthRepository) TryAcquireHalfOpenProbe(ctx context.Context, gateway string, transactionID string, maxInFlight int, expiresAt time.Time, now time.Time) (domain.GatewayRuntimeState, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.states[gateway]
	if !exists {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
	}
	if state.State != domain.GatewayStateHalfOpen {
		return state, false, nil
	}
	state = pruneExpiredHalfOpenProbes(state, now)
	if state.HalfOpenInFlight >= maxInFlight {
		r.states[gateway] = state
		return state, false, nil
	}
	state.HalfOpenProbes = append(state.HalfOpenProbes, domain.HalfOpenProbe{
		TransactionID:      transactionID,
		ExpiresAtUnixMilli: expiresAt.UnixMilli(),
	})
	state.HalfOpenInFlight = len(state.HalfOpenProbes)
	state.UpdatedAt = now
	r.states[gateway] = state
	return state, true, nil
}

func (r *GatewayHealthRepository) ReleaseHalfOpenProbe(ctx context.Context, gateway string, transactionID string, now time.Time) (domain.GatewayRuntimeState, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	state, exists := r.states[gateway]
	if !exists {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
	}
	if state.State != domain.GatewayStateHalfOpen {
		return state, false, nil
	}
	state = pruneExpiredHalfOpenProbes(state, now)
	probes := state.HalfOpenProbes[:0]
	released := false
	for _, probe := range state.HalfOpenProbes {
		if probe.TransactionID == transactionID {
			released = true
			continue
		}
		probes = append(probes, probe)
	}
	state.HalfOpenProbes = probes
	state.HalfOpenInFlight = len(probes)
	state.UpdatedAt = now
	r.states[gateway] = state
	return state, released, nil
}

func (r *GatewayHealthRepository) RecordEvent(ctx context.Context, event domain.GatewayEvent, retention time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cutoff := event.CreatedAt.Add(-retention)
	retained := r.events[:0]
	for _, existing := range r.events {
		if !existing.CreatedAt.Before(cutoff) {
			retained = append(retained, existing)
		}
	}
	r.events = retained
	r.events = append(r.events, event)
	return nil
}

func (r *GatewayHealthRepository) StatsSince(ctx context.Context, gateway string, since time.Time, until time.Time, bucketSize time.Duration) (domain.GatewayStats, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	stats := domain.GatewayStats{Gateway: gateway}
	for _, event := range r.events {
		if event.Gateway == gateway && !event.CreatedAt.Before(since) && !event.CreatedAt.After(until) {
			switch event.Status {
			case domain.TransactionStatusSuccess:
				stats.Successes++
			case domain.TransactionStatusFailure:
				stats.Failures++
			}
		}
	}
	stats.Total = stats.Successes + stats.Failures
	if stats.Total > 0 {
		stats.SuccessRate = float64(stats.Successes) / float64(stats.Total)
	}
	return stats, nil
}

func pruneExpiredHalfOpenProbes(state domain.GatewayRuntimeState, now time.Time) domain.GatewayRuntimeState {
	if len(state.HalfOpenProbes) == 0 {
		state.HalfOpenInFlight = 0
		return state
	}
	nowMillis := now.UnixMilli()
	probes := state.HalfOpenProbes[:0]
	for _, probe := range state.HalfOpenProbes {
		if probe.TransactionID != "" && probe.ExpiresAtUnixMilli > nowMillis {
			probes = append(probes, probe)
		}
	}
	state.HalfOpenProbes = probes
	state.HalfOpenInFlight = len(probes)
	return state
}
