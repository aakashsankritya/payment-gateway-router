package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
)

type HealthService struct {
	repository     ports.GatewayHealthRepository
	configProvider ports.GatewayConfigProvider
	clock          Clock
	logger         *slog.Logger
	cacheTTL       time.Duration
	cacheMu        sync.RWMutex
	stateCache     map[string]cachedGatewayState
}

type cachedGatewayState struct {
	state     domain.GatewayRuntimeState
	expiresAt time.Time
}

func NewHealthService(repository ports.GatewayHealthRepository, configProvider ports.GatewayConfigProvider, clock Clock, logger *slog.Logger) *HealthService {
	return &HealthService{
		repository:     repository,
		configProvider: configProvider,
		clock:          clock,
		logger:         logger,
		cacheTTL:       500 * time.Millisecond,
		stateCache:     make(map[string]cachedGatewayState),
	}
}

func (s *HealthService) SetCacheTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.cacheTTL = ttl
	s.stateCache = make(map[string]cachedGatewayState)
}

func (s *HealthService) PrepareForRouting(ctx context.Context, gateway string, routing domain.RoutingConfig) (domain.GatewayRuntimeState, error) {
	now := s.clock.Now()
	state, err := s.cachedState(ctx, gateway, now)
	if err != nil {
		return domain.GatewayRuntimeState{}, err
	}

	if state.State == domain.GatewayStateUnhealthy && !state.UnhealthyUntil.IsZero() && !now.Before(state.UnhealthyUntil) {
		state.State = domain.GatewayStateHalfOpen
		state.HalfOpenInFlight = 0
		state.UpdatedAt = now
		if err := s.repository.Save(ctx, state); err != nil {
			return domain.GatewayRuntimeState{}, err
		}
		s.setCachedState(state, now)
		s.logger.Info("gateway moved to half-open", "gateway", gateway)
	}

	return state, nil
}

func (s *HealthService) MarkProbeSelected(ctx context.Context, gateway string) error {
	cfg := s.configProvider.Current(ctx).WithDefaults()
	state, acquired, err := s.repository.TryAcquireHalfOpenProbe(ctx, gateway, cfg.Routing.HalfOpenProbeCount, s.clock.Now())
	if err != nil {
		return err
	}
	s.setCachedState(state, s.clock.Now())
	if !acquired {
		s.logger.Warn("half-open probe was not acquired", "gateway", gateway, "state", state.State, "in_flight", state.HalfOpenInFlight)
		return domain.ErrNoAvailableGateway
	}
	s.logger.Info("half-open probe selected", "gateway", gateway, "in_flight", state.HalfOpenInFlight)
	return nil
}

func (s *HealthService) RecordOutcome(ctx context.Context, gateway string, transactionID string, status domain.TransactionStatus) (domain.GatewayRuntimeState, domain.GatewayStats, error) {
	now := s.clock.Now()
	cfg := s.configProvider.Current(ctx).WithDefaults()

	if err := s.repository.Ensure(ctx, gateway, now); err != nil {
		return domain.GatewayRuntimeState{}, domain.GatewayStats{}, err
	}
	if status.IsFinal() {
		retention := time.Duration(cfg.Routing.HealthWindowSeconds*2) * time.Second
		if err := s.repository.RecordEvent(ctx, domain.GatewayEvent{
			Gateway:       gateway,
			TransactionID: transactionID,
			Status:        status,
			CreatedAt:     now,
		}, retention); err != nil {
			return domain.GatewayRuntimeState{}, domain.GatewayStats{}, err
		}
	}

	state, err := s.repository.Get(ctx, gateway)
	if err != nil {
		return domain.GatewayRuntimeState{}, domain.GatewayStats{}, err
	}

	windowStart := now.Add(-time.Duration(cfg.Routing.HealthWindowSeconds) * time.Second)
	stats, err := s.repository.StatsSince(ctx, gateway, windowStart, now, time.Minute)
	if err != nil {
		return domain.GatewayRuntimeState{}, domain.GatewayStats{}, err
	}

	switch state.State {
	case domain.GatewayStateHalfOpen:
		if state.HalfOpenInFlight > 0 {
			state.HalfOpenInFlight--
		}
		if status == domain.TransactionStatusSuccess {
			state.State = domain.GatewayStateHealthy
			state.UnhealthyUntil = time.Time{}
			state.UpdatedAt = now
			s.logger.Info("gateway recovered from half-open probe", "gateway", gateway)
		} else if status == domain.TransactionStatusFailure {
			state = s.markUnhealthy(state, cfg.Routing, now)
			s.logger.Warn("gateway failed half-open probe", "gateway", gateway)
		}
	case domain.GatewayStateUnhealthy:
		if state.UnhealthyUntil.IsZero() || !now.Before(state.UnhealthyUntil) {
			state.State = domain.GatewayStateHalfOpen
			state.HalfOpenInFlight = 0
			state.UpdatedAt = now
		}
	default:
		if stats.Total >= cfg.Routing.MinCallbackCount && stats.SuccessRate < cfg.Routing.SuccessRateThreshold {
			state = s.markUnhealthy(state, cfg.Routing, now)
			s.logger.Warn("gateway marked unhealthy", "gateway", gateway, "success_rate", stats.SuccessRate, "total", stats.Total)
		} else {
			state.State = domain.GatewayStateHealthy
			state.UnhealthyUntil = time.Time{}
			state.UpdatedAt = now
		}
	}

	if err := s.repository.Save(ctx, state); err != nil {
		return domain.GatewayRuntimeState{}, domain.GatewayStats{}, err
	}
	s.setCachedState(state, now)
	return state, stats, nil
}

func (s *HealthService) State(ctx context.Context, gateway string) (domain.GatewayRuntimeState, error) {
	now := s.clock.Now()
	if err := s.repository.Ensure(ctx, gateway, now); err != nil {
		return domain.GatewayRuntimeState{}, err
	}
	state, err := s.repository.Get(ctx, gateway)
	if err != nil {
		return domain.GatewayRuntimeState{}, err
	}
	s.setCachedState(state, now)
	return state, nil
}

func (s *HealthService) Stats(ctx context.Context, gateway string) (domain.GatewayStats, error) {
	cfg := s.configProvider.Current(ctx).WithDefaults()
	now := s.clock.Now()
	windowStart := now.Add(-time.Duration(cfg.Routing.HealthWindowSeconds) * time.Second)
	return s.repository.StatsSince(ctx, gateway, windowStart, now, time.Minute)
}

func (s *HealthService) markUnhealthy(state domain.GatewayRuntimeState, routing domain.RoutingConfig, now time.Time) domain.GatewayRuntimeState {
	state.State = domain.GatewayStateUnhealthy
	state.UnhealthyUntil = now.Add(time.Duration(routing.UnhealthyCooldownSeconds) * time.Second)
	state.HalfOpenInFlight = 0
	state.UpdatedAt = now
	return state
}

func (s *HealthService) cachedState(ctx context.Context, gateway string, now time.Time) (domain.GatewayRuntimeState, error) {
	s.cacheMu.RLock()
	cached, exists := s.stateCache[gateway]
	s.cacheMu.RUnlock()
	if exists && now.Before(cached.expiresAt) {
		return cached.state, nil
	}

	if err := s.repository.Ensure(ctx, gateway, now); err != nil {
		return domain.GatewayRuntimeState{}, err
	}
	state, err := s.repository.Get(ctx, gateway)
	if err != nil {
		return domain.GatewayRuntimeState{}, err
	}
	s.setCachedState(state, now)
	return state, nil
}

func (s *HealthService) setCachedState(state domain.GatewayRuntimeState, now time.Time) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.stateCache[state.Gateway] = cachedGatewayState{
		state:     state,
		expiresAt: now.Add(s.cacheTTL),
	}
}
