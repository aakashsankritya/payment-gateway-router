package service

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
)

type RoutingService struct {
	configProvider ports.GatewayConfigProvider
	healthService  *HealthService
	counter        atomic.Uint64
	clock          Clock
	logger         *slog.Logger
	cacheTTL       time.Duration
	cacheMu        sync.RWMutex
	cache          routingSnapshot
}

type routingSnapshot struct {
	healthy   []domain.GatewayConfig
	probes    []domain.GatewayConfig
	expiresAt time.Time
}

func NewRoutingService(configProvider ports.GatewayConfigProvider, healthService *HealthService, clock Clock, logger *slog.Logger) *RoutingService {
	return &RoutingService{
		configProvider: configProvider,
		healthService:  healthService,
		clock:          clock,
		logger:         logger,
		cacheTTL:       100 * time.Millisecond,
	}
}

func (s *RoutingService) SetCacheTTL(ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.cacheTTL = ttl
	s.cache = routingSnapshot{}
}

func (s *RoutingService) SelectGateway(ctx context.Context) (domain.GatewayConfig, error) {
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return domain.GatewayConfig{}, err
	}

	if len(snapshot.probes) > 0 {
		selected, ok := s.selectWeighted(snapshot.probes)
		if !ok {
			return domain.GatewayConfig{}, domain.ErrNoAvailableGateway
		}
		if err := s.healthService.MarkProbeSelected(ctx, selected.Name); err != nil {
			s.invalidateCache()
			if len(snapshot.healthy) == 0 {
				return domain.GatewayConfig{}, err
			}
		} else {
			s.logger.Info("selected half-open probe gateway", "gateway", selected.Name)
			return selected, nil
		}
	}

	selected, ok := s.selectWeighted(snapshot.healthy)
	if !ok {
		s.logger.Warn("no healthy gateway available")
		return domain.GatewayConfig{}, domain.ErrNoAvailableGateway
	}

	s.logger.Info("selected healthy gateway", "gateway", selected.Name)
	return selected, nil
}

func (s *RoutingService) snapshot(ctx context.Context) (routingSnapshot, error) {
	now := s.clock.Now()

	s.cacheMu.RLock()
	cached := s.cache
	s.cacheMu.RUnlock()
	if now.Before(cached.expiresAt) {
		return cached, nil
	}

	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if now.Before(s.cache.expiresAt) {
		return s.cache, nil
	}

	cfg := s.configProvider.Current(ctx).WithDefaults()
	next := routingSnapshot{
		healthy:   make([]domain.GatewayConfig, 0, len(cfg.Gateways)),
		probes:    make([]domain.GatewayConfig, 0),
		expiresAt: now.Add(s.cacheTTL),
	}

	for _, gateway := range cfg.Gateways {
		if !gateway.Enabled || gateway.Weight <= 0 {
			continue
		}

		state, err := s.healthService.PrepareForRouting(ctx, gateway.Name, cfg.Routing)
		if err != nil {
			return routingSnapshot{}, err
		}

		switch state.State {
		case domain.GatewayStateHealthy:
			next.healthy = append(next.healthy, gateway)
		case domain.GatewayStateHalfOpen:
			if state.HalfOpenInFlight < cfg.Routing.HalfOpenProbeCount {
				next.probes = append(next.probes, gateway)
			}
		}
	}

	s.cache = next
	return next, nil
}

func (s *RoutingService) invalidateCache() {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.cache = routingSnapshot{}
}

func (s *RoutingService) selectWeighted(candidates []domain.GatewayConfig) (domain.GatewayConfig, bool) {
	if len(candidates) == 0 {
		return domain.GatewayConfig{}, false
	}
	total := 0
	for _, candidate := range candidates {
		if candidate.Weight > 0 {
			total += candidate.Weight
		}
	}
	if total <= 0 {
		return domain.GatewayConfig{}, false
	}

	slot := int(s.counter.Add(1) % uint64(total))
	running := 0
	for _, candidate := range candidates {
		if candidate.Weight <= 0 {
			continue
		}
		running += candidate.Weight
		if slot < running {
			return candidate, true
		}
	}
	return candidates[len(candidates)-1], true
}
