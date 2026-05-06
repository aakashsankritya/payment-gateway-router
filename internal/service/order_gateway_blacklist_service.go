package service

import (
	"context"
	"log/slog"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
)

type OrderGatewayBlacklistService struct {
	repository     ports.OrderGatewayBlacklistRepository
	configProvider ports.GatewayConfigProvider
	clock          Clock
	logger         *slog.Logger
}

func NewOrderGatewayBlacklistService(repository ports.OrderGatewayBlacklistRepository, configProvider ports.GatewayConfigProvider, clock Clock, logger *slog.Logger) *OrderGatewayBlacklistService {
	return &OrderGatewayBlacklistService{
		repository:     repository,
		configProvider: configProvider,
		clock:          clock,
		logger:         logger,
	}
}

func (s *OrderGatewayBlacklistService) BlacklistedGateways(ctx context.Context, orderID string) (map[string]struct{}, error) {
	if orderID == "" {
		return map[string]struct{}{}, nil
	}
	return s.repository.BlacklistedGateways(ctx, orderID)
}

func (s *OrderGatewayBlacklistService) RecordOutcome(ctx context.Context, transaction domain.Transaction) (domain.OrderGatewayAttemptSummary, error) {
	cfg := s.configProvider.Current(ctx).WithDefaults()
	summary, newlyBlacklisted, err := s.repository.RecordOutcome(
		ctx,
		transaction.OrderID,
		transaction.Gateway,
		transaction.Status,
		cfg.Routing.OrderGatewayFailureThreshold,
		s.clock.Now(),
	)
	if err != nil {
		return domain.OrderGatewayAttemptSummary{}, err
	}
	if newlyBlacklisted {
		s.logger.Warn(
			"gateway blacklisted for order",
			"order_id", transaction.OrderID,
			"gateway", transaction.Gateway,
			"failures", summary.Failures,
			"threshold", cfg.Routing.OrderGatewayFailureThreshold,
		)
	}
	return summary, nil
}
