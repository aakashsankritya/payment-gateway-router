package memory

import (
	"context"
	"sync"
	"time"

	"payment-gateway-router/internal/domain"
)

type OrderGatewayBlacklistRepository struct {
	mu        sync.RWMutex
	summaries map[string]map[string]domain.OrderGatewayAttemptSummary
}

func NewOrderGatewayBlacklistRepository() *OrderGatewayBlacklistRepository {
	return &OrderGatewayBlacklistRepository{
		summaries: make(map[string]map[string]domain.OrderGatewayAttemptSummary),
	}
}

func (r *OrderGatewayBlacklistRepository) BlacklistedGateways(ctx context.Context, orderID string) (map[string]struct{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string]struct{})
	for gateway, summary := range r.summaries[orderID] {
		if summary.Blacklisted {
			result[gateway] = struct{}{}
		}
	}
	return result, nil
}

func (r *OrderGatewayBlacklistRepository) RecordOutcome(ctx context.Context, orderID string, gateway string, status domain.TransactionStatus, failureThreshold int, now time.Time) (domain.OrderGatewayAttemptSummary, bool, error) {
	if !status.IsFinal() {
		return domain.OrderGatewayAttemptSummary{}, false, domain.ErrInvalidStatus
	}
	if failureThreshold <= 0 {
		failureThreshold = 1
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	byGateway := r.summaries[orderID]
	if byGateway == nil {
		byGateway = make(map[string]domain.OrderGatewayAttemptSummary)
		r.summaries[orderID] = byGateway
	}

	summary := byGateway[gateway]
	summary.OrderID = orderID
	summary.Gateway = gateway
	switch status {
	case domain.TransactionStatusSuccess:
		summary.Successes++
		summary.Blacklisted = false
	case domain.TransactionStatusFailure:
		summary.Failures++
		if summary.Successes == 0 && summary.Failures >= failureThreshold {
			summary.Blacklisted = true
		}
	}
	newlyBlacklisted := !byGateway[gateway].Blacklisted && summary.Blacklisted
	summary.UpdatedAt = now
	byGateway[gateway] = summary
	return summary, newlyBlacklisted, nil
}
