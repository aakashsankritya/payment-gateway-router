package ports

import (
	"context"
	"time"

	"payment-gateway-router/internal/domain"
)

type TransactionRepository interface {
	NextAttempt(ctx context.Context, orderID string) (int, error)
	Save(ctx context.Context, transaction domain.Transaction) error
	GetByID(ctx context.Context, transactionID string) (domain.Transaction, error)
	Complete(ctx context.Context, transactionID string, status domain.TransactionStatus, reason string, completedAt time.Time) (domain.Transaction, bool, error)
}

type GatewayHealthRepository interface {
	Ensure(ctx context.Context, gateway string, now time.Time) error
	Get(ctx context.Context, gateway string) (domain.GatewayRuntimeState, error)
	Save(ctx context.Context, state domain.GatewayRuntimeState) error
	PruneExpiredHalfOpenProbes(ctx context.Context, gateway string, now time.Time) (domain.GatewayRuntimeState, error)
	TryAcquireHalfOpenProbe(ctx context.Context, gateway string, transactionID string, maxInFlight int, expiresAt time.Time, now time.Time) (domain.GatewayRuntimeState, bool, error)
	ReleaseHalfOpenProbe(ctx context.Context, gateway string, transactionID string, now time.Time) (domain.GatewayRuntimeState, bool, error)
	RecordEvent(ctx context.Context, event domain.GatewayEvent, retention time.Duration) error
	StatsSince(ctx context.Context, gateway string, since time.Time, until time.Time, bucketSize time.Duration) (domain.GatewayStats, error)
}

type GatewayConfigProvider interface {
	Current(ctx context.Context) domain.AppConfig
}

type PaymentGatewayClient interface {
	Initiate(ctx context.Context, transaction domain.Transaction) (domain.GatewayInitiation, error)
}

type GatewayClientRegistry interface {
	Client(gateway string) (PaymentGatewayClient, bool)
}
