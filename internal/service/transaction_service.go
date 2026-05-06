package service

import (
	"context"
	"fmt"
	"log/slog"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
)

type TransactionService struct {
	transactions  ports.TransactionRepository
	router        *RoutingService
	health        *HealthService
	orderGateways *OrderGatewayBlacklistService
	gateways      ports.GatewayClientRegistry
	idGenerator   IDGenerator
	clock         Clock
	logger        *slog.Logger
}

type InitiateTransactionInput struct {
	OrderID           string
	Amount            float64
	PaymentInstrument map[string]any
}

type CallbackInput struct {
	TransactionID string
	OrderID       string
	Gateway       string
	Status        domain.TransactionStatus
	Reason        string
}

type CallbackResult struct {
	Transaction          domain.Transaction                 `json:"transaction"`
	GatewayState         domain.GatewayRuntimeState         `json:"gateway_state"`
	GatewayStats         domain.GatewayStats                `json:"gateway_stats"`
	OrderGatewayAttempts *domain.OrderGatewayAttemptSummary `json:"order_gateway_attempts,omitempty"`
	Idempotent           bool                               `json:"idempotent"`
}

func NewTransactionService(
	transactions ports.TransactionRepository,
	router *RoutingService,
	health *HealthService,
	orderGateways *OrderGatewayBlacklistService,
	gateways ports.GatewayClientRegistry,
	idGenerator IDGenerator,
	clock Clock,
	logger *slog.Logger,
) *TransactionService {
	return &TransactionService{
		transactions:  transactions,
		router:        router,
		health:        health,
		orderGateways: orderGateways,
		gateways:      gateways,
		idGenerator:   idGenerator,
		clock:         clock,
		logger:        logger,
	}
}

func (s *TransactionService) Initiate(ctx context.Context, input InitiateTransactionInput) (domain.Transaction, error) {
	transactionID := s.idGenerator.NewID("txn")
	excludedGateways, err := s.blacklistedGateways(ctx, input.OrderID)
	if err != nil {
		return domain.Transaction{}, err
	}
	selection, err := s.router.SelectGateway(ctx, GatewaySelectionRequest{
		TransactionID:    transactionID,
		OrderID:          input.OrderID,
		ExcludedGateways: excludedGateways,
	})
	if err != nil {
		return domain.Transaction{}, err
	}
	gateway := selection.Gateway
	releaseProbe := func(cause error) {
		if !selection.HalfOpenProbe {
			return
		}
		if releaseErr := s.health.ReleaseProbe(ctx, gateway.Name, transactionID); releaseErr != nil {
			s.logger.Error("failed to release half-open probe", "transaction_id", transactionID, "gateway", gateway.Name, "cause", cause, "error", releaseErr)
		}
	}

	client, ok := s.gateways.Client(gateway.Name)
	if !ok {
		err := fmt.Errorf("gateway client %q is not registered", gateway.Name)
		releaseProbe(err)
		return domain.Transaction{}, err
	}

	attemptNo, err := s.transactions.NextAttempt(ctx, input.OrderID)
	if err != nil {
		releaseProbe(err)
		return domain.Transaction{}, err
	}

	now := s.clock.Now()
	transaction := domain.Transaction{
		ID:                transactionID,
		OrderID:           input.OrderID,
		AttemptNo:         attemptNo,
		Amount:            input.Amount,
		PaymentInstrument: input.PaymentInstrument,
		Gateway:           gateway.Name,
		Status:            domain.TransactionStatusPending,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	initiation, err := client.Initiate(ctx, transaction)
	if err != nil {
		releaseProbe(err)
		return domain.Transaction{}, err
	}
	transaction.GatewayReferenceID = initiation.ReferenceID

	if err := s.transactions.Save(ctx, transaction); err != nil {
		releaseProbe(err)
		return domain.Transaction{}, err
	}

	s.logger.Info("transaction initiated", "transaction_id", transaction.ID, "order_id", transaction.OrderID, "gateway", transaction.Gateway, "attempt_no", transaction.AttemptNo)
	return transaction, nil
}

func (s *TransactionService) Callback(ctx context.Context, input CallbackInput) (CallbackResult, error) {
	transaction, err := s.transactions.GetByID(ctx, input.TransactionID)
	if err != nil {
		return CallbackResult{}, err
	}
	if transaction.Gateway != input.Gateway {
		return CallbackResult{}, fmt.Errorf("%w: transaction gateway %q callback gateway %q", domain.ErrGatewayMismatch, transaction.Gateway, input.Gateway)
	}
	if input.OrderID != "" && transaction.OrderID != input.OrderID {
		return CallbackResult{}, fmt.Errorf("%w: transaction order %q callback order %q", domain.ErrConflict, transaction.OrderID, input.OrderID)
	}

	completed, changed, err := s.transactions.Complete(ctx, input.TransactionID, input.Status, input.Reason, s.clock.Now())
	if err != nil {
		return CallbackResult{}, err
	}

	result := CallbackResult{
		Transaction: completed,
		Idempotent:  !changed,
	}
	if !changed {
		state, stateErr := s.health.State(ctx, completed.Gateway)
		if stateErr != nil {
			return CallbackResult{}, stateErr
		}
		stats, statsErr := s.health.Stats(ctx, completed.Gateway)
		if statsErr != nil {
			return CallbackResult{}, statsErr
		}
		result.GatewayState = state
		result.GatewayStats = stats
		s.logger.Info("duplicate callback ignored", "transaction_id", completed.ID, "status", completed.Status)
		return result, nil
	}

	state, stats, err := s.health.RecordOutcome(ctx, completed.Gateway, completed.ID, completed.Status)
	if err != nil {
		return CallbackResult{}, err
	}
	result.GatewayState = state
	result.GatewayStats = stats
	if s.orderGateways != nil {
		summary, err := s.orderGateways.RecordOutcome(ctx, completed)
		if err != nil {
			return CallbackResult{}, err
		}
		result.OrderGatewayAttempts = &summary
	}
	s.logger.Info("callback processed", "transaction_id", completed.ID, "gateway", completed.Gateway, "status", completed.Status, "gateway_state", state.State)
	return result, nil
}

func (s *TransactionService) blacklistedGateways(ctx context.Context, orderID string) (map[string]struct{}, error) {
	if s.orderGateways == nil {
		return map[string]struct{}{}, nil
	}
	return s.orderGateways.BlacklistedGateways(ctx, orderID)
}
