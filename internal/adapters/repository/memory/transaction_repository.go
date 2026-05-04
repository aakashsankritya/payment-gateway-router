package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"payment-gateway-router/internal/domain"
)

type TransactionRepository struct {
	mu           sync.RWMutex
	transactions map[string]domain.Transaction
	attempts     map[string]int
}

func NewTransactionRepository() *TransactionRepository {
	return &TransactionRepository{
		transactions: make(map[string]domain.Transaction),
		attempts:     make(map[string]int),
	}
}

func (r *TransactionRepository) NextAttempt(ctx context.Context, orderID string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.attempts[orderID]++
	return r.attempts[orderID], nil
}

func (r *TransactionRepository) Save(ctx context.Context, transaction domain.Transaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.transactions[transaction.ID]; exists {
		return fmt.Errorf("%w: transaction %q already exists", domain.ErrConflict, transaction.ID)
	}
	r.transactions[transaction.ID] = transaction
	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, transactionID string) (domain.Transaction, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	transaction, exists := r.transactions[transactionID]
	if !exists {
		return domain.Transaction{}, fmt.Errorf("%w: transaction %q", domain.ErrNotFound, transactionID)
	}
	return transaction, nil
}

func (r *TransactionRepository) Complete(ctx context.Context, transactionID string, status domain.TransactionStatus, reason string, completedAt time.Time) (domain.Transaction, bool, error) {
	if !status.IsFinal() {
		return domain.Transaction{}, false, domain.ErrInvalidStatus
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	transaction, exists := r.transactions[transactionID]
	if !exists {
		return domain.Transaction{}, false, fmt.Errorf("%w: transaction %q", domain.ErrNotFound, transactionID)
	}

	if transaction.Status.IsFinal() {
		if transaction.Status == status {
			return transaction, false, nil
		}
		return domain.Transaction{}, false, fmt.Errorf("%w: transaction %q already completed as %q", domain.ErrConflict, transactionID, transaction.Status)
	}

	transaction.Status = status
	transaction.FailureReason = reason
	transaction.UpdatedAt = completedAt
	transaction.CompletedAt = &completedAt
	r.transactions[transactionID] = transaction
	return transaction, true, nil
}
