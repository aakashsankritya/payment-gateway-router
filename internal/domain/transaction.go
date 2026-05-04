package domain

import (
	"fmt"
	"strings"
	"time"
)

type TransactionStatus string

const (
	TransactionStatusPending TransactionStatus = "pending"
	TransactionStatusSuccess TransactionStatus = "success"
	TransactionStatusFailure TransactionStatus = "failure"
)

type Transaction struct {
	ID                 string            `json:"transaction_id"`
	OrderID            string            `json:"order_id"`
	AttemptNo          int               `json:"attempt_no"`
	Amount             float64           `json:"amount"`
	PaymentInstrument  map[string]any    `json:"payment_instrument,omitempty"`
	Gateway            string            `json:"gateway"`
	GatewayReferenceID string            `json:"gateway_reference_id,omitempty"`
	Status             TransactionStatus `json:"status"`
	FailureReason      string            `json:"failure_reason,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
	UpdatedAt          time.Time         `json:"updated_at"`
	CompletedAt        *time.Time        `json:"completed_at,omitempty"`
}

func ParseTransactionStatus(value string) (TransactionStatus, error) {
	status := TransactionStatus(strings.ToLower(strings.TrimSpace(value)))
	switch status {
	case TransactionStatusPending, TransactionStatusSuccess, TransactionStatusFailure:
		return status, nil
	default:
		return "", fmt.Errorf("%w: %s", ErrInvalidStatus, value)
	}
}

func (s TransactionStatus) IsFinal() bool {
	return s == TransactionStatusSuccess || s == TransactionStatusFailure
}
