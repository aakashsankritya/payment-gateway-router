package gateway

import (
	"context"
	"fmt"
	"strings"

	"payment-gateway-router/internal/domain"
)

// JuspayClient is the mock Juspay adapter. Swap the body of Initiate for real
// Express Checkout /orders calls without changing routing or callback contracts.
type JuspayClient struct{}

func (JuspayClient) Initiate(ctx context.Context, transaction domain.Transaction) (domain.GatewayInitiation, error) {
	return domain.GatewayInitiation{
		ReferenceID: fmt.Sprintf("jp_%s", transaction.ID),
	}, nil
}

// JuspayCallbackDecoder maps Juspay ORDER_* webhook payloads into the router
// callback model. Merchants are expected to stash our transaction_id in udf1
// when creating the Juspay order (same pattern as PayU udf1).
type JuspayCallbackDecoder struct{}

func (JuspayCallbackDecoder) Decode(ctx context.Context, payload []byte) (domain.GatewayCallback, error) {
	var request struct {
		EventName string `json:"event_name"`
		Content   struct {
			Order struct {
				OrderID          string `json:"order_id"`
				Status           string `json:"status"`
				UDF1             string `json:"udf1"`
				BankErrorMessage string `json:"bank_error_message"`
				TxnDetail        struct {
					ErrorMessage string `json:"error_message"`
				} `json:"txn_detail"`
			} `json:"order"`
		} `json:"content"`
	}
	if err := decode(payload, &request); err != nil {
		return domain.GatewayCallback{}, err
	}
	if strings.TrimSpace(request.EventName) == "" || strings.TrimSpace(request.Content.Order.OrderID) == "" {
		return domain.GatewayCallback{}, fmt.Errorf("%w: missing juspay order callback fields", domain.ErrInvalidCallback)
	}

	status, err := parseGatewayStatus(request.Content.Order.Status, map[string]domain.TransactionStatus{
		"charged":               domain.TransactionStatusSuccess,
		"authorized":            domain.TransactionStatusSuccess,
		"cod_initiated":         domain.TransactionStatusSuccess,
		"authorization_failed":  domain.TransactionStatusFailure,
		"authentication_failed": domain.TransactionStatusFailure,
		"juspay_declined":       domain.TransactionStatusFailure,
		"not_found":             domain.TransactionStatusFailure,
	})
	if err != nil {
		return domain.GatewayCallback{}, err
	}

	reason := strings.TrimSpace(request.Content.Order.BankErrorMessage)
	if reason == "" {
		reason = strings.TrimSpace(request.Content.Order.TxnDetail.ErrorMessage)
	}

	return domain.GatewayCallback{
		TransactionID: strings.TrimSpace(request.Content.Order.UDF1),
		OrderID:       strings.TrimSpace(request.Content.Order.OrderID),
		Gateway:       "juspay",
		Status:        status,
		Reason:        reason,
	}, nil
}
