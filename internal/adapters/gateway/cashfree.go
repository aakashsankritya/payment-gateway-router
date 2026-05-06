package gateway

import (
	"context"
	"fmt"
	"strings"

	"payment-gateway-router/internal/domain"
)

type CashfreeClient struct{}

func (CashfreeClient) Initiate(ctx context.Context, transaction domain.Transaction) (domain.GatewayInitiation, error) {
	return domain.GatewayInitiation{
		ReferenceID: fmt.Sprintf("cf_%s_%s", transaction.OrderID, transaction.ID),
	}, nil
}

type CashfreeCallbackDecoder struct{}

func (CashfreeCallbackDecoder) Decode(ctx context.Context, payload []byte) (domain.GatewayCallback, error) {
	var request struct {
		Order struct {
			OrderID   string            `json:"order_id"`
			OrderTags map[string]string `json:"order_tags"`
		} `json:"order"`
		Payment struct {
			PaymentStatus  string `json:"payment_status"`
			PaymentMessage string `json:"payment_message"`
		} `json:"payment"`
	}
	if err := decode(payload, &request); err != nil {
		return domain.GatewayCallback{}, err
	}

	status, err := parseGatewayStatus(request.Payment.PaymentStatus, map[string]domain.TransactionStatus{
		"success": domain.TransactionStatusSuccess,
		"paid":    domain.TransactionStatusSuccess,
		"failed":  domain.TransactionStatusFailure,
		"failure": domain.TransactionStatusFailure,
	})
	if err != nil {
		return domain.GatewayCallback{}, err
	}

	return domain.GatewayCallback{
		TransactionID: strings.TrimSpace(request.Order.OrderTags["transaction_id"]),
		OrderID:       strings.TrimSpace(request.Order.OrderID),
		Gateway:       "cashfree",
		Status:        status,
		Reason:        strings.TrimSpace(request.Payment.PaymentMessage),
	}, nil
}
