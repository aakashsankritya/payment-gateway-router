package gateway

import (
	"context"
	"fmt"
	"strings"

	"payment-gateway-router/internal/domain"
)

type RazorpayClient struct{}

func (RazorpayClient) Initiate(ctx context.Context, transaction domain.Transaction) (domain.GatewayInitiation, error) {
	return domain.GatewayInitiation{
		ReferenceID: fmt.Sprintf("rzp_%s", transaction.ID),
	}, nil
}

type RazorpayCallbackDecoder struct{}

func (RazorpayCallbackDecoder) Decode(ctx context.Context, payload []byte) (domain.GatewayCallback, error) {
	var request struct {
		Event   string `json:"event"`
		Payload struct {
			Payment struct {
				Entity struct {
					Status           string            `json:"status"`
					ErrorDescription string            `json:"error_description"`
					Notes            map[string]string `json:"notes"`
				} `json:"entity"`
			} `json:"payment"`
		} `json:"payload"`
	}
	if err := decode(payload, &request); err != nil {
		return domain.GatewayCallback{}, err
	}

	status, err := parseGatewayStatus(request.Payload.Payment.Entity.Status, map[string]domain.TransactionStatus{
		"captured": domain.TransactionStatusSuccess,
		"paid":     domain.TransactionStatusSuccess,
		"failed":   domain.TransactionStatusFailure,
	})
	if err != nil {
		return domain.GatewayCallback{}, err
	}

	return domain.GatewayCallback{
		TransactionID: strings.TrimSpace(request.Payload.Payment.Entity.Notes["transaction_id"]),
		OrderID:       strings.TrimSpace(request.Payload.Payment.Entity.Notes["order_id"]),
		Gateway:       "razorpay",
		Status:        status,
		Reason:        strings.TrimSpace(request.Payload.Payment.Entity.ErrorDescription),
	}, nil
}
