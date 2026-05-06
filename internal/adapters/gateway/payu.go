package gateway

import (
	"context"
	"fmt"
	"strings"

	"payment-gateway-router/internal/domain"
)

type PayUClient struct{}

func (PayUClient) Initiate(ctx context.Context, transaction domain.Transaction) (domain.GatewayInitiation, error) {
	return domain.GatewayInitiation{
		ReferenceID: fmt.Sprintf("payu_%s_%d", transaction.OrderID, transaction.AttemptNo),
	}, nil
}

type PayUCallbackDecoder struct{}

func (PayUCallbackDecoder) Decode(ctx context.Context, payload []byte) (domain.GatewayCallback, error) {
	var request struct {
		TransactionID string `json:"txnid"`
		OrderID       string `json:"udf1"`
		Status        string `json:"status"`
		ErrorMessage  string `json:"error_Message"`
	}
	if err := decode(payload, &request); err != nil {
		return domain.GatewayCallback{}, err
	}

	status, err := parseGatewayStatus(request.Status, map[string]domain.TransactionStatus{
		"success": domain.TransactionStatusSuccess,
		"failed":  domain.TransactionStatusFailure,
		"failure": domain.TransactionStatusFailure,
	})
	if err != nil {
		return domain.GatewayCallback{}, err
	}

	return domain.GatewayCallback{
		TransactionID: strings.TrimSpace(request.TransactionID),
		OrderID:       strings.TrimSpace(request.OrderID),
		Gateway:       "payu",
		Status:        status,
		Reason:        strings.TrimSpace(request.ErrorMessage),
	}, nil
}
