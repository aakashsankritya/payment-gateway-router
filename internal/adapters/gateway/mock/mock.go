package mock

import (
	"context"
	"fmt"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
)

type Registry struct{}

func (Registry) Client(gateway string) (ports.PaymentGatewayClient, bool) {
	return Client{gateway: gateway}, true
}

type Client struct {
	gateway string
}

func (c Client) Initiate(ctx context.Context, transaction domain.Transaction) (domain.GatewayInitiation, error) {
	return domain.GatewayInitiation{
		ReferenceID: fmt.Sprintf("mock_%s_%s", c.gateway, transaction.ID),
	}, nil
}
