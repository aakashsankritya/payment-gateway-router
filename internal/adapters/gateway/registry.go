package gateway

import (
	"context"
	"fmt"
	"sort"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
)

type Registry struct {
	clients  map[string]ports.PaymentGatewayClient
	decoders map[string]ports.GatewayCallbackDecoder
}

func NewRegistry() Registry {
	return Registry{
		clients: map[string]ports.PaymentGatewayClient{
			"razorpay": RazorpayClient{},
			"payu":     PayUClient{},
			"cashfree": CashfreeClient{},
		},
		decoders: map[string]ports.GatewayCallbackDecoder{
			"razorpay": RazorpayCallbackDecoder{},
			"payu":     PayUCallbackDecoder{},
			"cashfree": CashfreeCallbackDecoder{},
		},
	}
}

func (r Registry) Client(gateway string) (ports.PaymentGatewayClient, bool) {
	client, ok := r.clientMap()[gateway]
	return client, ok
}

func (r Registry) Decode(ctx context.Context, payload []byte) (domain.GatewayCallback, error) {
	decoders := r.decoderMap()
	gateways := make([]string, 0, len(decoders))
	for gateway := range decoders {
		gateways = append(gateways, gateway)
	}
	sort.Strings(gateways)

	for _, gateway := range gateways {
		callback, err := decoders[gateway].Decode(ctx, payload)
		if err == nil {
			return callback, nil
		}
	}
	return domain.GatewayCallback{}, fmt.Errorf("%w: unsupported gateway callback payload", domain.ErrInvalidCallback)
}

func (r Registry) clientMap() map[string]ports.PaymentGatewayClient {
	if r.clients == nil {
		return NewRegistry().clients
	}
	return r.clients
}

func (r Registry) decoderMap() map[string]ports.GatewayCallbackDecoder {
	if r.decoders == nil {
		return NewRegistry().decoders
	}
	return r.decoders
}
