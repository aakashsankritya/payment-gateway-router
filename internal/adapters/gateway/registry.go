package gateway

import (
	"context"
	"fmt"
	"sort"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
)

// Registration pairs a routing name with its client and callback decoder.
// Add new gateways here (or via Registry.Register) without touching routing logic.
type Registration struct {
	Name    string
	Client  ports.PaymentGatewayClient
	Decoder ports.GatewayCallbackDecoder
}

// DefaultRegistrations is the built-in gateway catalog used by NewRegistry.
func DefaultRegistrations() []Registration {
	return []Registration{
		{Name: "razorpay", Client: RazorpayClient{}, Decoder: RazorpayCallbackDecoder{}},
		{Name: "payu", Client: PayUClient{}, Decoder: PayUCallbackDecoder{}},
		{Name: "cashfree", Client: CashfreeClient{}, Decoder: CashfreeCallbackDecoder{}},
		{Name: "juspay", Client: JuspayClient{}, Decoder: JuspayCallbackDecoder{}},
	}
}

type Registry struct {
	clients  map[string]ports.PaymentGatewayClient
	decoders map[string]ports.GatewayCallbackDecoder
}

// NewRegistry builds a registry from the given registrations.
// With no arguments it uses DefaultRegistrations().
func NewRegistry(registrations ...Registration) Registry {
	if len(registrations) == 0 {
		registrations = DefaultRegistrations()
	}

	r := Registry{
		clients:  make(map[string]ports.PaymentGatewayClient, len(registrations)),
		decoders: make(map[string]ports.GatewayCallbackDecoder, len(registrations)),
	}
	for _, registration := range registrations {
		r.mustRegister(registration)
	}
	return r
}

// Register returns a copy of the registry with an additional gateway.
// Prefer this over editing call sites when composing custom catalogs in tests.
func (r Registry) Register(registration Registration) Registry {
	clients := cloneClientMap(r.clientMap())
	decoders := cloneDecoderMap(r.decoderMap())
	next := Registry{clients: clients, decoders: decoders}
	next.mustRegister(registration)
	return next
}

func (r Registry) mustRegister(registration Registration) {
	name := registration.Name
	if name == "" {
		panic("gateway registration name is required")
	}
	if registration.Client == nil {
		panic(fmt.Sprintf("gateway %q client is required", name))
	}
	if registration.Decoder == nil {
		panic(fmt.Sprintf("gateway %q decoder is required", name))
	}
	if _, exists := r.clients[name]; exists {
		panic(fmt.Sprintf("duplicate gateway registration %q", name))
	}
	r.clients[name] = registration.Client
	r.decoders[name] = registration.Decoder
}

func (r Registry) Client(gateway string) (ports.PaymentGatewayClient, bool) {
	client, ok := r.clientMap()[gateway]
	return client, ok
}

// Names returns registered gateway names in sorted order.
func (r Registry) Names() []string {
	clients := r.clientMap()
	names := make([]string, 0, len(clients))
	for name := range clients {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Has reports whether a gateway client is registered.
func (r Registry) Has(gateway string) bool {
	_, ok := r.clientMap()[gateway]
	return ok
}

// ValidateConfig ensures every configured gateway has a registered client/decoder.
// Call this at startup so YAML typos fail fast instead of during initiate.
func (r Registry) ValidateConfig(cfg domain.AppConfig) error {
	for _, gateway := range cfg.Gateways {
		if !r.Has(gateway.Name) {
			return fmt.Errorf("configured gateway %q has no registered client; known gateways: %v", gateway.Name, r.Names())
		}
	}
	return nil
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

func cloneClientMap(src map[string]ports.PaymentGatewayClient) map[string]ports.PaymentGatewayClient {
	dst := make(map[string]ports.PaymentGatewayClient, len(src)+1)
	for name, client := range src {
		dst[name] = client
	}
	return dst
}

func cloneDecoderMap(src map[string]ports.GatewayCallbackDecoder) map[string]ports.GatewayCallbackDecoder {
	dst := make(map[string]ports.GatewayCallbackDecoder, len(src)+1)
	for name, decoder := range src {
		dst[name] = decoder
	}
	return dst
}
