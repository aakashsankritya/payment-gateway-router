package domain

import "time"

type GatewayState string

const (
	GatewayStateHealthy   GatewayState = "healthy"
	GatewayStateUnhealthy GatewayState = "unhealthy"
	GatewayStateHalfOpen  GatewayState = "half_open"
)

type GatewayRuntimeState struct {
	Gateway          string          `json:"gateway"`
	State            GatewayState    `json:"state"`
	UnhealthyUntil   time.Time       `json:"unhealthy_until,omitempty"`
	HalfOpenInFlight int             `json:"half_open_in_flight"`
	HalfOpenProbes   []HalfOpenProbe `json:"half_open_probes,omitempty"`
	UpdatedAt        time.Time       `json:"updated_at"`
}

type HalfOpenProbe struct {
	TransactionID      string `json:"transaction_id"`
	ExpiresAtUnixMilli int64  `json:"expires_at_unix_milli"`
}

type GatewayEvent struct {
	Gateway       string            `json:"gateway"`
	TransactionID string            `json:"transaction_id"`
	Status        TransactionStatus `json:"status"`
	CreatedAt     time.Time         `json:"created_at"`
}

type GatewayStats struct {
	Gateway     string  `json:"gateway"`
	Successes   int     `json:"successes"`
	Failures    int     `json:"failures"`
	Total       int     `json:"total"`
	SuccessRate float64 `json:"success_rate"`
}

type GatewayInitiation struct {
	ReferenceID string
}
