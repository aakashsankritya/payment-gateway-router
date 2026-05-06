package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"payment-gateway-router/internal/domain"
)

func decode(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func parseGatewayStatus(value string, statuses map[string]domain.TransactionStatus) (domain.TransactionStatus, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	status, ok := statuses[normalized]
	if !ok {
		return "", fmt.Errorf("%w: unsupported gateway status %q", domain.ErrInvalidCallback, value)
	}
	return status, nil
}
