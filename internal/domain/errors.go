package domain

import "errors"

var (
	ErrConflict           = errors.New("conflict")
	ErrGatewayMismatch    = errors.New("gateway mismatch")
	ErrInvalidStatus      = errors.New("invalid transaction status")
	ErrNoAvailableGateway = errors.New("no available gateway")
	ErrNotFound           = errors.New("not found")
)
