package domain

import "errors"

var (
	ErrConflict           = errors.New("conflict")
	ErrGatewayMismatch    = errors.New("gateway mismatch")
	ErrInvalidCallback    = errors.New("invalid callback")
	ErrInvalidStatus      = errors.New("invalid transaction status")
	ErrNoAvailableGateway = errors.New("no available gateway")
	ErrNotFound           = errors.New("not found")
	ErrUnsupportedGateway = errors.New("unsupported gateway")
)
