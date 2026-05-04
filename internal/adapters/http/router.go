package httpadapter

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/ports"
	"payment-gateway-router/internal/service"
)

type Router struct {
	transactions   *service.TransactionService
	health         *service.HealthService
	configProvider ports.GatewayConfigProvider
	logger         *slog.Logger
	mux            *http.ServeMux
}

func NewRouter(transactions *service.TransactionService, health *service.HealthService, configProvider ports.GatewayConfigProvider, logger *slog.Logger) http.Handler {
	router := &Router{
		transactions:   transactions,
		health:         health,
		configProvider: configProvider,
		logger:         logger,
		mux:            http.NewServeMux(),
	}
	router.routes()
	return router
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

func (r *Router) routes() {
	r.mux.HandleFunc("GET /health", r.handleHealth)
	r.mux.HandleFunc("GET /gateways", r.handleGateways)
	r.mux.HandleFunc("POST /transactions/initiate", r.handleInitiate)
	r.mux.HandleFunc("POST /transactions/callback", r.handleCallback)
}

type initiateRequest struct {
	OrderID           string         `json:"order_id"`
	Amount            float64        `json:"amount"`
	PaymentInstrument map[string]any `json:"payment_instrument"`
}

type callbackRequest struct {
	TransactionID string `json:"transaction_id"`
	OrderID       string `json:"order_id"`
	Status        string `json:"status"`
	Gateway       string `json:"gateway"`
	Reason        string `json:"reason,omitempty"`
}

func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (r *Router) handleGateways(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	cfg := r.configProvider.Current(ctx).WithDefaults()
	type gatewayStatus struct {
		domain.GatewayConfig
		Runtime domain.GatewayRuntimeState `json:"runtime"`
		Stats   domain.GatewayStats        `json:"stats"`
	}

	response := struct {
		Routing  domain.RoutingConfig `json:"routing"`
		Gateways []gatewayStatus      `json:"gateways"`
	}{
		Routing:  cfg.Routing,
		Gateways: make([]gatewayStatus, 0, len(cfg.Gateways)),
	}

	for _, gateway := range cfg.Gateways {
		state, err := r.health.State(ctx, gateway.Name)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "gateway_state_error", err.Error())
			return
		}
		stats, err := r.health.Stats(ctx, gateway.Name)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "gateway_stats_error", err.Error())
			return
		}
		response.Gateways = append(response.Gateways, gatewayStatus{
			GatewayConfig: gateway,
			Runtime:       state,
			Stats:         stats,
		})
	}

	writeJSON(w, http.StatusOK, response)
}

func (r *Router) handleInitiate(w http.ResponseWriter, req *http.Request) {
	var payload initiateRequest
	if err := decodeJSON(req, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	payload.OrderID = strings.TrimSpace(payload.OrderID)
	if payload.OrderID == "" {
		writeError(w, http.StatusBadRequest, "invalid_order_id", "order_id is required")
		return
	}
	if payload.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_amount", "amount must be greater than zero")
		return
	}
	if payload.PaymentInstrument == nil {
		payload.PaymentInstrument = map[string]any{}
	}

	transaction, err := r.transactions.Initiate(req.Context(), service.InitiateTransactionInput{
		OrderID:           payload.OrderID,
		Amount:            payload.Amount,
		PaymentInstrument: payload.PaymentInstrument,
	})
	if err != nil {
		r.writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, transaction)
}

func (r *Router) handleCallback(w http.ResponseWriter, req *http.Request) {
	var payload callbackRequest
	if err := decodeJSON(req, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	status, err := domain.ParseTransactionStatus(payload.Status)
	if err != nil || !status.IsFinal() {
		writeError(w, http.StatusBadRequest, "invalid_status", "status must be success or failure")
		return
	}

	payload.TransactionID = strings.TrimSpace(payload.TransactionID)
	payload.OrderID = strings.TrimSpace(payload.OrderID)
	payload.Gateway = strings.TrimSpace(payload.Gateway)
	if payload.TransactionID == "" {
		writeError(w, http.StatusBadRequest, "invalid_transaction_id", "transaction_id is required")
		return
	}
	if payload.Gateway == "" {
		writeError(w, http.StatusBadRequest, "invalid_gateway", "gateway is required")
		return
	}

	result, err := r.transactions.Callback(req.Context(), service.CallbackInput{
		TransactionID: payload.TransactionID,
		OrderID:       payload.OrderID,
		Gateway:       payload.Gateway,
		Status:        status,
		Reason:        payload.Reason,
	})
	if err != nil {
		r.writeServiceError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (r *Router) writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNoAvailableGateway):
		writeError(w, http.StatusServiceUnavailable, "no_available_gateway", err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, domain.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	case errors.Is(err, domain.ErrGatewayMismatch):
		writeError(w, http.StatusConflict, "gateway_mismatch", err.Error())
	case errors.Is(err, domain.ErrInvalidStatus):
		writeError(w, http.StatusBadRequest, "invalid_status", err.Error())
	default:
		r.logger.Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func decodeJSON(req *http.Request, target any) error {
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	writeJSON(w, status, map[string]string{
		"error":   code,
		"message": message,
	})
}
