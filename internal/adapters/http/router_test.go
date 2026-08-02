package httpadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gatewayadapter "payment-gateway-router/internal/adapters/gateway"
	"payment-gateway-router/internal/adapters/repository/memory"
	"payment-gateway-router/internal/domain"
	"payment-gateway-router/internal/service"
)

type testConfigProvider struct {
	cfg domain.AppConfig
}

func (p testConfigProvider) Current(ctx context.Context) domain.AppConfig {
	return p.cfg
}

type testClock struct {
	now time.Time
}

func (c testClock) Now() time.Time {
	return c.now
}

type testIDGenerator struct{}

func (testIDGenerator) NewID(prefix string) string {
	return prefix + "_http_test"
}

func TestInitiateAndCallbackHTTPFlow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := domain.AppConfig{
		Routing: domain.RoutingConfig{
			HealthWindowSeconds:      900,
			UnhealthyCooldownSeconds: 1800,
			MinCallbackCount:         10,
			SuccessRateThreshold:     0.90,
			HalfOpenProbeCount:       1,
		},
		Gateways: []domain.GatewayConfig{
			{Name: "razorpay", Enabled: true, Weight: 100},
		},
	}.WithDefaults()
	provider := testConfigProvider{cfg: cfg}
	clock := testClock{now: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)}
	health := service.NewHealthService(memory.NewGatewayHealthRepository(), provider, clock, logger)
	orderGateways := service.NewOrderGatewayBlacklistService(memory.NewOrderGatewayBlacklistRepository(), provider, clock, logger)
	routerService := service.NewRoutingService(provider, health, clock, logger)
	gatewayRegistry := gatewayadapter.NewRegistry()
	transactions := service.NewTransactionService(
		memory.NewTransactionRepository(),
		routerService,
		health,
		orderGateways,
		gatewayRegistry,
		testIDGenerator{},
		clock,
		logger,
	)
	handler := NewRouter(transactions, health, provider, gatewayRegistry, logger)

	initiateBody := []byte(`{"order_id":"ORD123","amount":499,"payment_instrument":{"type":"card"}}`)
	initiateReq := httptest.NewRequest(http.MethodPost, "/transactions/initiate", bytes.NewReader(initiateBody))
	initiateResp := httptest.NewRecorder()
	handler.ServeHTTP(initiateResp, initiateReq)
	if initiateResp.Code != http.StatusCreated {
		t.Fatalf("initiate status = %d, body = %s", initiateResp.Code, initiateResp.Body.String())
	}

	var transaction domain.Transaction
	if err := json.NewDecoder(initiateResp.Body).Decode(&transaction); err != nil {
		t.Fatalf("decode initiate response: %v", err)
	}
	if transaction.ID == "" || transaction.Gateway != "razorpay" || transaction.Status != domain.TransactionStatusPending {
		t.Fatalf("unexpected transaction: %+v", transaction)
	}

	callbackBody := []byte(`{"transaction_id":"txn_http_test","order_id":"ORD123","gateway":"razorpay","status":"success"}`)
	callbackReq := httptest.NewRequest(http.MethodPost, "/transactions/callback", bytes.NewReader(callbackBody))
	callbackResp := httptest.NewRecorder()
	handler.ServeHTTP(callbackResp, callbackReq)
	if callbackResp.Code != http.StatusOK {
		t.Fatalf("callback status = %d, body = %s", callbackResp.Code, callbackResp.Body.String())
	}

	var result service.CallbackResult
	if err := json.NewDecoder(callbackResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode callback response: %v", err)
	}
	if result.Transaction.Status != domain.TransactionStatusSuccess {
		t.Fatalf("callback status = %s, want success", result.Transaction.Status)
	}
	if result.GatewayStats.Total != 1 {
		t.Fatalf("stats total = %d, want 1", result.GatewayStats.Total)
	}
}

func TestGatewaySpecificCallbackHTTPFlow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := domain.AppConfig{
		Routing: domain.RoutingConfig{
			HealthWindowSeconds:          900,
			UnhealthyCooldownSeconds:     1800,
			MinCallbackCount:             10,
			SuccessRateThreshold:         0.90,
			HalfOpenProbeCount:           1,
			OrderGatewayFailureThreshold: 2,
		},
		Gateways: []domain.GatewayConfig{
			{Name: "razorpay", Enabled: true, Weight: 100},
		},
	}.WithDefaults()
	provider := testConfigProvider{cfg: cfg}
	clock := testClock{now: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)}
	health := service.NewHealthService(memory.NewGatewayHealthRepository(), provider, clock, logger)
	orderGateways := service.NewOrderGatewayBlacklistService(memory.NewOrderGatewayBlacklistRepository(), provider, clock, logger)
	routerService := service.NewRoutingService(provider, health, clock, logger)
	gatewayRegistry := gatewayadapter.NewRegistry()
	transactions := service.NewTransactionService(
		memory.NewTransactionRepository(),
		routerService,
		health,
		orderGateways,
		gatewayRegistry,
		testIDGenerator{},
		clock,
		logger,
	)
	handler := NewRouter(transactions, health, provider, gatewayRegistry, logger)

	initiateBody := []byte(`{"order_id":"ORD-RZP","amount":499,"payment_instrument":{"type":"card"}}`)
	initiateReq := httptest.NewRequest(http.MethodPost, "/transactions/initiate", bytes.NewReader(initiateBody))
	initiateResp := httptest.NewRecorder()
	handler.ServeHTTP(initiateResp, initiateReq)
	if initiateResp.Code != http.StatusCreated {
		t.Fatalf("initiate status = %d, body = %s", initiateResp.Code, initiateResp.Body.String())
	}

	callbackBody := []byte(`{
		"event":"payment.captured",
		"payload":{
			"payment":{
				"entity":{
					"status":"captured",
					"error_description":"",
					"notes":{
						"transaction_id":"txn_http_test",
						"order_id":"ORD-RZP"
					}
				}
			}
		}
	}`)
	callbackReq := httptest.NewRequest(http.MethodPost, "/transactions/callback", bytes.NewReader(callbackBody))
	callbackResp := httptest.NewRecorder()
	handler.ServeHTTP(callbackResp, callbackReq)
	if callbackResp.Code != http.StatusOK {
		t.Fatalf("gateway callback status = %d, body = %s", callbackResp.Code, callbackResp.Body.String())
	}

	var result service.CallbackResult
	if err := json.NewDecoder(callbackResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode gateway callback response: %v", err)
	}
	if result.Transaction.Status != domain.TransactionStatusSuccess {
		t.Fatalf("callback status = %s, want success", result.Transaction.Status)
	}
	if result.Transaction.GatewayReferenceID != "rzp_txn_http_test" {
		t.Fatalf("gateway reference = %s, want razorpay-specific reference", result.Transaction.GatewayReferenceID)
	}
}

func TestJuspayGatewaySpecificCallbackHTTPFlow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := domain.AppConfig{
		Routing: domain.RoutingConfig{
			HealthWindowSeconds:          900,
			UnhealthyCooldownSeconds:     1800,
			MinCallbackCount:             10,
			SuccessRateThreshold:         0.90,
			HalfOpenProbeCount:           1,
			OrderGatewayFailureThreshold: 2,
		},
		Gateways: []domain.GatewayConfig{
			{Name: "juspay", Enabled: true, Weight: 100},
		},
	}.WithDefaults()
	provider := testConfigProvider{cfg: cfg}
	clock := testClock{now: time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)}
	health := service.NewHealthService(memory.NewGatewayHealthRepository(), provider, clock, logger)
	orderGateways := service.NewOrderGatewayBlacklistService(memory.NewOrderGatewayBlacklistRepository(), provider, clock, logger)
	routerService := service.NewRoutingService(provider, health, clock, logger)
	gatewayRegistry := gatewayadapter.NewRegistry()
	transactions := service.NewTransactionService(
		memory.NewTransactionRepository(),
		routerService,
		health,
		orderGateways,
		gatewayRegistry,
		testIDGenerator{},
		clock,
		logger,
	)
	handler := NewRouter(transactions, health, provider, gatewayRegistry, logger)

	initiateBody := []byte(`{"order_id":"ORD-JP","amount":499,"payment_instrument":{"type":"card"}}`)
	initiateReq := httptest.NewRequest(http.MethodPost, "/transactions/initiate", bytes.NewReader(initiateBody))
	initiateResp := httptest.NewRecorder()
	handler.ServeHTTP(initiateResp, initiateReq)
	if initiateResp.Code != http.StatusCreated {
		t.Fatalf("initiate status = %d, body = %s", initiateResp.Code, initiateResp.Body.String())
	}

	callbackBody := []byte(`{
		"event_name":"ORDER_SUCCEEDED",
		"content":{
			"order":{
				"order_id":"ORD-JP",
				"status":"CHARGED",
				"udf1":"txn_http_test",
				"bank_error_message":"",
				"txn_detail":{"error_message":""}
			}
		}
	}`)
	callbackReq := httptest.NewRequest(http.MethodPost, "/transactions/callback", bytes.NewReader(callbackBody))
	callbackResp := httptest.NewRecorder()
	handler.ServeHTTP(callbackResp, callbackReq)
	if callbackResp.Code != http.StatusOK {
		t.Fatalf("juspay callback status = %d, body = %s", callbackResp.Code, callbackResp.Body.String())
	}

	var result service.CallbackResult
	if err := json.NewDecoder(callbackResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode juspay callback response: %v", err)
	}
	if result.Transaction.Status != domain.TransactionStatusSuccess {
		t.Fatalf("callback status = %s, want success", result.Transaction.Status)
	}
	if result.Transaction.GatewayReferenceID != "jp_txn_http_test" {
		t.Fatalf("gateway reference = %s, want juspay-specific reference", result.Transaction.GatewayReferenceID)
	}
	if result.Transaction.Gateway != "juspay" {
		t.Fatalf("gateway = %s, want juspay", result.Transaction.Gateway)
	}
}
