package gateway

import (
	"context"
	"testing"

	"payment-gateway-router/internal/domain"
)

func TestJuspayClientInitiate(t *testing.T) {
	initiation, err := JuspayClient{}.Initiate(context.Background(), domain.Transaction{
		ID:      "txn_123",
		OrderID: "ORD-1",
	})
	if err != nil {
		t.Fatalf("Initiate error: %v", err)
	}
	if initiation.ReferenceID != "jp_txn_123" {
		t.Fatalf("ReferenceID = %q, want jp_txn_123", initiation.ReferenceID)
	}
}

func TestJuspayCallbackDecoderSuccess(t *testing.T) {
	payload := []byte(`{
		"event_name":"ORDER_SUCCEEDED",
		"content":{
			"order":{
				"order_id":"ORD-JP-1",
				"status":"CHARGED",
				"udf1":"txn_jp_1",
				"bank_error_message":"",
				"txn_detail":{"error_message":""}
			}
		}
	}`)

	callback, err := JuspayCallbackDecoder{}.Decode(context.Background(), payload)
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	if callback.Gateway != "juspay" {
		t.Fatalf("Gateway = %q, want juspay", callback.Gateway)
	}
	if callback.TransactionID != "txn_jp_1" {
		t.Fatalf("TransactionID = %q, want txn_jp_1", callback.TransactionID)
	}
	if callback.OrderID != "ORD-JP-1" {
		t.Fatalf("OrderID = %q, want ORD-JP-1", callback.OrderID)
	}
	if callback.Status != domain.TransactionStatusSuccess {
		t.Fatalf("Status = %q, want success", callback.Status)
	}
}

func TestJuspayCallbackDecoderFailure(t *testing.T) {
	payload := []byte(`{
		"event_name":"ORDER_FAILED",
		"content":{
			"order":{
				"order_id":"ORD-JP-2",
				"status":"AUTHORIZATION_FAILED",
				"udf1":"txn_jp_2",
				"bank_error_message":"Not sufficient funds",
				"txn_detail":{"error_message":"declined"}
			}
		}
	}`)

	callback, err := JuspayCallbackDecoder{}.Decode(context.Background(), payload)
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	if callback.Status != domain.TransactionStatusFailure {
		t.Fatalf("Status = %q, want failure", callback.Status)
	}
	if callback.Reason != "Not sufficient funds" {
		t.Fatalf("Reason = %q, want Not sufficient funds", callback.Reason)
	}
}

func TestRegistryDecodesJuspayPayload(t *testing.T) {
	payload := []byte(`{
		"event_name":"ORDER_SUCCEEDED",
		"content":{
			"order":{
				"order_id":"ORD-JP-3",
				"status":"CHARGED",
				"udf1":"txn_jp_3",
				"bank_error_message":"",
				"txn_detail":{"error_message":""}
			}
		}
	}`)

	callback, err := NewRegistry().Decode(context.Background(), payload)
	if err != nil {
		t.Fatalf("Registry.Decode error: %v", err)
	}
	if callback.Gateway != "juspay" || callback.TransactionID != "txn_jp_3" {
		t.Fatalf("unexpected callback: %+v", callback)
	}
}

func TestRegistryIncludesJuspay(t *testing.T) {
	registry := NewRegistry()
	if !registry.Has("juspay") {
		t.Fatal("expected juspay to be registered")
	}
	client, ok := registry.Client("juspay")
	if !ok || client == nil {
		t.Fatal("expected juspay client")
	}
}
