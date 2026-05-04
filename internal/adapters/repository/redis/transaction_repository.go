package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"payment-gateway-router/internal/domain"
)

type TransactionRepository struct {
	store *Store
}

func NewTransactionRepository(store *Store) *TransactionRepository {
	return &TransactionRepository{store: store}
}

func (r *TransactionRepository) NextAttempt(ctx context.Context, orderID string) (int, error) {
	attempt, err := r.store.client.Incr(ctx, r.store.Key("order", orderID, "attempt")).Result()
	return int(attempt), err
}

func (r *TransactionRepository) Save(ctx context.Context, transaction domain.Transaction) error {
	payload, err := json.Marshal(transaction)
	if err != nil {
		return err
	}

	key := r.transactionKey(transaction.ID)
	created, err := r.store.client.SetNX(ctx, key, payload, 0).Result()
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("%w: transaction %q already exists", domain.ErrConflict, transaction.ID)
	}
	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, transactionID string) (domain.Transaction, error) {
	return r.get(ctx, r.transactionKey(transactionID))
}

func (r *TransactionRepository) Complete(ctx context.Context, transactionID string, status domain.TransactionStatus, reason string, completedAt time.Time) (domain.Transaction, bool, error) {
	if !status.IsFinal() {
		return domain.Transaction{}, false, domain.ErrInvalidStatus
	}

	result, err := completeTransactionScript.Run(
		ctx,
		r.store.client,
		[]string{r.transactionKey(transactionID)},
		string(status),
		reason,
		completedAt.UTC().Format(time.RFC3339Nano),
	).Slice()
	if err != nil {
		return domain.Transaction{}, false, err
	}
	if len(result) < 1 {
		return domain.Transaction{}, false, fmt.Errorf("unexpected redis complete response: %v", result)
	}

	code := fmt.Sprint(result[0])
	switch code {
	case "not_found":
		return domain.Transaction{}, false, fmt.Errorf("%w: transaction %q", domain.ErrNotFound, transactionID)
	case "conflict":
		current := "unknown"
		if len(result) > 1 {
			current = fmt.Sprint(result[1])
		}
		return domain.Transaction{}, false, fmt.Errorf("%w: transaction %q already completed as %q", domain.ErrConflict, transactionID, current)
	case "changed", "idempotent":
		if len(result) < 2 {
			return domain.Transaction{}, false, fmt.Errorf("missing transaction payload for redis complete response: %v", result)
		}
		transaction, err := decodeTransaction([]byte(fmt.Sprint(result[1])))
		return transaction, code == "changed", err
	default:
		return domain.Transaction{}, false, fmt.Errorf("unexpected redis complete code %q", code)
	}
}

func (r *TransactionRepository) get(ctx context.Context, key string) (domain.Transaction, error) {
	payload, err := r.store.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == goredis.Nil {
			return domain.Transaction{}, fmt.Errorf("%w: transaction %q", domain.ErrNotFound, key)
		}
		return domain.Transaction{}, err
	}
	return decodeTransaction(payload)
}

func (r *TransactionRepository) transactionKey(transactionID string) string {
	return r.store.Key("transaction", transactionID)
}

func decodeTransaction(payload []byte) (domain.Transaction, error) {
	var transaction domain.Transaction
	if err := json.Unmarshal(payload, &transaction); err != nil {
		return domain.Transaction{}, err
	}
	return transaction, nil
}

var completeTransactionScript = goredis.NewScript(`
local raw = redis.call("GET", KEYS[1])
if not raw then return {"not_found"} end

local txn = cjson.decode(raw)
local current = txn["status"]
if current == "success" or current == "failure" then
  if current == ARGV[1] then return {"idempotent", raw} end
  return {"conflict", current}
end

txn["status"] = ARGV[1]
txn["failure_reason"] = ARGV[2]
txn["updated_at"] = ARGV[3]
txn["completed_at"] = ARGV[3]

local updated = cjson.encode(txn)
redis.call("SET", KEYS[1], updated)
return {"changed", updated}
`)
