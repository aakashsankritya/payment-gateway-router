package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"payment-gateway-router/internal/domain"
)

type OrderGatewayBlacklistRepository struct {
	store *Store
}

func NewOrderGatewayBlacklistRepository(store *Store) *OrderGatewayBlacklistRepository {
	return &OrderGatewayBlacklistRepository{store: store}
}

func (r *OrderGatewayBlacklistRepository) BlacklistedGateways(ctx context.Context, orderID string) (map[string]struct{}, error) {
	gateways, err := r.store.client.SMembers(ctx, r.blacklistKey(orderID)).Result()
	if err != nil && err != goredis.Nil {
		return nil, err
	}
	result := make(map[string]struct{}, len(gateways))
	for _, gateway := range gateways {
		result[gateway] = struct{}{}
	}
	return result, nil
}

func (r *OrderGatewayBlacklistRepository) RecordOutcome(ctx context.Context, orderID string, gateway string, status domain.TransactionStatus, failureThreshold int, now time.Time) (domain.OrderGatewayAttemptSummary, bool, error) {
	if !status.IsFinal() {
		return domain.OrderGatewayAttemptSummary{}, false, domain.ErrInvalidStatus
	}
	if failureThreshold <= 0 {
		failureThreshold = 1
	}

	result, err := recordOrderGatewayOutcomeScript.Run(
		ctx,
		r.store.client,
		[]string{r.summaryKey(orderID, gateway), r.blacklistKey(orderID)},
		orderID,
		gateway,
		string(status),
		failureThreshold,
		now.UTC().Format(time.RFC3339Nano),
	).Slice()
	if err != nil {
		return domain.OrderGatewayAttemptSummary{}, false, err
	}
	if len(result) < 5 {
		return domain.OrderGatewayAttemptSummary{}, false, fmt.Errorf("unexpected redis order gateway response: %v", result)
	}

	failures, err := strconv.Atoi(fmt.Sprint(result[0]))
	if err != nil {
		return domain.OrderGatewayAttemptSummary{}, false, err
	}
	successes, err := strconv.Atoi(fmt.Sprint(result[1]))
	if err != nil {
		return domain.OrderGatewayAttemptSummary{}, false, err
	}
	blacklisted, err := strconv.ParseBool(fmt.Sprint(result[2]))
	if err != nil {
		return domain.OrderGatewayAttemptSummary{}, false, err
	}
	newlyBlacklisted, err := strconv.ParseBool(fmt.Sprint(result[3]))
	if err != nil {
		return domain.OrderGatewayAttemptSummary{}, false, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, fmt.Sprint(result[4]))
	if err != nil {
		return domain.OrderGatewayAttemptSummary{}, false, err
	}

	return domain.OrderGatewayAttemptSummary{
		OrderID:     orderID,
		Gateway:     gateway,
		Failures:    failures,
		Successes:   successes,
		Blacklisted: blacklisted,
		UpdatedAt:   updatedAt,
	}, newlyBlacklisted, nil
}

func (r *OrderGatewayBlacklistRepository) summaryKey(orderID string, gateway string) string {
	return r.store.Key("order", orderID, "gateway", gateway, "attempts")
}

func (r *OrderGatewayBlacklistRepository) blacklistKey(orderID string) string {
	return r.store.Key("order", orderID, "blacklisted_gateways")
}

var recordOrderGatewayOutcomeScript = goredis.NewScript(`
local failures = tonumber(redis.call("HGET", KEYS[1], "failures") or "0")
local successes = tonumber(redis.call("HGET", KEYS[1], "successes") or "0")
local was_blacklisted = redis.call("SISMEMBER", KEYS[2], ARGV[2]) == 1

local status = ARGV[3]
local threshold = tonumber(ARGV[4])
local blacklisted = was_blacklisted
if status == "success" then
  successes = successes + 1
  blacklisted = false
  redis.call("SREM", KEYS[2], ARGV[2])
elseif status == "failure" then
  failures = failures + 1
  if successes == 0 and failures >= threshold then
    blacklisted = true
    redis.call("SADD", KEYS[2], ARGV[2])
  end
end

local blacklisted_value = "false"
if blacklisted then blacklisted_value = "true" end

redis.call("HSET", KEYS[1],
  "order_id", ARGV[1],
  "gateway", ARGV[2],
  "failures", failures,
  "successes", successes,
  "blacklisted", blacklisted_value,
  "updated_at", ARGV[5]
)

local newly_blacklisted = "false"
if blacklisted and not was_blacklisted then newly_blacklisted = "true" end
return {failures, successes, blacklisted_value, newly_blacklisted, ARGV[5]}
`)
