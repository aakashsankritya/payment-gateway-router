package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"payment-gateway-router/internal/domain"
)

type GatewayHealthRepository struct {
	store *Store
}

func NewGatewayHealthRepository(store *Store) *GatewayHealthRepository {
	return &GatewayHealthRepository{store: store}
}

func (r *GatewayHealthRepository) Ensure(ctx context.Context, gateway string, now time.Time) error {
	state := domain.GatewayRuntimeState{
		Gateway:   gateway,
		State:     domain.GatewayStateHealthy,
		UpdatedAt: now,
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return r.store.client.SetNX(ctx, r.stateKey(gateway), payload, 0).Err()
}

func (r *GatewayHealthRepository) Get(ctx context.Context, gateway string) (domain.GatewayRuntimeState, error) {
	return r.getState(ctx, gateway)
}

func (r *GatewayHealthRepository) Save(ctx context.Context, state domain.GatewayRuntimeState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.store.client.Set(ctx, r.stateKey(state.Gateway), payload, 0).Err(); err != nil {
		return err
	}
	return nil
}

func (r *GatewayHealthRepository) PruneExpiredHalfOpenProbes(ctx context.Context, gateway string, now time.Time) (domain.GatewayRuntimeState, error) {
	result, err := pruneExpiredProbesScript.Run(
		ctx,
		r.store.client,
		[]string{r.stateKey(gateway)},
		now.UnixMilli(),
		now.UTC().Format(time.RFC3339Nano),
	).Slice()
	if err != nil {
		return domain.GatewayRuntimeState{}, err
	}
	if len(result) == 0 {
		return domain.GatewayRuntimeState{}, fmt.Errorf("unexpected redis prune response: %v", result)
	}
	code := fmt.Sprint(result[0])
	if code == "missing" {
		return domain.GatewayRuntimeState{}, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
	}
	if len(result) < 2 {
		return domain.GatewayRuntimeState{}, fmt.Errorf("missing gateway state in redis prune response: %v", result)
	}
	return decodeGatewayState([]byte(fmt.Sprint(result[1])))
}

func (r *GatewayHealthRepository) TryAcquireHalfOpenProbe(ctx context.Context, gateway string, transactionID string, maxInFlight int, expiresAt time.Time, now time.Time) (domain.GatewayRuntimeState, bool, error) {
	result, err := acquireProbeScript.Run(
		ctx,
		r.store.client,
		[]string{r.stateKey(gateway)},
		maxInFlight,
		transactionID,
		expiresAt.UnixMilli(),
		now.UnixMilli(),
		now.UTC().Format(time.RFC3339Nano),
	).Slice()
	if err != nil {
		return domain.GatewayRuntimeState{}, false, err
	}
	if len(result) == 0 {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("unexpected redis probe response: %v", result)
	}

	code := fmt.Sprint(result[0])
	if code == "missing" {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
	}
	if len(result) < 2 {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("missing gateway state in redis probe response: %v", result)
	}

	state, err := decodeGatewayState([]byte(fmt.Sprint(result[1])))
	return state, code == "acquired", err
}

func (r *GatewayHealthRepository) ReleaseHalfOpenProbe(ctx context.Context, gateway string, transactionID string, now time.Time) (domain.GatewayRuntimeState, bool, error) {
	result, err := releaseProbeScript.Run(
		ctx,
		r.store.client,
		[]string{r.stateKey(gateway)},
		transactionID,
		now.UnixMilli(),
		now.UTC().Format(time.RFC3339Nano),
	).Slice()
	if err != nil {
		return domain.GatewayRuntimeState{}, false, err
	}
	if len(result) == 0 {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("unexpected redis release response: %v", result)
	}

	code := fmt.Sprint(result[0])
	if code == "missing" {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
	}
	if len(result) < 2 {
		return domain.GatewayRuntimeState{}, false, fmt.Errorf("missing gateway state in redis release response: %v", result)
	}

	state, err := decodeGatewayState([]byte(fmt.Sprint(result[1])))
	return state, code == "released", err
}

func (r *GatewayHealthRepository) RecordEvent(ctx context.Context, event domain.GatewayEvent, retention time.Duration) error {
	bucket := event.CreatedAt.UTC().Truncate(time.Minute).Unix()
	field := "failures"
	if event.Status == domain.TransactionStatusSuccess {
		field = "successes"
	}

	ttl := retention + 2*time.Minute
	key := r.statsKey(event.Gateway, bucket)
	pipe := r.store.client.Pipeline()
	pipe.HIncrBy(ctx, key, field, 1)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *GatewayHealthRepository) StatsSince(ctx context.Context, gateway string, since time.Time, until time.Time, bucketSize time.Duration) (domain.GatewayStats, error) {
	stats := domain.GatewayStats{Gateway: gateway}
	startBucket := since.UTC().Truncate(bucketSize).Unix()
	endBucket := until.UTC().Truncate(bucketSize).Unix()
	step := int64(bucketSize.Seconds())
	if step <= 0 {
		step = 60
	}

	pipe := r.store.client.Pipeline()
	commands := make([]*goredis.MapStringStringCmd, 0)
	for bucket := startBucket; bucket <= endBucket; bucket += step {
		commands = append(commands, pipe.HGetAll(ctx, r.statsKey(gateway, bucket)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return domain.GatewayStats{}, err
	}

	for _, command := range commands {
		fields, err := command.Result()
		if err != nil {
			return domain.GatewayStats{}, err
		}
		successes, err := strconv.Atoi(defaultString(fields["successes"], "0"))
		if err != nil {
			return domain.GatewayStats{}, err
		}
		failures, err := strconv.Atoi(defaultString(fields["failures"], "0"))
		if err != nil {
			return domain.GatewayStats{}, err
		}
		stats.Successes += successes
		stats.Failures += failures
	}

	stats.Total = stats.Successes + stats.Failures
	if stats.Total > 0 {
		stats.SuccessRate = float64(stats.Successes) / float64(stats.Total)
	}
	return stats, nil
}

func (r *GatewayHealthRepository) getState(ctx context.Context, gateway string) (domain.GatewayRuntimeState, error) {
	payload, err := r.store.client.Get(ctx, r.stateKey(gateway)).Bytes()
	if err != nil {
		if err == goredis.Nil {
			return domain.GatewayRuntimeState{}, fmt.Errorf("%w: gateway %q", domain.ErrNotFound, gateway)
		}
		return domain.GatewayRuntimeState{}, err
	}
	return decodeGatewayState(payload)
}

func (r *GatewayHealthRepository) stateKey(gateway string) string {
	return r.store.Key("gateway", gateway, "state")
}

func (r *GatewayHealthRepository) statsKey(gateway string, bucket int64) string {
	return r.store.Key("gateway", gateway, "stats", strconv.FormatInt(bucket, 10))
}

func decodeGatewayState(payload []byte) (domain.GatewayRuntimeState, error) {
	var state domain.GatewayRuntimeState
	if err := json.Unmarshal(payload, &state); err != nil {
		return domain.GatewayRuntimeState{}, err
	}
	return state, nil
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

var acquireProbeScript = goredis.NewScript(`
local raw = redis.call("GET", KEYS[1])
if not raw then return {"missing"} end

local state = cjson.decode(raw)
if state["state"] ~= "half_open" then
  return {"not_half_open", raw}
end

local active = {}
local now_millis = tonumber(ARGV[4])
for _, probe in ipairs(state["half_open_probes"] or {}) do
  local expires_at = tonumber(probe["expires_at_unix_milli"] or 0)
  if probe["transaction_id"] and expires_at > now_millis then
    table.insert(active, probe)
  end
end

local max = tonumber(ARGV[1])
if #active > 0 then
  state["half_open_probes"] = active
else
  state["half_open_probes"] = nil
end
state["half_open_in_flight"] = #active
if #active >= max then
  state["updated_at"] = ARGV[5]
  local updated = cjson.encode(state)
  redis.call("SET", KEYS[1], updated)
  return {"full", updated}
end

table.insert(active, {
  transaction_id = ARGV[2],
  expires_at_unix_milli = tonumber(ARGV[3])
})
state["half_open_probes"] = active
state["half_open_in_flight"] = #active
state["updated_at"] = ARGV[5]

local updated = cjson.encode(state)
redis.call("SET", KEYS[1], updated)
return {"acquired", updated}
`)

var releaseProbeScript = goredis.NewScript(`
local raw = redis.call("GET", KEYS[1])
if not raw then return {"missing"} end

local state = cjson.decode(raw)
if state["state"] ~= "half_open" then
  return {"not_half_open", raw}
end

local active = {}
local released = false
local transaction_id = ARGV[1]
local now_millis = tonumber(ARGV[2])
for _, probe in ipairs(state["half_open_probes"] or {}) do
  local expires_at = tonumber(probe["expires_at_unix_milli"] or 0)
  if probe["transaction_id"] and expires_at > now_millis then
    if probe["transaction_id"] == transaction_id then
      released = true
    else
      table.insert(active, probe)
    end
  end
end

if #active > 0 then
  state["half_open_probes"] = active
else
  state["half_open_probes"] = nil
end
state["half_open_in_flight"] = #active
state["updated_at"] = ARGV[3]

local updated = cjson.encode(state)
redis.call("SET", KEYS[1], updated)
if released then return {"released", updated} end
return {"not_found", updated}
`)

var pruneExpiredProbesScript = goredis.NewScript(`
local raw = redis.call("GET", KEYS[1])
if not raw then return {"missing"} end

local state = cjson.decode(raw)
if state["state"] ~= "half_open" then
  return {"ok", raw}
end

local active = {}
local now_millis = tonumber(ARGV[1])
for _, probe in ipairs(state["half_open_probes"] or {}) do
  local expires_at = tonumber(probe["expires_at_unix_milli"] or 0)
  if probe["transaction_id"] and expires_at > now_millis then
    table.insert(active, probe)
  end
end

if #active > 0 then
  state["half_open_probes"] = active
else
  state["half_open_probes"] = nil
end
state["half_open_in_flight"] = #active
state["updated_at"] = ARGV[2]

local updated = cjson.encode(state)
redis.call("SET", KEYS[1], updated)
return {"ok", updated}
`)
