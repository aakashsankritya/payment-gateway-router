package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type initiateRequest struct {
	OrderID           string         `json:"order_id"`
	Amount            float64        `json:"amount"`
	PaymentInstrument map[string]any `json:"payment_instrument"`
}

type transactionResponse struct {
	TransactionID string  `json:"transaction_id"`
	OrderID       string  `json:"order_id"`
	AttemptNo     int     `json:"attempt_no"`
	Amount        float64 `json:"amount"`
	Gateway       string  `json:"gateway"`
	Status        string  `json:"status"`
}

type callbackRequest struct {
	TransactionID string `json:"transaction_id"`
	OrderID       string `json:"order_id"`
	Gateway       string `json:"gateway"`
	Status        string `json:"status"`
	Reason        string `json:"reason,omitempty"`
}

type gatewayResponse struct {
	Routing struct {
		HealthWindowSeconds          int     `json:"health_window_seconds"`
		UnhealthyCooldownSeconds     int     `json:"unhealthy_cooldown_seconds"`
		MinCallbackCount             int     `json:"min_callback_count"`
		SuccessRateThreshold         float64 `json:"success_rate_threshold"`
		HalfOpenProbeCount           int     `json:"half_open_probe_count"`
		HalfOpenProbeTimeoutSeconds  int     `json:"half_open_probe_timeout_seconds"`
		OrderGatewayFailureThreshold int     `json:"order_gateway_failure_threshold"`
	} `json:"routing"`
	Gateways []gatewayStatus `json:"gateways"`
}

type gatewayStatus struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	Weight  int    `json:"weight"`
	Runtime struct {
		Gateway          string    `json:"gateway"`
		State            string    `json:"state"`
		UnhealthyUntil   time.Time `json:"unhealthy_until"`
		HalfOpenInFlight int       `json:"half_open_in_flight"`
		UpdatedAt        time.Time `json:"updated_at"`
	} `json:"runtime"`
	Stats struct {
		Gateway     string  `json:"gateway"`
		Successes   int     `json:"successes"`
		Failures    int     `json:"failures"`
		Total       int     `json:"total"`
		SuccessRate float64 `json:"success_rate"`
	} `json:"stats"`
}

type simulatorConfig struct {
	baseURL       string
	scenario      string
	total         int
	concurrency   int
	successRate   float64
	callbackRate  float64
	duplicateRate float64
	amount        float64
	timeout       time.Duration
	orderPrefix   string
	maxAttempts   int
}

type result struct {
	gateway         string
	initiated       bool
	callbackSent    bool
	callbackSuccess bool
	duplicateSent   bool
	initiateLatency time.Duration
	callbackLatency time.Duration
	initiateErr     error
	callbackErr     error
}

type aggregate struct {
	totalInitiated       int64
	totalCallbacks       int64
	totalDuplicates      int64
	successCallbacks     int64
	failureCallbacks     int64
	initiateErrors       int64
	callbackErrors       int64
	initiateLatencyNanos int64
	callbackLatencyNanos int64
	mu                   sync.Mutex
	byGateway            map[string]*gatewayAggregate
}

type gatewayAggregate struct {
	initiated        int64
	callbacks        int64
	successCallbacks int64
	failureCallbacks int64
	duplicates       int64
}

func main() {
	cfg := parseFlags()
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid config:", err)
		os.Exit(2)
	}

	client := &http.Client{
		Timeout: cfg.timeout,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.concurrency * 4,
			MaxIdleConnsPerHost: cfg.concurrency * 4,
			MaxConnsPerHost:     cfg.concurrency * 4,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	ctx := context.Background()
	if err := healthCheck(ctx, client, cfg.baseURL); err != nil {
		fmt.Fprintln(os.Stderr, "service health check failed:", err)
		os.Exit(1)
	}
	if cfg.scenario == "order-blacklist" {
		if err := runOrderBlacklistScenario(ctx, client, cfg); err != nil {
			fmt.Fprintln(os.Stderr, "order blacklist scenario failed:", err)
			os.Exit(1)
		}
		if gateways, err := fetchGatewayStats(ctx, client, cfg.baseURL); err != nil {
			fmt.Fprintln(os.Stderr, "failed to fetch server gateway stats:", err)
		} else {
			printServerStats(gateways)
		}
		return
	}

	start := time.Now()
	agg := &aggregate{byGateway: make(map[string]*gatewayAggregate)}
	jobs := make(chan int, cfg.concurrency*2)
	results := make(chan result, cfg.concurrency*2)

	var workers sync.WaitGroup
	for workerID := 0; workerID < cfg.concurrency; workerID++ {
		workers.Add(1)
		go func(workerID int) {
			defer workers.Done()
			random := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*7919))
			for id := range jobs {
				results <- runTransaction(ctx, client, cfg, random, id)
			}
		}(workerID)
	}

	var collector sync.WaitGroup
	collector.Add(1)
	go func() {
		defer collector.Done()
		for item := range results {
			agg.record(item)
		}
	}()

	for i := 1; i <= cfg.total; i++ {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	close(results)
	collector.Wait()
	duration := time.Since(start)

	printClientStats(cfg, agg, duration)
	if gateways, err := fetchGatewayStats(ctx, client, cfg.baseURL); err != nil {
		fmt.Fprintln(os.Stderr, "failed to fetch server gateway stats:", err)
	} else {
		printServerStats(gateways)
	}
}

func parseFlags() simulatorConfig {
	var cfg simulatorConfig
	flag.StringVar(&cfg.baseURL, "base-url", "http://localhost:8080", "Payment Gateway Router base URL")
	flag.StringVar(&cfg.scenario, "scenario", "traffic", "scenario to run: traffic or order-blacklist")
	flag.IntVar(&cfg.total, "total", 1000, "number of transactions to initiate")
	flag.IntVar(&cfg.concurrency, "concurrency", 50, "number of concurrent simulator workers")
	flag.Float64Var(&cfg.successRate, "success-rate", 0.95, "probability that a callback is success")
	flag.Float64Var(&cfg.callbackRate, "callback-rate", 1.0, "probability that an initiated transaction receives a callback")
	flag.Float64Var(&cfg.duplicateRate, "duplicate-rate", 0.0, "probability of sending one duplicate callback after the first callback")
	flag.Float64Var(&cfg.amount, "amount", 499.0, "transaction amount")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "HTTP request timeout")
	flag.StringVar(&cfg.orderPrefix, "order-prefix", "ORD-SIM", "order ID prefix")
	flag.IntVar(&cfg.maxAttempts, "max-attempts", 20, "maximum attempts for deterministic scenarios")
	flag.Parse()
	cfg.baseURL = strings.TrimRight(cfg.baseURL, "/")
	return cfg
}

func (c simulatorConfig) validate() error {
	if c.total <= 0 {
		return fmt.Errorf("total must be > 0")
	}
	if c.concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}
	if c.successRate < 0 || c.successRate > 1 {
		return fmt.Errorf("success-rate must be between 0 and 1")
	}
	if c.callbackRate < 0 || c.callbackRate > 1 {
		return fmt.Errorf("callback-rate must be between 0 and 1")
	}
	if c.duplicateRate < 0 || c.duplicateRate > 1 {
		return fmt.Errorf("duplicate-rate must be between 0 and 1")
	}
	if c.timeout <= 0 {
		return fmt.Errorf("timeout must be > 0")
	}
	if c.scenario != "traffic" && c.scenario != "order-blacklist" {
		return fmt.Errorf("scenario must be traffic or order-blacklist")
	}
	if c.maxAttempts <= 0 {
		return fmt.Errorf("max-attempts must be > 0")
	}
	return nil
}

func runOrderBlacklistScenario(ctx context.Context, client *http.Client, cfg simulatorConfig) error {
	gateways, err := fetchGatewayStats(ctx, client, cfg.baseURL)
	if err != nil {
		return err
	}
	enabledGateways := 0
	for _, gateway := range gateways.Gateways {
		if gateway.Enabled {
			enabledGateways++
		}
	}
	if enabledGateways < 2 {
		return fmt.Errorf("order-blacklist scenario needs at least two enabled gateways")
	}

	threshold := gateways.Routing.OrderGatewayFailureThreshold
	if threshold <= 0 {
		threshold = 2
	}

	orderID := cfg.orderPrefix + "-BLACKLIST"
	failuresByGateway := map[string]int{}
	blacklistedGateway := ""

	fmt.Printf("=== Order Gateway Blacklist Scenario ===\n")
	fmt.Printf("Order ID: %s\n", orderID)
	fmt.Printf("Failure threshold: %d\n", threshold)

	for attempt := 1; attempt <= cfg.maxAttempts; attempt++ {
		transaction, err := initiate(ctx, client, cfg, orderID)
		if err != nil {
			return fmt.Errorf("attempt %d initiate failed before blacklist threshold was reached: %w", attempt, err)
		}

		if err := sendCallback(ctx, client, cfg.baseURL, transaction, "failure"); err != nil {
			return fmt.Errorf("attempt %d failure callback failed: %w", attempt, err)
		}

		failuresByGateway[transaction.Gateway]++
		fmt.Printf("attempt=%d gateway=%s failure_count_for_gateway=%d\n", attempt, transaction.Gateway, failuresByGateway[transaction.Gateway])
		if failuresByGateway[transaction.Gateway] >= threshold {
			blacklistedGateway = transaction.Gateway
			break
		}
	}

	if blacklistedGateway == "" {
		return fmt.Errorf("no gateway reached failure threshold within %d attempts", cfg.maxAttempts)
	}

	next, err := initiate(ctx, client, cfg, orderID)
	if err != nil {
		return fmt.Errorf("post-blacklist initiate failed: %w", err)
	}
	if next.Gateway == blacklistedGateway {
		return fmt.Errorf("blacklisted gateway %q was selected again for order %q", blacklistedGateway, orderID)
	}

	fmt.Printf("blacklisted_gateway=%s next_gateway=%s result=passed\n", blacklistedGateway, next.Gateway)
	return nil
}

func runTransaction(ctx context.Context, client *http.Client, cfg simulatorConfig, random *rand.Rand, id int) result {
	orderID := fmt.Sprintf("%s-%d", cfg.orderPrefix, id)

	start := time.Now()
	transaction, err := initiate(ctx, client, cfg, orderID)
	item := result{initiateLatency: time.Since(start), initiateErr: err}
	if err != nil {
		return item
	}

	item.initiated = true
	item.gateway = transaction.Gateway
	if random.Float64() > cfg.callbackRate {
		return item
	}

	status := "failure"
	if random.Float64() < cfg.successRate {
		status = "success"
	}
	start = time.Now()
	if err := sendCallback(ctx, client, cfg.baseURL, transaction, status); err != nil {
		item.callbackLatency = time.Since(start)
		item.callbackErr = err
		return item
	}
	item.callbackLatency = time.Since(start)
	item.callbackSent = true
	item.callbackSuccess = status == "success"

	if random.Float64() < cfg.duplicateRate {
		item.duplicateSent = true
		_ = sendCallback(ctx, client, cfg.baseURL, transaction, status)
	}

	return item
}

func initiate(ctx context.Context, client *http.Client, cfg simulatorConfig, orderID string) (transactionResponse, error) {
	payload := initiateRequest{
		OrderID: orderID,
		Amount:  cfg.amount,
		PaymentInstrument: map[string]any{
			"type":        "card",
			"card_number": "****",
			"expiry":      "12/30",
		},
	}
	return postJSON[transactionResponse](ctx, client, cfg.baseURL+"/transactions/initiate", payload)
}

func sendCallback(ctx context.Context, client *http.Client, baseURL string, transaction transactionResponse, status string) error {
	payload := callbackRequest{
		TransactionID: transaction.TransactionID,
		OrderID:       transaction.OrderID,
		Gateway:       transaction.Gateway,
		Status:        status,
	}
	if status == "failure" {
		payload.Reason = "simulated_failure"
	}
	_, err := postJSON[map[string]any](ctx, client, baseURL+"/transactions/callback", payload)
	return err
}

func postJSON[T any](ctx context.Context, client *http.Client, url string, payload any) (T, error) {
	var zero T
	body, err := json.Marshal(payload)
	if err != nil {
		return zero, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return zero, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return zero, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return zero, fmt.Errorf("POST %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var decoded T
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return zero, err
	}
	return decoded, nil
}

func healthCheck(ctx context.Context, client *http.Client, baseURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET /health returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func fetchGatewayStats(ctx context.Context, client *http.Client, baseURL string) (gatewayResponse, error) {
	var gateways gatewayResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/gateways", nil)
	if err != nil {
		return gateways, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return gateways, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return gateways, fmt.Errorf("GET /gateways returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(&gateways); err != nil {
		return gateways, err
	}
	return gateways, nil
}

func (a *aggregate) record(item result) {
	if item.initiateErr != nil {
		atomic.AddInt64(&a.initiateErrors, 1)
		return
	}
	atomic.AddInt64(&a.totalInitiated, 1)
	atomic.AddInt64(&a.initiateLatencyNanos, item.initiateLatency.Nanoseconds())

	a.mu.Lock()
	gatewayStats := a.byGateway[item.gateway]
	if gatewayStats == nil {
		gatewayStats = &gatewayAggregate{}
		a.byGateway[item.gateway] = gatewayStats
	}
	gatewayStats.initiated++
	a.mu.Unlock()

	if item.callbackErr != nil {
		atomic.AddInt64(&a.callbackErrors, 1)
		return
	}
	if !item.callbackSent {
		return
	}

	atomic.AddInt64(&a.totalCallbacks, 1)
	atomic.AddInt64(&a.callbackLatencyNanos, item.callbackLatency.Nanoseconds())
	if item.duplicateSent {
		atomic.AddInt64(&a.totalDuplicates, 1)
	}
	if item.callbackSuccess {
		atomic.AddInt64(&a.successCallbacks, 1)
	} else {
		atomic.AddInt64(&a.failureCallbacks, 1)
	}

	a.mu.Lock()
	gatewayStats.callbacks++
	if item.duplicateSent {
		gatewayStats.duplicates++
	}
	if item.callbackSuccess {
		gatewayStats.successCallbacks++
	} else {
		gatewayStats.failureCallbacks++
	}
	a.mu.Unlock()
}

func printClientStats(cfg simulatorConfig, agg *aggregate, duration time.Duration) {
	initiated := atomic.LoadInt64(&agg.totalInitiated)
	callbacks := atomic.LoadInt64(&agg.totalCallbacks)
	initiateErrors := atomic.LoadInt64(&agg.initiateErrors)
	callbackErrors := atomic.LoadInt64(&agg.callbackErrors)
	avgInitiateLatency := avgDuration(atomic.LoadInt64(&agg.initiateLatencyNanos), initiated)
	avgCallbackLatency := avgDuration(atomic.LoadInt64(&agg.callbackLatencyNanos), callbacks)

	fmt.Println("=== Simulator Summary ===")
	fmt.Printf("Base URL:             %s\n", cfg.baseURL)
	fmt.Printf("Requested total:      %d\n", cfg.total)
	fmt.Printf("Concurrency:          %d\n", cfg.concurrency)
	fmt.Printf("Duration:             %s\n", duration.Round(time.Millisecond))
	fmt.Printf("Throughput:           %.2f txn/sec\n", float64(initiated)/duration.Seconds())
	fmt.Printf("Initiated:            %d\n", initiated)
	fmt.Printf("Callbacks sent:       %d\n", callbacks)
	fmt.Printf("Success callbacks:    %d\n", atomic.LoadInt64(&agg.successCallbacks))
	fmt.Printf("Failure callbacks:    %d\n", atomic.LoadInt64(&agg.failureCallbacks))
	fmt.Printf("Duplicate callbacks:  %d\n", atomic.LoadInt64(&agg.totalDuplicates))
	fmt.Printf("Initiate errors:      %d\n", initiateErrors)
	fmt.Printf("Callback errors:      %d\n", callbackErrors)
	fmt.Printf("Avg initiate latency: %s\n", avgInitiateLatency.Round(time.Microsecond))
	fmt.Printf("Avg callback latency: %s\n", avgCallbackLatency.Round(time.Microsecond))

	fmt.Println("\n=== Client Gateway Distribution ===")
	names := make([]string, 0, len(agg.byGateway))
	agg.mu.Lock()
	for name := range agg.byGateway {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		stats := agg.byGateway[name]
		fmt.Printf("%-12s initiated=%d callbacks=%d success=%d failure=%d duplicates=%d\n",
			name,
			stats.initiated,
			stats.callbacks,
			stats.successCallbacks,
			stats.failureCallbacks,
			stats.duplicates,
		)
	}
	agg.mu.Unlock()
}

func printServerStats(response gatewayResponse) {
	fmt.Println("\n=== Server Gateway Stats ===")
	fmt.Printf("Health window: %ds, threshold: %.2f, min callbacks: %d, cooldown: %ds, probes: %d, probe timeout: %ds, order gateway failure threshold: %d\n",
		response.Routing.HealthWindowSeconds,
		response.Routing.SuccessRateThreshold,
		response.Routing.MinCallbackCount,
		response.Routing.UnhealthyCooldownSeconds,
		response.Routing.HalfOpenProbeCount,
		response.Routing.HalfOpenProbeTimeoutSeconds,
		response.Routing.OrderGatewayFailureThreshold,
	)
	for _, gateway := range response.Gateways {
		fmt.Printf("%-12s enabled=%t weight=%d state=%s in_flight=%d total=%d success=%d failure=%d success_rate=%.4f\n",
			gateway.Name,
			gateway.Enabled,
			gateway.Weight,
			gateway.Runtime.State,
			gateway.Runtime.HalfOpenInFlight,
			gateway.Stats.Total,
			gateway.Stats.Successes,
			gateway.Stats.Failures,
			gateway.Stats.SuccessRate,
		)
	}
}

func avgDuration(totalNanos int64, count int64) time.Duration {
	if count == 0 {
		return 0
	}
	return time.Duration(totalNanos / count)
}
