package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"payment-gateway-router/internal/adapters/config"
	gatewayadapter "payment-gateway-router/internal/adapters/gateway"
	httpadapter "payment-gateway-router/internal/adapters/http"
	"payment-gateway-router/internal/adapters/repository/memory"
	redisrepo "payment-gateway-router/internal/adapters/repository/redis"
	"payment-gateway-router/internal/ports"
	"payment-gateway-router/internal/service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	runtimeConfig, err := config.LoadRuntimeConfig()
	if err != nil {
		logger.Error("failed to load runtime config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	configProvider, err := config.NewFileProvider(runtimeConfig.ConfigPath, logger)
	if err != nil {
		logger.Error("failed to load config", "path", runtimeConfig.ConfigPath, "error", err)
		os.Exit(1)
	}
	go configProvider.Watch(ctx, runtimeConfig.ConfigReloadInterval())

	var transactionRepo ports.TransactionRepository = memory.NewTransactionRepository()
	var healthRepo ports.GatewayHealthRepository = memory.NewGatewayHealthRepository()
	var orderGatewayRepo ports.OrderGatewayBlacklistRepository = memory.NewOrderGatewayBlacklistRepository()
	var redisStore *redisrepo.Store
	if runtimeConfig.StateBackend == "redis" {
		redisStore = redisrepo.NewStore(redisrepo.Options{
			Addr:      runtimeConfig.Redis.Addr,
			Password:  runtimeConfig.Redis.Password,
			DB:        runtimeConfig.Redis.DB,
			KeyPrefix: runtimeConfig.Redis.KeyPrefix,
			PoolSize:  runtimeConfig.Redis.PoolSize,
		})
		if err := redisStore.Ping(ctx); err != nil {
			logger.Error("failed to connect to redis", "addr", runtimeConfig.Redis.Addr, "error", err)
			os.Exit(1)
		}
		defer redisStore.Close()
		transactionRepo = redisrepo.NewTransactionRepository(redisStore)
		healthRepo = redisrepo.NewGatewayHealthRepository(redisStore)
		orderGatewayRepo = redisrepo.NewOrderGatewayBlacklistRepository(redisStore)
		logger.Info("using redis state backend", "addr", runtimeConfig.Redis.Addr)
	} else {
		logger.Info("using in-memory state backend")
	}
	clock := service.SystemClock{}
	idGenerator := service.RandomIDGenerator{}
	gatewayRegistry := gatewayadapter.NewRegistry()
	if err := gatewayRegistry.ValidateConfig(configProvider.Current(ctx)); err != nil {
		logger.Error("gateway config is incompatible with registered clients", "error", err)
		os.Exit(1)
	}

	healthService := service.NewHealthService(healthRepo, configProvider, clock, logger)
	healthService.SetCacheTTL(runtimeConfig.GatewayStateCacheTTL())
	orderGatewayService := service.NewOrderGatewayBlacklistService(orderGatewayRepo, configProvider, clock, logger)
	routingService := service.NewRoutingService(configProvider, healthService, clock, logger)
	routingService.SetCacheTTL(runtimeConfig.RoutingCacheTTL())
	transactionService := service.NewTransactionService(
		transactionRepo,
		routingService,
		healthService,
		orderGatewayService,
		gatewayRegistry,
		idGenerator,
		clock,
		logger,
	)

	router := httpadapter.NewRouter(transactionService, healthService, configProvider, gatewayRegistry, logger)
	server := &http.Server{
		Addr:              runtimeConfig.Addr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("payment gateway router listening", "addr", runtimeConfig.Addr, "config_path", runtimeConfig.ConfigPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped unexpectedly", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}
