COMPOSE ?= docker compose
APP_SERVICE ?= gateway
SIMULATOR_SERVICE ?= simulator

.PHONY: help deps fmt test verify build up down restart rebuild logs ps simulator simulator-order-blacklist redis-cli clean

help:
	@printf "Payment Gateway Router\n\n"
	@printf "Usage:\n"
	@printf "  make <target>\n\n"
	@printf "Targets:\n"
	@printf "  deps        Download and tidy Go dependencies\n"
	@printf "  fmt         Format Go source files\n"
	@printf "  test        Run Go tests\n"
	@printf "  verify      Run formatter and tests\n"
	@printf "  build       Build the application image\n"
	@printf "  up          Start app and dependencies\n"
	@printf "  down        Stop app and dependencies\n"
	@printf "  restart     Restart the app service\n"
	@printf "  rebuild     Rebuild and restart the app service\n"
	@printf "  logs        Follow app logs\n"
	@printf "  ps          Show compose service status\n"
	@printf "  simulator   Run the traffic simulator\n"
	@printf "  simulator-order-blacklist Run deterministic order blacklist scenario\n"
	@printf "  redis-cli   Open redis-cli inside the Redis container\n"
	@printf "  clean       Stop services and remove volumes\n"

deps:
	$(COMPOSE) run --rm --no-deps $(SIMULATOR_SERVICE) go mod tidy

fmt:
	$(COMPOSE) run --rm --no-deps $(SIMULATOR_SERVICE) sh -c 'find . -name "*.go" -not -path "./.git/*" -exec gofmt -w {} +'

test:
	$(COMPOSE) run --rm --no-deps $(SIMULATOR_SERVICE) go test ./...

verify: fmt test

build:
	$(COMPOSE) build $(APP_SERVICE)

up:
	$(COMPOSE) up -d $(APP_SERVICE)

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart $(APP_SERVICE)

rebuild:
	$(COMPOSE) up -d --build $(APP_SERVICE)

logs:
	$(COMPOSE) logs -f $(APP_SERVICE)

ps:
	$(COMPOSE) ps

simulator:
	$(COMPOSE) --profile tools run --rm $(SIMULATOR_SERVICE)

simulator-order-blacklist:
	$(COMPOSE) --profile tools run --rm $(SIMULATOR_SERVICE) go run ./cmd/simulator -base-url http://$(APP_SERVICE):8080 -scenario order-blacklist

redis-cli:
	$(COMPOSE) exec redis redis-cli

clean:
	$(COMPOSE) down -v --remove-orphans
