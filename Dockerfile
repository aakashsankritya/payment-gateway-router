FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
COPY cmd ./cmd
COPY internal ./internal
COPY configs ./configs

RUN go test ./...
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/payment-gateway-router ./cmd/payment-gateway-router

FROM alpine:3.21

WORKDIR /app
COPY --from=build /out/payment-gateway-router /app/payment-gateway-router
COPY configs /app/configs

ENV ADDR=:8080
ENV CONFIG_PATH=/app/configs/gateways.yaml
ENV CONFIG_RELOAD_INTERVAL_SECONDS=2

EXPOSE 8080

ENTRYPOINT ["/app/payment-gateway-router"]
