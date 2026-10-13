FROM golang:1.27.2-bookworm AS build
WORKDIR /source
ENV GOWORK=off GOPROXY=off CGO_ENABLED=0
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY . .
RUN go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/service ./cmd/runtime && go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate && go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/retire-access ./cmd/retire-access && go build -mod=vendor -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
ENV LISTEN=0.0.0.0:8094 HEALTH_URL=http://127.0.0.1:8094/readyz
EXPOSE 8094
HEALTHCHECK --interval=15s --timeout=4s --start-period=10s CMD ["/usr/local/bin/healthcheck"]
ENTRYPOINT ["/usr/local/bin/service"]
CMD ["--config", "/run/service.json"]
