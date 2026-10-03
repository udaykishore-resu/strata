VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
STRATA_TEST_DATABASE_URL ?= postgres://postgres:postgres@localhost:5432/strata_test?sslmode=disable

.PHONY: help run demo build test test-integration cover lint fmt synth docs docker compose-up compose-down bootstrap clean

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

run: ## Run the engine locally (in-memory store, fake cloud, no auth) on :8080
	go run ./cmd/strata-server --dev

demo: ## End-to-end demo: start a dev server, deploy the example, inspect, destroy
	./scripts/demo.sh

build: ## Build bin/strata-server and bin/strata
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/strata-server ./cmd/strata-server
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/strata ./cmd/strata

test: ## Unit tests with the race detector
	go test -race ./...

test-integration: ## Tests including the Postgres store (needs a database; see compose-up)
	STRATA_TEST_DATABASE_URL="$(STRATA_TEST_DATABASE_URL)" go test -race ./...

cover: ## Coverage report
	go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out | tail -1

lint: ## gofmt, go vet and staticcheck
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck@latest ./...

fmt: ## Format code
	gofmt -w .

synth: ## Re-synthesize the example template
	go run ./examples/orders-platform -out examples/orders-platform

docs: ## Regenerate docs/resource-types.md from provider schemas
	go run ./tools/docgen > docs/resource-types.md

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t strata-server:$(VERSION) .

compose-up: ## Postgres + engine (fake cloud) via Docker Compose
	docker compose up --build -d

compose-down: ## Stop Docker Compose
	docker compose down -v

bootstrap: ## Deploy the engine to GCP (PROJECT_ID=... REGION=...)
	./deploy/bootstrap.sh

clean:
	rm -rf bin strata.out coverage.out
