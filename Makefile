CURL               ?= curl
DOCKER_COMPOSE      = docker-compose
GOLANGCI_VERSION    = v1.61.0
GOPATH              = $(shell go env GOPATH)

all: lint test ## Run linting and testing

# Use `-run==` to support Windows where ^ is an escape sequence and no test can
# ever start with literal `=`.
bench: ## Run benchmarks only
	go test ./... -bench=. -run== -v

compose: ## Start the docker-compose environment
	$(DOCKER_COMPOSE) up -d

compose-down: ## Stop the docker-compose environment
	$(DOCKER_COMPOSE) down

$(GOPATH)/bin/golangci-lint: ## Ensure golangci-lint is installed
	$(CURL) -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin $(GOLANGCI_VERSION)

# This will grep the double comment marker (##) and map all targets to the
# comment which will just print the comment next to each target for documenting purposes.
help: ## Show this help text
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN {FS = ":.*?## "}; \
		{printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

hooks: ## Install tracked git hooks
	script/install-hooks

lint: $(GOPATH)/bin/golangci-lint ## Lint the code
	$(GOPATH)/bin/golangci-lint run ./...

test: compose ## Run all tests (with race detection)
	go test ./... -race -v

coverage:
	go test -coverprofile c.out ./...
	@sed -i "s%github.com/0x4b53/amqp-rpc/v4/%amqp-rpc/%" c.out

.PHONY: all compose compose-down help hooks lint test
