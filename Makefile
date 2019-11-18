CURL               ?= curl
DOCKER_COMPOSE      = docker-compose
GOLANGCI_VERSION    = v1.21.0
GOPATH              = $(shell go env GOPATH)

all: lint test

compose:
	$(DOCKER_COMPOSE) up -d

compose-down:
	$(DOCKER_COMPOSE) down

$(GOPATH)/bin/golangci-lint:
	$(CURL) -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin $(GOLANGCI_VERSION)

hooks:
	script/install-hooks

lint: $(GOPATH)/bin/golangci-lint
	$(GOPATH)/bin/golangci-lint run ./...

test: compose
	go test ./...

.PHONY: all compose compose-down hooks lint test
