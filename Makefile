LDFLAGS += -X "github.com/amyangfei/data-dam/pkg/utils.ReleaseVersion=$(shell git describe --tags --dirty="-dev")"
LDFLAGS += -X "github.com/amyangfei/data-dam/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/amyangfei/data-dam/pkg/utils.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/amyangfei/data-dam/pkg/utils.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"
LDFLAGS += -X "github.com/amyangfei/data-dam/pkg/utils.GoVersion=$(shell go version)"

CURDIR   := $(shell pwd)
GO       := GO111MODULE=on go
GOBUILD  := CGO_ENABLED=0 $(GO) build
GOTEST   := CGO_ENABLED=1 $(GO) test
PACKAGES  := $$(go list ./... | grep -vE 'tests|cmd|vendor')
FILES    := $$(find . -name "*.go" | grep -vE "vendor")
TOPDIRS  := $$(ls -d */ | grep -vE "vendor")
SHELL    := /usr/bin/env bash

.PHONY: build test data-dam

build: check test data-dam

data-dam:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/data-dam ./cmd/data-dam

check: fmt lint vet

fmt:
	@echo "gofmt (simplify)"
	@ gofmt -s -l -w $(FILES) 2>&1 | awk '{print} END{if(NR>0) {exit 1}}'

lint:
	GO111MODULE=off go get golang.org/x/lint/golint
	@echo "golint"
	@ golint -set_exit_status $(PACKAGES)

vet:
	$(GO) build -o bin/shadow golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow
	@echo "vet"
	@$(GO) vet -composites=false $(PACKAGES)
	@$(GO) vet -vettool=$(CURDIR)/bin/shadow $(PACKAGES) || true
