GO      ?= go
PKGS    := ./...
BIN_DIR := bin

.PHONY: all
all: fmt vet test

.PHONY: fmt
fmt:
	$(GO) fmt $(PKGS)

# The check target is what CI runs: it reports formatting problems instead of
# silently rewriting files, which is the opposite of what `make fmt` is for.
.PHONY: fmt-check
fmt-check:
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "these files are not gofmt'd:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

.PHONY: vet
vet:
	$(GO) vet $(PKGS)

.PHONY: build
build:
	$(GO) build $(PKGS)

# The race detector stays on by default. This is a server that will spend its
# life fanning spans across goroutines, and a green run without it proves less
# than it appears to.
.PHONY: test
test:
	$(GO) test -race -count=1 -timeout 5m $(PKGS)

.PHONY: cover
cover:
	$(GO) test -covermode=atomic -coverpkg=$(PKGS) -coverprofile=coverage.out -timeout 5m $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: cover-html
cover-html: cover
	$(GO) tool cover -html=coverage.out

.PHONY: clean
clean:
	rm -rf $(BIN_DIR) coverage.out
