.PHONY: build test fmt vet tidy clean gen proto-bootstrap proto-check

GO ?= go
PYTHON ?= python3

build:
	$(GO) build -o bin/lsd ./cmd/lsd

test:
	$(GO) test ./...

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf bin

gen:
	PATH="$(HOME)/go/bin:$$PATH" buf generate

proto-bootstrap:
	@while IFS= read -r mod; do \
		$(PYTHON) scripts/dump_proto.py "$$mod"; \
	done < scripts/proto_modules.txt

proto-check: proto-bootstrap
	@if ! git diff --exit-code -- proto/ >/dev/null; then \
		echo "ERROR: vendored protos drifted from Python source. Run 'make proto-bootstrap PYTHON=<your-python>' and commit."; \
		git --no-pager diff --stat -- proto/; \
		exit 1; \
	fi
