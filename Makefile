.PHONY: setup lint test audit build bench e2e ci

EXE := $(if $(filter Windows_NT,$(OS)),.exe,)
DST := .tmp/bin/dst$(EXE)
SRC := cmd dstlint env example internal realenv sim
# CI adds -race (it needs cgo, which not every machine has).
GOTESTFLAGS ?=

setup:
	go mod verify

lint:
	@files="$$(gofmt -l $(SRC))"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w $(SRC)"; exit 1)
	go vet ./...
	go run ./cmd/dst lint example/kv

test:
	go test $(GOTESTFLAGS) -count=1 ./...

# Known vulnerabilities in the code paths actually called (Go 1.26.10, as in CI).
audit:
	go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./...

build:
	go build -trimpath -o $(DST) ./cmd/dst

# The numbers in docs/results/m1.md: 1,000 seeds each for the correct store and the seeded bug,
# a replay of the first failing seed and a run on the real runtime.
bench: build
	bash scripts/e2e.sh $(DST) 1000

e2e: bench

ci: setup lint test
