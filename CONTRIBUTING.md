# Contributing

Thanks for helping. This is a Go project with no dependencies outside the standard library;
issues and pull requests are welcome.

## Set up and check your change

Needs Go 1.26+, make and bash (Git Bash on Windows). Build output and caches stay in git-ignored
folders inside the repo (`.tmp/`).

```bash
make setup   # go mod verify
make lint    # gofmt, go vet, and dst lint on the example service
make test    # all tests (CI adds the race detector: make test GOTESTFLAGS=-race)
make audit   # govulncheck, as in CI
make e2e     # 1,000-seed sweeps, a replay and a real-runtime run, about 10 seconds
```

A pull request is ready when `make lint` and `make test` pass and CI is green.

## Guidelines

- Services must reach time, the network, the disk and randomness only through `env`; `dst lint`
  enforces this for `example/kv`. Never range over a map where the order changes what a node does.
- Any change to the simulator's event order or random draws changes every trace hash. If that is
  intended, update `goldenSeed1` in `internal/scenario/scenario_test.go` in the same commit and say
  why in the pull request.
- Add or update a test for every behaviour change, and update `docs/results/` when numbers change.
- Significant design changes need an ADR in `docs/adr/` (copy `0001` for the format).
- Keep function comments to one short phrase; let names and tests explain the rest.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

By contributing you agree that your work is released under the [MIT License](LICENSE).
