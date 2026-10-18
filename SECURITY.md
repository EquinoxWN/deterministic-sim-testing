# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
**Security > Report a vulnerability** on this repository
(`https://github.com/EquinoxWN/deterministic-sim-testing/security/advisories/new`). Do not open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You will get an answer
within 7 days. Fixes are released as soon as they are ready, and you are credited unless you ask
not to be.

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives fixes.

## Scope

- **In scope:** the real runtime (`realenv`) accepting datagrams from outside the cluster or
  reaching beyond the loopback interface, file access outside a node's directory, unbounded memory
  or time use from crafted messages, files or flags, and the CLI writing outside the paths it
  was given.
- **Out of scope:** bugs in services you test with the framework, and the example store's
  deliberate `-bug` mode.

## How this repository protects itself

- The real runtime binds UDP sockets to 127.0.0.1 only, drops datagrams whose source port is not a
  cluster member or whose header names a different node, and caps every message at 64 KiB.
- File names are checked against `[a-z0-9._-]{1,64}` (no leading dot, no separators) before any
  disk access, so a node cannot leave its own directory; files are capped at 64 MiB.
- Messages are decoded strictly (unknown fields refused, key and value lengths bounded); malformed
  input is ignored.
- CLI inputs are bounded (seed counts, cluster size, operations), and runs stop at an event and a
  virtual-time limit.
- The module has no dependencies outside the Go standard library.
- Every GitHub Action is pinned to a full commit SHA, and workflows run with read-only
  permissions and without persisted credentials.
- Dependabot update pull requests are paused (`open-pull-requests-limit: 0`) so the repository keeps a single `main` branch; dependencies are updated by hand and checked by the CI audit.
- CI runs gofmt, go vet, the determinism lint, tests with the race detector and govulncheck on
  every push and pull request.
