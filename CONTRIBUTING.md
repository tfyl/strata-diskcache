# Contributing

Issues and pull requests are welcome.

- `make test` and `make lint` must pass.
- New behaviour needs a test. `internal/strata/stratatest` is a fake Strata to test against;
  `internal/proxy/proxy_test.go` runs the proxy through it.
- Edge cases that protect correctness or the disk go in `docs/design.md` with the test that covers them.
- Add a dependency only when it clearly beats a few lines of our own, and say why in the pull request.
- If you change how the proxy talks to Strata, say which Strata version you checked it against.
- Use conventional commit messages (`fix: ...`, `feat: ...`) and add a line to CHANGELOG.md.
