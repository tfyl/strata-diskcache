# Changelog

## Unreleased

### Changed

- **Flags take two dashes** (`--session-dir`, not `-session-dir`). The environment variables are unchanged.
- `go install` path is now `github.com/tfyl/strata-diskcache/cmd/strata-diskcache@latest`.
- Session files are named `dc-<fingerprint>-<key>-<tokens>.session`. Files in the first version's format are
  deleted at start.
- `strata_diskcache_save_seconds` and `strata_diskcache_restore_seconds` are histograms now (`_sum` as before, plus
  `_count` and buckets). Go runtime and process metrics are exported too.
- Restructured into `cmd/` and three `internal/` packages, with slog, Prometheus client_golang, kong, errgroup and
  lumberjack in place of hand-rolled code.

### Added

- Logging to stderr, a file, or both; text or JSON; a level. Log files rotate by size, are gzipped and pruned by
  count and age (`--log-*`). One `request` line per generation request, with no message content.
- Files are tied to Strata's model, version, context, KV type and control vector, and deleted when those change.
- Strata restarts and unloaded engines are noticed (from `/v1/status`), and so are 5xx answers: the proxy stops
  assuming anything is in RAM.
- `--max-body-mib`: larger bodies are streamed through without buffering.
- `--shutdown-s`: on SIGTERM, running requests and a save under way get this long to finish.
- Any non-GET request outside the three generation endpoints makes the proxy forget the live conversation.
- `/health` on the admin listener; golangci-lint in CI; `docs/design.md` with the edge cases and their tests.

### Fixed

- `/metrics` of an unloaded engine no longer reads as a fingerprint, so an unload no longer evicted the session
  files of the engine that was loaded.
- An engine unloaded by `--lazy` / `idle_unload_s` wakes for a restore: the proxy sends a 1-token request, checks
  that the engine is still the one the file was written for, and retries the restore the engine refused. A failed
  wakeup, or a woken engine with another fingerprint, is reported as what it is.
- A client that closes the connection after the stream's terminal event counts as a complete answer: the
  conversation stays live and is saved. The marker is looked for in the whole chunk, because Responses writes
  kilobytes after `response.completed`, and `[DONE]` is anchored to its line, so the model quoting it in the
  answer is not the end of the stream.
- A session file that restores less than half the tokens its name claims is forgotten, so the conversation is
  saved afresh instead of being skipped as already on disk.

## 0.1.0 (unreleased)

- First version: pass-through proxy for Strata with an on-disk conversation cache. Saves while idle or when a
  switch would lose a conversation, restores from disk when it is no longer in RAM, and enforces an age limit, a
  size cap and a free-space floor. Prometheus metrics and a `/requests` log. Tested with Strata v0.1.45.
