# strata-diskcache

A small reverse proxy that gives [Strata](https://github.com/Niko1221/Strata) a disk cache for conversations.

Strata can keep a few long conversations parked in RAM (`--conversation-cache-mib`), so switching between them is
cheap. Once one is pushed out, coming back to it means reading the whole prompt again. That takes about 9 s at 17K
tokens on an RTX 5070, and far longer at 200K. Strata can also save and restore a conversation as a file
(`POST /slots/0?action=save|restore`), but it never does that on its own. This proxy does it for you: it sits in
front of Strata, saves long conversations to disk, and restores one from disk when it comes back and is no longer
in RAM.

Clients don't change anything. Point them at the proxy instead of Strata.

## Quick start

You need Go 1.26 or newer and a Strata server with session files turned on (`"slot_save_path"` in its config, or
`--slot-save-path DIR`) and `parallel` at 1.

```sh
go install github.com/tfyl/strata-diskcache/cmd/strata-diskcache@latest

# Strata on 127.0.0.1:8081, session files in /var/lib/strata/sessions
strata-diskcache --upstream http://127.0.0.1:8081 --session-dir /var/lib/strata/sessions
```

Clients now use `http://<host>:8080` the way they used Strata. Without `--session-dir` it is a plain pass-through
proxy.

Run it as the same user as Strata, so it can delete the files Strata writes. `contrib/strata-diskcache.service` is
an example systemd unit.

If Strata had been listening on all interfaces and you move it to loopback, add the names clients use to its
`allowed_hosts` (or `STRATA_ALLOWED_HOSTS`). The proxy passes the client's `Host` header through, and Strata's DNS
rebinding check only accepts names it knows.

## How it works

**Keying.** Each request to `/v1/chat/completions`, `/v1/messages` or `/v1/responses` is hashed as a chain:
first the system prompt, tools and reasoning settings, then one link per message (`cache_control` markers are
ignored). A later turn repeats the earlier messages, so its chain starts with the earlier turn's chain. That is how
the proxy recognises a conversation without a session id. It's only a guess: Strata matches tokens itself, so a
wrong guess costs a wasted restore, never a wrong answer.

**Restoring.** A request that continues the conversation Strata is holding goes straight through. When a request
switches to another conversation, the proxy checks whether that conversation is still parked in RAM. If it isn't
and there's a file for it (or for an earlier turn of it), the proxy restores that file first. An earlier turn still
helps: Strata only reads what came after it.

"Still parked in RAM" comes from the proxy's own list of the conversations it has seen Strata park, newest first,
cut to the `conversation_cache.parked` count that Strata reports in `/metrics`. Strata evicts the oldest first, so
the list matches as long as all traffic goes through the proxy.

**Saving.** Strata can only save the conversation it is holding live. Once a conversation is parked it can't be
saved, so the proxy can't wait until just before RAM evicts it. Instead it saves:

- **when idle.** 3 s after a turn with no new request, the live conversation is saved in the background if it has at
  least `--min-tokens` and its file is missing or more than `--regrow-tokens` behind.
- **at a switch**, but only when the conversation would be lost there. That happens when a disk restore is about to
  replace it (Strata's restore drops the live conversation without parking it), or when Strata would refuse to park
  it (bigger than the RAM budget, or not enough free RAM above `--conversation-cache-min-free-mib`).

Save and restore go through Strata's own request queue, so they never run in the middle of a generation. When more
than one request is in flight, the proxy can't know which order Strata takes them in. It passes them through
untouched and waits until a request runs alone before it trusts its view again.

**Fail-open.** If a save or restore fails, or the proxy's own logic panics, the request goes on to Strata as if the
proxy weren't there. A file Strata rejects is deleted. A file it couldn't load for lack of RAM is kept for next time.

**Files.** The proxy only touches its own files (`dc-<fingerprint>-<key>-<tokens>.session`) in the session
folder. The fingerprint covers Strata's model, version, context, KV type and control vector; when any of
those change, the old files are deleted, since Strata would refuse them anyway. Files unused for
`--retention-days` are deleted, at start and every 10 minutes. The total is capped at `--max-gib`, least recently
used first. Before each save it evicts until the new file fits under the cap and leaves `--min-free-mib` free on
the disk, and if it still doesn't fit, it skips the save. Empty files and leftovers of saves that died halfway are
cleaned up at start.

[docs/design.md](docs/design.md) lists the edge cases that matter (concurrent requests, disconnects, restarts,
corrupt files, full disks), what the proxy does in each, and the test that covers it.

## Architecture

```
cmd/strata-diskcache   main: flags, logging, metrics registry, servers, shutdown
internal/proxy         the reverse proxy, the cache policy and the conversation keys
internal/store         the session files: index, age limit, size cap, free-space floor
internal/strata        client for Strata's slot API, /v1/status and /metrics (stratatest: a fake for tests)
```

## Numbers

Measured with Strata v0.1.45, Qwen3.8-Flash-Next IQ3_S at 262K context, an RTX 5070 (12 GB) and 64 GB of RAM.
Strata ran with a 3.5 GiB, 4-slot RAM cache. Conversations of about 16.7K tokens, time to first token as the client
sees it:

| | first token |
| --- | ---: |
| fresh (nothing cached) | 8.8-9.0 s |
| resume from RAM | 0.48 s |
| resume from disk (after six other conversations pushed it out of RAM) | 0.47 s |

- A 16.7K-token conversation is a 423 MB file. Saving it takes about 0.43 s and restoring it about 0.30 s.
- A request that arrives while a background save is running waits for it. That was about +0.3 s in the test.
- The proxy adds about 0.2 ms per request. A generation request also asks Strata for its status first, about
  0.5 ms on loopback.

These were measured with the first release, which had the same save and restore policy.

## Configuration

Each flag can also be set through its environment variable. The flag wins.

| flag | env | default | |
| --- | --- | --- | --- |
| `--listen` | `LISTEN` | `:8080` | address clients connect to |
| `--upstream` | `UPSTREAM` | `http://127.0.0.1:8081` | Strata's URL |
| `--session-dir` | `SESSION_DIR` | (empty) | Strata's `slot_save_path`. Empty means pass-through only |
| `--max-gib` | `MAX_GIB` | `40` | total size cap for the files |
| `--retention-days` | `RETENTION_DAYS` | `7` | delete files unused for this long |
| `--min-free-mib` | `MIN_FREE_MIB` | `4096` | free space to keep on the disk; set it to Strata's `--session-min-free-mib` |
| `--min-tokens` | `MIN_TOKENS` | `15000` | don't save shorter conversations |
| `--regrow-tokens` | `REGROW_TOKENS` | `4096` | save again once a conversation has grown this much past its file |
| `--idle-save-s` | `IDLE_SAVE_S` | `3` | quiet seconds after a turn before saving; `0` saves only when a switch would lose it |
| `--park-min-tokens` | `PARK_MIN_TOKENS` | `-1` | Strata's `--conversation-cache-min-tokens`; `-1` reads it from Strata |
| `--snapshot-bytes-per-token` | `SNAPSHOT_BYTES_PER_TOKEN` | `65536` | RAM a parked conversation takes per token, used to predict when Strata will refuse to park one |
| `--max-body-mib` | `MAX_BODY_MIB` | `64` | larger request bodies are streamed through without caching |
| `--shutdown-s` | `SHUTDOWN_S` | `20` | on SIGTERM, how long running requests and a save under way get to finish |
| `--metrics-listen` | `METRICS_LISTEN` | `127.0.0.1:8082` | where `/metrics`, `/requests` and `/health` are served. Keep it private |
| `--log-output` | `LOG_OUTPUT` | `stderr` | `stderr`, `file` or `both` |
| `--log-file` | `LOG_FILE` | `/var/log/strata-diskcache/strata-diskcache.log` | the log file, for `file` and `both` |
| `--log-format` | `LOG_FORMAT` | `text` | `text` or `json` |
| `--log-level` | `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error` |
| `--log-max-size-mib` | `LOG_MAX_SIZE_MIB` | `100` | rotate the log file at this size |
| `--log-max-backups` | `LOG_MAX_BACKUPS` | `10` | rotated files to keep (`0` keeps all) |
| `--log-max-age-days` | `LOG_MAX_AGE_DAYS` | `30` | delete rotated files older than this (`0` never) |
| `--log-compress` | `LOG_COMPRESS` | `true` | gzip rotated files (`--no-log-compress` to turn off) |

## Metrics and logs

`/metrics` on `--metrics-listen` is Prometheus text, with the Go runtime and process metrics alongside:

- `strata_diskcache_requests_total{action}`: what happened to each generation request. `continue` means it
  continued the live conversation, `ram` a switch to a parked one, `disk` a restore from file, `miss` no copy
  anywhere, `busy` other requests in flight, `unkeyed` a body it couldn't key, `pass` Strata didn't answer, `error`
  the proxy's own failure.
- `strata_diskcache_saves_total`, `strata_diskcache_saves_by_trigger_total{trigger=idle|switch}`,
  `strata_diskcache_saves_skipped_total{reason}`, `strata_diskcache_restores_total`.
- `strata_diskcache_saved_bytes_total`, `strata_diskcache_restored_bytes_total`, the histograms
  `strata_diskcache_save_seconds` and `strata_diskcache_restore_seconds`, `strata_diskcache_last_save_ms`,
  `strata_diskcache_last_restore_ms`.
- `strata_diskcache_failures_total{op,reason}`, `strata_diskcache_panics_total`.
- `strata_diskcache_evictions_total{reason}`, `strata_diskcache_evicted_bytes_total{reason}`,
  `strata_diskcache_files`, `strata_diskcache_bytes`, `strata_diskcache_max_bytes`.

`/requests` lists the last 256 generation requests with the client address, User-Agent and cache action. Strata
only sees the proxy's address, so a tracer that reads Strata's request log can join these on by path and start time.

Logs are structured (`slog`). Every generation request gets one `request` line at info level, with the path,
client, action, reason, tokens, how long the cache took and the whole request took, the status, and whether the
client went away. Saves, restores, skipped saves and evictions get a line each. Prompt and answer text is never
logged. A log file is appended to across restarts, rotated by size, gzipped, and pruned by count and age.

## Limitations

- Needs Strata's `parallel` at 1. Strata refuses session files with batch slots.
- All traffic has to go through the proxy. Requests sent to Strata directly make its view of what's live wrong, which
  only costs speed until the next request runs alone.
- A conversation that parks in RAM and gets evicted before it has had a quiet moment has no file. Coming back to it
  means a fresh read, the same as without the proxy.
- A background save holds Strata's queue for its duration: about 26 ms per 1K tokens, so several seconds for a
  200K-token conversation.
- Images in prompts are keyed by their bytes. Requests that rely on server-side state (`previous_response_id`) are
  passed through without caching.
- Files hold the engine's state and are tied to the Strata build, model and settings that wrote them. After an
  upgrade Strata rejects old files and the proxy deletes them.

## Compatibility

Tested with Strata v0.1.45 on Linux. It relies on `POST /slots/0?action=save|restore`, `/v1/status`
(`last_timings`) and the JSON `/metrics` (`conversation_cache`, `engine`). If a later Strata changes those, the
proxy fails open and becomes a plain pass-through.

## Development

```sh
make test    # gofmt check, go vet, go test -race
make lint    # golangci-lint
make build   # ./strata-diskcache
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT, like Strata. See [LICENSE](LICENSE).
