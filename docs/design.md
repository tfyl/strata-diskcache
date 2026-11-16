# Design notes

## What Strata gives us (v0.1.45)

- `POST /slots/0?action=save` writes the **live** conversation (the last request's) to a file. A conversation
  parked in RAM can't be saved.
- `action=restore` loads a file as the live conversation and **drops** the one that was live; it isn't parked.
- Both run in Strata's request queue, between requests, so they never interleave with a generation.
- A file is tied to the model and engine settings that wrote it; Strata refuses others. It writes under a
  temporary name and renames into place.
- The RAM cache parks a conversation when another comes in, evicting the oldest. It refuses one bigger than its
  budget or one that would leave less than its RAM floor.
- Strata matches prompts by tokens, so a file of an earlier turn still saves everything up to that turn.
- With `--lazy` / `idle_unload_s` the engine goes away when idle: `/metrics` reports no context and no KV type,
  slot calls answer 503 `not loaded`, and a generation request loads it again.

## Policy

A continuing turn passes straight through. On a switch, the incoming conversation's file is restored unless the
conversation is believed parked in RAM. The outgoing one is saved at the switch only when it would be lost there:
a restore is about to drop it, or Strata would refuse to park it. Otherwise it's saved after `--idle-save-s` of
quiet, if its file is more than `--regrow-tokens` behind.

Every choice affects speed only. Strata decides what to reuse from the tokens, so a wrong guess costs a wasted
restore or a fresh read, never a wrong answer. That makes failing open safe.

## Edge cases that matter

| case | behaviour | test |
| --- | --- | --- |
| Any error or panic in the cache | the request goes through as if the proxy weren't there | `TestFailOpen`, `TestRestoreFailure` |
| Strata version, model or settings changed | new fingerprint; files of the old one are deleted | `TestStrataRestartAndFingerprint`, `TestFingerprint` |
| Partial, truncated or corrupt files | Strata writes atomically; leftover temporaries, empty files and old formats go at start; a file whose size changed is dropped before restore; one Strata rejects is deleted (one it lacked RAM for is kept) | `TestLoadCleansUp`, `TestCommitLookupAndCorruptFiles`, `TestRestoreFailure` |
| Disk nearly full, or the cap too small | the oldest files go until a save fits under the cap and above the free-space floor; if it can't, no save | `TestDiskFloorWins`, `TestCapAndLRU` |
| Overlapping requests (incl. one arriving mid-save or mid-restore) | passed through untouched; the live conversation is forgotten until a request runs alone | `TestOverlappingRequests`, `TestRequestDuringIdleSave` |
| Client cancels mid-stream or mid-restore | the restore finishes and the file stays; a cut-off answer isn't trusted as live | `TestClientCancel` |
| Strata restarted, its engine unloaded, or a 5xx | the RAM view and the live conversation reset; files still restore | `TestStrataRestartAndFingerprint`, `TestUnloadedEngineKeepsFiles` |
| The engine is unloaded (`--lazy`, `idle_unload_s`) | `/metrics` of an unloaded engine is not read as a fingerprint, so the files stay; a restore refused with 503 `not loaded` wakes the engine with a 1-token request, checks its fingerprint, and retries the restore once; a refused wakeup or another fingerprint is reported as what it is; with no file to restore, the request passes through | `TestColdEngineWakesAndRestores`, `TestColdEngineNoFilePassesThrough`, `TestWokenEngineWithNewFingerprint`, `TestWakeupFailurePropagates` |
| A file that restores far fewer tokens than its name claims | forgotten, so the conversation is saved afresh instead of being skipped as on disk | `TestTruncatedSessionFile` |
| The client closes the socket after the stream's terminal event (`data: [DONE]` on its own line, a `finish_reason`, `message_stop`, `response.completed`) | a complete answer: the live conversation is kept and saved; the marker is searched in every chunk written, so a `response.completed` followed by kilobytes counts, and `[DONE]` quoted inside streamed content does not; a cancel while the stream runs is still a cancellation and is not trusted | `TestClientGoneAfterCompletion`, `TestClientCancel` |
| Body over `--max-body-mib`, malformed JSON, other endpoints | passed through unbuffered or as they are; anything that may change Strata's state makes the live conversation unknown | `TestFailOpen` |
| SIGTERM mid-save | requests and the save get `--shutdown-s`; Strata completes a save cut off later, and the next start indexes it | `TestShutdownMidSave` |
| Prompt or answer text | never logged | `TestDialectsAndNoContentInLogs` |

## Known gaps

- A conversation that parks and is evicted before a quiet moment has no file. That's the same as running without
  the proxy.
- The RAM view assumes all traffic goes through the proxy.
- An edit to an earlier message shares too little to restore anything, so it's read fresh.
