# Suspend-to-Disk Design

**Date:** 2026-05-26
**Status:** Approved

## Overview

Adds the ability to suspend a running wayback-scraper session to disk (`s` key) and resume it later (`--resume` / `--resume-file`). The full download state is serialised to a JSON file so the session can be continued identically — including dedup hard-link tracking and the 4XX URL cache.

## State Serialisation

A `SuspendState` struct (with `serde::Serialize/Deserialize`) is written as JSON to:

```
<output>/.wayback-scraper/suspend_<apex>_<last_timestamp>.json
```

### Fields

| Field | Type | Description |
|---|---|---|
| `version` | `u32` | Schema version (currently 1) |
| `suspended_at` | `String` | ISO 8601 datetime, informational |
| `apex` | `String` | Normalised domain (e.g. `example.com`) |
| `args` | `SavedArgs` | Original CLI args (url, output, verbose, include_exact_copies, after, before) |
| `last_timestamp` | `String` | Wayback timestamp that was in progress when `s` was hit |
| `current_delay_ms` | `u64` | Adaptive throttle state |
| `failed_urls` | `HashSet<String>` | 4XX URL cache |
| `memo` | `HashMap<String, (PathBuf, u64)>` | Dedup hash map |
| `ts_done` | `usize` | Cumulative stat |
| `downloaded` | `usize` | Cumulative stat |
| `linked` | `usize` | Cumulative stat |
| `skipped` | `usize` | Cumulative stat |
| `errors` | `usize` | Cumulative stat |
| `discovered` | `usize` | Cumulative stat |
| `total_bytes` | `u64` | Cumulative stat |
| `total_saved` | `u64` | Cumulative stat |

`existing` (the set of on-disk files) is **not** serialised — it is always rebuilt fresh via `scan_existing_files` on resume.

`Cargo.toml` gains `serde = { version = "1", features = ["derive"] }`. (`serde_json` is already present.)

## CLI Changes

`url` and `output` become `Option<String>` / `Option<PathBuf>` with `required_unless_present_any = ["resume", "resume_file"]`. In normal mode Clap enforces them; in resume mode they are omitted.

### New flags

```
--resume / -r [OUTPUT_DIR]
    Optional path argument (num_args = 0..=1, default_missing_value = ".").
    Searches OUTPUT_DIR/.wayback-scraper/ for suspend_*.json files.
    • Exactly one found  → loads automatically, prints which file.
    • Multiple found     → numbered list + "Choose [1-N]:" prompt.
    • None found         → error: "No suspend files found in <dir>/.wayback-scraper/"
    Conflicts with: positional args, --resume-file.

--resume-file <PATH>
    Loads a specific suspend file. No interactive prompt.
    Conflicts with: positional args, --resume.
```

The controls line printed at startup gains `'s' + Enter to suspend`.

## Suspend Flow

1. A new `suspending: Arc<AtomicBool>` is created alongside `paused` and `shutdown`.
2. The stdin task gains a new branch: `"s"` sets `suspending = true` and logs `"Suspending — finishing current download…"`.
3. After each URL completes in the inner loop, `suspending` is checked at the same points as `shutdown`. When set, the inner loop breaks (the current download has already written its file cleanly).
4. `save_suspend_state()` is called:
   - Serialises `SuspendState` to JSON.
   - Writes to `<output>/.wayback-scraper/suspend_<apex>_<last_timestamp>.json` (overwrites silently if same name exists).
   - Prints the full path.
5. Prints the summary line and exits cleanly (`Ok(())`).

**Edge case — no progress yet** (`last_timestamp` is empty): prints `"Nothing to suspend yet — no timestamps processed."` and exits cleanly. Does **not** write a file.

## Resume Flow

1. **Locate file**: `--resume-file` uses the path directly. `--resume [dir]` globs `<dir>/.wayback-scraper/suspend_*.json` sorted by modification time (newest first).
2. **Load**: Deserialise JSON into `SuspendState`. Version > 1 prints a warning but continues. Deserialisation failure prints the error and exits non-zero.
3. **Reconstruct args**: `url`, `output`, `verbose`, `include_exact_copies`, `after`, `before` are taken from `state.args`. Any other flags passed on the resume command line (e.g. `--verbose`) are ignored — the saved args govern the session.
4. **Load CDX**: Read `<output>/.wayback-scraper/cdx_<apex>.json`. If missing, exit with: `"Cannot resume: CDX cache not found at <path>."` Filter entries to `timestamp >= state.last_timestamp` and load into `by_timestamp`.
5. **Restore state**: `current_delay_ms`, `failed_urls`, `memo`, and all stat counters are initialised from the suspend file.
6. **Announce**: `"Resuming from suspend — continuing at timestamp <last_timestamp>"`.
7. Normal operation continues — pause/resume (`p`/`r`), circuit breaker, and CDX pagination all work identically.

## Error Handling Summary

| Situation | Behaviour |
|---|---|
| `s` pressed, `last_timestamp` empty | Print message, exit cleanly, no file written |
| Suspend file overwrite (same name) | Silent overwrite |
| `--resume` finds no files | Error with directory checked |
| `--resume` finds one file | Auto-load, print filename |
| `--resume` finds multiple files | Numbered prompt |
| Corrupt suspend file (bad JSON) | Print serde error, exit non-zero |
| CDX cache missing on resume | Print clear error, exit non-zero |

## Implementation Notes

- `last_timestamp` is set at the **start** of each timestamp group, so it holds the in-progress timestamp when `s` fires. Resume uses `>=` (not `>`) so the partial timestamp is replayed; `existing` skips already-downloaded files within it. HTML-discovered extra links are re-extracted from cached HTML via the `CachedHtml` path.
- No new dependencies beyond `serde` with `derive` feature.
- The `--resume` interactive prompt reads from real stdin (not the async stdin used for `p`/`r`/`s` during a run). This is fine because the prompt runs before the tokio runtime's download tasks start.
