# wayback-scraper

We (can) have the internet archives at home! or Who archives the archivers?

## What it does

Downloads every archived snapshot of a domain from the [Internet Archive Wayback Machine](https://web.archive.org/) into a locally browsable directory tree, one subdirectory per timestamp:

```
<OUTPUT>/
  <YYYYMMDDHHMMSS>/
    index.html
    about/
      index.html
    images/
      logo.png
    ...
```

## Usage

```
wayback-scraper <URL> <OUTPUT> [OPTIONS]
```

| Argument | Description |
|---|---|
| `URL` | URL of the site to archive (e.g. `http://example.com`) |
| `OUTPUT` | Directory to write files into (created if absent) |
| `--verbose` / `-v` | Print a line for every request |
| `--include-exact-copies` | Save a full copy of every file even when content is identical to an earlier timestamp; disables hard-link deduplication |
| `--after TIMESTAMP` | Only download snapshots at or after this timestamp (e.g. `20100101`) |
| `--before TIMESTAMP` | Only download snapshots at or before this timestamp (e.g. `20101231235959`) |
| `--resume [OUTPUT_DIR]` | Resume from a suspended session; searches `OUTPUT_DIR/.wayback-scraper/` for suspend files (defaults to current directory) |
| `--resume-file FILE` | Resume from a specific suspend file |

## Runtime controls

While running, the process reads single-character commands from stdin:

| Input | Effect |
|---|---|
| `p` + Enter | Pause — finish the current download then wait |
| `r` + Enter | Resume downloading |
| `s` + Enter | Suspend — finish the current download, save state to disk, then exit |
| Ctrl+C | Stop after the current download completes |

Suspend writes a JSON file to `<OUTPUT>/.wayback-scraper/` and prints its path to stdout.  Pass that path to `--resume-file` (or use `--resume OUTPUT`) to pick up exactly where you left off.

This makes it easy for a parent process to throttle or coordinate multiple
scraper instances: write `p\n` / `r\n` / `s\n` to the child's stdin pipe.

## Build

```
cargo build --release
./target/release/wayback-scraper http://example.com ./output
```

Requires Rust 1.80+ (uses `std::sync::LazyLock`).

## Behavior

- **Snapshot ordering**: Snapshots are processed oldest-first; each timestamp is fully completed before moving to the next.
- **Link following**: HTML pages are crawled for links to additional same-domain resources not listed in the CDX index.
- **URL rewriting**: Internal URLs in HTML and CSS are rewritten to relative local paths so snapshots are browsable offline without a web server.
- **Banner stripping**: The Wayback Machine JS toolbar injected into archived HTML is removed.
- **Domain matching**: `www.example.com` and `example.com` are treated as the same site; all subdomains are included.
- **Rate limit**: ~4 requests per second to archive.org by default.  The inter-request delay starts at 250 ms and doubles (up to 4 s) each time a request is blocked, then decays back to 250 ms as requests succeed.
- **Retries**: Up to 4 retries on connection/timeout errors with exponential backoff (2 s → 3 s → 4.5 s → 6.75 s).
- **Circuit breaker**: 5XX responses and connection failures both count toward the circuit breaker. After 5 consecutive failures, pauses for a cooldown (60 s on trip 1, 120 s on trip 2) then resumes at 1 req/s. Aborts only after 3 trips without recovery.
- **4XX caching**: URLs that return a client error (404, 403, etc.) are skipped for the remainder of the session, avoiding redundant requests when the same broken URL is linked from multiple pages.
- **CDX cache**: The CDX index is saved to `<OUTPUT>/.wayback-scraper/cdx_<domain>.json` as each page is fetched. Subsequent runs load from this cache instead of re-querying the API.
- **Suspend / resume**: Send `s` + Enter to save the full session state (progress counters, per-timestamp position, adaptive delay, failed URLs, dedup memo) to a JSON file in `<OUTPUT>/.wayback-scraper/` and exit cleanly. Resume with `--resume-file <path>` or `--resume <OUTPUT>`. Already-downloaded files are always skipped, so re-running on the same output directory also picks up where it left off without an explicit suspend file.
- **Deduplication**: When a file's content is identical to an earlier timestamp's copy, a hard link is created instead of saving a new copy, saving disk space. Pass `--include-exact-copies` to disable this and always write independent files.
