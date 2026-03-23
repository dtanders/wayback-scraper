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
- **Circuit breaker**: Aborts after 5 consecutive exhausted retries, indicating a sustained IP block.
- **CDX cache**: The CDX index is saved to `<OUTPUT>/.wayback-scraper/cdx_<domain>.json` as each page is fetched. Subsequent runs load from this cache instead of re-querying the API.
- **Resumable**: Already-downloaded files are skipped. Re-running on the same output directory picks up where it left off and re-crawls cached HTML pages for new links.
- **Deduplication**: When a file's content is identical to an earlier timestamp's copy, a hard link is created instead of saving a new copy, saving disk space. Pass `--include-exact-copies` to disable this and always write independent files.
