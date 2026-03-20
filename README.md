# wayback-scraper

Downloads locally browsable snapshots of a website from the [Internet Archive Wayback Machine](https://web.archive.org/).

## What it does

1. Queries the CDX API for every archived snapshot of the target domain (including subdomains).
2. Processes snapshots oldest-first, fully completing each timestamp before moving to the next.
3. For each snapshot, downloads all captured files and follows links in HTML pages to discover additional same-domain resources.
4. Rewrites internal URLs in HTML and CSS to relative local paths so snapshots are browsable without a web server.
5. Strips the Wayback Machine JS banner injected into archived HTML pages.
6. Skips files already on disk — runs are resumable.

Output layout:

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

## Behaviour

- **Rate limit**: ~2 requests per second to archive.org.
- **Retries**: Up to 4 retries on connection/timeout errors with exponential backoff (5 s → 10 s → 20 s → 40 s).
- **Circuit breaker**: Aborts after 5 consecutive exhausted retries, indicating a sustained IP block.
- **CDX cache**: The CDX index is saved to `<OUTPUT>/.wayback-scraper/cdx_<domain>.json` as each page is fetched. Subsequent runs load from this cache instead of re-querying the API.
- **Deduplication**: When a file's content is identical to an earlier timestamp's copy, a hard link is created instead of saving a new copy, saving disk space. Pass `--include-exact-copies` to disable this and always write independent files.
- **Resumable**: Already-downloaded files are skipped. Re-running on the same output directory picks up where it left off and re-crawls cached HTML pages for new links.
- **Domain matching**: `www.example.com` and `example.com` are treated as the same site; all subdomains are included.
