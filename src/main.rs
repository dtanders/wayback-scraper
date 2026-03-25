use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;

macro_rules! log {
    () => { eprintln!() };
    ($($arg:tt)*) => {
        eprintln!("[{}] {}", Local::now().format("%H:%M:%S"), format_args!($($arg)*))
    };
}
use lol_html::{comments, element, HtmlRewriter, Settings};
use regex::Regex;
use reqwest::Client;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::sleep;
use url::Url;

const WAYBACK_CDX: &str = "https://web.archive.org/cdx/search/cdx";
const WAYBACK_WEB: &str = "https://web.archive.org/web";

/// Minimum inter-request delay (4 requests / second).  The actual delay
/// grows automatically when archive.org starts throttling and decays back
/// to this floor once requests succeed again.
const MIN_REQUEST_DELAY_MS: u64 = 250;

/// Maximum inter-request delay the adaptive throttle will reach.
const MAX_REQUEST_DELAY_MS: u64 = 4_000;

/// Nominal request rate derived from the minimum delay.
const REQUEST_RATE: u64 = 1_000 / MIN_REQUEST_DELAY_MS;

/// Retry up to this many times on transient connection errors.
const MAX_RETRIES: u32 = 4;

/// First retry waits this long; each subsequent retry is 1.5× the previous.
/// 2 s → 3 s → 4.5 s → 6.75 s
const RETRY_BASE_MS: u64 = 2_000;

/// Abort the run if this many consecutive requests exhaust all retries.
/// Indicates a sustained IP block rather than isolated failures.
const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;

/// CDX records per API page.
const CDX_PAGE_SIZE: u32 = 10_000;

// ─── Regexes ─────────────────────────────────────────────────────────────────

/// Rewrite CSS url() references (handles quoted and unquoted forms).
static CSS_URL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)(url\(\s*['"]?)([^'"\)\s]+)(['"]?\s*\))"#).unwrap());

/// Strip the Wayback Machine wrapper from a URL.
static WAYBACK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)https?://web\.archive\.org/web/\d+[^/]*/(.+)").unwrap());

// ─── CLI ─────────────────────────────────────────────────────────────────────

// long_about is built at runtime in main() so it can reference REQUEST_RATE.
static LONG_ABOUT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "Queries the Wayback Machine CDX API for every snapshot of <URL> (including \
subdomains), downloads each captured file, rewrites internal URLs to relative \
local paths, and stores everything under <OUTPUT> as:\n\n  \
  <OUTPUT>/<YYYYMMDDHHMMSS>/<url-path>\n\n\
Each timestamp directory is a self-contained, locally browsable copy of the \
site as it appeared at that moment.  HTML and CSS files have their same-domain \
links rewritten to relative paths so they work without a web server.  After \
each HTML page is saved, its links are parsed and any same-domain resources \
not already queued are fetched at the same snapshot timestamp.\n\n\
Already-downloaded files are skipped.  Requests to archive.org are \
rate-limited to roughly {REQUEST_RATE} per second, backing off automatically if throttled."
    )
});

#[derive(Parser, Debug)]
#[command(
    name = "wayback-scraper",
    about = "Create locally browsable snapshots of a site from the Internet Archive"
)]
struct Args {
    /// URL of the site to archive (e.g. https://example.com)
    url: String,

    /// Root directory for downloaded files
    output: PathBuf,

    /// Print detailed progress for every request
    #[arg(short, long)]
    verbose: bool,

    /// Save a separate copy of each file even when its content is identical to
    /// an earlier timestamp's copy.  By default, duplicates are replaced with
    /// hard links to save disk space.
    #[arg(long)]
    include_exact_copies: bool,

    /// Only download snapshots at or after this timestamp (YYYYMMDDHHMMSS or
    /// any prefix, e.g. 20100101).
    #[arg(long, value_name = "TIMESTAMP")]
    after: Option<String>,

    /// Only download snapshots at or before this timestamp (YYYYMMDDHHMMSS or
    /// any prefix, e.g. 20101231235959).
    #[arg(long, value_name = "TIMESTAMP")]
    before: Option<String>,
}

// ─── Domain helpers ───────────────────────────────────────────────────────────

/// Strip leading `www.` and lower-case the host.
fn normalize_host(host: &str) -> String {
    let h = host.trim_end_matches('.');
    h.strip_prefix("www.").unwrap_or(h).to_lowercase()
}

/// Return the normalised apex domain from an arbitrary URL string.
fn apex_from_url(url_str: &str) -> Result<String> {
    let u = Url::parse(url_str).with_context(|| format!("invalid URL: {url_str}"))?;
    let host = u.host_str().context("URL has no host")?;
    Ok(normalize_host(host))
}

/// True when `url_str`'s normalised host equals `apex` or is a subdomain of it.
fn matches_domain(url_str: &str, apex: &str) -> bool {
    let Ok(u) = Url::parse(url_str) else {
        return false;
    };
    let Some(host) = u.host_str() else {
        return false;
    };
    let n = normalize_host(host);
    n == apex || n.ends_with(&format!(".{apex}"))
}

// ─── Path helpers ─────────────────────────────────────────────────────────────

fn ts_to_dir(ts: &str) -> &str {
    ts
}

fn sanitize_component(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect()
}

/// Derive a relative `PathBuf` from a URL's path component (for filesystem use).
fn url_to_rel_path(url_str: &str) -> PathBuf {
    let Ok(u) = Url::parse(url_str) else {
        return PathBuf::from("unknown");
    };
    let raw = u.path();
    let mut pb = PathBuf::new();
    for seg in raw.split('/') {
        if seg.is_empty() || seg == "." || seg == ".." {
            continue;
        }
        pb.push(sanitize_component(seg));
    }
    if raw.ends_with('/') || pb.as_os_str().is_empty() {
        pb.push("index.html");
    }
    pb
}

/// Like `url_to_rel_path` but returns a forward-slash string suitable for use
/// in HTML/CSS URLs and relative-path arithmetic.
fn url_to_local_str(url_str: &str) -> Option<String> {
    let u = Url::parse(url_str).ok()?;
    let raw = u.path();
    let parts: Vec<String> = raw
        .split('/')
        .filter(|s| !s.is_empty() && *s != "." && *s != "..")
        .map(sanitize_component)
        .collect();

    if raw.ends_with('/') || parts.is_empty() {
        let mut p = parts;
        p.push("index.html".to_string());
        Some(p.join("/"))
    } else {
        Some(parts.join("/"))
    }
}

/// Compute the relative path (using `/`) from `from_file` to `to_file`,
/// where both are forward-slash snapshot-relative paths (e.g. `about/index.html`).
fn rel_path_from_to(from_file: &str, to_file: &str) -> String {
    let from_parts: Vec<&str> = from_file.split('/').collect();
    let to_parts: Vec<&str> = to_file.split('/').collect();

    // Directory containing from_file.
    let from_dir = &from_parts[..from_parts.len().saturating_sub(1)];

    // Common prefix length between from_dir and to_parts.
    let common = from_dir
        .iter()
        .zip(to_parts.iter())
        .take_while(|(a, b)| a == b)
        .count();

    let ups = from_dir.len() - common;
    let mut result: Vec<&str> = (0..ups).map(|_| "..").collect();
    result.extend_from_slice(&to_parts[common..]);

    if result.is_empty() {
        ".".to_string()
    } else {
        result.join("/")
    }
}

// ─── URL rewriting ────────────────────────────────────────────────────────────

/// If `url` is wrapped in a Wayback Machine path, return the original URL.
/// Strip any Wayback Machine wrapper from a URL, returning the original.
///
/// Handles two forms:
/// * Standard — `https://web.archive.org/web/{ts}{mod}/{original}`
/// * Embedded — `http://any.host/web/{ts}{mod}/http[s]_/{original}`
///   (Wayback replaces `://` with `_/` in embedded resource paths when the
///   page is fetched without the `id_` modifier.)
fn unwrap_wayback(url: &str) -> String {
    // Standard web.archive.org wrapper.
    if let Some(caps) = WAYBACK_RE.captures(url) {
        return caps[1].to_string();
    }

    // Embedded form: /{anything}/web/{timestamp}{modifier}/http[s][_:]//{rest}
    // e.g. http://www.rifters.org/web/20090306084941im_/http_/www.rifters.org/img.png
    static EMBEDDED: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)/web/\d{8,14}[a-z_]*/(https?)[_:]/+(.+)$").unwrap());
    if let Some(caps) = EMBEDDED.captures(url) {
        let scheme = &caps[1]; // "http" or "https"
        let rest = &caps[2]; // "www.rifters.org/img.png"
        return format!("{scheme}://{rest}");
    }

    url.to_string()
}

/// Try to rewrite a single URL found in content to a local relative path.
///
/// Returns `None` when the URL should be left unchanged (external, non-HTTP, etc.).
fn rewrite_url(raw: &str, base_url: &str, file_in_snapshot: &str, apex: &str) -> Option<String> {
    let raw = raw.trim();

    if raw.is_empty()
        || raw.starts_with('#')
        || raw.starts_with("javascript:")
        || raw.starts_with("data:")
        || raw.starts_with("mailto:")
        || raw.starts_with("tel:")
    {
        return None;
    }

    // Preserve the fragment so page anchors still work locally.
    let (url_part, fragment) = match raw.find('#') {
        Some(i) => (&raw[..i], &raw[i..]),
        None => (raw, ""),
    };

    // Strip any Wayback wrapper.
    let url_part = unwrap_wayback(url_part);

    // Resolve to absolute URL.
    let absolute = if url_part.starts_with("http://") || url_part.starts_with("https://") {
        url_part
    } else {
        let base = Url::parse(base_url).ok()?;
        base.join(&url_part).ok()?.to_string()
    };

    if !matches_domain(&absolute, apex) {
        return None;
    }

    let target = url_to_local_str(&absolute)?;
    let rel = rel_path_from_to(file_in_snapshot, &target);
    Some(format!("{rel}{fragment}"))
}

/// Rewrite all href/src/action/data-src attributes in an HTML document using a
/// proper HTML parser, and strip the Wayback Machine JS banner injected into
/// the `<head>` (everything up to `<!-- End Wayback Rewrite JS Include -->`).
fn rewrite_html(content: &str, base_url: &str, file_in_snapshot: &str, apex: &str) -> String {
    // Shared flag: true while we're still inside the Wayback banner block.
    let in_banner: Rc<Cell<bool>> = Rc::new(Cell::new(false));
    let ib_head = in_banner.clone();
    let ib_elem = in_banner.clone();
    let ib_cmnt = in_banner.clone();

    let base_url = base_url.to_string();
    let file_in_snapshot = file_in_snapshot.to_string();
    let apex = apex.to_string();

    let mut output = Vec::with_capacity(content.len());

    let mut rewriter = HtmlRewriter::new(
        Settings {
            element_content_handlers: vec![
                // Banner: set flag when <head> opens.
                element!("head", move |_el| {
                    ib_head.set(true);
                    Ok(())
                }),
                // Banner: remove injected script/link/style/noscript elements.
                element!(
                    "head script, head link, head style, head noscript",
                    move |el| {
                        if ib_elem.get() {
                            el.remove();
                        }
                        Ok(())
                    }
                ),
                // Banner: remove comments inside <head>; clear flag at end marker.
                comments!("head", move |comment| {
                    if ib_cmnt.get() {
                        if comment.text().contains("End Wayback Rewrite JS Include") {
                            ib_cmnt.set(false);
                        }
                        comment.remove();
                    }
                    Ok(())
                }),
                // URL rewriting: rewrite same-domain attributes to relative paths.
                element!("[href],[src],[action],[data-src]", move |el| {
                    for attr in &["href", "src", "action", "data-src"] {
                        if let Some(val) = el.get_attribute(attr) {
                            if let Some(new) =
                                rewrite_url(&val, &base_url, &file_in_snapshot, &apex)
                            {
                                el.set_attribute(attr, &new)?;
                            }
                        }
                    }
                    Ok(())
                }),
            ],
            ..Settings::default()
        },
        |c: &[u8]| output.extend_from_slice(c),
    );

    if rewriter.write(content.as_bytes()).is_err() || rewriter.end().is_err() {
        return content.to_owned();
    }

    String::from_utf8_lossy(&output).into_owned()
}

/// Rewrite all url() references in a CSS string.
fn rewrite_css(content: &str, base_url: &str, file_in_snapshot: &str, apex: &str) -> String {
    CSS_URL_RE
        .replace_all(content, |caps: &regex::Captures| {
            let open = &caps[1]; // `url(` or `url("` or `url('`
            let raw_url = &caps[2];
            let close = &caps[3]; // `)` or `")` or `')`
            let new_url = rewrite_url(raw_url, base_url, file_in_snapshot, apex)
                .unwrap_or_else(|| raw_url.to_string());
            format!("{open}{new_url}{close}")
        })
        .into_owned()
}

// ─── Link extraction ──────────────────────────────────────────────────────────

/// True if the first bytes look like HTML.
fn looks_like_html(bytes: &[u8]) -> bool {
    let head = std::str::from_utf8(&bytes[..bytes.len().min(512)])
        .unwrap_or("")
        .to_ascii_lowercase();
    head.contains("<!doctype html") || head.contains("<html") || head.contains("<head")
}

/// Parse content for href/src/action/data-src values, resolve them against
/// `base_url`, and return absolute URLs that belong to `apex`.
/// Query strings and fragments are stripped so different variants of the same
/// path map to the same local file.
fn extract_links(content: &[u8], base_url: &str, apex: &str) -> Vec<String> {
    let Ok(html) = std::str::from_utf8(content) else {
        return vec![];
    };

    let base = Url::parse(base_url).ok();
    let apex = apex.to_string();
    let links: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let links_c = links.clone();

    let mut rewriter = HtmlRewriter::new(
        Settings {
            element_content_handlers: vec![element!(
                "[href],[src],[action],[data-src]",
                move |el| {
                    for attr in &["href", "src", "action", "data-src"] {
                        let Some(raw) = el.get_attribute(attr) else {
                            continue;
                        };
                        let raw = raw.trim().to_string();
                        if raw.starts_with('#')
                            || raw.starts_with("javascript:")
                            || raw.starts_with("data:")
                        {
                            continue;
                        }
                        let resolved = if raw.starts_with("http://") || raw.starts_with("https://")
                        {
                            unwrap_wayback(&raw)
                        } else if let Some(ref b) = base {
                            // Unwrap after resolution: catches relative embedded Wayback paths.
                            match b.join(&raw) {
                                Ok(u) => unwrap_wayback(u.as_ref()),
                                Err(_) => continue,
                            }
                        } else {
                            continue;
                        };
                        if !matches_domain(&resolved, &apex) {
                            continue;
                        }
                        if let Ok(mut u) = Url::parse(&resolved) {
                            u.set_query(None);
                            u.set_fragment(None);
                            links_c.borrow_mut().push(u.to_string());
                        }
                    }
                    Ok(())
                }
            )],
            ..Settings::default()
        },
        |_: &[u8]| {},
    );

    let _ = rewriter.write(html.as_bytes());
    let _ = rewriter.end();

    let mut result = Rc::try_unwrap(links).unwrap().into_inner();
    result.sort_unstable();
    result.dedup();
    result
}

// ─── CDX fetching ─────────────────────────────────────────────────────────────

/// Fetch one page of (timestamp, original_url) pairs from the CDX API.
/// Returns the entries for this page; the caller determines whether more
/// pages exist by checking whether the returned length equals `CDX_PAGE_SIZE`.
async fn fetch_cdx_page(
    client: &Client,
    apex: &str,
    offset: u32,
    verbose: bool,
    after: Option<&str>,
    before: Option<&str>,
) -> Result<Vec<(String, String)>> {
    let mut req_url = format!(
        "{WAYBACK_CDX}?url={apex}&matchType=domain\
         &output=json&fl=timestamp,original\
         &limit={CDX_PAGE_SIZE}&offset={offset}"
    );
    if let Some(ts) = after {
        req_url.push_str(&format!("&from={ts}"));
    }
    if let Some(ts) = before {
        req_url.push_str(&format!("&to={ts}"));
    }

    if verbose {
        log!("[CDX] GET {req_url}");
    } else {
        log!("Fetching CDX index (offset {offset})…");
    }

    let resp = {
        let mut attempt = 0u32;
        loop {
            match client.get(&req_url).send().await {
                Ok(r) => break r,
                Err(e) if attempt < MAX_RETRIES && (e.is_connect() || e.is_timeout()) => {
                    attempt += 1;
                    let delay_ms = (RETRY_BASE_MS as f64 * 1.5f64.powi(attempt as i32 - 1)) as u64;
                    log!("[CDX RETRY {attempt}/{MAX_RETRIES}] {e} — waiting {delay_ms}ms");
                    sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(e) => return Err(e).context("CDX request failed"),
            }
        }
    };

    if !resp.status().is_success() {
        log!("CDX API returned {}: returning empty page", resp.status());
        return Ok(vec![]);
    }

    let body: serde_json::Value = resp.json().await.context("CDX JSON parse failed")?;

    let rows = match body.as_array() {
        Some(a) if a.len() > 1 => a,
        _ => return Ok(vec![]),
    };

    let mut page: Vec<(String, String)> = Vec::new();
    for row in &rows[1..] {
        if let Some(fields) = row.as_array() {
            if fields.len() >= 2 {
                let ts = fields[0].as_str().unwrap_or("").to_owned();
                let orig = fields[1].as_str().unwrap_or("").to_owned();
                if !ts.is_empty() && !orig.is_empty() {
                    page.push((ts, orig));
                }
            }
        }
    }

    if verbose {
        log!("[CDX] Page contained {} entries", page.len());
    }

    Ok(page)
}

// ─── Formatting helpers ───────────────────────────────────────────────────────

fn format_bytes(n: u64) -> String {
    const KB: u64 = 1_024;
    const MB: u64 = 1_024 * KB;
    const GB: u64 = 1_024 * MB;
    if n >= GB {
        format!("{:.1} GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{:.1} KB", n as f64 / KB as f64)
    } else {
        format!("{n} B")
    }
}

// ─── Content hashing ──────────────────────────────────────────────────────────

fn hash_bytes(bytes: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

// ─── Filesystem helpers ───────────────────────────────────────────────────────

/// Walk `output` and collect every regular file under 14-digit timestamp
/// directories into a set.  Used once at startup so resume skips can be done
/// as O(1) hash-set lookups instead of per-file stat calls.
fn scan_existing_files(output: &Path) -> Result<HashSet<PathBuf>> {
    let mut existing = HashSet::new();
    let Ok(entries) = fs::read_dir(output) else {
        return Ok(existing);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.len() == 14 && name.bytes().all(|b| b.is_ascii_digit()) {
            collect_files_recursive(&path, &mut existing)?;
        }
    }
    Ok(existing)
}

fn collect_files_recursive(dir: &Path, set: &mut HashSet<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, set)?;
        } else {
            set.insert(path);
        }
    }
    Ok(())
}

/// Like `fs::create_dir_all`, but handles the case where a path component
/// already exists as a *file* when we need it to be a *directory*.
///
/// The existing file is moved to `<component>/index.html` so its content is
/// preserved.
fn ensure_dir_all(path: &Path) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            ensure_dir_all(parent)?;
        }
    }

    if path.exists() {
        let tmp = path.with_file_name(format!(
            "__tmp_{}",
            path.file_name().unwrap_or_default().to_string_lossy()
        ));
        fs::rename(path, &tmp)
            .with_context(|| format!("rename {} for promotion", path.display()))?;
        fs::create_dir(path).with_context(|| format!("mkdir {}", path.display()))?;
        fs::rename(&tmp, path.join("index.html"))
            .with_context(|| format!("promote {} to index.html", path.display()))?;
    } else {
        fs::create_dir(path).with_context(|| format!("mkdir {}", path.display()))?;
    }

    Ok(())
}

// ─── Downloading ─────────────────────────────────────────────────────────────

enum SnapshotOutcome {
    /// Freshly fetched, rewritten, and written to disk.
    Downloaded(Vec<u8>),
    /// Already on disk and HTML — bytes returned for link parsing.  No network
    /// request was made.
    CachedHtml(Vec<u8>),
    /// Already on disk (non-HTML) or is a directory — no network request made.
    SkippedLocal,
    /// Got a non-success HTTP response — a network request was made.
    Skipped,
    /// Content matched an earlier timestamp; a hard link was created instead of
    /// a new copy.  Raw bytes returned for link extraction; u64 is bytes saved.
    Hardlinked(Vec<u8>, u64),
    /// All retries were exhausted due to a connection error (IP block).
    Blocked,
}

/// Fetch a single Wayback snapshot, rewrite internal URLs to relative local
/// paths, and write the result to `output/<ts>/<rel_path>`.
#[allow(clippy::too_many_arguments)]
async fn download_snapshot(
    client: &Client,
    timestamp: &str,
    orig_url: &str,
    output: &Path,
    apex: &str,
    verbose: bool,
    dedup: bool,
    memo: &mut HashMap<String, (PathBuf, u64)>,
    existing: &mut HashSet<PathBuf>,
    failed_urls: &mut HashSet<String>,
) -> Result<SnapshotOutcome> {
    let rel = url_to_rel_path(orig_url);
    // Forward-slash version used for URL arithmetic inside rewriting.
    let file_in_snapshot = rel.to_string_lossy().replace('\\', "/");
    let dest = output.join(ts_to_dir(timestamp)).join(&rel);

    if existing.contains(&dest) {
        if verbose {
            log!("[SKIP] {}", dest.display());
        }
        let bytes = fs::read(&dest).with_context(|| format!("read {}", dest.display()))?;
        if dedup {
            let url_key = url_to_local_str(orig_url).unwrap_or_default();
            memo.entry(url_key)
                .or_insert_with(|| (dest.clone(), hash_bytes(&bytes)));
        }
        return if looks_like_html(&bytes) {
            Ok(SnapshotOutcome::CachedHtml(bytes))
        } else {
            Ok(SnapshotOutcome::SkippedLocal)
        };
    }
    // Rare edge case: dest is a directory (file promoted to dir by ensure_dir_all).
    if dest.is_dir() {
        return Ok(SnapshotOutcome::SkippedLocal);
    }

    // `if_` tells Wayback to return raw content without toolbar injection.
    let wayback_url = format!("{WAYBACK_WEB}/{timestamp}if_/{orig_url}");

    if failed_urls.contains(&wayback_url) {
        return Ok(SnapshotOutcome::SkippedLocal);
    }

    if verbose {
        log!("[FETCH] {wayback_url}");
    }

    // Retry on transient connection/timeout errors with exponential backoff.
    let resp = {
        let mut attempt = 0u32;
        loop {
            match client.get(&wayback_url).send().await {
                Ok(r) => break r,
                Err(e) if attempt < MAX_RETRIES && (e.is_connect() || e.is_timeout()) => {
                    attempt += 1;
                    let delay_ms = (RETRY_BASE_MS as f64 * 1.5f64.powi(attempt as i32 - 1)) as u64;
                    log!("[RETRY {attempt}/{MAX_RETRIES}] {orig_url} — {e} — waiting {delay_ms}ms");
                    sleep(Duration::from_millis(delay_ms)).await;
                }
                Err(e) if e.is_connect() || e.is_timeout() => {
                    log!("[BLOCKED] {orig_url}");
                    return Ok(SnapshotOutcome::Blocked);
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("request failed: {wayback_url}"));
                }
            }
        }
    };

    let status = resp.status();

    if !status.is_success() {
        log!("[{status}] {wayback_url}");
        if status.is_client_error() {
            failed_urls.insert(wayback_url);
        }
        return Ok(SnapshotOutcome::Skipped);
    }

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_lowercase();

    let raw = resp
        .bytes()
        .await
        .context("failed reading response body")?
        .to_vec();

    // Rewrite same-domain URLs to relative local paths before saving.
    let is_html = content_type.contains("text/html") || looks_like_html(&raw);
    let is_css = !is_html
        && (content_type.contains("text/css")
            || orig_url
                .split('?')
                .next()
                .map(|u| u.to_lowercase().ends_with(".css"))
                .unwrap_or(false));

    let final_bytes: Vec<u8> = if is_html {
        let text = String::from_utf8_lossy(&raw);
        rewrite_html(&text, orig_url, &file_in_snapshot, apex).into_bytes()
    } else if is_css {
        let text = String::from_utf8_lossy(&raw);
        rewrite_css(&text, orig_url, &file_in_snapshot, apex).into_bytes()
    } else {
        raw.clone()
    };

    if let Some(parent) = dest.parent() {
        ensure_dir_all(parent)?;
    }

    if dedup {
        let url_key = url_to_local_str(orig_url).unwrap_or_default();
        let h = hash_bytes(&final_bytes);
        if let Some((src, src_hash)) = memo.get(&url_key) {
            if *src_hash == h {
                let src = src.clone();
                match fs::hard_link(&src, &dest) {
                    Ok(()) => {
                        existing.insert(dest.clone());
                        if verbose {
                            log!("[LINKED] {} -> {}", dest.display(), src.display());
                        }
                        return Ok(SnapshotOutcome::Hardlinked(raw, final_bytes.len() as u64));
                    }
                    Err(e) => {
                        // Fallback: different volume or unsupported filesystem.
                        if verbose {
                            log!("[LINK-FALLBACK] {}: {e}", dest.display());
                        }
                    }
                }
            }
        }
        fs::write(&dest, &final_bytes)
            .with_context(|| format!("could not write: {}", dest.display()))?;
        existing.insert(dest.clone());
        memo.insert(url_key, (dest.clone(), h));
    } else {
        fs::write(&dest, &final_bytes)
            .with_context(|| format!("could not write: {}", dest.display()))?;
        existing.insert(dest.clone());
    }

    if verbose {
        log!(
            "[{status}] [SAVED] {} ({})",
            dest.display(),
            format_bytes(final_bytes.len() as u64)
        );
    }

    // Return the *raw* bytes so link extraction sees original absolute URLs,
    // which are easier to resolve than the rewritten relative paths.
    Ok(SnapshotOutcome::Downloaded(raw))
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = {
        use clap::{CommandFactory, FromArgMatches};
        let cmd = Args::command().long_about(LONG_ABOUT.as_str());
        Args::from_arg_matches(&cmd.get_matches()).unwrap()
    };

    let apex = apex_from_url(&args.url)?;
    log!("Domain  : {apex}  (subdomains included)");
    log!("Output  : {}", args.output.display());
    if let Some(ts) = &args.after {
        log!("After   : {ts}");
    }
    if let Some(ts) = &args.before {
        log!("Before  : {ts}");
    }
    log!("Rate    : ~{REQUEST_RATE} requests / second");
    eprintln!();

    fs::create_dir_all(&args.output).with_context(|| {
        format!(
            "could not create output directory: {}",
            args.output.display()
        )
    })?;

    let client = Client::builder()
        .user_agent("wayback-scraper/0.1 (+https://github.com/; archival/research use)")
        .timeout(Duration::from_secs(60))
        .build()
        .context("failed to build HTTP client")?;

    // ── CDX index + interleaved downloading ────────────────────────────────
    //
    // After each CDX page we immediately process timestamps whose last entry
    // has already been received (all timestamps strictly less than the last
    // timestamp on the page).  The final page flushes everything.  This lets
    // downloading begin without waiting for the full index.
    let cache_dir = args.output.join(".wayback-scraper");
    fs::create_dir_all(&cache_dir)
        .with_context(|| format!("create cache dir {}", cache_dir.display()))?;
    let cdx_cache_path = cache_dir.join(format!("cdx_{apex}.json"));

    let is_cached = cdx_cache_path.exists();
    let mut all_cdx: Vec<(String, String)> = Vec::new(); // accumulated for cache write
    let mut by_timestamp: BTreeMap<String, VecDeque<String>> = BTreeMap::new();
    let mut cdx_count: usize = 0;
    let mut offset: u32 = 0;

    let dedup = !args.include_exact_copies;
    let mut memo: HashMap<String, (PathBuf, u64)> = HashMap::new();

    log!("Scanning output directory for existing files…");
    let mut existing =
        scan_existing_files(&args.output).context("failed to scan existing files")?;
    if !existing.is_empty() {
        log!(
            "Resume: {} files already on disk (skipping via index)",
            existing.len()
        );
        eprintln!();
    }

    let mut ts_done: usize = 0;
    let mut downloaded: usize = 0;
    let mut linked: usize = 0;
    let mut skipped: usize = 0;
    let mut errors: usize = 0;
    let mut discovered: usize = 0;
    let mut total_bytes: u64 = 0;
    let mut total_saved: u64 = 0;
    let mut consecutive_blocks: u32 = 0;
    // Adaptive inter-request delay.  Bumps up on throttling, decays on success.
    let mut current_delay_ms: u64 = MIN_REQUEST_DELAY_MS;
    // 4XX responses cached for the session — same Wayback URL won't be re-fetched.
    let mut failed_urls: HashSet<String> = HashSet::new();

    loop {
        // Obtain the next batch of CDX entries.
        let (page, is_last) = if is_cached && offset == 0 {
            log!("Loading CDX index from cache…");
            let raw = fs::read_to_string(&cdx_cache_path)
                .with_context(|| format!("read CDX cache {}", cdx_cache_path.display()))?;
            let mut pairs: Vec<(String, String)> = serde_json::from_str(&raw)
                .with_context(|| format!("parse CDX cache {}", cdx_cache_path.display()))?;
            // Apply date filters — cache may have been built without them.
            if args.after.is_some() || args.before.is_some() {
                pairs.retain(|(ts, _)| {
                    if let Some(after) = &args.after {
                        if ts.as_str() < after.as_str() {
                            return false;
                        }
                    }
                    if let Some(before) = &args.before {
                        if ts.as_str() > before.as_str() {
                            return false;
                        }
                    }
                    true
                });
            }
            log!("CDX entries found : {}", pairs.len());
            eprintln!();
            (pairs, true)
        } else {
            let page = fetch_cdx_page(
                &client,
                &apex,
                offset,
                args.verbose,
                args.after.as_deref(),
                args.before.as_deref(),
            )
            .await?;
            let is_last = page.len() < CDX_PAGE_SIZE as usize;
            (page, is_last)
        };

        cdx_count += page.len();

        // For fresh runs, accumulate and persist after every page so a partial
        // index is available if the run is interrupted.
        if !is_cached {
            all_cdx.extend_from_slice(&page);
            let json = serde_json::to_string(&all_cdx).context("serialize CDX cache")?;
            fs::write(&cdx_cache_path, &json)
                .with_context(|| format!("write CDX cache {}", cdx_cache_path.display()))?;
        }

        // The last timestamp on the page may continue onto the next page, so
        // only process timestamps strictly below it.  On the final page, flush all.
        let cutoff = if is_last {
            None
        } else {
            page.last().map(|(ts, _)| ts.clone())
        };

        for (ts, url) in page {
            by_timestamp.entry(ts).or_default().push_back(url);
        }

        // Drain all timestamps that are ready to process.
        loop {
            let Some(smallest) = by_timestamp.keys().next().cloned() else {
                break;
            };
            if cutoff
                .as_deref()
                .map(|c| smallest.as_str() >= c)
                .unwrap_or(false)
            {
                break;
            }
            let (timestamp, cdx_urls) = by_timestamp.pop_first().unwrap();

            ts_done += 1;

            // Per-timestamp queue and seen-set (URL only; timestamp is fixed here).
            let mut queue: VecDeque<String> = cdx_urls;
            let mut seen: HashSet<String> = queue.iter().cloned().collect();

            let mut ts_dl = 0usize;
            let mut ts_linked = 0usize;
            let mut ts_skip = 0usize;
            let mut ts_err = 0usize;
            let mut ts_disc = 0usize;
            let mut ts_processed = 0usize;
            let mut ts_bytes = 0u64;
            let mut ts_saved = 0u64;

            if !args.verbose {
                log!("[ts {ts_done}] {timestamp}  ({} CDX URLs)", queue.len());
            }

            while let Some(orig_url) = queue.pop_front() {
                ts_processed += 1;

                if !matches_domain(&orig_url, &apex) {
                    if args.verbose {
                        log!("[SKIP-DOMAIN] {orig_url}");
                    }
                    ts_skip += 1;
                    continue;
                }

                // Parse bytes for links and push unseen ones into this timestamp's queue.
                let mut enqueue_links = |bytes: &[u8]| {
                    if !looks_like_html(bytes) {
                        return;
                    }
                    let links = extract_links(bytes, &orig_url, &apex);
                    let mut new_count = 0usize;
                    for url in links {
                        if seen.insert(url.clone()) {
                            queue.push_back(url);
                            new_count += 1;
                        }
                    }
                    if new_count > 0 {
                        ts_disc += new_count;
                        if args.verbose {
                            log!("[LINKS] +{new_count} queued from {orig_url}");
                        }
                    }
                };

                match download_snapshot(
                    &client,
                    &timestamp,
                    &orig_url,
                    &args.output,
                    &apex,
                    args.verbose,
                    dedup,
                    &mut memo,
                    &mut existing,
                    &mut failed_urls,
                )
                .await
                {
                    Ok(SnapshotOutcome::Downloaded(bytes)) => {
                        consecutive_blocks = 0;
                        current_delay_ms =
                            (current_delay_ms.saturating_sub(25)).max(MIN_REQUEST_DELAY_MS);
                        ts_dl += 1;
                        ts_bytes += bytes.len() as u64;
                        enqueue_links(&bytes);
                        sleep(Duration::from_millis(current_delay_ms)).await;
                    }
                    Ok(SnapshotOutcome::CachedHtml(bytes)) => {
                        consecutive_blocks = 0;
                        ts_skip += 1;
                        enqueue_links(&bytes);
                        // No network request — no delay needed.
                    }
                    Ok(SnapshotOutcome::Hardlinked(bytes, saved)) => {
                        consecutive_blocks = 0;
                        current_delay_ms =
                            (current_delay_ms.saturating_sub(25)).max(MIN_REQUEST_DELAY_MS);
                        ts_linked += 1;
                        ts_saved += saved;
                        enqueue_links(&bytes);
                        sleep(Duration::from_millis(current_delay_ms)).await;
                    }
                    Ok(SnapshotOutcome::SkippedLocal) => {
                        ts_skip += 1;
                    }
                    Ok(SnapshotOutcome::Skipped) => {
                        consecutive_blocks = 0;
                        current_delay_ms =
                            (current_delay_ms.saturating_sub(25)).max(MIN_REQUEST_DELAY_MS);
                        ts_skip += 1;
                        sleep(Duration::from_millis(current_delay_ms)).await;
                    }
                    Ok(SnapshotOutcome::Blocked) => {
                        consecutive_blocks += 1;
                        ts_err += 1;
                        current_delay_ms = (current_delay_ms * 2).min(MAX_REQUEST_DELAY_MS);
                        log!("[THROTTLE] backing off to {current_delay_ms}ms inter-request delay");
                        if consecutive_blocks >= CIRCUIT_BREAKER_THRESHOLD {
                            log!(
                                "[CIRCUIT BREAKER] {consecutive_blocks} consecutive blocks — aborting run in timestamp {timestamp}"
                            );
                            std::process::exit(1);
                        }
                        sleep(Duration::from_millis(current_delay_ms)).await;
                    }
                    Err(e) => {
                        ts_err += 1;
                        current_delay_ms = (current_delay_ms * 2).min(MAX_REQUEST_DELAY_MS);
                        log!("[ERROR] {orig_url}: {e:#}");
                        sleep(Duration::from_millis(current_delay_ms)).await;
                    }
                }

                if !args.verbose && ts_processed.is_multiple_of(50) {
                    log!(
                        "  … {ts_processed} processed, {} queued  \
                        dl={ts_dl} linked={ts_linked} skip={ts_skip} err={ts_err} disc={ts_disc}",
                        queue.len()
                    );
                }
            }

            downloaded += ts_dl;
            linked += ts_linked;
            skipped += ts_skip;
            errors += ts_err;
            discovered += ts_disc;
            total_bytes += ts_bytes;
            total_saved += ts_saved;

            if !args.verbose {
                log!(
                    "  done  dl={ts_dl}  linked={ts_linked}  skip={ts_skip}  err={ts_err}  disc={ts_disc}"
                );
            }
        } // end drain loop

        if is_last {
            break;
        }

        offset += CDX_PAGE_SIZE;
        sleep(Duration::from_millis(MIN_REQUEST_DELAY_MS)).await;
    } // end CDX page loop

    eprintln!();
    log!(
        "Done.  timestamps={ts_done}  cdx={cdx_count}  discovered={discovered}  \
         downloaded={downloaded}  linked={linked}  skipped={skipped}  errors={errors}  \
         bytes={}  saved={}",
        format_bytes(total_bytes),
        format_bytes(total_saved)
    );

    Ok(())
}
