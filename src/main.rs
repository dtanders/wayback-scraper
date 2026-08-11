use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use lol_html::{comments, element, HtmlRewriter, Settings};
use regex::Regex;
use reqwest::Client;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::time::sleep;
use url::Url;

macro_rules! log {
    () => {{
        prepare_log_line();
        eprintln!();
        if *IS_TTY { eprint!("{}", CONTROLS_HINT); }
    }};
    ($($arg:tt)*) => {{
        prepare_log_line();
        eprintln!("[{}] {}", Local::now().format("%H:%M:%S"), format_args!($($arg)*));
        if *IS_TTY { eprint!("{}", CONTROLS_HINT); }
    }};
}

const WAYBACK_CDX: &str = "https://web.archive.org/cdx/search/cdx";
const WAYBACK_WEB: &str = "https://web.archive.org/web";

/// Minimum inter-request delay (4 requests / second).  The actual delay
/// grows automatically when archive.org starts throttling and decays back
/// to this floor once requests succeed again.
const MIN_REQUEST_DELAY_MS: u64 = 250;

/// Maximum inter-request delay the adaptive throttle will reach.
const MAX_REQUEST_DELAY_MS: u64 = 4_000;

/// How much to reduce the inter-request delay after each successful response.
const DELAY_DECAY_MS: u64 = 25;

/// Nominal request rate derived from the minimum delay.
const REQUEST_RATE: u64 = 1_000 / MIN_REQUEST_DELAY_MS;

/// Retry up to this many times on transient connection errors.
const MAX_RETRIES: u32 = 4;

/// First retry waits this long; each subsequent retry is 1.5× the previous.
/// 2 s → 3 s → 4.5 s → 6.75 s
const RETRY_BASE_MS: u64 = 2_000;

/// Open the circuit breaker after this many consecutive exhausted-retry blocks.
const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;

/// How long to cool down when the circuit first opens; doubles on each trip.
/// 60 s → 120 s → pause (trip 3 pauses instead of sleeping)
const CIRCUIT_BREAKER_COOLDOWN_MS: u64 = 60_000;

/// Pause the run after the circuit opens this many times without recovery.
const CIRCUIT_BREAKER_MAX_TRIPS: u32 = 4;

/// CDX records per API page.
const CDX_PAGE_SIZE: u32 = 10_000;

static IS_TTY: LazyLock<bool> = LazyLock::new(|| {
    use std::io::IsTerminal;
    std::io::stderr().is_terminal()
});

/// Set by the stdin task before each log! call so the macro knows the cursor
/// moved down one line when the user pressed Enter.
static STDIN_ECHOED: AtomicBool = AtomicBool::new(false);

const CONTROLS_HINT: &str = "  p=pause  r=resume  s=suspend  ^C=quit ";

fn prepare_log_line() {
    if !*IS_TTY {
        return;
    }
    if STDIN_ECHOED.swap(false, Ordering::Relaxed) {
        // Enter moved the cursor past the hint line; clear the empty current
        // line, then move up and clear the dirty hint+input line above it.
        eprint!("\r\x1b[K\x1b[1A\r\x1b[K");
    } else {
        eprint!("\r\x1b[K");
    }
}

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
    #[arg(required_unless_present_any = ["resume", "resume_file"])]
    url: Option<String>,

    /// Root directory for downloaded files
    #[arg(required_unless_present_any = ["resume", "resume_file"])]
    output: Option<PathBuf>,

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

    /// Resume from a suspended session.  Searches OUTPUT_DIR/.wayback-scraper/
    /// for suspend files.  If OUTPUT_DIR is omitted, searches the current
    /// directory.  If exactly one file is found it is loaded automatically;
    /// otherwise you are prompted to choose.
    #[arg(
        short = 'r',
        long,
        value_name = "OUTPUT_DIR",
        num_args = 0..=1,
        default_missing_value = ".",
        conflicts_with = "resume_file"
    )]
    resume: Option<PathBuf>,

    /// Resume from a specific suspend file.
    #[arg(long, value_name = "FILE", conflicts_with = "resume")]
    resume_file: Option<PathBuf>,
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

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

/// True for IO error kinds indicating the peer reset or closed the
/// connection mid-request (e.g. Windows os error 10054 / Unix ECONNRESET) —
/// safe to retry since every request we send is a GET.
fn is_transient_io_error(kind: std::io::ErrorKind) -> bool {
    use std::io::ErrorKind::*;
    matches!(
        kind,
        ConnectionReset | ConnectionAborted | BrokenPipe | UnexpectedEof
    )
}

/// True for network errors worth retrying. Beyond `is_connect()` /
/// `is_timeout()`, this walks the error's source chain looking for a
/// transient IO error — connection resets on a pooled keep-alive connection
/// surface as a "request" error (not "connect"), since the connection had
/// already been established when the peer closed it.
fn is_transient(e: &reqwest::Error) -> bool {
    if e.is_connect() || e.is_timeout() {
        return true;
    }
    let mut source = std::error::Error::source(e);
    while let Some(err) = source {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            if is_transient_io_error(io_err.kind()) {
                return true;
            }
        }
        source = err.source();
    }
    false
}

fn backoff_ms(attempt: u32) -> u64 {
    let mut ms = RETRY_BASE_MS;
    for _ in 1..attempt {
        ms = ms * 3 / 2;
    }
    ms
}

/// GET `url` with up to `MAX_RETRIES` retries on transient (connect / timeout)
/// errors.  Returns the last error unchanged so callers can inspect whether it
/// was transient.
async fn send_with_retry(
    client: &Client,
    url: &str,
    tag: &str,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut attempt = 0u32;
    loop {
        match client.get(url).send().await {
            Ok(r) => return Ok(r),
            Err(e) if attempt < MAX_RETRIES && is_transient(&e) => {
                attempt += 1;
                let ms = backoff_ms(attempt);
                log!("[RETRY {attempt}/{MAX_RETRIES}] {tag} — {e} — waiting {ms}ms");
                sleep(Duration::from_millis(ms)).await;
            }
            Err(e) => return Err(e),
        }
    }
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

    let resp = send_with_retry(client, &req_url, "CDX")
        .await
        .context("CDX request failed")?;

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

// ─── Suspend / resume types ───────────────────────────────────────────────────

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SavedArgs {
    url: String,
    output: PathBuf,
    verbose: bool,
    include_exact_copies: bool,
    after: Option<String>,
    before: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct SuspendState {
    version: u32,
    suspended_at: String,
    apex: String,
    args: SavedArgs,
    last_timestamp: String,
    current_delay_ms: u64,
    failed_urls: HashSet<String>,
    /// url path (no query/fragment) → (absolute local file path, content hash)
    memo: HashMap<String, (PathBuf, u64)>,
    ts_done: usize,
    downloaded: usize,
    linked: usize,
    skipped: usize,
    errors: usize,
    discovered: usize,
    total_bytes: u64,
    total_saved: u64,
}

fn suspend_file_path(cache_dir: &Path, apex: &str, last_timestamp: &str) -> PathBuf {
    cache_dir.join(format!("suspend_{apex}_{last_timestamp}.json"))
}

fn save_suspend_state(cache_dir: &Path, state: &SuspendState) -> Result<PathBuf> {
    let path = suspend_file_path(cache_dir, &state.apex, &state.last_timestamp);
    let json = serde_json::to_string_pretty(state).context("serialize suspend state")?;
    fs::write(&path, &json).with_context(|| format!("write suspend file: {}", path.display()))?;
    Ok(path)
}

fn load_suspend_file(path: &Path) -> Result<SuspendState> {
    let json = fs::read_to_string(path)
        .with_context(|| format!("read suspend file: {}", path.display()))?;
    serde_json::from_str(&json).with_context(|| format!("parse suspend file: {}", path.display()))
}

/// Search `dir/.wayback-scraper/` for suspend files.
/// If exactly one is found, auto-selects it. If multiple, prompts the user.
/// Returns an error if none are found.
fn pick_suspend_file(dir: &Path) -> Result<PathBuf> {
    let cache_dir = dir.join(".wayback-scraper");

    let mut files: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    if let Ok(entries) = fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with("suspend_") && name.ends_with(".json") {
                if let Ok(meta) = entry.metadata() {
                    if let Ok(mtime) = meta.modified() {
                        files.push((path, mtime));
                    }
                }
            }
        }
    }

    if files.is_empty() {
        anyhow::bail!("No suspend files found in {}", cache_dir.display());
    }

    // Newest first.
    files.sort_by_key(|f| std::cmp::Reverse(f.1));

    if files.len() == 1 {
        let path = files.remove(0).0;
        eprintln!("Loading suspend file: {}", path.display());
        return Ok(path);
    }

    eprintln!("Suspend files found:");
    for (i, (path, mtime)) in files.iter().enumerate() {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        let dt: chrono::DateTime<chrono::Local> = (*mtime).into();
        eprintln!("  {})  {}  ({})", i + 1, name, dt.format("%Y-%m-%d %H:%M"));
    }
    eprint!("Choose [1-{}]: ", files.len());

    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("failed to read choice")?;
    let choice: usize = line
        .trim()
        .parse()
        .context("invalid choice — enter a number")?;
    anyhow::ensure!(
        choice >= 1 && choice <= files.len(),
        "choice {choice} out of range [1-{}]",
        files.len()
    );

    Ok(files.remove(choice - 1).0)
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
        return fs::rename(&tmp, path.join("index.html"))
            .with_context(|| format!("promote {} to index.html", path.display()));
    }

    fs::create_dir(path).with_context(|| format!("mkdir {}", path.display()))
}

fn write_file(dest: &Path, bytes: &[u8], existing: &mut HashSet<PathBuf>) -> Result<()> {
    fs::write(dest, bytes).with_context(|| format!("could not write: {}", dest.display()))?;
    existing.insert(dest.to_owned());
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
    /// Got a 4XX HTTP response — a network request was made.
    Skipped,
    /// Got a 5XX HTTP response — server-side error or rate limit.
    ServerError,
    /// Content matched an earlier timestamp; a hard link was created instead of
    /// a new copy.  Raw bytes returned for link extraction; u64 is bytes saved.
    Hardlinked(Vec<u8>, u64),
    /// All retries were exhausted due to a connection error (IP block).
    Blocked,
}

/// Outcome of fetching one Wayback snapshot's HTTP response.
enum FetchOutcome {
    /// All retries were exhausted due to a connection error (IP block).
    Blocked,
    /// Got a non-2xx HTTP response.
    Status(reqwest::StatusCode),
    /// 2xx response with its status code, content-type header, and full body.
    Success {
        status: reqwest::StatusCode,
        content_type: String,
        bytes: Vec<u8>,
    },
}

/// Send `wayback_url` and read its full body, retrying the *entire
/// request* (not just the initial send) if a transient error interrupts
/// downloading the body — once `.bytes()` fails partway through, the
/// response can't be resumed, so the whole GET is reissued via
/// `send_with_retry`.
async fn fetch_snapshot(client: &Client, wayback_url: &str, tag: &str) -> Result<FetchOutcome> {
    let mut attempt = 0u32;
    loop {
        let resp = match send_with_retry(client, wayback_url, tag).await {
            Ok(r) => r,
            Err(e) if is_transient(&e) => return Ok(FetchOutcome::Blocked),
            Err(e) => return Err(e).with_context(|| format!("request failed: {wayback_url}")),
        };

        let status = resp.status();
        if !status.is_success() {
            return Ok(FetchOutcome::Status(status));
        }

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_lowercase();

        match resp.bytes().await {
            Ok(bytes) => {
                return Ok(FetchOutcome::Success {
                    status,
                    content_type,
                    bytes: bytes.to_vec(),
                })
            }
            Err(e) if attempt < MAX_RETRIES && is_transient(&e) => {
                attempt += 1;
                let ms = backoff_ms(attempt);
                log!("[RETRY {attempt}/{MAX_RETRIES}] {tag} (body) — {e} — waiting {ms}ms");
                sleep(Duration::from_millis(ms)).await;
            }
            Err(e) => return Err(e).context("failed reading response body"),
        }
    }
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

    let (status, content_type, raw) = match fetch_snapshot(client, &wayback_url, orig_url).await? {
        FetchOutcome::Blocked => {
            log!("[BLOCKED] {wayback_url}");
            return Ok(SnapshotOutcome::Blocked);
        }
        FetchOutcome::Status(status) => {
            log!("[{status}] {wayback_url}");
            if status.is_client_error() {
                failed_urls.insert(wayback_url);
                return Ok(SnapshotOutcome::Skipped);
            } else {
                return Ok(SnapshotOutcome::ServerError);
            }
        }
        FetchOutcome::Success {
            status,
            content_type,
            bytes,
        } => (status, content_type, bytes),
    };

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
        write_file(&dest, &final_bytes, existing)?;
        memo.insert(url_key, (dest.clone(), h));
    } else {
        write_file(&dest, &final_bytes, existing)?;
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

/// Pure step of `decay_delay`, split out so the arithmetic is testable
/// without exercising the `log!` macro's `Local::now()` call.
fn decayed_delay(current_delay_ms: u64) -> u64 {
    current_delay_ms
        .saturating_sub(DELAY_DECAY_MS)
        .max(MIN_REQUEST_DELAY_MS)
}

/// Decay the adaptive delay after a successful request, logging `[SPEEDUP]`
/// once it fully recovers to the floor after having been throttled.
fn decay_delay(current_delay_ms: &mut u64) {
    let before = *current_delay_ms;
    *current_delay_ms = decayed_delay(before);
    if *current_delay_ms == MIN_REQUEST_DELAY_MS && before > MIN_REQUEST_DELAY_MS {
        log!(
            "[SPEEDUP] back to full speed at {}ms inter-request delay",
            current_delay_ms
        );
    }
}

// ─── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args = {
        use clap::{CommandFactory, FromArgMatches};
        let cmd = Args::command().long_about(LONG_ABOUT.as_str());
        Args::from_arg_matches(&cmd.get_matches()).unwrap()
    };

    // Load resume state (if --resume or --resume-file was given).
    let resume_state: Option<SuspendState> = if let Some(ref path) = args.resume_file {
        Some(load_suspend_file(path)?)
    } else if let Some(ref dir) = args.resume {
        Some(load_suspend_file(&pick_suspend_file(dir)?)?)
    } else {
        None
    };

    // Effective run config — comes from suspend file in resume mode.
    let (url, output, verbose, include_exact_copies, after, before) =
        if let Some(ref s) = resume_state {
            (
                s.args.url.clone(),
                s.args.output.clone(),
                s.args.verbose,
                s.args.include_exact_copies,
                s.args.after.clone(),
                s.args.before.clone(),
            )
        } else {
            (
                args.url.clone().unwrap(),
                args.output.clone().unwrap(),
                args.verbose,
                args.include_exact_copies,
                args.after.clone(),
                args.before.clone(),
            )
        };

    let resume_from_timestamp: Option<String> =
        resume_state.as_ref().map(|s| s.last_timestamp.clone());

    let apex = apex_from_url(&url)?;
    if let Some(ref s) = resume_state {
        log!(
            "Resuming from suspend — continuing at timestamp {}",
            s.last_timestamp
        );
    }
    log!("Domain  : {apex}  (subdomains included)");
    log!("Output  : {}", output.display());
    if let Some(ts) = &after {
        log!("After   : {ts}");
    }
    if let Some(ts) = &before {
        log!("Before  : {ts}");
    }
    log!("Rate    : ~{REQUEST_RATE} requests / second");
    log!(
        "Controls: 'p' + Enter to pause  |  'r' + Enter to resume  |  \
         's' + Enter to suspend  |  Ctrl+C to stop"
    );
    log!();

    fs::create_dir_all(&output)
        .with_context(|| format!("could not create output directory: {}", output.display()))?;

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
    let cache_dir = output.join(".wayback-scraper");
    fs::create_dir_all(&cache_dir)
        .with_context(|| format!("create cache dir {}", cache_dir.display()))?;
    let cdx_cache_path = cache_dir.join(format!("cdx_{apex}.json"));

    let is_cached = cdx_cache_path.exists();
    let mut all_cdx: Vec<(String, String)> = Vec::new(); // accumulated for cache write
    let mut by_timestamp: BTreeMap<String, VecDeque<String>> = BTreeMap::new();
    let mut cdx_count: usize = 0;
    let mut offset: u32 = 0;

    let dedup = !include_exact_copies;

    // For resume, CDX cache must already exist on disk.
    if resume_state.is_some() && !is_cached {
        anyhow::bail!(
            "Cannot resume: CDX cache not found at {}",
            cdx_cache_path.display()
        );
    }

    log!("Scanning output directory for existing files…");
    let mut existing = scan_existing_files(&output).context("failed to scan existing files")?;
    if !existing.is_empty() {
        log!(
            "Resume: {} files already on disk (skipping via index)",
            existing.len()
        );
        log!();
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            log!("Interrupted — finishing current download then stopping…");
            shutdown.store(true, Ordering::Relaxed);
        });
    }

    let paused = Arc::new(AtomicBool::new(false));
    let suspending = Arc::new(AtomicBool::new(false));
    {
        let paused = paused.clone();
        let suspending = suspending.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt as _;
            let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match line.trim() {
                    "p" => {
                        paused.store(true, Ordering::Relaxed);
                        STDIN_ECHOED.store(true, Ordering::Relaxed);
                        log!("Paused — send 'r' to resume");
                    }
                    "r" => {
                        paused.store(false, Ordering::Relaxed);
                        STDIN_ECHOED.store(true, Ordering::Relaxed);
                        log!("Resumed");
                    }
                    "s" => {
                        suspending.store(true, Ordering::Relaxed);
                        STDIN_ECHOED.store(true, Ordering::Relaxed);
                        log!("Suspending — finishing current download…");
                    }
                    _ => {}
                }
            }
        });
    }

    let mut ts_done: usize = resume_state.as_ref().map_or(0, |s| s.ts_done);
    let mut downloaded: usize = resume_state.as_ref().map_or(0, |s| s.downloaded);
    let mut linked: usize = resume_state.as_ref().map_or(0, |s| s.linked);
    let mut skipped: usize = resume_state.as_ref().map_or(0, |s| s.skipped);
    let mut errors: usize = resume_state.as_ref().map_or(0, |s| s.errors);
    let mut discovered: usize = resume_state.as_ref().map_or(0, |s| s.discovered);
    let mut total_bytes: u64 = resume_state.as_ref().map_or(0, |s| s.total_bytes);
    let mut total_saved: u64 = resume_state.as_ref().map_or(0, |s| s.total_saved);
    let mut consecutive_blocks: u32 = 0;
    let mut circuit_trips: u32 = 0;
    let mut last_timestamp = String::new();
    let mut current_delay_ms: u64 = resume_state
        .as_ref()
        .map_or(MIN_REQUEST_DELAY_MS, |s| s.current_delay_ms);
    let mut failed_urls: HashSet<String> = resume_state
        .as_ref()
        .map_or_else(HashSet::new, |s| s.failed_urls.clone());
    let mut memo: HashMap<String, (PathBuf, u64)> = resume_state
        .as_ref()
        .map_or_else(HashMap::new, |s| s.memo.clone());

    loop {
        // Obtain the next batch of CDX entries.
        let (page, is_last) = if is_cached && offset == 0 {
            log!("Loading CDX index from cache…");
            let raw = fs::read_to_string(&cdx_cache_path)
                .with_context(|| format!("read CDX cache {}", cdx_cache_path.display()))?;
            let mut pairs: Vec<(String, String)> = serde_json::from_str(&raw)
                .with_context(|| format!("parse CDX cache {}", cdx_cache_path.display()))?;
            // Apply date filters — cache may have been built without them.
            if after.is_some() || before.is_some() {
                pairs.retain(|(ts, _)| {
                    if let Some(a) = &after {
                        if ts.as_str() < a.as_str() {
                            return false;
                        }
                    }
                    if let Some(b) = &before {
                        if ts.as_str() > b.as_str() {
                            return false;
                        }
                    }
                    true
                });
            }
            // Resume filter — skip timestamps already processed.
            if let Some(ref from_ts) = resume_from_timestamp {
                pairs.retain(|(ts, _)| ts.as_str() >= from_ts.as_str());
                log!(
                    "Resuming: {} CDX entries remaining (>= {})",
                    pairs.len(),
                    from_ts
                );
            }
            log!("CDX entries found : {}", pairs.len());
            log!();
            (pairs, true)
        } else {
            let page = fetch_cdx_page(
                &client,
                &apex,
                offset,
                verbose,
                after.as_deref(),
                before.as_deref(),
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
            if shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
                break;
            }
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
            last_timestamp.clone_from(&timestamp);

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

            if !verbose {
                log!("[ts {ts_done}] {timestamp}  ({} CDX URLs)", queue.len());
            }

            while let Some(orig_url) = queue.pop_front() {
                ts_processed += 1;

                if !matches_domain(&orig_url, &apex) {
                    if verbose {
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
                        if verbose {
                            log!("[LINKS] +{new_count} queued from {orig_url}");
                        }
                    }
                };

                match download_snapshot(
                    &client,
                    &timestamp,
                    &orig_url,
                    &output,
                    &apex,
                    verbose,
                    dedup,
                    &mut memo,
                    &mut existing,
                    &mut failed_urls,
                )
                .await
                {
                    Ok(SnapshotOutcome::Downloaded(bytes)) => {
                        consecutive_blocks = 0;
                        decay_delay(&mut current_delay_ms);
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
                        decay_delay(&mut current_delay_ms);
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
                        decay_delay(&mut current_delay_ms);
                        ts_skip += 1;
                        sleep(Duration::from_millis(current_delay_ms)).await;
                    }
                    Ok(SnapshotOutcome::Blocked | SnapshotOutcome::ServerError) => {
                        consecutive_blocks += 1;
                        ts_err += 1;
                        current_delay_ms = (current_delay_ms * 2).min(MAX_REQUEST_DELAY_MS);
                        if consecutive_blocks >= CIRCUIT_BREAKER_THRESHOLD {
                            circuit_trips += 1;
                            if circuit_trips >= CIRCUIT_BREAKER_MAX_TRIPS {
                                log!(
                                    "[CIRCUIT BREAKER] tripped {circuit_trips} times — pausing \
                                    (send 'r' + Enter to resume, Ctrl+C to abort)"
                                );
                                paused.store(true, Ordering::Relaxed);
                                circuit_trips = 0;
                                consecutive_blocks = 0;
                                current_delay_ms = MAX_REQUEST_DELAY_MS / 4;
                                // Fall through to the pause check below rather than
                                // continue-ing past it to the next queue item.
                            } else {
                                let cooldown_ms =
                                    CIRCUIT_BREAKER_COOLDOWN_MS * (1 << (circuit_trips - 1));
                                log!("[THROTTLE] backing off to {current_delay_ms}ms inter-request delay");
                                log!(
                                    "[CIRCUIT BREAKER] trip {circuit_trips}/{CIRCUIT_BREAKER_MAX_TRIPS} \
                                    — cooling down for {}s",
                                    cooldown_ms / 1000
                                );
                                sleep(Duration::from_millis(cooldown_ms)).await;
                                consecutive_blocks = 0;
                                // Resume cautiously rather than at full speed.
                                current_delay_ms = MAX_REQUEST_DELAY_MS / 4;
                            }
                        } else {
                            log!("[THROTTLE] backing off to {current_delay_ms}ms inter-request delay");
                            sleep(Duration::from_millis(current_delay_ms)).await;
                        }
                    }
                    Err(e) => {
                        ts_err += 1;
                        current_delay_ms = (current_delay_ms * 2).min(MAX_REQUEST_DELAY_MS);
                        log!("[ERROR] {orig_url}: {e:#}");
                        sleep(Duration::from_millis(current_delay_ms)).await;
                    }
                }

                if !verbose && ts_processed.is_multiple_of(50) {
                    log!(
                        "  … {ts_processed} processed, {} queued  \
                        dl={ts_dl} linked={ts_linked} skip={ts_skip} err={ts_err} disc={ts_disc} in {timestamp}",
                        queue.len()
                    );
                }

                while paused.load(Ordering::Relaxed)
                    && !shutdown.load(Ordering::Relaxed)
                    && !suspending.load(Ordering::Relaxed)
                {
                    sleep(Duration::from_millis(200)).await;
                }
                if shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
                    break;
                }
            }

            downloaded += ts_dl;
            linked += ts_linked;
            skipped += ts_skip;
            errors += ts_err;
            discovered += ts_disc;
            total_bytes += ts_bytes;
            total_saved += ts_saved;

            if !verbose {
                log!(
                    "  done  dl={ts_dl}  linked={ts_linked}  skip={ts_skip}  err={ts_err}  disc={ts_disc}"
                );
            }
        } // end drain loop

        if is_last || shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
            break;
        }

        offset += CDX_PAGE_SIZE;
        sleep(Duration::from_millis(MIN_REQUEST_DELAY_MS)).await;
    } // end CDX page loop

    log!();
    if shutdown.load(Ordering::Relaxed) && !last_timestamp.is_empty() {
        log!("Stopped at timestamp {last_timestamp}");
    }

    if suspending.load(Ordering::Relaxed) {
        if last_timestamp.is_empty() {
            log!("Nothing to suspend yet — no timestamps processed.");
        } else {
            let state = SuspendState {
                version: 1,
                suspended_at: chrono::Local::now().to_rfc3339(),
                apex: apex.clone(),
                args: SavedArgs {
                    url: url.clone(),
                    output: output.clone(),
                    verbose,
                    include_exact_copies,
                    after: after.clone(),
                    before: before.clone(),
                },
                last_timestamp: last_timestamp.clone(),
                current_delay_ms,
                failed_urls: failed_urls.clone(),
                memo: memo.clone(),
                ts_done,
                downloaded,
                linked,
                skipped,
                errors,
                discovered,
                total_bytes,
                total_saved,
            };
            match save_suspend_state(&cache_dir, &state) {
                Ok(path) => {
                    let path_str = path.display().to_string();
                    log!("Suspended — resume with: wayback-scraper --resume-file {path_str}");
                    println!("{path_str}");
                }
                Err(e) => log!("[ERROR] Failed to save suspend state: {e:#}"),
            }
        }
    }

    if !suspending.load(Ordering::Relaxed) {
        log!(
            "Done.  timestamps={ts_done}  cdx={cdx_count}  discovered={discovered}  \
             downloaded={downloaded}  linked={linked}  skipped={skipped}  errors={errors}  \
             bytes={}  saved={}",
            format_bytes(total_bytes),
            format_bytes(total_saved)
        );
    }

    if *IS_TTY {
        eprint!("\r\x1b[K");
    }

    // The stdin task blocks an OS thread that won't unblock until the process
    // exits, so force an immediate clean exit rather than waiting for it.
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // ── normalize_host ────────────────────────────────────────────────────────

    #[test]
    fn normalize_host_strips_www() {
        assert_eq!(normalize_host("www.example.com"), "example.com");
    }

    #[test]
    fn normalize_host_lowercases() {
        assert_eq!(normalize_host("EXAMPLE.COM"), "example.com");
    }

    #[test]
    fn normalize_host_strips_trailing_dot() {
        assert_eq!(normalize_host("www.example.com."), "example.com");
    }

    #[test]
    fn normalize_host_no_www_unchanged() {
        assert_eq!(normalize_host("sub.example.com"), "sub.example.com");
    }

    // ── apex_from_url ─────────────────────────────────────────────────────────

    #[test]
    fn apex_from_url_strips_www_and_path() {
        assert_eq!(
            apex_from_url("https://www.example.com/foo/bar").unwrap(),
            "example.com"
        );
    }

    #[test]
    fn apex_from_url_case_insensitive() {
        assert_eq!(apex_from_url("http://EXAMPLE.COM").unwrap(), "example.com");
    }

    #[test]
    fn apex_from_url_invalid_is_err() {
        assert!(apex_from_url("not a url").is_err());
    }

    // ── matches_domain ────────────────────────────────────────────────────────

    #[test]
    fn matches_domain_exact_apex() {
        assert!(matches_domain("https://example.com/page", "example.com"));
    }

    #[test]
    fn matches_domain_subdomain() {
        assert!(matches_domain(
            "https://sub.example.com/page",
            "example.com"
        ));
    }

    #[test]
    fn matches_domain_www_normalised() {
        assert!(matches_domain(
            "https://www.example.com/page",
            "example.com"
        ));
    }

    #[test]
    fn matches_domain_different_domain() {
        assert!(!matches_domain("https://other.com/page", "example.com"));
    }

    // ── sanitize_component ────────────────────────────────────────────────────

    #[test]
    fn sanitize_component_replaces_forbidden() {
        assert_eq!(
            sanitize_component(r#"a\b:c*d?e"f<g>h|i"#),
            "a_b_c_d_e_f_g_h_i"
        );
    }

    #[test]
    fn sanitize_component_normal_chars_pass_through() {
        assert_eq!(sanitize_component("normal-file.html"), "normal-file.html");
    }

    // ── url_to_rel_path ───────────────────────────────────────────────────────

    #[test]
    fn url_to_rel_path_root_becomes_index() {
        assert_eq!(
            url_to_rel_path("https://example.com/"),
            PathBuf::from("index.html")
        );
    }

    #[test]
    fn url_to_rel_path_no_path_becomes_index() {
        assert_eq!(
            url_to_rel_path("https://example.com"),
            PathBuf::from("index.html")
        );
    }

    #[test]
    fn url_to_rel_path_file() {
        assert_eq!(
            url_to_rel_path("https://example.com/foo/bar.html"),
            PathBuf::from("foo/bar.html")
        );
    }

    #[test]
    fn url_to_rel_path_trailing_slash_becomes_index() {
        assert_eq!(
            url_to_rel_path("https://example.com/foo/"),
            PathBuf::from("foo/index.html")
        );
    }

    // ── url_to_local_str ──────────────────────────────────────────────────────

    #[test]
    fn url_to_local_str_root() {
        assert_eq!(
            url_to_local_str("https://example.com/").unwrap(),
            "index.html"
        );
    }

    #[test]
    fn url_to_local_str_nested() {
        assert_eq!(
            url_to_local_str("https://example.com/a/b/c.js").unwrap(),
            "a/b/c.js"
        );
    }

    // ── rel_path_from_to ──────────────────────────────────────────────────────

    #[test]
    fn rel_path_from_to_same_directory() {
        assert_eq!(rel_path_from_to("index.html", "about.html"), "about.html");
    }

    #[test]
    fn rel_path_from_to_up_one_level() {
        assert_eq!(
            rel_path_from_to("blog/post.html", "about.html"),
            "../about.html"
        );
    }

    #[test]
    fn rel_path_from_to_down_into_subdir() {
        assert_eq!(
            rel_path_from_to("index.html", "blog/post.html"),
            "blog/post.html"
        );
    }

    #[test]
    fn rel_path_from_to_sibling_subtree() {
        assert_eq!(rel_path_from_to("a/b/c.html", "a/d/e.html"), "../d/e.html");
    }

    // ── unwrap_wayback ────────────────────────────────────────────────────────

    #[test]
    fn unwrap_wayback_standard_url() {
        assert_eq!(
            unwrap_wayback(
                "https://web.archive.org/web/20090306084941/https://example.com/page.html"
            ),
            "https://example.com/page.html"
        );
    }

    #[test]
    fn unwrap_wayback_with_modifier() {
        assert_eq!(
            unwrap_wayback(
                "https://web.archive.org/web/20090306084941im_/https://example.com/img.png"
            ),
            "https://example.com/img.png"
        );
    }

    #[test]
    fn unwrap_wayback_embedded_form() {
        assert_eq!(
            unwrap_wayback(
                "http://www.rifters.org/web/20090306084941im_/http_/www.rifters.org/img.png"
            ),
            "http://www.rifters.org/img.png"
        );
    }

    #[test]
    fn unwrap_wayback_non_wayback_unchanged() {
        let url = "https://example.com/normal/url.html";
        assert_eq!(unwrap_wayback(url), url);
    }

    // ── looks_like_html ───────────────────────────────────────────────────────

    #[test]
    fn looks_like_html_doctype() {
        assert!(looks_like_html(b"<!DOCTYPE html><html>"));
    }

    #[test]
    fn looks_like_html_html_tag() {
        assert!(looks_like_html(b"<html lang='en'>"));
    }

    #[test]
    fn looks_like_html_head_tag() {
        assert!(looks_like_html(b"  <head><title>Test</title></head>"));
    }

    #[test]
    fn looks_like_html_png_header_is_not_html() {
        assert!(!looks_like_html(&[0x89, 0x50, 0x4E, 0x47]));
    }

    #[test]
    fn looks_like_html_css_is_not_html() {
        assert!(!looks_like_html(b"body { margin: 0; }"));
    }

    // ── format_bytes ──────────────────────────────────────────────────────────

    #[test]
    fn format_bytes_raw_bytes() {
        assert_eq!(format_bytes(512), "512 B");
    }

    #[test]
    fn format_bytes_kilobytes() {
        assert_eq!(format_bytes(1_024), "1.0 KB");
    }

    #[test]
    fn format_bytes_megabytes() {
        assert_eq!(format_bytes(1_024 * 1_024), "1.0 MB");
    }

    #[test]
    fn format_bytes_gigabytes() {
        assert_eq!(format_bytes(1_024 * 1_024 * 1_024), "1.0 GB");
    }

    // ── backoff_ms ────────────────────────────────────────────────────────────

    #[test]
    fn backoff_ms_exponential_sequence() {
        assert_eq!(backoff_ms(1), 2_000); // 2000 * 1.5^0
        assert_eq!(backoff_ms(2), 3_000); // 2000 * 1.5^1
        assert_eq!(backoff_ms(3), 4_500); // 2000 * 1.5^2
        assert_eq!(backoff_ms(4), 6_750); // 2000 * 1.5^3
    }

    // ── is_transient_io_error ────────────────────────────────────────────────

    #[test]
    fn is_transient_io_error_matches_reset_like_kinds() {
        use std::io::ErrorKind::*;
        assert!(is_transient_io_error(ConnectionReset));
        assert!(is_transient_io_error(ConnectionAborted));
        assert!(is_transient_io_error(BrokenPipe));
        assert!(is_transient_io_error(UnexpectedEof));
    }

    #[test]
    fn is_transient_io_error_rejects_other_kinds() {
        use std::io::ErrorKind::*;
        assert!(!is_transient_io_error(NotFound));
        assert!(!is_transient_io_error(PermissionDenied));
        assert!(!is_transient_io_error(InvalidData));
    }

    // ── decayed_delay ─────────────────────────────────────────────────────────

    #[test]
    fn decayed_delay_reduces_by_step() {
        assert_eq!(decayed_delay(500), 475);
    }

    #[test]
    fn decayed_delay_floors_at_minimum() {
        assert_eq!(
            decayed_delay(MIN_REQUEST_DELAY_MS + 10),
            MIN_REQUEST_DELAY_MS
        );
        // Already at the floor — stays put.
        assert_eq!(decayed_delay(MIN_REQUEST_DELAY_MS), MIN_REQUEST_DELAY_MS);
    }

    // ── rewrite_url ───────────────────────────────────────────────────────────

    #[test]
    fn rewrite_url_same_domain_absolute() {
        let r = rewrite_url(
            "https://example.com/about.html",
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert_eq!(r, Some("about.html".to_string()));
    }

    #[test]
    fn rewrite_url_relative_input_resolved() {
        // "../images/logo.png" from blog/post.html → resolves to /images/logo.png
        let r = rewrite_url(
            "../images/logo.png",
            "https://example.com/blog/post.html",
            "blog/post.html",
            "example.com",
        );
        assert_eq!(r, Some("../images/logo.png".to_string()));
    }

    #[test]
    fn rewrite_url_preserves_fragment() {
        let r = rewrite_url(
            "https://example.com/about.html#section",
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert_eq!(r, Some("about.html#section".to_string()));
    }

    #[test]
    fn rewrite_url_external_domain_is_none() {
        let r = rewrite_url(
            "https://other.com/page",
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert!(r.is_none());
    }

    #[test]
    fn rewrite_url_javascript_is_none() {
        let r = rewrite_url(
            "javascript:void(0)",
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert!(r.is_none());
    }

    #[test]
    fn rewrite_url_data_uri_is_none() {
        let r = rewrite_url(
            "data:image/png;base64,abc",
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert!(r.is_none());
    }

    #[test]
    fn rewrite_url_unwraps_wayback_wrapper() {
        let r = rewrite_url(
            "https://web.archive.org/web/20090306084941/https://example.com/img.png",
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert_eq!(r, Some("img.png".to_string()));
    }

    // ── rewrite_html ──────────────────────────────────────────────────────────
    // Excluded from Miri: lol_html → cssparser uses unsafe that triggers a
    // Stacked Borrows false positive (experimental rules, not our bug).

    #[cfg(not(miri))]
    #[test]
    fn rewrite_html_rewrites_href() {
        let html = r#"<a href="https://example.com/about.html">About</a>"#;
        let out = rewrite_html(
            html,
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert!(out.contains(r#"href="about.html""#), "got: {out}");
    }

    #[cfg(not(miri))]
    #[test]
    fn rewrite_html_rewrites_src() {
        let html = r#"<img src="https://example.com/img/logo.png">"#;
        let out = rewrite_html(
            html,
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert!(out.contains(r#"src="img/logo.png""#), "got: {out}");
    }

    #[cfg(not(miri))]
    #[test]
    fn rewrite_html_leaves_external_links() {
        let html = r#"<a href="https://external.com/page">Ext</a>"#;
        let out = rewrite_html(
            html,
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert!(out.contains("https://external.com/page"), "got: {out}");
    }

    #[cfg(not(miri))]
    #[test]
    fn rewrite_html_subdomain_link_rewritten() {
        let html = r#"<a href="https://sub.example.com/page.html">Sub</a>"#;
        let out = rewrite_html(
            html,
            "https://example.com/index.html",
            "index.html",
            "example.com",
        );
        assert!(out.contains(r#"href="page.html""#), "got: {out}");
    }

    // ── rewrite_css ───────────────────────────────────────────────────────────

    #[test]
    fn rewrite_css_url_unquoted() {
        let css = "body { background: url(https://example.com/bg.png); }";
        let out = rewrite_css(
            css,
            "https://example.com/style.css",
            "style.css",
            "example.com",
        );
        assert!(out.contains("url(bg.png)"), "got: {out}");
    }

    #[test]
    fn rewrite_css_url_double_quoted() {
        let css = r#"body { background: url("https://example.com/bg.png"); }"#;
        let out = rewrite_css(
            css,
            "https://example.com/style.css",
            "style.css",
            "example.com",
        );
        assert!(out.contains(r#"url("bg.png")"#), "got: {out}");
    }

    #[test]
    fn rewrite_css_external_url_unchanged() {
        let css = "body { background: url(https://other.com/bg.png); }";
        let out = rewrite_css(
            css,
            "https://example.com/style.css",
            "style.css",
            "example.com",
        );
        assert!(out.contains("https://other.com/bg.png"), "got: {out}");
    }

    // ── extract_links ─────────────────────────────────────────────────────────
    // Excluded from Miri: same cssparser false positive as rewrite_html above.

    #[cfg(not(miri))]
    #[test]
    fn extract_links_returns_internal_only() {
        let html = br#"
            <a href="https://example.com/page1.html">P1</a>
            <a href="https://example.com/page2.html">P2</a>
            <a href="https://other.com/external">Ext</a>
        "#;
        let links = extract_links(html, "https://example.com/index.html", "example.com");
        assert!(links.contains(&"https://example.com/page1.html".to_string()));
        assert!(links.contains(&"https://example.com/page2.html".to_string()));
        assert!(!links.iter().any(|l| l.contains("other.com")));
    }

    #[cfg(not(miri))]
    #[test]
    fn extract_links_deduplicates() {
        let html = br#"
            <a href="https://example.com/page.html">1</a>
            <a href="https://example.com/page.html">2</a>
        "#;
        let links = extract_links(html, "https://example.com/", "example.com");
        assert_eq!(links.iter().filter(|l| l.contains("page.html")).count(), 1);
    }

    #[cfg(not(miri))]
    #[test]
    fn extract_links_strips_query_and_fragment() {
        let html = br#"<a href="https://example.com/page.html?q=1#section">link</a>"#;
        let links = extract_links(html, "https://example.com/", "example.com");
        assert!(links.contains(&"https://example.com/page.html".to_string()));
        assert!(!links.iter().any(|l| l.contains('?') || l.contains('#')));
    }

    #[cfg(not(miri))]
    #[test]
    fn extract_links_includes_src_attributes() {
        let html = br#"<img src="https://example.com/logo.png">"#;
        let links = extract_links(html, "https://example.com/", "example.com");
        assert!(links.contains(&"https://example.com/logo.png".to_string()));
    }

    // ── suspend state ─────────────────────────────────────────────────────────

    #[test]
    fn suspend_file_path_format() {
        let p = suspend_file_path(
            Path::new("/out/.wayback-scraper"),
            "example.com",
            "20091204120000",
        );
        assert_eq!(
            p,
            Path::new("/out/.wayback-scraper/suspend_example.com_20091204120000.json")
        );
    }

    #[test]
    fn suspend_state_round_trips() {
        let state = SuspendState {
            version: 1,
            suspended_at: "2026-05-26T12:00:00+00:00".to_string(),
            apex: "example.com".to_string(),
            args: SavedArgs {
                url: "https://example.com".to_string(),
                output: PathBuf::from("/tmp/out"),
                verbose: false,
                include_exact_copies: false,
                after: None,
                before: Some("20100101".to_string()),
            },
            last_timestamp: "20091204120000".to_string(),
            current_delay_ms: 500,
            failed_urls: std::collections::HashSet::from(["http://example.com/404".to_string()]),
            memo: std::collections::HashMap::from([(
                "index.html".to_string(),
                (
                    PathBuf::from("/tmp/out/20091204120000/index.html"),
                    0xdeadbeef_u64,
                ),
            )]),
            ts_done: 10,
            downloaded: 8,
            linked: 2,
            skipped: 1,
            errors: 0,
            discovered: 5,
            total_bytes: 1024,
            total_saved: 512,
        };
        let json = serde_json::to_string(&state).unwrap();
        let got: SuspendState = serde_json::from_str(&json).unwrap();
        assert_eq!(got.version, 1);
        assert_eq!(got.apex, "example.com");
        assert_eq!(got.last_timestamp, "20091204120000");
        assert_eq!(got.current_delay_ms, 500);
        assert!(got.failed_urls.contains("http://example.com/404"));
        assert_eq!(got.memo["index.html"].1, 0xdeadbeef_u64);
        assert_eq!(got.args.before.as_deref(), Some("20100101"));
        assert_eq!(got.ts_done, 10);
        assert_eq!(got.total_bytes, 1024);
    }

    // ── save_suspend_state & load_suspend_file ────────────────────────────────

    // Excluded from Miri: uses SystemTime::now() which requires OS isolation.
    #[cfg(not(miri))]
    #[test]
    fn save_and_reload_suspend_state() {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let dir = std::env::temp_dir().join(format!("wayback_test_{nonce}"));
        let cache_dir = dir.join(".wayback-scraper");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let state = SuspendState {
            version: 1,
            suspended_at: "2026-05-26T12:00:00+00:00".to_string(),
            apex: "test.com".to_string(),
            args: SavedArgs {
                url: "https://test.com".to_string(),
                output: dir.clone(),
                verbose: false,
                include_exact_copies: false,
                after: None,
                before: None,
            },
            last_timestamp: "20200101000000".to_string(),
            current_delay_ms: 250,
            failed_urls: std::collections::HashSet::new(),
            memo: std::collections::HashMap::new(),
            ts_done: 1,
            downloaded: 1,
            linked: 0,
            skipped: 0,
            errors: 0,
            discovered: 0,
            total_bytes: 100,
            total_saved: 0,
        };

        let path = save_suspend_state(&cache_dir, &state).unwrap();
        assert!(path.exists());
        assert_eq!(path, cache_dir.join("suspend_test.com_20200101000000.json"));

        let loaded = load_suspend_file(&path).unwrap();
        assert_eq!(loaded.apex, "test.com");
        assert_eq!(loaded.last_timestamp, "20200101000000");
        assert_eq!(loaded.ts_done, 1);

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
