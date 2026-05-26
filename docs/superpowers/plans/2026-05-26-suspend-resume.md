# Suspend-to-Disk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `s` key to suspend the running scraper session to a JSON file, and `--resume` / `--resume-file` flags to restore it identically.

**Architecture:** All session state (last timestamp processed, dedup memo, 4XX cache, adaptive delay, stats, and original CLI args) is serialised to `<output>/.wayback-scraper/suspend_<apex>_<timestamp>.json`. On resume the CDX cache on disk is filtered to `>= last_timestamp` and all runtime variables are initialised from the file instead of their zero defaults. No new files are created — everything stays in `src/main.rs`.

**Tech Stack:** Rust, serde + serde_json (serde_json already present; only `serde` with `derive` feature is new), chrono (already present), clap (already present).

---

## File Map

| File | Change |
|---|---|
| `Cargo.toml` | Add `serde = { version = "1", features = ["derive"] }` |
| `src/main.rs` | All implementation changes (structs, helpers, Args, main logic) |

---

### Task 1: Add serde derive to Cargo.toml

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Add serde with derive feature**

In `Cargo.toml`, add to `[dependencies]`:
```toml
serde = { version = "1", features = ["derive"] }
```

The final `[dependencies]` block should look like:
```toml
[dependencies]
anyhow = "1"
chrono = "0.4"
clap = { version = "4", features = ["derive"] }
lol_html = "1"
regex = "1"
reqwest = { version = "0.12", features = ["json"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
url = "2"
```

- [ ] **Step 2: Verify it compiles**

```
cargo check
```

Expected: no errors.

- [ ] **Step 3: Commit**

```
git add Cargo.toml Cargo.lock
git commit -m "feat: add serde derive for suspend state serialisation"
```

---

### Task 2: Add serialisation types and helpers

**Files:**
- Modify: `src/main.rs` — add after the `// ─── Formatting helpers ───` section (around line 563)
- Test: `src/main.rs` — add tests to the `#[cfg(test)] mod tests` block

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `src/main.rs`:

```rust
// ── suspend state ─────────────────────────────────────────────────────────

#[test]
fn suspend_file_path_format() {
    let p = suspend_file_path(Path::new("/out/.wayback-scraper"), "example.com", "20091204120000");
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
            (PathBuf::from("/tmp/out/20091204120000/index.html"), 0xdeadbeef_u64),
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
```

- [ ] **Step 2: Run tests to verify they fail**

```
cargo test suspend 2>&1
```

Expected: compile error — `SuspendState`, `SavedArgs`, `suspend_file_path` not defined.

- [ ] **Step 3: Add the structs and helper**

Add the following after the `// ─── Formatting helpers ───` section (after `format_bytes`):

```rust
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
```

- [ ] **Step 4: Run tests to verify they pass**

```
cargo test suspend 2>&1
```

Expected: `suspend_file_path_format` and `suspend_state_round_trips` both PASS.

- [ ] **Step 5: Commit**

```
git add src/main.rs
git commit -m "feat: add SuspendState serialisation types"
```

---

### Task 3: Add `save_suspend_state`, `load_suspend_file` helpers and I/O test

**Files:**
- Modify: `src/main.rs` — add two functions after `suspend_file_path`
- Test: `src/main.rs` — add one I/O test to `mod tests`

- [ ] **Step 1: Write the failing test**

Add to `mod tests`:

```rust
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
    assert_eq!(
        path,
        cache_dir.join("suspend_test.com_20200101000000.json")
    );

    let loaded = load_suspend_file(&path).unwrap();
    assert_eq!(loaded.apex, "test.com");
    assert_eq!(loaded.last_timestamp, "20200101000000");
    assert_eq!(loaded.ts_done, 1);

    std::fs::remove_dir_all(&dir).unwrap();
}
```

- [ ] **Step 2: Run test to verify it fails**

```
cargo test save_and_reload 2>&1
```

Expected: compile error — `save_suspend_state` and `load_suspend_file` not defined.

- [ ] **Step 3: Implement the two helpers**

Add immediately after `suspend_file_path`:

```rust
fn save_suspend_state(cache_dir: &Path, state: &SuspendState) -> Result<PathBuf> {
    let path = suspend_file_path(cache_dir, &state.apex, &state.last_timestamp);
    let json = serde_json::to_string_pretty(state).context("serialize suspend state")?;
    fs::write(&path, &json)
        .with_context(|| format!("write suspend file: {}", path.display()))?;
    Ok(path)
}

fn load_suspend_file(path: &Path) -> Result<SuspendState> {
    let json = fs::read_to_string(path)
        .with_context(|| format!("read suspend file: {}", path.display()))?;
    serde_json::from_str(&json)
        .with_context(|| format!("parse suspend file: {}", path.display()))
}
```

- [ ] **Step 4: Run test to verify it passes**

```
cargo test save_and_reload 2>&1
```

Expected: PASS.

- [ ] **Step 5: Run all tests to catch regressions**

```
cargo test 2>&1
```

Expected: all 58 tests pass (55 original + 3 new).

- [ ] **Step 6: Commit**

```
git add src/main.rs
git commit -m "feat: add save_suspend_state and load_suspend_file helpers"
```

---

### Task 4: Add `pick_suspend_file` helper

**Files:**
- Modify: `src/main.rs` — add function after `load_suspend_file`

No unit test for this function — it reads from `stdin` interactively. It will be exercised via manual testing at the end.

- [ ] **Step 1: Implement `pick_suspend_file`**

Add immediately after `load_suspend_file`:

```rust
/// Search `dir/.wayback-scraper/` for suspend files.
/// If exactly one is found, auto-selects it. If multiple, prompts the user.
/// Returns an error if none are found.
fn pick_suspend_file(dir: &Path) -> Result<PathBuf> {
    let cache_dir = dir.join(".wayback-scraper");

    let mut files: Vec<(PathBuf, std::time::SystemTime)> = Vec::new();
    if let Ok(entries) = fs::read_dir(&cache_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
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
    files.sort_by(|a, b| b.1.cmp(&a.1));

    if files.len() == 1 {
        let path = files.remove(0).0;
        eprintln!("Loading suspend file: {}", path.display());
        return Ok(path);
    }

    eprintln!("Suspend files found:");
    for (i, (path, mtime)) in files.iter().enumerate() {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?");
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
```

- [ ] **Step 2: Verify compilation**

```
cargo check 2>&1
```

Expected: no errors.

- [ ] **Step 3: Commit**

```
git add src/main.rs
git commit -m "feat: add pick_suspend_file interactive selector"
```

---

### Task 5: Update `Args` struct — add `--resume` / `--resume-file`, make `url`/`output` optional

**Files:**
- Modify: `src/main.rs` — `Args` struct (lines 88–119)

- [ ] **Step 1: Replace the `Args` struct**

Find and replace the entire `Args` struct (from `struct Args {` through its closing `}`):

```rust
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
```

- [ ] **Step 2: Verify compilation**

```
cargo check 2>&1
```

Expected: errors about `args.url` and `args.output` — those will be fixed in the next task when we thread the effective config through `main()`.

- [ ] **Step 3: Commit the partial change**

```
git add src/main.rs
git commit -m "feat: add --resume / --resume-file to Args (main() not yet updated)"
```

---

### Task 6: Thread effective config through `main()` and add resume entry path

**Files:**
- Modify: `src/main.rs` — `main()` function

This task replaces all `args.url`, `args.output`, `args.verbose`, `args.include_exact_copies`, `args.after`, `args.before` references in `main()` with local variables, and adds the resume state load at the top.

- [ ] **Step 1: Replace the opening block of `main()` (arg parsing through apex extraction)**

Find:
```rust
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
    log!("Controls: 'p' + Enter to pause  |  'r' + Enter to resume  |  Ctrl+C to stop");
    eprintln!();
```

Replace with:
```rust
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
    eprintln!();
```

- [ ] **Step 2: Replace `&args.output` references in the `create_dir_all` and client section**

Find:
```rust
    fs::create_dir_all(&args.output).with_context(|| {
        format!(
            "could not create output directory: {}",
            args.output.display()
        )
    })?;
```

Replace with:
```rust
    fs::create_dir_all(&output).with_context(|| {
        format!("could not create output directory: {}", output.display())
    })?;
```

- [ ] **Step 3: Replace `args.output` in cache-dir setup and CDX cache path**

Find:
```rust
    let cache_dir = args.output.join(".wayback-scraper");
```

Replace with:
```rust
    let cache_dir = output.join(".wayback-scraper");
```

Find:
```rust
    let cdx_cache_path = cache_dir.join(format!("cdx_{apex}.json"));
```
This line does not reference args, leave it as-is.

Find:
```rust
    log!("Scanning output directory for existing files…");
    let mut existing =
        scan_existing_files(&args.output).context("failed to scan existing files")?;
```

Replace with:
```rust
    // For resume, CDX cache must already exist on disk.
    if resume_state.is_some() && !cdx_cache_path.exists() {
        anyhow::bail!(
            "Cannot resume: CDX cache not found at {}",
            cdx_cache_path.display()
        );
    }

    log!("Scanning output directory for existing files…");
    let mut existing =
        scan_existing_files(&output).context("failed to scan existing files")?;
```

- [ ] **Step 4: Replace `args.include_exact_copies` in dedup setup**

Find:
```rust
    let dedup = !args.include_exact_copies;
```

Replace with:
```rust
    let dedup = !include_exact_copies;
```

- [ ] **Step 5: Initialise runtime state from resume_state**

Find the stats variable block:
```rust
    let mut ts_done: usize = 0;
    let mut downloaded: usize = 0;
    let mut linked: usize = 0;
    let mut skipped: usize = 0;
    let mut errors: usize = 0;
    let mut discovered: usize = 0;
    let mut total_bytes: u64 = 0;
    let mut total_saved: u64 = 0;
    let mut consecutive_blocks: u32 = 0;
    let mut circuit_trips: u32 = 0;
    let mut last_timestamp = String::new();
    // Adaptive inter-request delay.  Bumps up on throttling, decays on success.
    let mut current_delay_ms: u64 = MIN_REQUEST_DELAY_MS;
    // 4XX responses cached for the session — same Wayback URL won't be re-fetched.
    let mut failed_urls: HashSet<String> = HashSet::new();
```

Replace with:
```rust
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
    let mut current_delay_ms: u64 =
        resume_state.as_ref().map_or(MIN_REQUEST_DELAY_MS, |s| s.current_delay_ms);
    let mut failed_urls: HashSet<String> =
        resume_state.as_ref().map_or_else(HashSet::new, |s| s.failed_urls.clone());
    let mut memo: HashMap<String, (PathBuf, u64)> =
        resume_state.as_ref().map_or_else(HashMap::new, |s| s.memo.clone());
```

Note: this replaces the `let mut memo: HashMap<...> = HashMap::new();` that currently lives near `let dedup = ...`. Find and remove that original `memo` declaration:
```rust
    let mut memo: HashMap<String, (PathBuf, u64)> = HashMap::new();
```
(It appears just before `scan_existing_files`.) Delete it — it's now part of the block above.

- [ ] **Step 6: Apply resume filter in CDX cached loading**

Find the `is_cached && offset == 0` branch:
```rust
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
```

Replace with:
```rust
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
                log!("Resuming: {} CDX entries remaining (>= {})", pairs.len(), from_ts);
            }
            log!("CDX entries found : {}", pairs.len());
            eprintln!();
            (pairs, true)
```

- [ ] **Step 7: Replace remaining `args.` references in CDX fetch call**

Find:
```rust
            let page = fetch_cdx_page(
                &client,
                &apex,
                offset,
                args.verbose,
                args.after.as_deref(),
                args.before.as_deref(),
            )
```

Replace with:
```rust
            let page = fetch_cdx_page(
                &client,
                &apex,
                offset,
                verbose,
                after.as_deref(),
                before.as_deref(),
            )
```

- [ ] **Step 8: Replace `args.verbose` in download_snapshot calls and progress logging**

There are two `args.verbose` references inside the download loop. Find each instance of `args.verbose` in `main()` and replace with `verbose`. Also find `args.include_exact_copies` if any remain and replace with `include_exact_copies`.

Use: `cargo check 2>&1 | grep "args\."` to find any remaining references and fix them.

- [ ] **Step 9: Verify compilation**

```
cargo check 2>&1
```

Expected: no errors. If any `args.` references remain, fix them now.

- [ ] **Step 10: Run all tests**

```
cargo test 2>&1
```

Expected: all 58 tests pass.

- [ ] **Step 11: Commit**

```
git add src/main.rs
git commit -m "feat: thread effective config through main(), add resume entry path"
```

---

### Task 7: Add `suspending` flag, stdin `s` handler, and suspend exit path

**Files:**
- Modify: `src/main.rs` — `main()` function

- [ ] **Step 1: Add `suspending` Arc alongside `paused` and `shutdown`**

Find the block that creates `paused`:
```rust
    let paused = Arc::new(AtomicBool::new(false));
    {
        let paused = paused.clone();
        tokio::spawn(async move {
            use tokio::io::AsyncBufReadExt as _;
            let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match line.trim() {
                    "p" => {
                        paused.store(true, Ordering::Relaxed);
                        log!("Paused — send 'r' to resume");
                    }
                    "r" => {
                        paused.store(false, Ordering::Relaxed);
                        log!("Resumed");
                    }
                    _ => {}
                }
            }
        });
    }
```

Replace with:
```rust
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
                        log!("Paused — send 'r' to resume");
                    }
                    "r" => {
                        paused.store(false, Ordering::Relaxed);
                        log!("Resumed");
                    }
                    "s" => {
                        suspending.store(true, Ordering::Relaxed);
                        log!("Suspending — finishing current download…");
                    }
                    _ => {}
                }
            }
        });
    }
```

- [ ] **Step 2: Add `suspending` checks in the inner URL loop**

Find (the first shutdown check at the top of the inner loop):
```rust
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                while paused.load(Ordering::Relaxed) {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    sleep(Duration::from_millis(200)).await;
                }
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
```

Replace with:
```rust
                if shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
                    break;
                }
                while paused.load(Ordering::Relaxed) {
                    if shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
                        break;
                    }
                    sleep(Duration::from_millis(200)).await;
                }
                if shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
                    break;
                }
```

- [ ] **Step 3: Add `suspending` check in the timestamp drain loop**

Find (at the top of the inner drain `loop {`):
```rust
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
```

Replace with:
```rust
        loop {
            if shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
                break;
            }
```

- [ ] **Step 4: Add `suspending` to the CDX page loop exit condition**

Find:
```rust
        if is_last || shutdown.load(Ordering::Relaxed) {
            break;
        }
```

Replace with:
```rust
        if is_last || shutdown.load(Ordering::Relaxed) || suspending.load(Ordering::Relaxed) {
            break;
        }
```

- [ ] **Step 5: Add suspend save logic after the main loop**

Find (the block immediately after the CDX page loop closes — the `eprintln!()` and summary):
```rust
    eprintln!();
    if shutdown.load(Ordering::Relaxed) && !last_timestamp.is_empty() {
        log!("Stopped at timestamp {last_timestamp}");
    }
    log!(
        "Done.  timestamps={ts_done}  cdx={cdx_count}  discovered={discovered}  \
         downloaded={downloaded}  linked={linked}  skipped={skipped}  errors={errors}  \
         bytes={}  saved={}",
        format_bytes(total_bytes),
        format_bytes(total_saved)
    );

    Ok(())
```

Replace with:
```rust
    eprintln!();
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
                    log!(
                        "Suspended — resume with: wayback-scraper --resume-file {path_str}"
                    );
                    println!("{path_str}");
                }
                Err(e) => log!("[ERROR] Failed to save suspend state: {e:#}"),
            }
        }
    }

    log!(
        "Done.  timestamps={ts_done}  cdx={cdx_count}  discovered={discovered}  \
         downloaded={downloaded}  linked={linked}  skipped={skipped}  errors={errors}  \
         bytes={}  saved={}",
        format_bytes(total_bytes),
        format_bytes(total_saved)
    );

    Ok(())
```

- [ ] **Step 6: Verify compilation**

```
cargo check 2>&1
```

Expected: no errors.

- [ ] **Step 7: Run all tests**

```
cargo test 2>&1
```

Expected: all 58 tests pass.

- [ ] **Step 8: Commit**

```
git add src/main.rs
git commit -m "feat: add suspend flag, stdin s handler, and resume entry path"
```

---

### Task 8: Format, CI, and push

**Files:**
- Run: `cargo fmt`, `python ci.py`, `git push`

- [ ] **Step 1: Format**

```
cargo fmt
```

- [ ] **Step 2: Verify no changes broke formatting**

```
cargo fmt --check
```

Expected: no output (already formatted).

- [ ] **Step 3: Run full CI suite locally**

```
python ci.py
```

Expected: all 5 jobs pass (Check, Rustfmt, Clippy, Test, Miri).

- [ ] **Step 4: Commit format-only changes if any**

```
git diff --quiet || git commit -am "style: cargo fmt after suspend feature"
```

- [ ] **Step 5: Push**

```
git push
```

---

## Self-Review Against Spec

**Spec coverage check:**

| Spec requirement | Covered by task |
|---|---|
| `s` key triggers suspend | Task 7, Step 1 |
| Save to `<output>/.wayback-scraper/suspend_<apex>_<ts>.json` | Task 2 (`suspend_file_path`), Task 3 (`save_suspend_state`) |
| Print file path on command line (`println!`) | Task 7, Step 5 |
| `--resume / -r [OUTPUT_DIR]` flag | Task 5 |
| `--resume` searches `<dir>/.wayback-scraper/` | Task 4 (`pick_suspend_file`) |
| Auto-select when exactly one file | Task 4, Step 1 |
| Numbered prompt when multiple files | Task 4, Step 1 |
| Error message when none found | Task 4, Step 1 |
| `--resume-file <PATH>` flag | Task 5 |
| `url`/`output` read from suspend file in resume mode | Task 6, Step 1 |
| CDX filtered to `>= last_timestamp` | Task 6, Step 6 |
| `current_delay_ms` restored | Task 6, Step 5 |
| `failed_urls` restored | Task 6, Step 5 |
| `memo` restored | Task 6, Step 5 |
| Stats restored (cumulative across suspend/resume) | Task 6, Step 5 |
| CDX cache must exist on resume or error | Task 6, Step 3 |
| `--resume` conflicts with `--resume-file` | Task 5, Step 1 (`conflicts_with`) |
| `s` with no timestamps yet → message + exit | Task 7, Step 5 |
| Resume announce log line | Task 6, Step 1 |
| Controls line updated to include `s` | Task 6, Step 1 |
| `serde` with derive added | Task 1 |
| Version field for future compat | Task 2 |
| `suspended_at` ISO 8601 field | Task 7, Step 5 |

All spec requirements are covered. ✓
