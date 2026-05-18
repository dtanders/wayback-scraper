#!/usr/bin/env python3
"""Run CI checks locally by reading .github/workflows/ci.yml.

Extracts every `run:` step from each job (skipping `uses:` actions) and
executes them in order with the same global env vars CI sets.
"""

import os
import subprocess
import sys
from pathlib import Path

# Force UTF-8 + line-buffered output on Windows so headers interleave
# correctly with subprocess output and Unicode symbols render correctly
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

COLORS = sys.stdout.isatty()
CYAN  = "\033[36m"  if COLORS else ""
GREEN = "\033[32m"  if COLORS else ""
RED   = "\033[31m"  if COLORS else ""
BOLD  = "\033[1m"   if COLORS else ""
DIM   = "\033[2m"   if COLORS else ""
RESET = "\033[0m"   if COLORS else ""


def parse_global_env(lines: list[str]) -> dict[str, str]:
    """Parse the top-level `env:` block."""
    env: dict[str, str] = {}
    in_env = False
    for line in lines:
        raw = line.rstrip()
        stripped = raw.lstrip()
        indent = len(raw) - len(stripped)
        if raw == "env:":
            in_env = True
            continue
        if in_env:
            if indent == 2 and ":" in stripped and not stripped.startswith("-"):
                k, _, v = stripped.partition(":")
                env[k.strip()] = v.strip()
            elif indent == 0 and stripped:
                break
    return env


def parse_jobs(lines: list[str]) -> list[tuple[str, list[str]]]:
    """Return list of (display_name, [run_command, ...]) in definition order.

    `run:` steps are collected directly.  `uses: dtolnay/rust-toolchain@<channel>`
    steps with a `components:` key are translated to the equivalent
    `rustup component add --toolchain <channel> <components>` command so that
    locally required toolchain components (e.g. miri, rustfmt) are installed
    automatically.  All other `uses:` steps are skipped.
    """
    jobs: list[dict] = []
    cur: dict | None = None
    in_jobs = in_steps = collecting_block = False
    block_indent: int | None = None
    pending_toolchain: str | None = None  # channel from dtolnay/rust-toolchain@<channel>

    for line in lines:
        raw = line.rstrip()
        stripped = raw.lstrip()
        indent = len(raw) - len(stripped)

        if not stripped:
            if collecting_block:
                collecting_block = False
            continue

        if raw == "jobs:":
            in_jobs = True
            continue

        if not in_jobs:
            continue

        # New job key at indent 2
        if indent == 2 and stripped.endswith(":") and not stripped.startswith("-"):
            cur = {"name": stripped[:-1], "runs": []}
            jobs.append(cur)
            in_steps = collecting_block = pending_toolchain = False
            continue

        if cur is None:
            continue

        if indent == 4 and stripped.startswith("name:"):
            cur["name"] = stripped[5:].strip().strip("'\"")
            continue

        if indent == 4 and stripped == "steps:":
            in_steps = True
            continue

        if not in_steps:
            continue

        # Any new step at indent 6 resets state
        if indent == 6 and stripped.startswith("- "):
            collecting_block = False
            pending_toolchain = None

            if stripped.startswith("- run:"):
                cmd = stripped[len("- run:"):].strip()
                if cmd in ("|", "|-", ">", ">-"):
                    collecting_block = True
                    block_indent = None
                elif cmd:
                    cur["runs"].append(cmd)
            elif "dtolnay/rust-toolchain@" in stripped:
                pending_toolchain = stripped.split("dtolnay/rust-toolchain@", 1)[1].strip()
            continue

        # `components:` inside a dtolnay/rust-toolchain `with:` block
        if pending_toolchain and indent == 10 and stripped.startswith("components:"):
            components = stripped[len("components:"):].strip()
            cur["runs"].append(
                f"rustup component add --toolchain {pending_toolchain} {components}"
            )
            pending_toolchain = None
            continue

        # Body of a multi-line run block
        if collecting_block and indent > 6:
            if block_indent is None:
                block_indent = indent
            if indent >= block_indent:
                cur["runs"].append(raw[block_indent:])
            else:
                collecting_block = False
            continue

    return [(j["name"], j["runs"]) for j in jobs if j["runs"]]


def run_job(name: str, cmds: list[str], env: dict[str, str]) -> bool:
    print(f"\n{BOLD}{CYAN}=== {name} ==={RESET}")
    for cmd in cmds:
        print(f"{DIM}  $ {cmd}{RESET}")
        result = subprocess.run(cmd, shell=True, env=env)
        if result.returncode != 0:
            print(f"\n{RED}FAILED (exit {result.returncode}): {cmd}{RESET}")
            return False
    print(f"{GREEN}  ✓ passed{RESET}")
    return True


def main() -> None:
    yaml_path = Path(__file__).parent / ".github" / "workflows" / "ci.yml"

    if not yaml_path.exists():
        print(f"{RED}Not found: {yaml_path}{RESET}", file=sys.stderr)
        sys.exit(1)

    lines = yaml_path.read_text(encoding="utf-8").splitlines()
    ci_env = parse_global_env(lines)
    jobs = parse_jobs(lines)

    if not jobs:
        print(f"No runnable steps found in {yaml_path.name}", file=sys.stderr)
        sys.exit(1)

    # Merge CI env vars on top of the current environment
    env = {**os.environ, **ci_env}

    print(f"{BOLD}CI checks — {yaml_path.name} — {len(jobs)} jobs{RESET}")
    if ci_env:
        for k, v in ci_env.items():
            print(f"{DIM}  env: {k}={v}{RESET}")

    failures: list[str] = []
    for name, cmds in jobs:
        if not run_job(name, cmds, env):
            failures.append(name)

    print(f"\n{'─' * 40}")
    if failures:
        print(f"{RED}{BOLD}✗ Failed: {', '.join(failures)}{RESET}")
        sys.exit(1)
    else:
        print(f"{GREEN}{BOLD}✓ All checks passed{RESET}")


if __name__ == "__main__":
    main()
