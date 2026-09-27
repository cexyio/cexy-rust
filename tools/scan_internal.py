#!/usr/bin/env python3
"""Fail if the repository mentions internal infrastructure (shared by all cexy repos).

Checks every tracked or untracked text file for private IPs, localhost ports, home paths
and secret files, plus any locally configured infrastructure patterns (see below).
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

# Generic patterns only. Infrastructure-specific patterns (hostnames, service ports, container
# names) are kept out of the repository: they are read from a local file named by
# $CEXY_SCAN_PATTERNS_FILE (default ~/.config/cexy/scan-patterns.txt), one regex per line,
# optionally "label<TAB>regex". In CI the file comes from a repository secret.
PATTERNS = {
    "private IP": r"\b(10\.\d{1,3}|192\.168|172\.(1[6-9]|2\d|3[01]))\.\d{1,3}\.\d{1,3}\b",
    "localhost port": r"\b(localhost|127\.0\.0\.1):\d{2,5}\b",
    "home path": r"/root/|/home/[a-z]+/",
    "secret file": r"(^|[\s/'\"])\.env(\.[a-z]+)?\b",
}


def load_local_patterns() -> dict[str, str]:
    import os

    path = Path(os.environ.get("CEXY_SCAN_PATTERNS_FILE", Path.home() / ".config" / "cexy" / "scan-patterns.txt"))
    extra: dict[str, str] = {}
    if path.is_file():
        for i, line in enumerate(path.read_text().splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            label, _, pattern = line.partition("\t") if "\t" in line else (f"local pattern {i}", "", line)
            extra[label] = pattern
    else:
        print(f"note: {path} not found; only generic patterns are checked", file=sys.stderr)
    return extra


SKIP_DIRS = {".git", "node_modules", ".venv", "dist", "build", "__pycache__"}
SELF = Path(__file__).resolve()


def files(root: Path):
    try:
        out = subprocess.run(["git", "ls-files", "--cached", "--others", "--exclude-standard"], cwd=root,
                             capture_output=True, text=True, check=True).stdout.split()
        yield from (root / f for f in out)
    except (subprocess.CalledProcessError, FileNotFoundError):
        for p in root.rglob("*"):
            if p.is_file() and not SKIP_DIRS & set(p.parts):
                yield p


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    patterns = {**PATTERNS, **load_local_patterns()}
    hits = []
    for f in files(root):
        if f.resolve() == SELF or f.name == ".gitignore" or not f.is_file() or f.stat().st_size > 5_000_000:
            continue
        try:
            text = f.read_text()
        except (UnicodeDecodeError, OSError):
            continue
        for n, line in enumerate(text.splitlines(), 1):
            for label, pat in patterns.items():
                if re.search(pat, line):
                    hits.append(f"{f.relative_to(root)}:{n}: {label}: {line.strip()[:120]}")
    if hits:
        print("internal references found:", *hits, sep="\n  ", file=sys.stderr)
        return 1
    print("no internal references")
    return 0


if __name__ == "__main__":
    sys.exit(main())
