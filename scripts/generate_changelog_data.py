#!/usr/bin/env python3
"""
Collect commit data between two refs and output structured JSON for changelog generation.

Usage:
    python3 scripts/generate_changelog_data.py <current_ref> <previous_ref>

If <current_ref> tag doesn't exist, HEAD is used instead.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import date


def run_command(cmd, cwd=None):
    """Run command and return stdout. Returns empty string on failure."""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {cmd}", file=sys.stderr)
        if e.stderr:
            print(f"  stderr: {e.stderr.strip()}", file=sys.stderr)
        return ""


def get_repo_root():
    root = run_command("git rev-parse --show-toplevel")
    if not root:
        print("Error: not inside a git repository", file=sys.stderr)
        sys.exit(1)
    return root


def ref_exists(ref, cwd):
    """Check whether a git ref (tag, branch, commit) exists."""
    try:
        subprocess.run(
            f"git rev-parse --verify {ref}",
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def resolve_ref(ref, cwd):
    """Return the ref itself if it exists, otherwise fall back to HEAD."""
    if ref_exists(ref, cwd):
        return ref
    print(f"Ref '{ref}' not found, using HEAD instead", file=sys.stderr)
    return "HEAD"


# ---------------------------------------------------------------------------
# Category detection (tuned for waldur-site-agent commit style)
# ---------------------------------------------------------------------------

CATEGORY_RULES = [
    (
        "features",
        re.compile(
            r"^(Add |Implement |Support |Introduce |Create )", re.IGNORECASE
        ),
    ),
    (
        "fixes",
        re.compile(
            r"^(Fix |fix:|Correct |Handle .*(error|fail|invalid|missing|broken))",
            re.IGNORECASE,
        ),
    ),
    (
        "refactor",
        re.compile(
            r"^(Move |Refactor |Migrate |Remove |Clean )", re.IGNORECASE
        ),
    ),
    (
        "chore",
        re.compile(
            r"^(Bump |Release )|version.bump|dependency.update",
            re.IGNORECASE,
        ),
    ),
    (
        "docs",
        re.compile(r"^docs:", re.IGNORECASE),
    ),
]


def categorize_commit(subject, changed_files):
    """Return a category string for a commit."""
    for category, pattern in CATEGORY_RULES:
        if pattern.search(subject):
            return category

    # Docs heuristic: if all changed files are docs/ or README
    if changed_files and all(
        f.startswith("docs/") or f.upper().startswith("README") for f in changed_files
    ):
        return "docs"

    return "other"


# ---------------------------------------------------------------------------
# Commit collection
# ---------------------------------------------------------------------------


def collect_commits(prev_ref, current_ref, cwd):
    """Return list of commit dicts between prev_ref..current_ref."""
    fmt = "%h|%s|%an|%ad|%b%x00"
    cmd = (
        f"git log {prev_ref}..{current_ref} "
        f"--pretty=format:'{fmt}' --date=short --no-merges"
    )
    output = run_command(cmd, cwd=cwd)
    if not output:
        return []

    commits = []
    entries = [e.strip() for e in output.split("\x00") if e.strip()]
    for entry in entries:
        parts = entry.split("|", 4)
        if len(parts) < 4:
            continue
        commit_hash = parts[0]
        changed_files = get_commit_files(commit_hash, cwd)
        subject = parts[1]
        category = categorize_commit(subject, changed_files)

        commits.append(
            {
                "hash": commit_hash,
                "subject": subject,
                "author": parts[2],
                "date": parts[3],
                "body": (parts[4].strip()[:500] if len(parts) > 4 else ""),
                "changed_files": changed_files,
                "category": category,
            }
        )
    return commits


def get_commit_files(commit_hash, cwd):
    """Return list of files changed in a single commit."""
    output = run_command(
        f"git diff-tree --no-commit-id --name-only -r {commit_hash}", cwd=cwd
    )
    if not output:
        return []
    return [f for f in output.split("\n") if f.strip()]


# ---------------------------------------------------------------------------
# Aggregate stats
# ---------------------------------------------------------------------------


def aggregate_stats(prev_ref, current_ref, cwd):
    """Return dict with files_changed, lines_added, lines_removed."""
    numstat = run_command(f"git diff --numstat {prev_ref}..{current_ref}", cwd=cwd)
    files_changed = 0
    lines_added = 0
    lines_removed = 0

    for line in numstat.split("\n"):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 3:
            files_changed += 1
            try:
                lines_added += int(parts[0]) if parts[0] != "-" else 0
                lines_removed += int(parts[1]) if parts[1] != "-" else 0
            except ValueError:
                pass

    return {
        "files_changed": files_changed,
        "lines_added": lines_added,
        "lines_removed": lines_removed,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def build_categories(commits):
    """Group commits by category, returning {category: [commit, ...]}."""
    cats = {}
    for c in commits:
        cats.setdefault(c["category"], []).append(c)
    return cats


def main():
    parser = argparse.ArgumentParser(
        description="Collect commit data as structured JSON for changelog generation."
    )
    parser.add_argument("current_ref", help="Current version ref (tag or branch)")
    parser.add_argument("previous_ref", help="Previous version ref (tag or branch)")
    args = parser.parse_args()

    repo_root = get_repo_root()

    current = resolve_ref(args.current_ref, repo_root)
    previous = resolve_ref(args.previous_ref, repo_root)

    print(
        f"Collecting commits {previous}..{current} in {repo_root}",
        file=sys.stderr,
    )

    commits = collect_commits(previous, current, repo_root)
    stats = aggregate_stats(previous, current, repo_root)

    output = {
        "version": args.current_ref,
        "previous_version": args.previous_ref,
        "date": date.today().isoformat(),
        "summary_stats": {
            "total_commits": len(commits),
            **stats,
        },
        "commits": commits,
        "categories": build_categories(commits),
    }

    json.dump(output, sys.stdout, indent=2)
    print()  # trailing newline


if __name__ == "__main__":
    main()
