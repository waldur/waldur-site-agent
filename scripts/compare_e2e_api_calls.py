#!/usr/bin/env python3
"""Compare API call counts and response sizes between two E2E test runs.

Usage:
    python scripts/compare_e2e_api_calls.py baseline.json optimized.json

The JSON files are generated alongside the markdown reports by the E2E tests.
"""

from __future__ import annotations

import json
import sys


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024 / 1024:.1f} MB"


def _pct(saved: int, base: int) -> str:
    if base <= 0:
        return "n/a"
    return f"{saved / base * 100:.0f}%"


def _get_test_data(data: dict, test_id: str) -> tuple[int, int]:
    """Return (calls, bytes) for a test, handling both old and new JSON format."""
    tests = data.get("tests", {})
    entry = tests.get(test_id)
    if entry is None:
        return 0, 0
    if isinstance(entry, dict):
        return entry.get("calls", 0), entry.get("response_bytes", 0)
    # Old format: just an int (call count)
    return entry, 0


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <baseline.json> <optimized.json>")
        sys.exit(1)

    with open(sys.argv[1]) as f:
        baseline = json.load(f)
    with open(sys.argv[2]) as f:
        optimized = json.load(f)

    all_tests = sorted(
        set(baseline.get("tests", {})) | set(optimized.get("tests", {}))
    )

    print(f"Baseline:  {baseline.get('date', '?')}")
    print(f"Optimized: {optimized.get('date', '?')}")

    # --- API Calls ---
    print()
    print("API CALLS")
    print(f"{'Test':<55} {'Base':>6} {'Opt':>6} {'Saved':>6} {'%':>7}")
    print("-" * 82)

    total_base_calls = 0
    total_opt_calls = 0

    for test in all_tests:
        bc, _ = _get_test_data(baseline, test)
        oc, _ = _get_test_data(optimized, test)
        saved = bc - oc
        marker = " *" if saved > 0 else ""
        print(f"{test:<55} {bc:>6} {oc:>6} {saved:>6} {_pct(saved, bc):>7}{marker}")
        total_base_calls += bc
        total_opt_calls += oc

    saved_calls = total_base_calls - total_opt_calls
    print("-" * 82)
    print(
        f"{'TOTAL':<55} {total_base_calls:>6} {total_opt_calls:>6} "
        f"{saved_calls:>6} {_pct(saved_calls, total_base_calls):>7}"
    )

    # --- Response Size ---
    total_base_bytes = baseline.get("total_response_bytes", 0)
    total_opt_bytes = optimized.get("total_response_bytes", 0)

    has_bytes = total_base_bytes > 0 or total_opt_bytes > 0
    if not has_bytes:
        return

    print()
    print("RESPONSE SIZE")
    print(f"{'Test':<55} {'Base':>10} {'Opt':>10} {'Saved':>10} {'%':>7}")
    print("-" * 94)

    total_base_b = 0
    total_opt_b = 0

    for test in all_tests:
        _, bb = _get_test_data(baseline, test)
        _, ob = _get_test_data(optimized, test)
        saved = bb - ob
        marker = " *" if saved > 0 else ""
        print(
            f"{test:<55} {_fmt_bytes(bb):>10} {_fmt_bytes(ob):>10} "
            f"{_fmt_bytes(saved):>10} {_pct(saved, bb):>7}{marker}"
        )
        total_base_b += bb
        total_opt_b += ob

    saved_b = total_base_b - total_opt_b
    print("-" * 94)
    print(
        f"{'TOTAL':<55} {_fmt_bytes(total_base_b):>10} {_fmt_bytes(total_opt_b):>10} "
        f"{_fmt_bytes(saved_b):>10} {_pct(saved_b, total_base_b):>7}"
    )


if __name__ == "__main__":
    main()
