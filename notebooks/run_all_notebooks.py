#!/usr/bin/env python3
"""
Integration test runner for all git-tracked notebooks.

Executes each notebook in-place (saves output back to the same file) using
papermill, running up to MAX_WORKERS notebooks concurrently.  Writes a
Markdown summary table to integration_test_results.md on completion.

Cells containing any pattern listed in SKIP_CELL_PATTERNS are stubbed out
before execution and restored in the saved output, so the notebook on disk
always contains the original source.

Usage:
    python run_all_notebooks.py [--workers N] [--timeout SECONDS]
"""

import argparse
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import nbformat

NOTEBOOKS_DIR = Path(__file__).resolve().parent
REPO_ROOT = NOTEBOOKS_DIR.parent

DEFAULT_WORKERS = 4
DEFAULT_TIMEOUT = 1800  # 30 min per notebook

# Cells whose source contains any of these strings will be skipped (not executed).
# The original source is restored in the saved notebook after the run.
SKIP_CELL_PATTERNS = [
    "plot_gridded_variable_viewer_calendar",
]


def get_tracked_notebooks() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "notebooks/"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return sorted(
        REPO_ROOT / p
        for p in result.stdout.splitlines()
        if p.endswith(".ipynb") and not Path(p).name.startswith("template_")
    )


def _cell_should_skip(source: str) -> bool:
    return any(pattern in source for pattern in SKIP_CELL_PATTERNS)


def run_notebook(path: Path, timeout: int) -> dict:
    start = time.monotonic()
    try:
        nb = nbformat.read(path, as_version=4)

        # Record original sources for cells we want to skip, then stub them out.
        skipped: dict[int, str] = {}
        for idx, cell in enumerate(nb.cells):
            if cell.cell_type == "code" and _cell_should_skip(cell.source):
                skipped[idx] = cell.source
                patterns = [p for p in SKIP_CELL_PATTERNS if p in cell.source]
                cell.source = f"# skipped by run_all_notebooks: {', '.join(patterns)}"

        # Write the (possibly modified) notebook to a temp file for papermill input.
        with tempfile.NamedTemporaryFile(
            suffix=".ipynb", delete=False, dir=NOTEBOOKS_DIR
        ) as tmp:
            tmp_path = Path(tmp.name)
        nbformat.write(nb, tmp_path)

        try:
            proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "papermill",
                    str(tmp_path),
                    str(path),  # save output in-place
                    "--no-progress-bar",
                    "--kernel",
                    "python3",
                ],
                cwd=NOTEBOOKS_DIR,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        finally:
            tmp_path.unlink(missing_ok=True)

        # Restore original sources in the saved output notebook.
        if skipped:
            out_nb = nbformat.read(path, as_version=4)
            for idx, original_source in skipped.items():
                if idx < len(out_nb.cells):
                    out_nb.cells[idx].source = original_source
            nbformat.write(out_nb, path)

        elapsed = time.monotonic() - start
        skipped_note = f" ({len(skipped)} cell(s) skipped)" if skipped else ""
        if proc.returncode == 0:
            return {
                "name": path.name,
                "status": "✅ pass",
                "elapsed": elapsed,
                "error": "",
                "skipped": len(skipped),
            }
        else:
            error_tail = (proc.stderr or proc.stdout or "")[-800:].strip()
            return {
                "name": path.name,
                "status": "❌ fail",
                "elapsed": elapsed,
                "error": error_tail,
                "skipped": len(skipped),
            }

    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        return {
            "name": path.name,
            "status": "⏱ timeout",
            "elapsed": elapsed,
            "error": f"Exceeded {timeout}s",
            "skipped": 0,
        }
    except Exception as exc:
        elapsed = time.monotonic() - start
        return {
            "name": path.name,
            "status": "❌ fail",
            "elapsed": elapsed,
            "error": str(exc),
            "skipped": 0,
        }


def fmt_time(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s:02d}s" if m else f"{s}s"


def write_report(results: list[dict], workers: int) -> Path:
    report_path = NOTEBOOKS_DIR / "integration_test_results.md"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    passed = sum(1 for r in results if "pass" in r["status"])
    failed = sum(1 for r in results if "fail" in r["status"])
    timed_out = sum(1 for r in results if "timeout" in r["status"])

    lines = [
        "# Notebook Integration Test Results",
        "",
        f"Run: {now}  |  {len(results)} notebooks  |  {workers} parallel workers",
        "",
        "| Notebook | Status | Time | Skipped cells |",
        "|----------|:------:|-----:|:-------------:|",
    ]

    for r in sorted(results, key=lambda x: x["name"]):
        skipped = f"{r['skipped']}" if r.get("skipped") else "—"
        lines.append(
            f"| `{r['name']}` | {r['status']} | {fmt_time(r['elapsed'])} | {skipped} |"
        )

    lines += [
        "",
        f"**Summary:** {passed} passed · {failed} failed · {timed_out} timed out",
    ]

    failures = [r for r in results if r["error"]]
    if failures:
        lines += ["", "## Failure details", ""]
        for r in sorted(failures, key=lambda x: x["name"]):
            lines += [
                f"### `{r['name']}`",
                "```",
                r["error"],
                "```",
                "",
            ]

    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Max parallel notebooks (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout per notebook in seconds (default: {DEFAULT_TIMEOUT})",
    )
    args = parser.parse_args()

    notebooks = get_tracked_notebooks()
    print(
        f"Found {len(notebooks)} notebooks. Running with {args.workers} workers "
        f"(timeout {args.timeout}s each)…\n"
    )

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_notebook, nb, args.timeout): nb for nb in notebooks}
        for future in as_completed(futures):
            r = future.result()
            print(f"  {r['status']}  {r['name']}  ({fmt_time(r['elapsed'])})")
            results.append(r)

    report_path = write_report(results, args.workers)

    passed = sum(1 for r in results if "pass" in r["status"])
    failed = sum(1 for r in results if "fail" in r["status"])
    timed_out = sum(1 for r in results if "timeout" in r["status"])
    print(f"\nDone — {passed} passed, {failed} failed, {timed_out} timed out")
    print(f"Report: {report_path}")

    # Exit non-zero if any notebook failed or timed out
    if failed or timed_out:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
