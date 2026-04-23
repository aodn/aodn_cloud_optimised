#!/usr/bin/env python3
"""
Fix text inconsistencies across AODN notebooks (markdown cells only).

Applies word-boundary substitutions to standardise capitalisation and
phrasing. Safe to re-run (idempotent). Does not touch code cells.

Usage:
    python fix_notebook_text.py [--dry-run]
"""

import argparse
import json
import re
from pathlib import Path

import nbformat

NOTEBOOKS_DIR = Path(__file__).resolve().parent
REPO_ROOT = NOTEBOOKS_DIR.parent

# ---------------------------------------------------------------------------
# Substitution rules: (pattern, replacement, description)
# Applied in order to every markdown cell.  All patterns are case-sensitive
# except where re.IGNORECASE is specified per-rule.
# ---------------------------------------------------------------------------
RULES = [
    # --- QC / Non-QC ---
    # Must run most-specific first
    (r"\bNonqc\b", "Non-QC", "Nonqc → Non-QC"),
    (r"\bnonqc\b", "Non-QC", "nonqc → Non-QC"),
    (r"\bNonQC\b", "Non-QC", "NonQC → Non-QC"),
    (r"\bNon QC\b", "Non-QC", "Non QC → Non-QC"),
    (r"\bnon-qc\b", "Non-QC", "non-qc → Non-QC"),
    (r"\bnon QC\b", "Non-QC", "non QC → Non-QC"),
    (r"\bQc\b", "QC", "Qc → QC"),
    (r"\bqc\b", "QC", "qc → QC"),
    # --- Real-Time ---
    (r"\bRealtime\b", "Real-Time", "Realtime → Real-Time"),
    (r"\brealtime\b", "Real-Time", "realtime → Real-Time"),
    (r"\bReal time\b", "Real-Time", "Real time → Real-Time"),
    (r"\breal time\b", "Real-Time", "real time → Real-Time"),
    # --- Acronyms ---
    (r"\bCtd\b", "CTD", "Ctd → CTD"),
    (r"\bSst\b", "SST", "Sst → SST"),
    (r"\bAmsa\b", "AMSA", "Amsa → AMSA"),
    (r"\bCsiro\b", "CSIRO", "Csiro → CSIRO"),
    (r"\bGsm\b", "GSM", "Gsm → GSM"),
    (r"\bGamssa\b", "GAMSSA", "Gamssa → GAMSSA"),
    (r"\bRamssa\b", "RAMSSA", "Ramssa → RAMSSA"),
    # --- Compound words / typos ---
    (r"\bSouthernocean\b", "Southern Ocean", "Southernocean → Southern Ocean"),
    (r"\bsouthernocean\b", "Southern Ocean", "southernocean → Southern Ocean"),
    (r"\bCoffsharbour\b", "Coffs Harbour", "Coffsharbour → Coffs Harbour"),
    (r"\bcoffsharbour\b", "Coffs Harbour", "coffsharbour → Coffs Harbour"),
    (r"\bDelayec\b", "Delayed", "Delayec → Delayed"),
    (r"\bdelayec\b", "delayed", "delayec → delayed"),
    # --- Zarr title suffix ---
    # "... data in Zarr" at end of heading line → "... (Zarr)"
    (r"\s+data in Zarr\s*$", " (Zarr)", "data in Zarr → (Zarr)"),
]


def apply_rules(text: str) -> tuple[str, list[str]]:
    """Apply all rules to text, return (new_text, list_of_changes)."""
    changes = []
    for pattern, replacement, desc in RULES:
        new_text, n = re.subn(pattern, replacement, text)
        if n:
            changes.append(f"{desc} ({n}×)")
            text = new_text
    return text, changes


def fix_notebook(path: Path, dry_run: bool) -> list[str]:
    """Fix a single notebook. Returns list of change descriptions."""
    nb = nbformat.read(path, as_version=4)
    all_changes = []

    for idx, cell in enumerate(nb.cells):
        if cell.cell_type != "markdown":
            continue
        source = "".join(cell.source) if isinstance(cell.source, list) else cell.source
        new_source, changes = apply_rules(source)
        if changes:
            all_changes.extend(f"  cell {idx}: {c}" for c in changes)
            if not dry_run:
                cell.source = new_source

    if all_changes and not dry_run:
        nbformat.write(nb, path)

    return all_changes


def get_tracked_notebooks() -> list[Path]:
    import subprocess

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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing files",
    )
    args = parser.parse_args()

    mode = "DRY RUN" if args.dry_run else "FIXING"
    print(f"[{mode}] Scanning git-tracked notebooks (excl. template_*)…\n")

    notebooks = get_tracked_notebooks()
    total_changed = 0

    for nb_path in notebooks:
        changes = fix_notebook(nb_path, dry_run=args.dry_run)
        if changes:
            total_changed += 1
            print(f"{'(dry) ' if args.dry_run else ''}✏️  {nb_path.name}")
            for c in changes:
                print(f"    {c}")

    print(
        f"\n{'Would modify' if args.dry_run else 'Modified'} {total_changed} / {len(notebooks)} notebooks."
    )


if __name__ == "__main__":
    main()
