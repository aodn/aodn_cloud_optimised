#!/usr/bin/env python3
import json
import re
from pathlib import Path


def extract_unique_variable_blocks(log_path):
    """
    Extract unique variable JSON blocks from a log file produced by GenericParquetHandler.

    Args:
        log_path (str | Path): Path to the log file.

    Returns:
        dict: A mapping of variable names to their configuration dicts.
    """
    log_path = Path(log_path)
    text = log_path.read_text(encoding="utf-8")

    # Regex to extract variable name and JSON block
    pattern = re.compile(
        r'Variable missing.*?\{\s*"(?P<var>[^"]+)":\s*(?P<json>\{.*?\})\s*\}', re.DOTALL
    )

    unique_vars = {}

    for match in pattern.finditer(text):
        var_name = match.group("var")
        json_block_str = match.group("json")

        try:
            json_block = json.loads(json_block_str)
        except json.JSONDecodeError:
            continue  # skip malformed ones safely

        # Ensure uniqueness
        if var_name not in unique_vars:
            unique_vars[var_name] = json_block

    return unique_vars


def main():
    import sys

    if len(sys.argv) != 2:
        print("Usage: python extract_missing_vars.py <log_file>")
        sys.exit(1)

    log_file = sys.argv[1]
    vars_dict = extract_unique_variable_blocks(log_file)

    if not vars_dict:
        print("No missing variable definitions found.")
    else:
        print(json.dumps(vars_dict, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
