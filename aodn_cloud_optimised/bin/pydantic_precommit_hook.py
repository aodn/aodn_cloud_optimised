#!/usr/bin/env python3
import argparse
import logging
import sys
from typing import Optional

from pydantic import (
    ValidationError,
)

from aodn_cloud_optimised.bin.generic_cloud_optimised_creation import (
    load_config_and_validate,
)
from aodn_cloud_optimised.lib.common import list_dataset_config

logger = logging.getLogger(__name__)


def validate_all_configs(config_dir: str, exclude_regex: Optional[str] = None) -> int:
    """Validate all JSON config files in a directory using the DatasetConfig schema.

    Args:
        config_dir: Path to the directory containing JSON configuration files.
        exclude_regex: Optional regex pattern to exclude files.

    Returns:
        Number of invalid configurations (0 if all valid).
    """
    json_files = list_dataset_config()

    if not json_files:
        print(f"ℹ️ No JSON files to validate in {config_dir}")
        return 0

    print(f"🔍 Validating {len(json_files)} config file(s) in {config_dir}")
    errors = 0
    for json_file in json_files:
        try:
            load_config_and_validate(json_file)
        except ValidationError as e:
            print(f"\n❌ Validation failed in: {json_file}")
            print("─" * 80)
            print(e)
            print("─" * 80)
            errors += 1
        except Exception as e:
            print(f"\n❌ Error reading {json_file}: {e}")
            errors += 1

    if errors > 0:
        print(f"\n❌ {errors} configuration file(s) failed validation.")
    else:
        print("✅ All configurations are valid.")

    return errors


def main():
    parser = argparse.ArgumentParser(description="Pydantic validator with pre-commit.")
    parser.add_argument(
        "--validate-configs",
        type=str,
        help="Validate all JSON configs in a directory (no processing is done).",
    )

    args = parser.parse_args()

    if args.validate_configs:
        # Pre-commit pattern: match full path string
        exclude_pattern = r"^.*dataset_template\.json$"
        exit_code = validate_all_configs(
            args.validate_configs, exclude_regex=exclude_pattern
        )
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
