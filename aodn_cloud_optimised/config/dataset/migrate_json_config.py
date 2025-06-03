import json
from pathlib import Path


def migrate_json_configs(directory: Path, verbose: bool = True) -> None:
    for json_file in directory.glob("*.json"):
        if verbose:
            print(f"Processing {json_file}...")

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to load JSON from {json_file}: {e}")
            continue

        # Initialise run_settings if missing
        run_settings = data.setdefault("run_settings", {})

        # Move coiled_cluster_options if at root
        if "coiled_cluster_options" in data:
            run_settings["coiled_cluster_options"] = data.pop("coiled_cluster_options")

        # Move batch_size if at root
        if "batch_size" in data:
            run_settings["batch_size"] = data.pop("batch_size")

        # Ensure cluster config exists
        run_settings.setdefault(
            "cluster", {"mode": "coiled", "restart_every_path": False}
        )

        # Ensure paths exist
        run_settings.setdefault(
            "paths",
            [{"s3_uri": "s3://imos-data/IMOS/", "filter": [], "year_range": []}],
        )

        run_settings.setdefault("clear_existing_data", True)
        run_settings.setdefault("raise_error", False)

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.write("\n")  # POSIX-compliant EOF newline

        if verbose:
            print(f"✓ Updated {json_file}\n")


# Usage
if __name__ == "__main__":
    config_dir = Path(
        "/home/lbesnard/github_repo/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
    )
    migrate_json_configs(config_dir)
