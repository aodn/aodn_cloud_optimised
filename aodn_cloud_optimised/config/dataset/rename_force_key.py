import json
from pathlib import Path


def rename_force_key(config_dir: str):
    config_dir = Path(config_dir)
    for json_file in config_dir.glob("*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to load {json_file.name}: {e}")
            continue

        run_settings = data.get("run_settings", {})

        if "force_old_pq_del" in run_settings:
            run_settings["force_previous_parquet_deletion"] = run_settings.pop(
                "force_old_pq_del"
            )
            data["run_settings"] = run_settings

            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"[INFO] Updated {json_file.name}: renamed 'force_old_pq_del'")


if __name__ == "__main__":
    # Replace with the path to your JSON configs
    config_dir = Path(
        "/home/lbesnard/github_repo/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
    )
    rename_force_key(config_dir)
