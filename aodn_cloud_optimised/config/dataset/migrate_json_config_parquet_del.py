import json
from pathlib import Path


def migrate_force_old_pq_del(config_dir: str):
    config_dir = Path(config_dir)
    for json_file in config_dir.glob("*.json"):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Failed to load {json_file.name}: {e}")
            continue

        if "force_old_pq_del" in data:
            run_settings = data.get("run_settings", {})
            # Preserve existing run_settings keys
            run_settings["force_old_pq_del"] = data.pop("force_old_pq_del")
            data["run_settings"] = run_settings

            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(
                f"[INFO] Updated {json_file.name}: moved 'force_old_pq_del' into run_settings"
            )


if __name__ == "__main__":
    # Replace with your actual directory path
    migrate_force_old_pq_del(
        "/home/lbesnard/github_repo/aodn_cloud_optimised/aodn_cloud_optimised/config/dataset"
    )
