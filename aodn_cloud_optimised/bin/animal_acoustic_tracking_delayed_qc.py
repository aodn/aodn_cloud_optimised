#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/AATAMS/acoustic_tagging/",
        "--suffix",
        ".csv",
        "--dataset-config",
        "animal_acoustic_tracking_delayed_qc.json",
        "--clear-existing-data",
        "--cluster-mode",
        "coiled",
    ]

    # Run the command
    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
