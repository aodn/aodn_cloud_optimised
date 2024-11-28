#!/usr/bin/env python3
import subprocess
import sys

ORGS = [
    "csiro",
    "kordi",
    "bodc",
    "coriolis",
    "csio",
    "incois",
    "jma",
    "aoml",
    "nmdis",
    "meds",
    "kma",
]


def main():
    # splitting the path in a few commands so that the clusters are being recreated to avoid memory issues:

    for i, org in enumerate(ORGS):
        command = [
            "generic_cloud_optimised_creation",
            "--paths",
            f"IMOS/Argo/dac/{org}",
            "--suffix",
            "_prof.nc",
            "--dataset-config",
            "argo.json",
            "--force-previous-parquet-deletion",
            "--cluster-mode",
            "coiled",
        ]

        # Add the `--clear-existing-data` flag if `i == 0`
        if i == 0:
            command.append("--clear-existing-data")

        # Run the command
        subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
    sys.exit(0)
