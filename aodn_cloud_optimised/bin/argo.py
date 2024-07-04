#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/Argo/dac/kordi",
        "IMOS/Argo/dac/csiro",
        "IMOS/Argo/dac/bodc",
        "IMOS/Argo/dac/csio",
        "IMOS/Argo/dac/incois",
        "IMOS/Argo/dac/jma",
        "IMOS/Argo/dac/coriolis",
        "IMOS/Argo/dac/aoml",
        "IMOS/Argo/dac/nmdis",
        "IMOS/Argo/dac/meds",
        "IMOS/Argo/dac/kma",
        "--suffix",
        "_prof.nc",
        "--dataset-config",
        "argo.json",
        "--clear-existing-data",
        "--force-previous-parquet-deletion",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
