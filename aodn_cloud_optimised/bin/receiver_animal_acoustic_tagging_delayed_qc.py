#!/usr/bin/env python3
import subprocess


def main():
    command = [
        "generic_cloud_optimised_creation",
        "--paths",
        "IMOS/AATAMS/acoustic_tagging/",
        "--dataset-config",
        "receiver_animal_acoustic_tagging_delayed_qc.json",
        "--clear-existing-data",
        "--cluster-mode",
        "remote",
    ]

    # Run the command
    subprocess.run(command, check=True)
