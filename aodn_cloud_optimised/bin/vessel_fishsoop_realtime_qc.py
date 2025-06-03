import subprocess
import os


def main():
    config_name = os.path.splitext(os.path.basename(__file__))[0]
    command = ["generic_cloud_optimised_creation", "--config", config_name]

    subprocess.run(command, check=True)
