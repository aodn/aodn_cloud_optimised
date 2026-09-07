import subprocess
import sys


def main():
    config_name = "ocean_glider_realtime_nonqc"
    command = ["generic_cloud_optimised_creation", "--config", config_name] + sys.argv[
        1:
    ]
    subprocess.run(command, check=True)
