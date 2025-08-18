import os
import subprocess
import sys


def main():
    invoked_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    # Strip the prefix if it exists:
    prefix = "cloud_optimised_"
    if invoked_name.startswith(prefix):
        invoked_name = invoked_name[len(prefix) :]

    command = ["generic_cloud_optimised_creation", "--config", invoked_name] + sys.argv[
        1:
    ]
    subprocess.run(command, check=True)
