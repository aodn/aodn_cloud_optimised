#!usr/bin/env python3
"""
Setup file for jupyter notebooks
"""

import importlib.util
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

import requests
from IPython import get_ipython
from packaging.version import InvalidVersion, Version

DATAQUERY_PATH = "DataQuery.py"
DATAQUERY_URL = "https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/aodn_cloud_optimised/lib/DataQuery.py"


def run_command(cmd):
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}")


def is_colab():
    # google colab
    try:
        import google.colab

        return True
    except ImportError:
        return False


def is_nectar():
    # For Nectar Instance https://jupyterhub.rc.nectar.org.au
    if "jupyter" in platform.uname().node:
        return True
    else:
        return False


def install_requirements():
    try:
        import uv
    except ImportError:
        run_command(f"{sys.executable} -m pip install uv")

    current_dir = Path.cwd()
    local_requirements = current_dir / "requirements.txt"
    if local_requirements.exists():
        requirements_path = local_requirements
    else:
        requirements_path = "https://raw.githubusercontent.com/aodn/aodn_cloud_optimised/main/notebooks/requirements.txt"

    if is_colab():
        import xarray as xr

        xr.set_options(display_style="text")
        run_command("uv pip install pyarrow --force-reinstall --upgrade")
        run_command(f"uv pip install --system -r {requirements_path}")
        run_command("uv pip install --system pyopenssl --upgrade")
        run_command("uv pip install numpy --force-reinstall")
        run_command("uv pip install pyarrow --force-reinstall --upgrade")

        # get_ipython().kernel.do_shutdown(restart=True)
    elif is_nectar():
        run_command(f"uv pip install --system -r {requirements_path}")
    else:
        run_command("uv venv")
        run_command(f"uv pip install -r {requirements_path}")


def load_dataquery():
    remote_version, remote_code = get_remote_version_and_code()
    if remote_version is None:
        print("❌ Remote file does not contain a valid __version__, skipping update.")
        return

    local_version = get_local_version()
    if local_version is None:
        print("⚠️ Local file has no version or is missing. Downloading remote file.")
        write_dataquery(remote_code)
    elif remote_version > local_version:
        print(
            f"🔄 Updating: local version {local_version} < remote version {remote_version}"
        )
        write_dataquery(remote_code)
    else:
        print(
            f"✅ Local version {local_version} is up to date (remote: {remote_version})"
        )


def get_local_version():
    if not os.path.exists(DATAQUERY_PATH):
        return None
    try:
        spec = importlib.util.spec_from_file_location("DataQuery", DATAQUERY_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        version_str = getattr(module, "__version__", None)
        return Version(version_str) if version_str else None
    except Exception as e:
        print(f"Error reading local version: {e}")
        return None


def get_remote_version_and_code():
    try:
        response = requests.get(DATAQUERY_URL)
        response.raise_for_status()
        code = response.text
        match = re.search(r'^__version__\s*=\s*["\']([^"\']+)["\']', code, re.MULTILINE)
        if match:
            version_str = match.group(1)
            return Version(version_str), code
        else:
            return None, code
    except Exception as e:
        print(f"Error fetching remote file: {e}")
        return None, None


def write_dataquery(code):
    with open(DATAQUERY_PATH, "w", encoding="utf-8") as f:
        f.write(code)
    print(f"📥 Wrote updated DataQuery.py")


if __name__ == "__main__":
    install_requirements()
    load_dataquery()
