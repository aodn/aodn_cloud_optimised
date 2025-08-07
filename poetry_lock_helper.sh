#!/usr/bin/env bash
# This script runs poetry lock inside a Docker container with a fixed Python and Poetry version, making the process platform-agnostic.
set -euo pipefail

docker run --rm -t \
  -v "$PWD":/app \
  -w /app \
  python:3.12.11 bash -c "
    pip install poetry==2.1.4 &&
    poetry lock
  "
