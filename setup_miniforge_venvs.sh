#!/bin/bash
# AODN Cloud Optimised - Custom Environment Setup Utility

# 1. Detect Package Manager (Mamba/Micromamba > Conda)
if command -v mamba &>/dev/null; then
  BIN="mamba"
elif command -v micromamba &>/dev/null; then
  BIN="micromamba"
elif command -v conda &>/dev/null; then
  BIN="conda"
else
  echo "ERROR: No compatible package manager (mamba, micromamba, conda) found."
  exit 1
fi

BASE_NAME="AodnCloudOptimised"
ALL_MODES=("core" "tests" "notebooks" "docs" "dev")

# 2. Logic for Dependency Mapping
get_poetry_cmd() {
  local MODE=$1
  if [ "$MODE" == "core" ]; then
    echo "poetry install"
  elif [ "$MODE" == "tests" ]; then
    # DataQuery tests require both extras
    echo "poetry install --extras \"tests notebooks\""
  elif [ "$MODE" == "notebooks" ]; then
    echo "poetry install --extras notebooks"
  elif [ "$MODE" == "docs" ]; then
    echo "poetry install --extras docs"
  elif [ "$MODE" == "dev" ]; then
    # Full contributor install
    echo "poetry install --extras \"notebooks tests docs dev\""
  fi
}

# 3. Environment Builder
build_env() {
  local MODE=$1
  local ENV_NAME="${BASE_NAME}_${MODE}"
  local POETRY_CMD=$(get_poetry_cmd "$MODE")

  echo "------------------------------------------------"
  echo "SETTING UP: $ENV_NAME"
  echo "COMMAND: $POETRY_CMD"
  echo "------------------------------------------------"

  # Create/Update Mamba or Conda env
  if ! { $BIN env list | grep -q "$ENV_NAME"; }; then
    $BIN env create -n "$ENV_NAME" -f environment.yml --yes
  else
    $BIN env update -n "$ENV_NAME" -f environment.yml --yes
  fi

  # Configure Poetry to stay inside the prefix
  $BIN run -n "$ENV_NAME" poetry config virtualenvs.create false --local
  eval "$BIN run -n \"$ENV_NAME\" $POETRY_CMD"
}

# 4. Execution
case $1 in
all)
  for m in "${ALL_MODES[@]}"; do build_env "$m"; done
  ;;
core | tests | notebooks | docs | dev)
  build_env "$1"
  ;;
*)
  echo "Usage: $0 {core|tests|notebooks|docs|dev|all}"
  exit 1
  ;;
esac
