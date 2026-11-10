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
get_uv_cmd() {
  local MODE=$1
  if [ "$MODE" == "core" ]; then
    echo "uv sync"
  elif [ "$MODE" == "tests" ]; then
    # DataQuery tests require both extras
    echo "uv sync --extra tests --extra notebooks"
  elif [ "$MODE" == "notebooks" ]; then
    echo "uv sync --extra notebooks"
  elif [ "$MODE" == "docs" ]; then
    echo "uv sync --extra docs"
  elif [ "$MODE" == "dev" ]; then
    # Full contributor install
    echo "uv sync --extra notebooks --extra tests --extra docs --extra dev"
  fi
}

# 3. Environment Builder
build_env() {
  local MODE=$1
  local ENV_NAME="${BASE_NAME}_${MODE}"
  local UV_CMD=$(get_uv_cmd "$MODE")

  echo "------------------------------------------------"
  echo "SETTING UP: $ENV_NAME"
  echo "COMMAND: $UV_CMD"
  echo "------------------------------------------------"

  # Create/Update Mamba or Conda env
  if ! { $BIN env list | grep -q "$ENV_NAME"; }; then
    $BIN env create -n "$ENV_NAME" -f environment.yml --yes
  else
    $BIN env update -n "$ENV_NAME" -f environment.yml --yes
  fi

  # Install into the conda env itself rather than a project .venv
  $BIN run -n "$ENV_NAME" sh -c "UV_PROJECT_ENVIRONMENT=\"\$CONDA_PREFIX\" $UV_CMD"
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
