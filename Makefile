.PHONY: check-uv core tests notebooks dev clean docs mcp setup-sys-deps

UV := $(shell command -v uv)
SAFE_RUN := env -u VIRTUAL_ENV

# OS Detection
UNAME_S := $(shell uname -s)

# Path definitions for local (no-sudo) installs
LOCAL_LIB := $(HOME)/.local/lib
export LD_LIBRARY_PATH := $(LOCAL_LIB)$(if $(LD_LIBRARY_PATH),:$(LD_LIBRARY_PATH))
export DYLD_LIBRARY_PATH := $(LOCAL_LIB)$(if $(DYLD_LIBRARY_PATH),:$(DYLD_LIBRARY_PATH))

setup-sys-deps:
ifeq ($(UNAME_S),Darwin)
	@echo "Checking for udunits via Homebrew..."
	@command -v brew > /dev/null 2>&1 || ( \
		echo "----------------------------------------------------------"; \
		echo "ERROR: Homebrew is required to check for udunits on macOS."; \
		echo "Install Homebrew from https://brew.sh/, then run:"; \
		echo "  brew install udunits"; \
		echo "----------------------------------------------------------"; \
		exit 1)
	@brew --prefix udunits > /dev/null 2>&1 || ( \
		echo "----------------------------------------------------------"; \
		echo "ERROR: udunits not found via Homebrew."; \
		echo "Install it with:"; \
		echo "  brew install udunits"; \
		echo "----------------------------------------------------------"; \
		exit 1)
endif
ifeq ($(UNAME_S),Linux)
	@echo "Checking for udunits2 development files..."
	@if (command -v pkg-config > /dev/null 2>&1 && pkg-config --exists udunits2) || \
		(command -v dpkg > /dev/null 2>&1 && dpkg -s libudunits2-dev > /dev/null 2>&1); then \
		echo "udunits2 development files found."; \
	else \
		echo "----------------------------------------------------------"; \
		echo "ERROR: libudunits2-dev / udunits2 pkg-config metadata not found."; \
		echo "If you have sudo, run: sudo apt-get install libudunits2-dev"; \
		echo "If not, install to ~/.local/ or use a Conda environment."; \
		echo "----------------------------------------------------------"; \
		exit 1; \
	fi
endif


check-uv:
	@if [ -z "$(UV)" ]; then \
		echo "----------------------------------------------------------"; \
		echo "ERROR: uv is not installed."; \
		echo "Install it with one of:"; \
		echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		echo "  pipx install uv"; \
		echo "See https://docs.astral.sh/uv/getting-started/installation/"; \
		echo "Then re-run make."; \
		echo "----------------------------------------------------------"; \
		exit 1; \
	fi

# Core sync
core: check-uv setup-sys-deps
	$(SAFE_RUN) $(UV) sync

# Sync including specific extras
tests: check-uv
	$(SAFE_RUN) $(UV) sync --extra tests --extra notebooks

mcp: check-uv
	$(SAFE_RUN) $(UV) sync --extra notebooks --extra mcp

notebooks: check-uv
	$(SAFE_RUN) $(UV) sync --extra notebooks

dev: check-uv setup-sys-deps
	$(SAFE_RUN) $(UV) sync --extra notebooks --extra tests --extra docs --extra dev --extra mcp
	$(SAFE_RUN) $(UV) run pre-commit install

docs: check-uv
	$(SAFE_RUN) $(UV) sync --extra docs

clean:
	rm -rf .venv
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf dist
	rm -rf *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} +
