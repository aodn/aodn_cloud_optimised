.PHONY: core tests notebooks dev clean docs mcp setup-sys-deps

GLOBAL_POETRY := $(shell which -a poetry | grep -v ".venv" | head -n 1)
SAFE_RUN := env -u VIRTUAL_ENV

# OS Detection
UNAME_S := $(shell uname -s)

# Path definitions for local (no-sudo) installs
LOCAL_LIB := $(HOME)/.local/lib
export LD_LIBRARY_PATH := $(LOCAL_LIB):$(LD_LIBRARY_PATH)
export DYLD_LIBRARY_PATH := $(LOCAL_LIB):$(DYLD_LIBRARY_PATH)

setup-sys-deps:
ifeq ($(UNAME_S),Darwin)
	@echo "Checking for udunits via Homebrew..."
	@brew install udunits || echo "Homebrew not found or install failed."
endif
ifeq ($(UNAME_S),Linux)
	@echo "Checking for udunits2 library..."
	@ldconfig -p | grep libudunits2 > /dev/null || ( \
		echo "----------------------------------------------------------"; \
		echo "ERROR: libudunits2-dev not found."; \
		echo "If you have sudo, run: sudo apt-get install libudunits2-dev"; \
		echo "If not, install to ~/.local/ or use a Conda environment."; \
		echo "----------------------------------------------------------"; \
		exit 1)
endif


# Core sync
core: setup-sys-deps
	$(SAFE_RUN) $(GLOBAL_POETRY) sync

# Sync including specific extras
tests:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras "tests notebooks"

mcp:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras "notebooks mcp"

notebooks:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras notebooks

dev: setup-sys-deps
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras "notebooks tests docs dev mcp"
	$(SAFE_RUN) $(GLOBAL_POETRY) run pre-commit install
	# Plugin management is global, so it doesn't need SAFE_RUN
	$(GLOBAL_POETRY) self add poetry-plugin-export

docs:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras docs

clean:
	rm -rf .venv
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf dist
	rm -rf *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} +
