.PHONY: core tests notebooks dev clean docs

GLOBAL_POETRY := $(shell which -a poetry | grep -v ".venv" | head -n 1)
SAFE_RUN := env -u VIRTUAL_ENV

# Core sync
core:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync

# Sync including specific extras
tests:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras "tests notebooks"

notebooks:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras notebooks

dev:
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras "notebooks tests docs dev"
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
