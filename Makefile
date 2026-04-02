.PHONY: core tests notebooks dev clean setup-config docs

# Core sync (removes all extras)
core: setup-config
	poetry sync

# Sync including specific extras
tests: setup-config
	poetry sync --extras "tests notebooks"

notebooks: setup-config
	poetry sync --extras notebooks

dev: setup-config
	poetry sync --extras "notebooks tests docs dev"
	poetry run pre-commit install

docs: setup-config
	poetry sync --extras docs

clean:
	rm -rf .venv
	poetry sync
