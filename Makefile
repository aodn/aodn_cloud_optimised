.PHONY: core tests notebooks dev clean setup-config docs

# 1. FIND GLOBAL POETRY (usually ~/.local/bin/poetry)
# This ignores any poetry found inside your current .venv to ensure we are using the global poetry installation for managing the project dependencies and plugins.
GLOBAL_POETRY := $(shell which -a poetry | grep -v ".venv" | head -n 1)

# 2. THE SAFE RUNNER
# This strips the venv from the PATH and unsets the VIRTUAL_ENV variable
SAFE_RUN := env -u VIRTUAL_ENV PATH="$(shell echo $$PATH | sed -e 's|:[^:]*/.venv/bin||g' -e 's|[^:]*/.venv/bin:||g')"

# Core sync
core: setup-config
	$(SAFE_RUN) $(GLOBAL_POETRY) sync

# Sync including specific extras
tests: setup-config
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras "tests notebooks"

notebooks: setup-config
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras notebooks

dev: setup-config
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras "notebooks tests docs dev"
	$(SAFE_RUN) $(GLOBAL_POETRY) run pre-commit install
	# Plugin management should always be global
	$(GLOBAL_POETRY) self add poetry-plugin-export

docs: setup-config
	$(SAFE_RUN) $(GLOBAL_POETRY) sync --extras docs

clean:
	rm -rf .venv
	$(SAFE_RUN) $(GLOBAL_POETRY) install
