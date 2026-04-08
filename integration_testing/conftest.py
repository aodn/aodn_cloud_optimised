"""
conftest.py for AODN MCP server integration tests.

Custom pytest CLI options
-------------------------
--run-s3
    Enable tests that make live anonymous S3 requests (coverage queries,
    live variable introspection).  Requires internet access (~10–60 s each).

--run-notebooks
    Enable tests that use notebooks (requires internet / --run-s3):

    • TestMcpValidateNotebook (2 tests, fast): unit tests for the
      ``validate_notebook`` tool using tiny synthetic notebooks.

    • TestMcpGeneratedNotebook (2 tests, ~5–10 min): scripted AI agent —
      calls ``get_dataset_schema``, ``check_dataset_coverage``, and
      ``execute_python_cell`` in sequence, assembles a NEW .ipynb from those
      tool outputs, and validates it.  Tests that the MCP schema/coverage/REPL
      tools together produce a working notebook.
      Requires ``--run-s3`` (or ``--run-all``) in addition.

--run-all
    Short-hand for --run-s3 + --run-notebooks combined.  Also enables the
    end-to-end scenario tests (TestMcpEndToEnd).

Usage examples
--------------
# Offline tests only (catalog, plot-guide, Python REPL — no network):
pytest integration_testing/test_mcp_server.py -v

# Include live S3 queries:
pytest integration_testing/test_mcp_server.py -v --run-s3

# Scripted-agent notebook generation + existing template regression:
pytest integration_testing/test_mcp_server.py -v --run-s3 --run-notebooks

# Run everything (S3 + notebooks + end-to-end scenarios):
pytest integration_testing/test_mcp_server.py -v --run-all
"""

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-s3",
        action="store_true",
        default=False,
        help="Run tests that make live anonymous S3 requests.",
    )
    parser.addoption(
        "--run-notebooks",
        action="store_true",
        default=False,
        help="Run tests that execute full Jupyter notebooks (slow).",
    )
    parser.addoption(
        "--run-all",
        action="store_true",
        default=False,
        help="Run all tests including live S3 and notebook execution.",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "s3: mark test as requiring live S3 access")
    config.addinivalue_line(
        "markers", "notebooks: mark test as executing full notebooks (slow)"
    )


def pytest_collection_modifyitems(config, items):
    run_s3 = config.getoption("--run-s3") or config.getoption("--run-all")
    run_notebooks = config.getoption("--run-notebooks") or config.getoption("--run-all")

    skip_s3 = pytest.mark.skip(
        reason="Pass --run-s3 or --run-all to enable live S3 tests"
    )
    skip_notebooks = pytest.mark.skip(
        reason="Pass --run-notebooks or --run-all to enable notebook execution tests"
    )

    for item in items:
        if "s3" in item.keywords and not run_s3:
            item.add_marker(skip_s3)
        if "notebooks" in item.keywords and not run_notebooks:
            item.add_marker(skip_notebooks)
