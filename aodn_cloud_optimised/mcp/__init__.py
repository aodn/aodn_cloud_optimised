"""
MCP (Model Context Protocol) server for the AODN Cloud Optimised dataset catalog.

This sub-package exposes the AODN dataset catalog, schema definitions, and
Jupyter notebook templates to AI assistants via the MCP protocol. It enables
AI tools to search datasets, understand their schemas, and generate or adapt
notebooks for specific user requests.

Usage (Claude Desktop or any MCP client):
    aodn-mcp-server

Environment Variables:
    AODN_NOTEBOOKS_PATH: Path to the directory containing AODN Jupyter notebooks.
                         Defaults to the ``notebooks/`` directory at the repo root.
"""
