# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import toml
import sphinx_rtd_theme

# Load version from pyproject.toml
with open("../pyproject.toml", "r") as toml_file:
    toml_data = toml.load(toml_file)
    version = toml_data["tool"]["poetry"]["version"]

project = "AODN Cloud Optimised"
copyright = "2024, Besnard, Laurent (AODN)"
author = "Besnard, Laurent (AODN)"
release = version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

exclude_patterns = ["aodn_cloud_optimised.bin.anmn_aqualogger_to_parquet"]
# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.coverage",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    "sphinx_rtd_theme",
    "sphinxcontrib.asciinema",
]

napoleon_google_docstring = True
napoleon_numpy_docstring = False


# Ensuring the code can be imported
import os
import sys

sys.path.insert(0, os.path.abspath("../aodn_cloud_optimised"))


master_doc = "index"


templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_theme = "sphinx_rtd_theme"
html_theme_path = [sphinx_rtd_theme.get_html_theme_path()]
html_static_path = ["_static"]
