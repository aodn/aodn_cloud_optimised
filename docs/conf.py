# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import toml

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
    # Other extensions...
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.todo",
    "sphinx.ext.githubpages",
    "sphinx.ext.intersphinx",
    # Other extensions...
]


# html_theme = 'alabaster'
html_theme = "sphinx_rtd_theme"
html_theme_path = [
    "_themes",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_static_path = ["_static"]
