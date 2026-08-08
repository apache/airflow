# Disable Flake8 because of all the sphinx imports
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from docs.utils.conf_constants import (
    AIRFLOW_FAVICON_PATH,
    SPHINX_DESIGN_STATIC_PATH,
    get_html_context,
    get_html_sidebars,
    get_html_theme_options,
)

CONF_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(CONF_DIR.parent.parent.joinpath("devel-common", "src", "sphinx_exts").resolve()))

PACKAGE_NAME = "ts-sdk"
os.environ["AIRFLOW_PACKAGE_NAME"] = PACKAGE_NAME

# The TypeScript SDK is versioned in its package.json, not a Python module.
PACKAGE_VERSION = json.loads((CONF_DIR.parent / "package.json").read_text())["version"]

project = "Apache Airflow TypeScript SDK"
# The version info for the project you're documenting
version = PACKAGE_VERSION
# The full version, including alpha/beta/rc tags.
release = PACKAGE_VERSION

language = "en"
locale_dirs: list[str] = []

# -- sphinx-js (TypeScript via TypeDoc) --------------------------------------
# The API reference is extracted from the TypeScript sources with sphinx-js,
# which drives TypeDoc. The TypeDoc toolchain is pinned in ``package.json`` /
# ``package-lock.json`` in this directory and must be installed (``npm ci``)
# before building these docs; sphinx-js locates it in ``docs/node_modules``.
# ``build-docs ts-sdk`` runs this install automatically when it is missing.
extensions = [
    "sphinx_js",
    "sphinx.ext.intersphinx",
    "airflow_intersphinx",
    "sphinxcontrib.spelling",
]

js_language = "typescript"
# Single root entry point: ``src/index.ts`` re-exports the whole public API, so
# TypeDoc's reachability analysis documents exactly the public surface and none
# of the internal/generated modules.
js_source_path = ["../src/index.ts"]
root_for_relative_js_paths = "../src"
# TypeDoc + sphinx-js configuration files, resolved relative to this conf.py.
jsdoc_config_path = "typedoc.json"
jsdoc_tsconfig_path = "tsconfig.json"
# sphinx-js passes this path through to its Node analyzer verbatim, so it must
# be absolute. Flattens unsupported TSDoc ``{@link}`` inline tags into text.
ts_sphinx_js_config = str((CONF_DIR / "sphinxJsConfig.mjs").resolve())

html_theme = "sphinx_airflow_theme"
html_title = "Apache Airflow TypeScript SDK Documentation"
html_short_title = "TypeScript SDK"
html_favicon = AIRFLOW_FAVICON_PATH.as_posix()
html_static_path = [SPHINX_DESIGN_STATIC_PATH.as_posix()]
html_css_files = ["custom.css"]
html_sidebars = get_html_sidebars(PACKAGE_VERSION)
html_theme_options = get_html_theme_options()
conf_py_path = "/ts-sdk/docs/"
html_context = get_html_context(conf_py_path)
html_use_index = True
html_show_copyright = False

intersphinx_resolve_self = "ts-sdk"
# ``airflow:`` resolves against the published site; ``apache-airflow:`` (added by
# airflow_intersphinx) against the local inventory — use it for not-yet-published pages.
intersphinx_mapping = {
    "airflow": ("https://airflow.apache.org/docs/apache-airflow/stable/", None),
}
# Suppress known warnings
suppress_warnings: list[str] = []

spelling_show_suggestions = False
spelling_word_list_filename = [
    str(CONF_DIR.parent.parent.joinpath("docs", "spelling_wordlist.txt").resolve())
]
spelling_ignore_importable_modules = True
spelling_ignore_contributor_names = True
