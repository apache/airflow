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

import sys
from pathlib import Path

CONF_DIR = Path(__file__).parent.absolute()
sys.path.insert(0, str(CONF_DIR.parent.parent.joinpath("devel-common", "src", "sphinx_exts").resolve()))

project = "Apache Airflow Task SDK"

language = "en"
locale_dirs: list[str] = []

extensions = [
    "sphinx.ext.autodoc",
    "autoapi.extension",
    "sphinx.ext.intersphinx",
    "exampleinclude",
    "sphinxcontrib.spelling",
]

autoapi_dirs = [CONF_DIR.joinpath("..", "src").resolve()]
autoapi_root = "api"
autoapi_ignore = [
    "*/airflow/sdk/execution_time",
    "*/airflow/sdk/api",
    "*/_internal*",
]
autoapi_options = [
    "undoc-members",
    "members",
    "imported-members",
]
autoapi_add_toctree_entry = False
autoapi_generate_api_docs = False

autodoc_typehints = "description"

# Prefer pyi over py files if both are found
autoapi_file_patterns = ["*.pyi", "*.py"]

html_theme = "sphinx_airflow_theme"


global_substitutions = {
    "experimental": "This is an :ref:`experimental feature <experimental>`.",
}

rst_epilog = "\n".join(f".. |{key}| replace:: {replace}" for key, replace in global_substitutions.items())


intersphinx_resolve_self = "airflow"
intersphinx_mapping = {
    "airflow": ("https://airflow.apache.org/docs/apache-airflow/stable/", None),
}
# Suppress known warnings
suppress_warnings = [
    "autoapi.python_import_resolution",
    "autodoc",
]

exampleinclude_sourceroot = str(CONF_DIR.joinpath("..").resolve())
spelling_show_suggestions = False
spelling_word_list_filename = [
    str(CONF_DIR.parent.parent.joinpath("docs", "spelling_wordlist.txt").resolve())
]
spelling_ignore_importable_modules = True
spelling_ignore_contributor_names = True
