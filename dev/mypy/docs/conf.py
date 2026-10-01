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

import os
from pathlib import Path

from docs.utils.conf_constants import (
    AIRFLOW_FAVICON_PATH,
    SPELLING_WORDLIST_PATH,
    SPHINX_DESIGN_STATIC_PATH,
    get_html_context,
    get_html_sidebars,
    get_html_theme_options,
)

import airflow_mypy

CONF_DIR = Path(__file__).parent.absolute()
PACKAGE_NAME = "apache-airflow-mypy"
PACKAGE_VERSION = airflow_mypy.__version__

os.environ["AIRFLOW_PACKAGE_NAME"] = PACKAGE_NAME

project = "Apache Airflow Mypy"
version = PACKAGE_VERSION
release = PACKAGE_VERSION

language = "en"
locale_dirs: list[str] = []

extensions = [
    "sphinx.ext.intersphinx",
    "airflow_intersphinx",
    "sphinxcontrib.spelling",
]

html_theme = "sphinx_airflow_theme"
html_title = "Apache Airflow Mypy Documentation"
html_short_title = "Airflow Mypy"
html_favicon = AIRFLOW_FAVICON_PATH.as_posix()
html_static_path = [SPHINX_DESIGN_STATIC_PATH.as_posix()]
html_css_files = ["custom.css"]
html_sidebars = get_html_sidebars(PACKAGE_VERSION)
html_theme_options = get_html_theme_options()
conf_py_path = "/dev/mypy/docs/"
html_context = get_html_context(conf_py_path)
html_use_index = True
html_show_copyright = False

intersphinx_mapping = {
    "airflow": ("https://airflow.apache.org/docs/apache-airflow/stable/", None),
}

spelling_show_suggestions = False
spelling_word_list_filename = [
    SPELLING_WORDLIST_PATH.as_posix(),
    (CONF_DIR / "spelling_wordlist.txt").as_posix(),
]
spelling_ignore_importable_modules = True
spelling_ignore_contributor_names = True
