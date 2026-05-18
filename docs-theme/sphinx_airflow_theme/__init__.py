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

from os import path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sphinx.application import Sphinx

__version__ = "0.3.9"
__version_full__ = __version__

_THEME_DIR = path.abspath(path.dirname(__file__))
_GEN_DIR = path.join(_THEME_DIR, "static", "_gen")


def get_html_theme_path():
    """Return list of HTML theme paths."""
    return path.abspath(path.dirname(_THEME_DIR))


def setup_my_func(app, config):
    config.html_theme_options.setdefault(
        "navbar_links",
        [
            {"href": "/index.html", "text": "Documentation"},
            {"href": "/registry/", "text": "Registry"},
        ],
    )


def setup(app: Sphinx):
    if not path.isdir(_GEN_DIR):
        raise FileNotFoundError(
            f"Theme static assets not found at {_GEN_DIR}. Run: python scripts/ci/fetch_theme_assets.py"
        )
    app.add_html_theme("sphinx_airflow_theme", _THEME_DIR)
    app.add_css_file("_gen/css/main-custom.min.css")
    app.add_js_file("js/globaltoc.js")
    app.connect("config-inited", setup_my_func)
    return {"version": __version__, "parallel_read_safe": True, "parallel_write_safe": True}
