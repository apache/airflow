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

"""
Sphinx extension to suppress the "Suggest a change on this page" button
on autoapi pages (i.e. anything under ``_api/``).

The ``sphinx_airflow_theme`` template constructs the edit URL from the
page's pagename. For autoapi pages this always 404s, because the ``.rst``
files are generated during the build and never exist in the repo. To work
around this we register a local templates directory that shadows the
upstream template: the local version renders nothing for ``_api/*`` and
falls through to the upstream version for every other page.

See: https://github.com/apache/airflow/issues/16654
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sphinx.application import Sphinx
    from sphinx.config import Config

_TEMPLATES_DIR = Path(__file__).resolve().parents[1] / "docs" / "sphinx_templates"


def _register_templates_path(app: Sphinx, config: Config) -> None:
    templates_path = list(config.templates_path or [])
    shared = _TEMPLATES_DIR.as_posix()
    if shared not in templates_path:
        templates_path.append(shared)
    config.templates_path = templates_path


def setup(app: Sphinx) -> dict[str, bool | str]:
    app.connect("config-inited", _register_templates_path)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
