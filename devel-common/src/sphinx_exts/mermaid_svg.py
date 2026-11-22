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
Draw ``.. mermaid::`` diagrams into SVG files at build time instead of in the reader's browser.

sphinxcontrib-mermaid's default ``raw`` output puts the diagram source on the page together with a
script that imports mermaid from cdn.jsdelivr.net. airflow.apache.org is served with the ASF
Content-Security-Policy (https://infra.apache.org/csp.html), whose ``script-src`` does not allow
that host, so the import is blocked and readers see the diagram source as text. Rendered at build
time by mermaid-cli (``mmdc``), each diagram is an SVG file served from the site itself, with no
script involved.

``breeze build-docs`` installs ``mmdc`` and the Chromium it drives into its container when a package
it builds has a diagram (``scripts/in_container/install_mermaid_cli.sh``). When ``mmdc`` is missing or
fails, sphinxcontrib-mermaid logs a warning naming the diagram, and the warning fails the docs build.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sphinx.application import Sphinx
    from sphinx.config import Config

# mermaid sizes each label's box from the font it measures in the build container, and the reader's
# browser draws the text in whatever font it has. With mermaid's default "trebuchet ms" first in the
# list, the two differ and labels are clipped. Arial is on macOS and Windows, and the
# fonts-liberation package installed with mermaid-cli gives the container Liberation Sans, which has
# the same metrics as Arial, so the boxes fit the text the reader sees.
MERMAID_CONFIG_PATH = Path(__file__).parent / "mermaid_config.json"

# The docs build runs as root inside breeze, and Chromium refuses to start as root with its sandbox
# on. The container is the isolation boundary, and the diagrams come from this repository.
PUPPETEER_CONFIG_PATH = Path(__file__).parent / "mermaid_puppeteer_config.json"


def _render_at_build_time(app: Sphinx, config: Config) -> None:
    config.mermaid_output_format = "svg"
    config.mermaid_params = [
        "--configFile",
        MERMAID_CONFIG_PATH.as_posix(),
        "--puppeteerConfigFile",
        PUPPETEER_CONFIG_PATH.as_posix(),
    ]


def setup(app: Sphinx) -> dict[str, bool]:
    app.setup_extension("sphinxcontrib.mermaid")
    app.connect("config-inited", _render_at_build_time)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
