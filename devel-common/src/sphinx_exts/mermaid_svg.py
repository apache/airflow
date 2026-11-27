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
script involved. The extension replaces sphinxcontrib-mermaid's HTML visitor so that each diagram
also links to its SVG file, which a reader opens to see a large diagram at full size.

``breeze build-docs`` installs ``mmdc`` and the Chromium it drives into its container when a package
it builds has a diagram (``scripts/in_container/install_mermaid_cli.sh``). When ``mmdc`` is missing,
sphinxcontrib-mermaid logs a warning; when it fails, this extension logs one naming the diagram.
Either warning fails the docs build.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from docutils import nodes
from sphinx.util import logging
from sphinxcontrib.mermaid import MermaidError, mermaid, render_mm

if TYPE_CHECKING:
    from sphinx.application import Sphinx
    from sphinx.config import Config
    from sphinx.writers.html5 import HTML5Translator

logger = logging.getLogger(__name__)

# mermaid sizes each label's box from the font it measures in the build container, and the reader's
# browser draws the text in whatever font it has. With mermaid's default "trebuchet ms" first in the
# list, the two differ and labels are clipped. Arial is on macOS and Windows, and the
# fonts-liberation package installed with mermaid-cli gives the container Liberation Sans, which has
# the same metrics as Arial, so the boxes fit the text the reader sees.
# The config also turns off useMaxWidth, so each SVG gets its size in pixels instead of width="100%"
# (see DIAGRAM_HTML). mermaid has no global switch for it: add a diagram type when the docs first use it.
MERMAID_CONFIG_PATH = Path(__file__).parent / "mermaid_config.json"

# The docs build runs as root inside breeze, and Chromium refuses to start as root with its sandbox
# on. The container is the isolation boundary, and the diagrams come from this repository.
PUPPETEER_CONFIG_PATH = Path(__file__).parent / "mermaid_puppeteer_config.json"

# Scaled to the text width, a large diagram such as the database ERD is too small to read, and the
# <object> offers no way to open the SVG on its own. So the diagram links to its SVG file, which the
# browser shows at full size, to scroll and zoom. The <object> ignores the pointer so that a click
# reaches the link. The styles are inline so that no docs package using this extension needs a
# stylesheet for it.
DIAGRAM_HTML = (
    '<a class="mermaid-diagram" href="{src}" title="Open the diagram at full size"'
    ' style="display: block; cursor: zoom-in">'
    '<object data="{src}" type="image/svg+xml"'
    ' style="display: block; max-width: 100%; height: auto; pointer-events: none">'
    '<p class="warning">{alt}</p></object></a>\n'
)


def _visit_mermaid_html(translator: HTML5Translator, node: mermaid) -> None:
    code = node["code"]
    try:
        src, _ = render_mm(translator, code, node["options"], "svg")
    except MermaidError as exc:
        logger.warning("mermaid code %r: %s", code, exc)
        raise nodes.SkipNode
    if src is None:
        # mmdc could not be started, and render_mm has logged a warning saying so.
        translator.body.append(translator.encode(code))
    else:
        alt = node.get("alt", translator.encode(code).strip())
        translator.body.append(DIAGRAM_HTML.format(src=src, alt=alt))
    raise nodes.SkipNode


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
    app.add_node(mermaid, override=True, html=(_visit_mermaid_html, None))
    app.connect("config-inited", _render_at_build_time)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
