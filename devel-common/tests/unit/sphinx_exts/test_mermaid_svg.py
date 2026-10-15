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

import io
import json
import shlex
import sys
from pathlib import Path

from sphinx.application import Sphinx

SPHINX_EXTS_PATH = Path(__file__).parents[3] / "src" / "sphinx_exts"
if SPHINX_EXTS_PATH.as_posix() not in sys.path:
    # The extensions are loaded by Sphinx from this directory and import each other by bare name.
    sys.path.append(SPHINX_EXTS_PATH.as_posix())

INDEX_RST = """\
Index
=====

.. mermaid::

    flowchart LR
        a --> b
"""

# Stands in for mermaid-cli: records the arguments it was given and writes an SVG to the -o path.
FAKE_MMDC = """\
import json
import sys
from pathlib import Path

args = sys.argv[1:]
Path(sys.argv[0]).with_name("mmdc-args.json").write_text(json.dumps(args))
Path(args[args.index("-o") + 1]).write_text('<svg xmlns="http://www.w3.org/2000/svg"></svg>')
"""


def test_mermaid_diagram_is_drawn_at_build_time_without_cdn_script(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "index.rst").write_text(INDEX_RST)
    (src / "conf.py").write_text("extensions = ['mermaid_svg']\n")
    fake_mmdc = tmp_path / "fake_mmdc.py"
    fake_mmdc.write_text(FAKE_MMDC)
    out = tmp_path / "out"
    warnings = io.StringIO()

    app = Sphinx(
        srcdir=src,
        confdir=src,
        outdir=out,
        doctreedir=tmp_path / "doctrees",
        buildername="html",
        confoverrides={"mermaid_cmd": shlex.join([sys.executable, fake_mmdc.as_posix()])},
        status=None,
        warning=warnings,
    )
    app.build()

    html = (out / "index.html").read_text()
    mmdc_args = json.loads((tmp_path / "mmdc-args.json").read_text())
    assert warnings.getvalue() == ""
    assert '<object data="_images/mermaid-' in html
    assert "cdn.jsdelivr.net" not in html
    assert 'class="mermaid"' not in html
    assert mmdc_args[mmdc_args.index("--configFile") + 1].endswith("mermaid_config.json")
    assert mmdc_args[mmdc_args.index("--puppeteerConfigFile") + 1].endswith("mermaid_puppeteer_config.json")
