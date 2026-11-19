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
import sys
from pathlib import Path

from sphinx.application import Sphinx

SPHINX_EXTS_PATH = Path(__file__).parents[3] / "src" / "sphinx_exts"
if SPHINX_EXTS_PATH.as_posix() not in sys.path:
    # The extensions are loaded by Sphinx from this directory and import each other by bare name.
    sys.path.append(SPHINX_EXTS_PATH.as_posix())

# Two documented attributes named like builtins, plus annotations that use the builtins
# and a project class. Sphinx 9 resolves the builtins to these attributes ambiguously.
INDEX_RST = """\
Index
=====

.. py:module:: pkg

.. py:class:: First

   .. py:attribute:: type
      :type: str

   .. py:attribute:: object
      :type: str

.. py:class:: Second

   .. py:attribute:: type
      :type: str

   .. py:attribute:: object
      :type: str

.. py:function:: make(kind: type, value: object, first: First) -> None
"""


def _build(tmp_path: Path) -> tuple[str, str]:
    """Build the project and return the warnings and the HTML of the ``make`` signature."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "conf.py").write_text('extensions = ["python_builtin_xrefs"]\n')
    (src / "index.rst").write_text(INDEX_RST)
    warnings = io.StringIO()
    app = Sphinx(
        srcdir=src,
        confdir=src,
        outdir=tmp_path / "out",
        doctreedir=tmp_path / "doctrees",
        buildername="html",
        status=None,
        warning=warnings,
        freshenv=True,
    )
    app.build()
    html = (tmp_path / "out" / "index.html").read_text()
    signature = html[html.index('id="pkg.make"') :]
    return warnings.getvalue(), signature[: signature.index("</dt>")]


def test_builtin_annotations_do_not_resolve_to_same_named_attributes(tmp_path):
    warnings, signature = _build(tmp_path)

    assert "more than one target found" not in warnings
    assert "#pkg.First.type" not in signature
    assert "#pkg.First.object" not in signature


def test_project_class_annotations_still_resolve(tmp_path):
    _, signature = _build(tmp_path)

    assert 'href="#pkg.First"' in signature
