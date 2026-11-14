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

import pytest

SPHINX_EXTS_PATH = Path(__file__).parents[3] / "src" / "sphinx_exts"
if SPHINX_EXTS_PATH.as_posix() not in sys.path:
    # The extensions are loaded by Sphinx from this directory and import each other by bare name.
    sys.path.append(SPHINX_EXTS_PATH.as_posix())

from sphinx_exts.redirects import build_redirect_page  # noqa: E402


@pytest.mark.parametrize(
    "to_path",
    [
        pytest.param("build-advanced.html#recipes", id="with-anchor"),
        pytest.param("../security/index.html", id="without-anchor"),
    ],
)
def test_redirect_page_keeps_the_requested_anchor(to_path):
    page = build_redirect_page(to_path)

    assert f'var target = "{to_path}";' in page
    assert 'target = target.split("#")[0] + window.location.hash;' in page
    assert "window.location.replace(target);" in page
    assert f'<meta http-equiv="refresh" content="0; url={to_path}"/>' in page
