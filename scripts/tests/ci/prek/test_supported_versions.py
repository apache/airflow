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

REPO_ROOT = Path(__file__).resolve().parents[4]
PREK_DIR = REPO_ROOT / "scripts/ci/prek"

if str(PREK_DIR) not in sys.path:
    sys.path.insert(0, str(PREK_DIR))

from supported_versions import (  # noqa: E402
    DOCS_PY_PATH,
    PYTHON_CONSTANT_END,
    PYTHON_CONSTANT_START,
    SUPPORTED_VERSIONS,
    TS_CONSTANT_PATH,
    get_latest_published_airflow_version,
    render_python_latest_published_airflow_version,
    render_typescript_latest_published_airflow_version,
)


def test_latest_published_version_is_current_patch_of_newest_line() -> None:
    assert get_latest_published_airflow_version() == SUPPORTED_VERSIONS[0][1]


def test_generated_constant_files_match_renderer() -> None:
    version = get_latest_published_airflow_version()
    docs_py = DOCS_PY_PATH.read_text()

    assert (
        PYTHON_CONSTANT_START + render_python_latest_published_airflow_version(version) + PYTHON_CONSTANT_END
        in docs_py
    )
    assert TS_CONSTANT_PATH.read_text() == render_typescript_latest_published_airflow_version(version)
