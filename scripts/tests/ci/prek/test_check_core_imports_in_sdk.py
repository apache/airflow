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

import textwrap
from pathlib import Path

import pytest
from check_core_imports_in_sdk import check_file_for_core_imports


class TestCheckFileForCoreImports:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                "from airflow.models import DagRun\n",
                [(1, "from airflow.models import DagRun")],
                id="from-import-core",
            ),
            pytest.param(
                "import airflow.models\n",
                [(1, "import airflow.models")],
                id="import-core",
            ),
            pytest.param(
                "import airflow.models as models\n",
                [(1, "import airflow.models as models")],
                id="import-core-aliased",
            ),
            pytest.param(
                "from airflow.sdk import DAG\n",
                [],
                id="sdk-import-allowed",
            ),
            pytest.param(
                "from airflow.sdk.definitions import dag\n",
                [],
                id="sdk-submodule-allowed",
            ),
            pytest.param(
                "import airflow.sdk\n",
                [],
                id="import-sdk-allowed",
            ),
            pytest.param(
                "import os\nimport sys\n",
                [],
                id="stdlib-only",
            ),
        ],
    )
    def test_detects_core_imports(self, tmp_path: Path, code: str, expected: list[tuple[int, str]]):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert check_file_for_core_imports(f) == expected


class TestNocheckMarker:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                "from airflow.models import DagRun  # nocheck: core-imports\n",
                [],
                id="from-import-suppressed",
            ),
            pytest.param(
                "import airflow.models  # nocheck: core-imports\n",
                [],
                id="import-suppressed",
            ),
            pytest.param(
                "from airflow.models import DagRun  # nocheck: core-imports - needed for compat\n",
                [],
                id="marker-with-extra-text",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.models import (
                        DagRun,
                        TaskInstance,
                    )  # nocheck: core-imports
                """),
                [],
                id="multiline-marker-on-closing-paren",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.models import (  # nocheck: core-imports
                        DagRun,
                        TaskInstance,
                    )
                """),
                [],
                id="multiline-marker-on-first-line",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: E402\n",
                [(1, "from airflow.models import DagRun")],
                id="wrong-marker-not-suppressed",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.models import (
                        DagRun,
                        TaskInstance,
                    )
                """),
                [(1, "from airflow.models import DagRun, TaskInstance")],
                id="multiline-without-marker-detected",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.models import DagRun  # nocheck: core-imports
                    from airflow.jobs import BaseJob
                """),
                [(2, "from airflow.jobs import BaseJob")],
                id="only-marked-line-suppressed",
            ),
        ],
    )
    def test_nocheck_marker(self, tmp_path: Path, code: str, expected: list[tuple[int, str]]):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert check_file_for_core_imports(f) == expected
