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
            pytest.param(
                "from airflow import settings\n",
                [(1, "from airflow import settings")],
                id="from-airflow-import-name",
            ),
            pytest.param(
                "from airflow import sdk\n",
                [],
                id="from-airflow-import-sdk-allowed",
            ),
            pytest.param(
                "from airflow import settings, models\n",
                [(1, "from airflow import settings, models")],
                id="from-airflow-import-multiple-names",
            ),
            pytest.param(
                "from airflow import sdk, settings\n",
                [(1, "from airflow import settings")],
                id="from-airflow-import-mixed-names",
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
                "from airflow.models import DagRun  # noqa: SDK002\n",
                [],
                id="from-import-suppressed",
            ),
            pytest.param(
                "import airflow.models  # noqa: SDK002\n",
                [],
                id="import-suppressed",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: SDK002 - needed for compat\n",
                [],
                id="marker-with-extra-text",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.models import (
                        DagRun,
                        TaskInstance,
                    )  # noqa: SDK002
                """),
                [],
                id="multiline-marker-on-closing-paren",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.models import (  # noqa: SDK002
                        DagRun,
                        TaskInstance,
                    )
                """),
                [],
                id="multiline-marker-on-first-line",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.models import (
                        DagRun,  # noqa: SDK002
                        TaskInstance,
                    )
                """),
                [],
                id="multiline-marker-on-middle-line",
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
                    from airflow.models import DagRun  # noqa: SDK002
                    from airflow.jobs import BaseJob
                """),
                [(2, "from airflow.jobs import BaseJob")],
                id="only-marked-line-suppressed",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: F401, SDK002\n",
                [],
                id="combined-codes-target-last",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: SDK002, F401\n",
                [],
                id="combined-codes-target-first",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: E402, SDK002, F401\n",
                [],
                id="combined-codes-target-middle",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa:SDK002\n",
                [],
                id="no-space-after-colon",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: F401\n",
                [(1, "from airflow.models import DagRun")],
                id="other-code-only-not-suppressed",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: F401 - see SDK002 docs\n",
                [(1, "from airflow.models import DagRun")],
                id="code-in-explanation-not-suppressed",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: F401, SDK002 - needed for compat\n",
                [],
                id="combined-codes-with-explanation-suppressed",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa: SDK002x\n",
                [(1, "from airflow.models import DagRun")],
                id="partial-code-match-not-suppressed",
            ),
            pytest.param(
                "from airflow import settings  # noqa: SDK002\n",
                [],
                id="from-airflow-import-name-suppressed",
            ),
            pytest.param(
                "from airflow.models import DagRun  # noqa\n",
                [(1, "from airflow.models import DagRun")],
                id="bare-noqa-not-suppressed",
            ),
        ],
    )
    def test_nocheck_marker(self, tmp_path: Path, code: str, expected: list[tuple[int, str]]):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert check_file_for_core_imports(f) == expected
