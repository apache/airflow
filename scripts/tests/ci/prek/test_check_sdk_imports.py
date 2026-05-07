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
from check_sdk_imports import check_file_for_sdk_imports


class TestCheckFileForSdkImports:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                "from airflow.sdk import DAG\n",
                [(1, "from airflow.sdk import DAG")],
                id="from-sdk-import",
            ),
            pytest.param(
                "from airflow.sdk.definitions import dag\n",
                [(1, "from airflow.sdk.definitions import dag")],
                id="from-sdk-submodule-import",
            ),
            pytest.param(
                "from airflow.models import DagRun\n",
                [],
                id="core-import-allowed",
            ),
            pytest.param(
                "import airflow.sdk\n",
                [],
                id="plain-import-not-checked",
            ),
            pytest.param(
                "import os\nimport sys\n",
                [],
                id="stdlib-only",
            ),
        ],
    )
    def test_detects_sdk_imports(self, tmp_path: Path, code: str, expected: list[tuple[int, str]]):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert check_file_for_sdk_imports(f) == expected


class TestNocheckMarker:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param(
                "from airflow.sdk import DAG  # noqa: SDK001\n",
                [],
                id="from-import-suppressed",
            ),
            pytest.param(
                "from airflow.sdk.definitions import dag  # noqa: SDK001\n",
                [],
                id="from-submodule-suppressed",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa: SDK001 - needed for compat\n",
                [],
                id="marker-with-extra-text",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.sdk import (
                        DAG,
                        Variable,
                    )  # noqa: SDK001
                """),
                [],
                id="multiline-marker-on-closing-paren",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.sdk import (  # noqa: SDK001
                        DAG,
                        Variable,
                    )
                """),
                [],
                id="multiline-marker-on-first-line",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.sdk import (
                        DAG,  # noqa: SDK001
                        Variable,
                    )
                """),
                [],
                id="multiline-marker-on-middle-line",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa: E402\n",
                [(1, "from airflow.sdk import DAG")],
                id="wrong-marker-not-suppressed",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.sdk import (
                        DAG,
                        Variable,
                    )
                """),
                [(1, "from airflow.sdk import DAG, Variable")],
                id="multiline-without-marker-detected",
            ),
            pytest.param(
                textwrap.dedent("""\
                    from airflow.sdk import DAG  # noqa: SDK001
                    from airflow.sdk.definitions import dag
                """),
                [(2, "from airflow.sdk.definitions import dag")],
                id="only-marked-line-suppressed",
            ),
        ],
    )
    def test_nocheck_marker(self, tmp_path: Path, code: str, expected: list[tuple[int, str]]):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert check_file_for_sdk_imports(f) == expected
