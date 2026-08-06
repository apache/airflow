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

import check_sdk_imports_in_core as hook
import pytest
from check_sdk_imports_in_core import SdkImportsAllowlistManager, check_file_for_sdk_imports


@pytest.fixture
def create_fake_core_repo(tmp_path, monkeypatch):
    monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(hook, "CORE_SRC_ROOT", tmp_path / "airflow-core" / "src" / "airflow")

    def _write(rel: str, code: str) -> Path:
        path = tmp_path / "airflow-core" / "src" / "airflow" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(code))
        return path

    return _write


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
            pytest.param(
                "from airflow.sdk import DAG  # noqa: F401, SDK001\n",
                [],
                id="combined-codes-target-last",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa: SDK001, F401\n",
                [],
                id="combined-codes-target-first",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa: E402, SDK001, F401\n",
                [],
                id="combined-codes-target-middle",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa:SDK001\n",
                [],
                id="no-space-after-colon",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa: F401\n",
                [(1, "from airflow.sdk import DAG")],
                id="other-code-only-not-suppressed",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa: F401 - see SDK001 docs\n",
                [(1, "from airflow.sdk import DAG")],
                id="code-in-explanation-not-suppressed",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa: F401, SDK001 - needed for compat\n",
                [],
                id="combined-codes-with-explanation-suppressed",
            ),
            pytest.param(
                "from airflow.sdk import DAG  # noqa\n",
                [(1, "from airflow.sdk import DAG")],
                id="bare-noqa-not-suppressed",
            ),
        ],
    )
    def test_nocheck_marker(self, tmp_path: Path, code: str, expected: list[tuple[int, str]]):
        f = tmp_path / "example.py"
        f.write_text(code)
        assert check_file_for_sdk_imports(f) == expected


class TestSdkImportsAllowlistRatchet:
    def test_no_violations_passes(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/clean.py",
            """\
            from airflow.models import DagRun
            """,
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.check([path], {}) == 0

    def test_new_violation_fails(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/bad.py",
            """\
            from airflow.sdk import DAG
            """,
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.check([path], {}) == 1

    def test_violation_within_allowlist_passes(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/grandfathered.py",
            """\
            from airflow.sdk import DAG
            """,
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/models/grandfathered.py": 1}
        assert manager.check([path], allowlist) == 0

    def test_exceeding_allowlist_fails(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/grew.py",
            """\
            from airflow.sdk import DAG
            from airflow.sdk.definitions.deadline import VariableInterval
            """,
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/models/grew.py": 1}
        assert manager.check([path], allowlist) == 1

    def test_reducing_violations_tightens_allowlist(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/improved.py",
            """\
            from airflow.sdk import DAG
            """,
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/models/improved.py": 2}
        assert manager.check([path], allowlist) == 1
        assert manager.load() == {"airflow-core/src/airflow/models/improved.py": 1}

    def test_fixing_all_violations_removes_entry(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/fixed.py",
            """\
            from airflow.models import DagRun
            """,
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/models/fixed.py": 1}
        assert manager.check([path], allowlist) == 1
        assert manager.load() == {}

    def test_noqa_marker_avoids_ratchet_entirely(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/one_off.py",
            """\
            from airflow.sdk import DAG  # noqa: SDK001
            """,
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.check([path], {}) == 0

    def test_non_python_file_is_skipped(self, create_fake_core_repo, tmp_path):
        path = create_fake_core_repo(
            "models/not_python.txt",
            "from airflow.sdk import DAG\n",
        )
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.check([path], {}) == 0


class TestSdkImportsAllowlistCleanup:
    def test_cleanup_removes_stale_entries(self, create_fake_core_repo, tmp_path):
        create_fake_core_repo("models/keeper.py", "from airflow.models import DagRun\n")
        allowlist_path = tmp_path / "allowlist.txt"
        manager = SdkImportsAllowlistManager(allowlist_path)
        manager.save(
            {
                "airflow-core/src/airflow/models/keeper.py": 0,
                "airflow-core/src/airflow/models/gone.py": 1,
            }
        )
        assert manager.cleanup() == 0
        assert manager.load() == {"airflow-core/src/airflow/models/keeper.py": 0}

    def test_cleanup_empty_allowlist(self, tmp_path):
        manager = SdkImportsAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.cleanup() == 0


class TestSdkImportsAllowlistGenerate:
    def test_generate_records_current_occurrences(self, create_fake_core_repo, tmp_path):
        create_fake_core_repo("models/a.py", "from airflow.sdk import DAG\n")
        create_fake_core_repo("models/b.py", "from airflow.models import DagRun\n")
        allowlist_path = tmp_path / "allowlist.txt"
        manager = SdkImportsAllowlistManager(allowlist_path)
        assert manager.generate() == 0
        assert manager.load() == {"airflow-core/src/airflow/models/a.py": 1}
