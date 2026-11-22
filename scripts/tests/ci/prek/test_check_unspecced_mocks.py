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
import yaml
from ci.prek import check_unspecced_mocks as hook
from ci.prek.check_unspecced_mocks import UnspeccedMockAllowlistManager
from rich.text import Text


@pytest.fixture
def count_violations(write_python_file, tmp_path):
    """Factory fixture: write code to a temp file and return its count of mocks without a spec."""

    def _count(code: str) -> int:
        manager = UnspeccedMockAllowlistManager(tmp_path / "allowlist.txt")
        return manager.count_occurrences(write_python_file(code))

    return _count


@pytest.fixture
def create_fake_repo(tmp_path, monkeypatch):
    """Create a fake repo layout and patch REPO_ROOT so paths resolve correctly."""
    monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)

    def _write(rel: str, code: str) -> Path:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(code))
        return path

    return _write


class TestUnspeccedMockAllowlistManager:
    @pytest.mark.parametrize(
        "code",
        [
            pytest.param("Mock()", id="Mock"),
            pytest.param("MagicMock()", id="MagicMock"),
            pytest.param("AsyncMock()", id="AsyncMock"),
            pytest.param("NonCallableMock()", id="NonCallableMock"),
            pytest.param("NonCallableMagicMock()", id="NonCallableMagicMock"),
            pytest.param("mock.MagicMock()", id="mock.MagicMock"),
            pytest.param("unittest.mock.MagicMock()", id="unittest.mock.MagicMock"),
            pytest.param("mocker.MagicMock()", id="mocker.MagicMock"),
            pytest.param("MagicMock(return_value=1, name='hook')", id="MagicMock-non-spec-keywords"),
            pytest.param("patch('airflow.models.Variable.get')", id="patch"),
            pytest.param("mock.patch('airflow.models.Variable.get')", id="mock.patch"),
            pytest.param("unittest.mock.patch('airflow.models.Variable.get')", id="unittest.mock.patch"),
            pytest.param("mocker.patch('airflow.models.Variable.get')", id="mocker.patch"),
            pytest.param(
                "mock.patch('airflow.models.Variable.get', return_value=1)", id="patch-return-value"
            ),
            pytest.param(
                "@mock.patch('airflow.models.Variable.get')\ndef test_get(mock_get): pass",
                id="patch-decorator",
            ),
            pytest.param("patch.object(Variable, 'get')", id="patch.object"),
            pytest.param("mock.patch.object(Variable, 'get')", id="mock.patch.object"),
            pytest.param("mocker.patch.object(Variable, 'get')", id="mocker.patch.object"),
            pytest.param(
                "mock.patch.object(Variable, 'get', return_value=1)", id="patch.object-return-value"
            ),
            pytest.param(
                "mock.patch('airflow.models.Variable.get', new_callable=mock.AsyncMock)",
                id="patch-new_callable-mock-class",
            ),
            pytest.param(
                "mock.patch.object(Variable, 'get', new_callable=MagicMock)",
                id="patch.object-new_callable-mock-class",
            ),
        ],
    )
    def test_mock_without_spec_is_counted(self, count_violations, code):
        assert count_violations(code) == 1

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param("MagicMock(spec=Variable)", id="MagicMock-spec"),
            pytest.param("MagicMock(spec_set=Variable)", id="MagicMock-spec_set"),
            pytest.param("MagicMock(Variable)", id="MagicMock-positional-spec"),
            pytest.param("MagicMock(**kwargs)", id="MagicMock-kwargs-unpacked"),
            pytest.param("mock.patch('airflow.models.Variable.get', spec=True)", id="patch-spec"),
            pytest.param("mock.patch('airflow.models.Variable.get', spec_set=True)", id="patch-spec_set"),
            pytest.param("mock.patch('airflow.models.Variable.get', autospec=True)", id="patch-autospec"),
            pytest.param("mock.patch('airflow.models.Variable.get', new=get)", id="patch-new"),
            pytest.param(
                "mock.patch('airflow.models.Variable.get', new_callable=PropertyMock)",
                id="patch-new_callable",
            ),
            pytest.param("mock.patch('airflow.models.Variable.get', get)", id="patch-positional-new"),
            pytest.param("mock.patch('airflow.models.Variable.get', **kwargs)", id="patch-kwargs-unpacked"),
            pytest.param("mock.patch.object(Variable, 'get', autospec=True)", id="patch.object-autospec"),
            pytest.param("mock.patch.object(Variable, 'get', get)", id="patch.object-positional-new"),
            pytest.param("mock.patch.dict(os.environ, {'AIRFLOW_HOME': '/tmp'})", id="patch.dict"),
            pytest.param("mock.patch.multiple('os', getcwd=DEFAULT)", id="patch.multiple"),
            pytest.param("mock.PropertyMock()", id="PropertyMock"),
            pytest.param("mock.create_autospec(Variable)", id="create_autospec"),
            pytest.param("test_client.patch('/api/v2/dags/example', json={})", id="http-client-patch"),
            pytest.param("requests_mock.patch('http://example.com')", id="requests_mock-patch"),
        ],
    )
    def test_mock_with_spec_or_other_call_is_not_counted(self, count_violations, code):
        assert count_violations(code) == 0

    def test_syntax_error_returns_no_violations(self, count_violations):
        assert count_violations("MagicMock(\n") == 0

    def test_invalid_utf8_is_still_counted(self, tmp_path):
        path = tmp_path / "invalid_utf8.py"
        path.write_bytes(b"# bad byte: \xff\nMagicMock()\n")

        assert UnspeccedMockAllowlistManager(tmp_path / "allowlist.txt").count_occurrences(path) == 1

    def test_lists_violation_lines_in_order(self, write_python_file, tmp_path):
        path = write_python_file(
            """\
            def test_hook():
                hook = mock.MagicMock()

            get = mock.patch("airflow.models.Variable.get")
            variable = mock.MagicMock(spec=Variable)
            """
        )
        manager = UnspeccedMockAllowlistManager(tmp_path / "allowlist.txt")

        details = [Text.from_markup(line).plain.strip() for line in manager.format_violation_details(path)]

        assert details == ["L2 mock.MagicMock(...)", "L4 mock.patch(...)"]

    def test_scans_only_tests_directories_under_project_roots(self, create_fake_repo, tmp_path):
        for rel in (
            "providers/amazon/tests/unit/amazon/test_hook.py",
            "airflow-core/tests/conftest.py",
            "airflow-core/src/airflow/models/variable.py",
            "clients/python/tests/test_client.py",
            "dev/breeze/.venv/lib/python3.12/site-packages/pkg/tests/test_pkg.py",
            "airflow-core/src/airflow/ui/node_modules/pkg/tests/test_pkg.py",
            "providers/.build/tests/test_build.py",
        ):
            create_fake_repo(rel, "MagicMock()\n")
        manager = UnspeccedMockAllowlistManager(tmp_path / "allowlist.txt")

        scanned = sorted(path.relative_to(tmp_path).as_posix() for path in manager.iter_files())

        assert scanned == [
            "airflow-core/tests/conftest.py",
            "providers/amazon/tests/unit/amazon/test_hook.py",
        ]

    def test_new_violation_in_allowlisted_file_fails(self, create_fake_repo, tmp_path):
        rel = "airflow-core/tests/unit/models/test_variable.py"
        path = create_fake_repo(
            rel,
            """\
            hook = mock.MagicMock()
            get = mock.patch("airflow.models.Variable.get")
            """,
        )
        manager = UnspeccedMockAllowlistManager(tmp_path / "allowlist.txt")
        manager.save({rel: 1})

        assert manager.check([path], manager.load()) == 1
        assert manager.load() == {rel: 1}


def test_hook_files_pattern_matches_project_roots():
    config = yaml.safe_load((hook.AIRFLOW_ROOT_PATH / ".pre-commit-config.yaml").read_text())
    [entry] = [
        hook_entry
        for repo in config["repos"]
        for hook_entry in repo.get("hooks", [])
        if hook_entry["id"] == "check-no-new-unspecced-mocks"
    ]

    assert entry["files"] == rf"^({'|'.join(hook._PROJECT_ROOTS)})/(.*/)?tests/.*\.py$"
