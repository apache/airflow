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
from ci.prek import check_new_airflow_exception_usage as hook
from ci.prek.check_new_airflow_exception_usage import (
    AirflowExceptionAllowlistManager,
    _check_airflow_exception_usage,
    _raise_lines,
)


@pytest.fixture
def create_fake_repo(tmp_path, monkeypatch):
    monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)

    def _write(rel: str, code: str) -> Path:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(code))
        return path

    return _write


class TestRaiseLines:
    def test_counts_raise_airflow_exception(self, write_python_file):
        path = write_python_file(
            """\
            from airflow.exceptions import AirflowException
            raise AirflowException("boom")
            raise AirflowException("bang")
            """
        )
        assert len(_raise_lines(path)) == 2

    def test_ignores_commented_lines(self, write_python_file):
        path = write_python_file(
            """\
            # raise AirflowException("commented out")
            raise AirflowException("real")
            """
        )
        assert len(_raise_lines(path)) == 1

    def test_ignores_other_raises(self, write_python_file):
        path = write_python_file(
            """\
            raise ValueError("not this")
            raise TypeError("nor this")
            """
        )
        assert len(_raise_lines(path)) == 0

    def test_missing_file_returns_empty(self, tmp_path):
        assert _raise_lines(tmp_path / "nonexistent.py") == []


class TestAirflowExceptionAllowlistManager:
    def test_load_missing_file_returns_empty(self, tmp_path):
        manager = AirflowExceptionAllowlistManager(tmp_path / "missing.txt")
        assert manager.load() == {}

    def test_save_and_load_round_trip(self, tmp_path):
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        manager.save({"b/file.py": 2, "a/file.py": 1})
        text = (tmp_path / "allowlist.txt").read_text()
        assert text.splitlines() == ["a/file.py::1", "b/file.py::2"]
        assert manager.load() == {"a/file.py": 1, "b/file.py": 2}

    def test_load_skips_blank_and_malformed_lines(self, tmp_path):
        path = tmp_path / "allowlist.txt"
        path.write_text("\nvalid/file.py::3\nnocount\n::5\nbad::notanumber\n")
        assert AirflowExceptionAllowlistManager(path).load() == {"valid/file.py": 3}

    @pytest.mark.usefixtures("create_fake_repo")
    def test_load_skips_unsafe_entries(self, tmp_path):
        path = tmp_path / "allowlist.txt"
        path.write_text("airflow-core/src/airflow/safe.py::1\n../escape.py::1\n/etc/passwd::1\n")
        assert AirflowExceptionAllowlistManager(path).load() == {"airflow-core/src/airflow/safe.py": 1}


class TestCheckAirflowExceptionUsage:
    def test_no_violations_passes(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/clean.py",
            """\
            raise ValueError("specific exception")
            """,
        )
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        assert _check_airflow_exception_usage([path], {}, manager) == 0

    def test_new_violation_fails(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/bad.py",
            """\
            raise AirflowException("boom")
            """,
        )
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        assert _check_airflow_exception_usage([path], {}, manager) == 1

    def test_violation_within_allowlist_passes(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/grandfathered.py",
            """\
            raise AirflowException("old")
            """,
        )
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/grandfathered.py": 1}
        assert _check_airflow_exception_usage([path], allowlist, manager) == 0

    def test_exceeding_allowlist_fails(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/grew.py",
            """\
            raise AirflowException("one")
            raise AirflowException("two")
            """,
        )
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/grew.py": 1}
        assert _check_airflow_exception_usage([path], allowlist, manager) == 1

    def test_reducing_violations_tightens_allowlist(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/improved.py",
            """\
            raise AirflowException("one remains")
            """,
        )
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/improved.py": 2}
        assert _check_airflow_exception_usage([path], allowlist, manager) == 1
        assert manager.load() == {"airflow-core/src/airflow/improved.py": 1}

    def test_fixing_all_violations_removes_entry(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/fixed.py",
            """\
            raise ValueError("migrated away from AirflowException")
            """,
        )
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        allowlist = {"airflow-core/src/airflow/fixed.py": 1}
        assert _check_airflow_exception_usage([path], allowlist, manager) == 1
        assert manager.load() == {}

    def test_non_python_file_is_skipped(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/not_python.txt",
            "raise AirflowException('in a text file')\n",
        )
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        assert _check_airflow_exception_usage([path], {}, manager) == 0


class TestCleanup:
    def test_cleanup_removes_stale_entries(self, create_fake_repo, tmp_path):
        create_fake_repo("airflow-core/src/airflow/keeper.py", "pass")
        allowlist_path = tmp_path / "allowlist.txt"
        manager = AirflowExceptionAllowlistManager(allowlist_path)
        manager.save(
            {
                "airflow-core/src/airflow/keeper.py": 1,
                "airflow-core/src/airflow/gone.py": 1,
            }
        )
        assert manager.cleanup() == 0
        assert manager.load() == {"airflow-core/src/airflow/keeper.py": 1}

    def test_cleanup_empty_allowlist(self, tmp_path):
        manager = AirflowExceptionAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.cleanup() == 0
