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

import re
import textwrap
from pathlib import Path

import pytest
import yaml
from ci.prek import check_new_fstring_exception_usage as hook
from ci.prek.check_new_fstring_exception_usage import (
    SCANNED_ROOTS,
    FStringExceptionAllowlistManager,
    _find_fstring_exception_messages,
    _find_python_files,
    main,
)
from ci.prek.common_prek_utils import AIRFLOW_ROOT_PATH


@pytest.fixture
def create_fake_repo(tmp_path, monkeypatch):
    monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)
    (tmp_path / "generated").mkdir()

    def _write(rel: str, code: str) -> Path:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(code))
        return path

    return _write


@pytest.fixture
def count_messages(write_python_file):
    """Factory fixture: write code to a temp file and count its f-string exception messages."""

    def _count(code: str) -> int:
        return len(_find_fstring_exception_messages(write_python_file(code)))

    return _count


class TestFindFstringExceptionMessages:
    @pytest.mark.parametrize(
        "code, expected",
        [
            pytest.param('raise ValueError(f"no such pool: {name}")', 1, id="f-string-message"),
            pytest.param('raise ValueError(f"static")', 1, id="f-string-without-placeholder"),
            pytest.param('raise ValueError(f"bad {x}") from err', 1, id="raise-from"),
            pytest.param('raise ValueError("a" f"{x}")', 1, id="implicit-concatenation"),
            pytest.param('raise ValueError("no such pool")', 0, id="plain-string-is-em101"),
            pytest.param('raise ValueError("{}".format(x))', 0, id="format-call-is-em103"),
            pytest.param("raise ValueError(msg)", 0, id="variable-message"),
            pytest.param("raise ValueError", 0, id="exception-class-without-call"),
            pytest.param('raise ValueError(code, f"bad {x}")', 0, id="f-string-not-first-argument"),
            pytest.param('raise ValueError(msg=f"bad {x}")', 0, id="f-string-keyword-argument"),
            pytest.param("raise ValueError(*args)", 0, id="starred-arguments"),
            pytest.param('log.info(f"bad {x}")', 0, id="f-string-outside-raise"),
        ],
    )
    def test_detection(self, count_messages, code, expected):
        assert count_messages(code) == expected

    def test_counts_every_occurrence(self, count_messages):
        assert (
            count_messages(
                """\
                def outer(x):
                    if x:
                        raise ValueError(f"first {x}")
                    raise TypeError(f"second {x}")
                """
            )
            == 2
        )

    def test_bare_raise_is_ignored(self, count_messages):
        assert (
            count_messages(
                """\
                try:
                    pass
                except ValueError:
                    raise
                """
            )
            == 0
        )

    def test_unparsable_file_is_unknown_rather_than_empty(self, write_python_file):
        path = write_python_file("def broken(:\n")
        assert _find_fstring_exception_messages(path) is None

    def test_missing_file_is_unknown_rather_than_empty(self, tmp_path):
        assert _find_fstring_exception_messages(tmp_path / "nonexistent.py") is None

    def test_reports_the_raising_line(self, write_python_file):
        path = write_python_file(
            """\
            x = 1
            raise ValueError(f"bad {x}")
            """
        )
        assert [message.lineno for message in _find_fstring_exception_messages(path)] == [2]


class TestFindPythonFiles:
    def test_scans_only_the_configured_roots(self, create_fake_repo, tmp_path):
        create_fake_repo("airflow-core/src/airflow/scanned.py", "pass")
        create_fake_repo("dev/breeze/skipped.py", "pass")
        assert _find_python_files() == [tmp_path / "airflow-core/src/airflow/scanned.py"]

    @pytest.mark.parametrize("skipped", [".tox", ".venv", "__pycache__", "_vendor"])
    def test_skips_excluded_directories(self, create_fake_repo, tmp_path, skipped):
        create_fake_repo(f"providers/amazon/{skipped}/module.py", "pass")
        assert _find_python_files() == []

    def test_excluded_names_above_the_repo_root_are_ignored(self, tmp_path, monkeypatch):
        repo_root = tmp_path / ".venv" / "airflow"
        monkeypatch.setattr(hook, "REPO_ROOT", repo_root)
        scanned = repo_root / "shared" / "logging" / "module.py"
        scanned.parent.mkdir(parents=True)
        scanned.write_text("pass")
        assert _find_python_files() == [scanned]

    def test_ignores_non_python_files(self, create_fake_repo):
        create_fake_repo("shared/logging/notes.txt", "pass")
        assert _find_python_files() == []


class TestScannedRoots:
    def test_match_the_registered_hook_file_pattern(self):
        """The roots `--generate` records must be the ones prek later feeds to the hook."""
        config = yaml.safe_load((AIRFLOW_ROOT_PATH / ".pre-commit-config.yaml").read_text())
        hooks = [h for repo in config["repos"] for h in repo["hooks"]]
        registered = next(h for h in hooks if h["id"] == "check-no-new-fstring-in-exception")
        roots = re.fullmatch(r"\^\((?P<roots>[^)]+)\)/\.\*\\\.py\$", registered["files"])
        assert roots, f"unrecognised files pattern: {registered['files']}"
        assert tuple(roots.group("roots").split("|")) == SCANNED_ROOTS


class TestFStringExceptionAllowlistManager:
    def test_count_occurrences(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "task-sdk/src/airflow/sdk/counted.py",
            """\
            raise ValueError(f"one {x}")
            raise ValueError("two")
            """,
        )
        manager = FStringExceptionAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.count_occurrences(path) == 1

    def test_format_violation_details_lists_line_numbers_in_source_order(self, create_fake_repo, tmp_path):
        path = create_fake_repo(
            "task-sdk/src/airflow/sdk/detailed.py",
            """\
            def nested(x):
                raise ValueError(f"nested {x}")

            raise ValueError(f"module level {x}")
            """,
        )
        manager = FStringExceptionAllowlistManager(tmp_path / "allowlist.txt")
        assert manager.format_violation_details(path) == [
            "    [dim]line 2[/dim]",
            "    [dim]line 4[/dim]",
        ]

    def test_violation_panel_text_names_the_rule(self, tmp_path):
        manager = FStringExceptionAllowlistManager(tmp_path / "allowlist.txt")
        assert "EM102" in manager.violation_panel_text()


class TestMain:
    @pytest.fixture
    def allowlist_path(self, tmp_path) -> Path:
        return tmp_path / "generated" / "known_fstring_exceptions.txt"

    def test_generate_records_existing_usages(self, create_fake_repo, allowlist_path):
        create_fake_repo("airflow-ctl/src/airflowctl/legacy.py", 'raise ValueError(f"old {x}")')
        assert main(["--generate"]) == 0
        assert allowlist_path.read_text() == "airflow-ctl/src/airflowctl/legacy.py::1\n"

    def test_new_usage_fails(self, create_fake_repo, allowlist_path):
        path = create_fake_repo("airflow-core/src/airflow/new.py", 'raise ValueError(f"new {x}")')
        allowlist_path.write_text("")
        assert main([str(path)]) == 1

    def test_grandfathered_usage_passes(self, create_fake_repo, allowlist_path):
        path = create_fake_repo("airflow-core/src/airflow/old.py", 'raise ValueError(f"old {x}")')
        allowlist_path.write_text("airflow-core/src/airflow/old.py::1\n")
        assert main([str(path)]) == 0

    def test_removed_usage_tightens_the_allowlist(self, create_fake_repo, allowlist_path):
        path = create_fake_repo("airflow-core/src/airflow/improved.py", 'raise ValueError("cleaned")')
        allowlist_path.write_text("airflow-core/src/airflow/improved.py::1\n")
        assert main([str(path)]) == 1
        assert "improved.py" not in allowlist_path.read_text()

    def test_all_files_checks_the_whole_scan_scope(self, create_fake_repo, allowlist_path):
        create_fake_repo("providers/amazon/src/amazon/bad.py", 'raise ValueError(f"bad {x}")')
        allowlist_path.write_text("")
        assert main(["--all-files"]) == 1

    def test_cleanup_drops_entries_for_deleted_files(self, create_fake_repo, allowlist_path):
        create_fake_repo("shared/logging/src/kept.py", 'raise ValueError(f"kept {x}")')
        allowlist_path.write_text("shared/logging/src/kept.py::1\nshared/logging/src/gone.py::1\n")
        assert main(["--cleanup"]) == 0
        assert allowlist_path.read_text() == "shared/logging/src/kept.py::1\n"

    def test_unparsable_file_keeps_its_allowlist_entry(self, create_fake_repo, allowlist_path):
        path = create_fake_repo(
            "airflow-core/src/airflow/wip.py",
            """\
            raise ValueError(f"one {x}")
            def half_written(:
            """,
        )
        allowlist_path.write_text("airflow-core/src/airflow/wip.py::1\n")
        assert main([str(path)]) == 0
        assert allowlist_path.read_text() == "airflow-core/src/airflow/wip.py::1\n"

    def test_no_files_passes(self, create_fake_repo, allowlist_path):
        allowlist_path.write_text("")
        assert main([]) == 0
