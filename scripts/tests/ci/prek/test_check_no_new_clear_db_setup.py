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

import os
import subprocess
import textwrap
from pathlib import Path

import pytest
from ci.prek import check_no_new_clear_db_setup as hook


@pytest.fixture
def write_python_file(tmp_path: Path):
    def write(source: str, name: str = "test_target.py") -> Path:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source))
        return path

    return write


def _violations(path: Path) -> list[tuple[str, str, str, int]]:
    return [
        (violation.owner, violation.phase, violation.helper, violation.line)
        for violation in hook.find_setup_db_cleanups(path)
    ]


class TestFindSetupDbCleanups:
    def test_reports_direct_import_alias_in_xunit_setup(self, write_python_file):
        path = write_python_file(
            """
            from tests_common.test_utils.db import clear_db_runs as clear_runs

            class TestRuns:
                def setup_method(self):
                    clear_runs()

                def teardown_method(self):
                    clear_runs()
            """
        )

        assert _violations(path) == [("TestRuns.setup_method", "xunit setup", "clear_db_runs", 6)]

    @pytest.mark.parametrize(
        "name",
        [
            "setup",
            "setup_class",
            "setup_function",
            "setup_method",
            "setup_module",
        ],
    )
    def test_reports_standard_xunit_setup_names(self, write_python_file, name):
        path = write_python_file(
            f"""
            from tests_common.test_utils.db import clear_db_dags

            def {name}():
                clear_db_dags()
            """
        )

        assert _violations(path) == [(name, "xunit setup", "clear_db_dags", 5)]

    @pytest.mark.parametrize("decorator", ["fixture", "pytest.fixture", "pytest.fixture()"])
    def test_reports_fixture_calls_before_yield(self, write_python_file, decorator):
        path = write_python_file(
            f"""
            import pytest
            from tests_common.test_utils.db import clear_db_assets as clear_assets

            @{decorator}
            def rows():
                clear_assets()
                yield
                clear_assets()
            """
        )

        assert _violations(path) == [("rows", "fixture setup", "clear_db_assets", 7)]

    def test_reports_all_calls_in_non_yielding_fixture(self, write_python_file):
        path = write_python_file(
            """
            from pytest import fixture
            from tests_common.test_utils.db import clear_db_jobs

            @fixture
            def rows():
                clear_db_jobs()
                return []
            """
        )

        assert _violations(path) == [("rows", "fixture setup", "clear_db_jobs", 7)]

    def test_reports_helper_called_in_yield_value(self, write_python_file):
        path = write_python_file(
            """
            import pytest
            from tests_common.test_utils.db import clear_db_runs

            @pytest.fixture
            def rows():
                yield clear_db_runs()
            """
        )

        assert _violations(path) == [("rows", "fixture setup", "clear_db_runs", 7)]

    def test_ignores_ordinary_tests_teardown_and_nested_scopes(self, write_python_file):
        path = write_python_file(
            """
            import pytest
            from tests_common.test_utils.db import clear_db_dags

            def test_example():
                clear_db_dags()

            def teardown_method():
                clear_db_dags()

            @pytest.fixture
            def rows():
                def nested():
                    clear_db_dags()
                yield
                clear_db_dags()
            """
        )

        assert _violations(path) == []

    def test_ignores_unrelated_clear_db_name(self, write_python_file):
        path = write_python_file(
            """
            from local_helpers import clear_db_dags

            def setup_method():
                clear_db_dags()
            """
        )

        assert _violations(path) == []

    def test_ignores_module_aliases_and_unittest_setup_names(self, write_python_file):
        path = write_python_file(
            """
            from tests_common.test_utils import db as test_db

            def setUp():
                test_db.clear_db_dags()

            def setup_method():
                test_db.clear_db_dags()
            """
        )

        assert _violations(path) == []

    def test_reports_function_local_simple_alias(self, write_python_file):
        path = write_python_file(
            """
            def setup_method():
                from tests_common.test_utils.db import clear_db_dags as clear_dags
                clear_dags()
            """
        )

        assert _violations(path) == [("setup_method", "xunit setup", "clear_db_dags", 4)]

    def test_ignores_async_class_and_lambda_nested_scopes(self, write_python_file):
        path = write_python_file(
            """
            from tests_common.test_utils.db import clear_db_dags

            def setup_method():
                async def nested_async():
                    clear_db_dags()
                class Nested:
                    clear_db_dags()
                callback = lambda: clear_db_dags()
            """
        )

        assert _violations(path) == []

    def test_does_not_apply_function_import_that_is_lexically_after_call(self, write_python_file):
        path = write_python_file(
            """
            def setup_method():
                clear_db_dags()
                from tests_common.test_utils.db import clear_db_dags
            """
        )

        assert _violations(path) == []

    def test_read_and_parse_errors_propagate(self, write_python_file, tmp_path):
        with pytest.raises(SyntaxError):
            hook.find_setup_db_cleanups(write_python_file("def broken(:\n"))
        with pytest.raises(OSError):
            hook.find_setup_db_cleanups(tmp_path / "missing.py")


@pytest.fixture
def fake_repo(tmp_path: Path, monkeypatch):
    allowlist = tmp_path / "generated" / "known_clear_db_setup.txt"
    allowlist.parent.mkdir(parents=True)
    allowlist.write_text("")
    monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(hook, "_ALLOWLIST_PATH", allowlist)
    _initialize_git(tmp_path)

    def write(relative_path: str, source: str) -> Path:
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source))
        return path

    return write, allowlist


def _initialize_git(repo: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Tests"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "tests@example.com"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", "baseline"], cwd=repo, check=True)


def _commit_all(repo: Path, message: str) -> None:
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "-qm", message], cwd=repo, check=True)


class TestHookCli:
    def test_hook_script_is_executable(self):
        assert os.access(Path(hook.__file__), os.X_OK)

    def test_generate_writes_sorted_file_baseline(self, fake_repo):
        write, allowlist = fake_repo
        write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )
        write(
            "providers/common/tests/unit/test_assets.py",
            """
            import pytest
            from tests_common.test_utils.db import clear_db_assets
            @pytest.fixture
            def rows():
                clear_db_assets()
                yield
            """,
        )

        assert hook.main(["--generate"]) == 0
        assert allowlist.read_text().splitlines() == [
            "airflow-core/tests/unit/test_runs.py",
            "providers/common/tests/unit/test_assets.py",
        ]

    def test_grandfathered_file_tolerates_line_and_cleanup_details_changing(self, fake_repo):
        write, allowlist = fake_repo
        source = write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )
        allowlist.write_text("airflow-core/tests/unit/test_runs.py\n")
        _commit_all(allowlist.parents[1], "allow existing site")
        source.write_text(
            "import pytest\n"
            "from tests_common.test_utils.db import clear_db_dags\n"
            "\n"
            "\n"
            "@pytest.fixture\n"
            "def renamed_fixture():\n"
            "    clear_db_dags()\n"
            "    clear_db_dags()\n"
            "    yield\n"
        )

        assert hook.main([str(source)]) == 0

    def test_rejects_unlisted_file_with_setup_cleanup(self, fake_repo):
        write, _ = fake_repo
        source = write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )

        assert hook.main([str(source)]) == 1

    def test_regeneration_removes_cleaned_file(self, fake_repo):
        write, allowlist = fake_repo
        source = write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )
        assert hook.main(["--generate"]) == 0
        assert allowlist.read_text() == "airflow-core/tests/unit/test_runs.py\n"

        source.write_text("def setup_method():\n    pass\n")
        assert hook.main(["--generate"]) == 0
        assert allowlist.read_text() == ""

    def test_rejects_stale_allowlist_file(self, fake_repo, capsys):
        write, allowlist = fake_repo
        source = write("airflow-core/tests/unit/test_runs.py", "def setup_method():\n    pass\n")
        allowlist.write_text("airflow-core/tests/unit/test_runs.py\n")
        _commit_all(allowlist.parents[1], "allow removed file")

        assert hook.main([str(source)]) == 1
        assert "stale" in capsys.readouterr().out.lower()

    def test_all_files_rejects_allowlist_file_for_missing_file(self, fake_repo, capsys):
        _, allowlist = fake_repo
        allowlist.write_text("airflow-core/tests/unit/test_missing.py\n")
        _commit_all(allowlist.parents[1], "allow missing file")

        assert hook.main(["--all-files"]) == 1
        assert "test_missing.py" in capsys.readouterr().out

    @pytest.mark.parametrize(
        "path",
        [
            "./airflow-core/tests/unit/test_runs.py\n",
            "airflow-core//tests/unit/test_runs.py\n",
            "airflow-core/tests/unit/./test_runs.py\n",
        ],
    )
    def test_rejects_noncanonical_allowlist_path_spellings(self, path):
        with pytest.raises(ValueError):
            hook._parse_allowlist(path)

    @pytest.mark.parametrize(
        "baseline",
        [
            "not canonical\n",
            "/tmp/test.py\n",
            "../evil.py\n",
            "./airflow-core/tests/unit/test_runs.py\n",
            "airflow-core//tests/unit/test_runs.py\n",
            "airflow-core/tests/unit/./test_runs.py\n",
            "airflow-core/tests/integration/test.py\n",
            "airflow-core/tests/unit/test_runs.py\nairflow-core/tests/unit/test_runs.py\n",
            "providers/z/tests/unit/test_b.py\nairflow-core/tests/unit/test_a.py\n",
        ],
    )
    def test_rejects_malformed_duplicate_unsorted_and_invalid_path_baselines(
        self, fake_repo, baseline, capsys
    ):
        _, allowlist = fake_repo
        allowlist.write_text(baseline)

        assert hook.main(["--all-files"]) == 1
        assert "allowlist" in capsys.readouterr().out.lower()

    def test_allowlist_edit_cannot_grandfather_a_new_file(self, fake_repo):
        write, allowlist = fake_repo
        source = write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )
        allowlist.write_text("airflow-core/tests/unit/test_runs.py\n")

        assert hook.main([str(source), str(allowlist)]) == 1

    def test_invalid_ci_baseline_ref_fails_closed(self, fake_repo, monkeypatch, capsys):
        _, allowlist = fake_repo
        monkeypatch.setenv("PRE_COMMIT_FROM_REF", "missing-ref")

        assert hook.main(["--all-files"]) == 1
        assert "trusted baseline" in capsys.readouterr().out.lower()

    def test_trusted_baseline_rejects_reintroduction_of_cleaned_file(self, fake_repo, monkeypatch, capsys):
        write, allowlist = fake_repo
        write("scripts/ci/prek/check_no_new_clear_db_setup.py", "# checker\n")
        _commit_all(allowlist.parents[1], "hook baseline")
        base_ref = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=allowlist.parents[1],
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        source = write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )
        allowlist.write_text("airflow-core/tests/unit/test_runs.py\n")
        monkeypatch.setenv("PRE_COMMIT_FROM_REF", base_ref)

        assert hook.main([str(source), str(allowlist)]) == 1
        output = capsys.readouterr().out
        assert "cannot add" in output.lower()
        assert "may only shrink" in output
        assert "https://github.com/apache/airflow/issues/71577" in output

    def test_valid_ci_baseline_with_hook_but_no_allowlist_fails_closed(self, fake_repo, monkeypatch, capsys):
        write, allowlist = fake_repo
        write("scripts/ci/prek/check_no_new_clear_db_setup.py", "# checker\n")
        allowlist.unlink()
        _commit_all(allowlist.parents[1], "hook without baseline")
        base_ref = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=allowlist.parents[1],
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        allowlist.write_text("")
        monkeypatch.setenv("PRE_COMMIT_FROM_REF", base_ref)

        assert hook.main(["--all-files"]) == 1
        assert "trusted baseline" in capsys.readouterr().out.lower()

    def test_ci_accepts_initial_baseline_when_base_predates_hook(self, fake_repo, monkeypatch):
        write, allowlist = fake_repo
        allowlist.unlink()
        _commit_all(allowlist.parents[1], "base before hook")
        base_ref = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=allowlist.parents[1],
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        source = write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )
        allowlist.write_text("airflow-core/tests/unit/test_runs.py\n")
        monkeypatch.setenv("PRE_COMMIT_FROM_REF", base_ref)

        assert hook.main([str(source), str(allowlist)]) == 0

    def test_source_error_fails_closed_with_filename(self, fake_repo, capsys):
        write, _ = fake_repo
        source = write("airflow-core/tests/unit/test_bad.py", "def broken(:\n")

        assert hook.main([str(source)]) == 1
        assert "test_bad.py" in capsys.readouterr().out

    def test_new_violation_diagnostic_is_actionable(self, fake_repo, capsys):
        write, _ = fake_repo
        source = write(
            "airflow-core/tests/unit/test_runs.py",
            """
            from tests_common.test_utils.db import clear_db_runs
            def setup_method():
                clear_db_runs()
            """,
        )

        assert hook.main([str(source)]) == 1
        output = capsys.readouterr().out
        assert "test_runs.py" in output
        assert "4" in output
        assert "clear_db_runs" in output
        assert "xUnit setup setup_method" in output
        assert "Setup-time database cleanup is not allowed in unit tests:" in output
        assert "How to fix:" in output
        assert "Remove this setup cleanup. The fixture or test that creates the database rows" in output
        assert "must clean them up during teardown." in output
        assert "Do not add this file to the allowlist." in output
        assert "More information:" in output
        assert "https://github.com/apache/airflow/issues/71577" in output

    def test_no_files_is_a_noop(self, fake_repo):
        assert hook.main([]) == 0
