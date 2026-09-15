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

import pytest
from ci.prek import check_exception_format_args as hook
from ci.prek.check_exception_format_args import (
    ExceptionFormatArgsAllowlistManager,
    count_positional_placeholders,
    find_format_arg_raises,
)


@pytest.fixture
def manager(tmp_path, monkeypatch):
    monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)
    return ExceptionFormatArgsAllowlistManager(tmp_path / "allowlist.txt")


class TestCountPositionalPlaceholders:
    @pytest.mark.parametrize(
        "message, expected",
        [
            pytest.param("%s and %s", 2, id="bare"),
            pytest.param("%d", 1, id="bare-integer"),
            pytest.param("%f", 1, id="bare-float"),
            pytest.param("%#x", 1, id="alternate-form-flag"),
            pytest.param("%10.4f", 1, id="width-and-precision-together"),
            pytest.param("%z", 0, id="not-a-conversion-character"),
            pytest.param("took %.2f sec", 1, id="precision"),
            pytest.param("%05d", 1, id="zero-padded-width"),
            pytest.param("%i items", 1, id="integer-alias"),
            pytest.param("%X hex", 1, id="upper-hex"),
            pytest.param("%-10s|", 1, id="left-align-flag"),
            pytest.param("%+d", 1, id="sign-flag"),
            pytest.param("%lu", 1, id="length-modifier"),
            pytest.param("50% off", 0, id="english-percent-is-not-a-conversion"),
            pytest.param("download is 50% complete", 0, id="english-percent-mid-sentence"),
            pytest.param("upload is 50% done", 0, id="english-percent-before-integer-conversion"),
            pytest.param("100%% done", 0, id="escaped-percent"),
            pytest.param("%%s", 0, id="escaped-percent-before-conversion"),
            pytest.param("%% and %s", 1, id="escaped-percent-plus-conversion"),
            pytest.param("no placeholders", 0, id="none"),
            pytest.param("%(name)s", None, id="mapping-key-is-out-of-scope"),
            pytest.param("%*d", None, id="star-width-is-out-of-scope"),
        ],
    )
    def test_counts_the_conversions_a_message_really_carries(self, message, expected):
        assert count_positional_placeholders(message) == expected


class TestFindFormatArgRaises:
    def test_finds_single_line_raise(self, write_python_file):
        path = write_python_file(
            """\
            raise AirflowException("TaskInstance %s is not found", ti.task_id)
            """
        )
        assert find_format_arg_raises(path) == [(1, "AirflowException")]

    def test_finds_raise_spanning_several_lines(self, write_python_file):
        path = write_python_file(
            """\
            raise AirflowException(
                "TaskInstance with dag_id: %s, task_id: %s is not found",
                ti.dag_id,
                ti.task_id,
            )
            """
        )
        assert find_format_arg_raises(path) == [(1, "AirflowException")]

    def test_finds_raise_with_a_cause(self, write_python_file):
        path = write_python_file(
            """\
            raise AirflowException("TaskInstance %s is not found", ti.task_id) from err
            """
        )
        assert find_format_arg_raises(path) == [(1, "AirflowException")]

    def test_finds_dotted_exception_name(self, write_python_file):
        path = write_python_file(
            """\
            raise exceptions.NotFound("The secret '%s' not found", secret_id)
            """
        )
        assert find_format_arg_raises(path) == [(1, "NotFound")]

    @pytest.mark.parametrize(
        "code",
        [
            pytest.param('raise ValueError("no placeholders here", extra)', id="no-placeholder"),
            pytest.param('raise ValueError("only %s here", one, two)', id="fewer-placeholders"),
            pytest.param('raise ValueError("%s and %s", only_one)', id="fewer-args"),
            pytest.param('raise HTTPException(404, "detail")', id="non-literal-first-arg"),
            pytest.param('raise ValueError("%s and %s", a, key=b)', id="keyword-argument-skews-count"),
            pytest.param('raise ValueError(f"already {formatted}", extra)', id="f-string"),
            pytest.param('raise ValueError("message %s", *args)', id="starred-args"),
            pytest.param('raise ValueError("single argument only")', id="single-argument"),
            pytest.param('err = ValueError("%s", v)', id="not-raised-inline"),
            pytest.param("raise ValueError", id="bare-raise-name"),
        ],
    )
    def test_leaves_other_shapes_alone(self, write_python_file, code):
        assert find_format_arg_raises(write_python_file(code + "\n")) == []

    def test_unparsable_file_returns_empty(self, write_python_file):
        assert find_format_arg_raises(write_python_file("def broken(:\n")) == []

    def test_missing_file_returns_empty(self, tmp_path):
        assert find_format_arg_raises(tmp_path / "nonexistent.py") == []


class TestManager:
    def _write(self, tmp_path, rel: str, code: str):
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(code))
        return path

    def test_exceeding_the_recorded_count_is_a_violation(self, manager, tmp_path):
        path = self._write(
            tmp_path,
            "mod.py",
            """\
            raise ValueError("boom %s", value)
            raise ValueError("bang %s", value)
            """,
        )
        assert manager.check([path], {"mod.py": 1}) == 1
        assert manager.check([path], {"mod.py": 2}) == 0

    def test_violation_details_report_line_numbers(self, manager, tmp_path):
        path = self._write(
            tmp_path,
            "mod.py",
            """\
            x = 1
            raise ValueError("boom %s", value)
            """,
        )
        assert manager.format_violation_details(path) == [
            "      line 2: [yellow]ValueError[/yellow]",
        ]

    def test_generate_records_only_files_with_occurrences(self, manager, tmp_path):
        (tmp_path / "bad.py").write_text('raise ValueError("boom %s", value)\n')
        (tmp_path / "good.py").write_text('raise ValueError("boom")\n')

        assert manager.generate() == 0
        assert manager.load() == {"bad.py": 1}

    def test_generate_skips_vendored_directories(self, manager, tmp_path):
        vendored = tmp_path / "dev" / "breeze" / ".venv" / "lib" / "dep.py"
        vendored.parent.mkdir(parents=True)
        vendored.write_text('raise ValueError("boom %s", value)\n')

        assert manager.generate() == 0
        assert manager.load() == {}


class TestMain:
    @pytest.fixture(autouse=True)
    def _repo(self, tmp_path, monkeypatch):
        monkeypatch.setattr(hook, "REPO_ROOT", tmp_path)
        self.allowlist = tmp_path / "generated" / "known_exception_format_args.txt"
        self.allowlist.parent.mkdir()
        (tmp_path / "mod.py").write_text('raise ValueError("boom %s", value)\n')

    def test_generate_writes_the_allowlist(self, tmp_path):
        assert hook.main(["--generate"]) == 0
        assert self.allowlist.read_text() == "mod.py::1\n"

    def test_all_files_reports_an_unrecorded_occurrence(self, tmp_path):
        assert hook.main(["--all-files"]) == 1

    def test_named_file_is_checked_against_the_allowlist(self, tmp_path):
        self.allowlist.write_text("mod.py::1\n")
        assert hook.main([str(tmp_path / "mod.py")]) == 0

    def test_cleanup_drops_entries_for_deleted_files(self, tmp_path):
        self.allowlist.write_text("mod.py::1\ngone.py::1\n")
        assert hook.main(["--cleanup"]) == 0
        assert self.allowlist.read_text() == "mod.py::1\n"

    def test_no_arguments_is_a_no_op(self):
        assert hook.main([]) == 0
