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

from pathlib import Path

import pytest
from check_cli_consistency import (
    check_code,
    check_file,
    is_cli_definition_file,
    validate_flag,
)


class TestValidateFlag:
    @pytest.mark.parametrize(
        "flag",
        [
            "--dag-id",
            "--task-id",
            "--execution-date",
            "--output-path",
            "--include-subdags",
            "--verbose",
            "--v2",
            "-v",
            "-l",
            "-B",
            "-1",
            "dag_id",
            "task_id",
            "run_id",
        ],
    )
    def test_valid_flags_pass(self, flag: str):
        assert validate_flag(flag) is None

    @pytest.mark.parametrize(
        ("flag", "expected_substring"),
        [
            ("--dag_id", "must use strict kebab-case"),
            ("--dagId", "must use strict kebab-case"),
            ("--DAG-ID", "must use strict kebab-case"),
            ("--dag--id", "must use strict kebab-case"),
            ("--", "must use strict kebab-case"),
            ("-vv", "must be a single alphanumeric character"),
            ("-", "must be a single alphanumeric character"),
            ("dag-id", "must be a valid identifier"),
        ],
    )
    def test_invalid_flags_fail(self, flag: str, expected_substring: str):
        error = validate_flag(flag)
        assert error is not None
        assert expected_substring in error


class TestCheckCode:
    def test_valid_arg_definitions_pass(self):
        code = """
        ARG_VERBOSE = Arg(("-v", "--verbose"), help="Make logging verbose", action="store_true")
        ARG_DAG_ID = Arg(("dag_id",), help="The id of the dag")
        ARG_START_DATE = Arg(flags=("-s", "--start-date"), help="Start date override")
        """
        violations = check_code(code)
        assert violations == []

    def test_arg_with_positional_help_passes(self):
        code = """
        ARG_POOL = Arg(("--pool",), "Resource pool to use")
        """
        violations = check_code(code)
        assert violations == []

    def test_arg_missing_help_fails(self):
        code = """
        ARG_MISSING = Arg(("-m", "--missing"), action="store_true")
        """
        violations = check_code(code)
        assert len(violations) == 1
        assert "missing a required help docstring" in violations[0]

    def test_arg_empty_help_fails(self):
        code = """
        ARG_EMPTY_1 = Arg(("--flag1",), help="")
        ARG_EMPTY_2 = Arg(("--flag2",), help="   ")
        """
        violations = check_code(code)
        assert len(violations) == 2
        assert all("Help docstring cannot be empty or whitespace-only" in v for v in violations)

    def test_arg_invalid_flag_naming_fails(self):
        code = """
        ARG_BAD = Arg(("--bad_flag",), help="Bad flag naming")
        """
        violations = check_code(code)
        assert len(violations) == 1
        assert "must use strict kebab-case" in violations[0]

    def test_valid_commands_and_groups_pass(self):
        code = """
        ARG_OUTPUT = Arg(("-o", "--output"), help="Output format")
        ARG_VERBOSE = Arg(("-v", "--verbose"), help="Verbose output")

        CMD = ActionCommand(
            name="list-runs",
            help="List DAG runs",
            func=None,
            args=(ARG_OUTPUT, ARG_VERBOSE),
        )
        GRP = GroupCommand(
            name="dags",
            help="Manage DAGs",
            subcommands=(CMD,),
        )
        """
        violations = check_code(code)
        assert violations == []

    def test_command_with_invalid_name_fails(self):
        code = """
        CMD = ActionCommand(
            name="list_runs",
            help="List DAG runs",
            func=None,
            args=(),
        )
        """
        violations = check_code(code)
        assert len(violations) == 1
        assert "must use kebab-case" in violations[0]

    def test_command_missing_help_fails(self):
        code = """
        CMD = ActionCommand(
            name="test-cmd",
            func=None,
            args=(),
        )
        """
        violations = check_code(code)
        assert len(violations) == 1
        assert "is missing a required help docstring" in violations[0]

    def test_command_empty_help_fails(self):
        code = """
        CMD = ActionCommand(
            name="test-cmd",
            help="  ",
            func=None,
            args=(),
        )
        """
        violations = check_code(code)
        assert len(violations) == 1
        assert "has an empty help docstring" in violations[0]

    def test_action_command_duplicate_flags_fail(self):
        code = """
        ARG_A = Arg(("-v", "--verbose"), help="Verbose level 1")
        ARG_B = Arg(("-v", "--variant"), help="Variant flag")

        CMD = ActionCommand(
            name="sample",
            help="Sample command",
            func=None,
            args=(ARG_A, ARG_B),
        )
        """
        violations = check_code(code)
        assert len(violations) == 1
        assert "has duplicate flag '-v' defined 2 times" in violations[0]

    def test_syntax_error_reported_gracefully(self):
        code = "def incomplete_code("
        violations = check_code(code)
        assert len(violations) == 1
        assert "Syntax error" in violations[0]


class TestCheckFile:
    def test_check_file_with_fixture(self, write_python_file):
        path = write_python_file(
            """
            ARG_TEST = Arg(("--test-flag",), help="A valid test flag")
            """
        )
        violations = check_file(path)
        assert violations == []

    def test_check_file_nonexistent_returns_error(self, tmp_path):
        nonexistent = tmp_path / "does_not_exist.py"
        violations = check_file(nonexistent)
        assert len(violations) == 1
        assert "Failed to read file" in violations[0]


class TestFileIdentification:
    def test_is_cli_definition_file(self):
        assert is_cli_definition_file(Path("airflow-core/src/airflow/cli/cli_config.py"))
        assert is_cli_definition_file(Path("providers/celery/src/airflow/providers/celery/cli/definition.py"))
        assert not is_cli_definition_file(Path("airflow-core/src/airflow/models/dag.py"))
        assert not is_cli_definition_file(Path("scripts/ci/prek/check_cli_consistency.py"))
