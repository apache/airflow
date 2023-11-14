#!/usr/bin/env python
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

import argparse
import contextlib
import os
import re
import subprocess
import sys
import timeit
from collections import Counter
from importlib import reload
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from airflow.cli import cli_config, cli_parser
from airflow.cli.cli_config import ActionCommand, core_commands, lazy_load_command
from airflow.cli.utils import CliConflictError
from airflow.configuration import AIRFLOW_HOME
from airflow.executors.local_executor import LocalExecutor
from tests.test_utils.config import conf_vars

# Can not be `--snake_case` or contain uppercase letter
ILLEGAL_LONG_OPTION_PATTERN = re.compile("^--[a-z]+_[a-z]+|^--.*[A-Z].*")
# Only can be `-[a-z]` or `-[A-Z]`
LEGAL_SHORT_OPTION_PATTERN = re.compile("^-[a-zA-z]$")

cli_args = {k: v for k, v in cli_parser.__dict__.items() if k.startswith("ARG_")}


class TestCli:
    def test_arg_option_long_only(self):
        """
        Test if the name of cli.args long option valid
        """
        optional_long = [
            arg for arg in cli_args.values() if len(arg.flags) == 1 and arg.flags[0].startswith("-")
        ]
        for arg in optional_long:
            assert ILLEGAL_LONG_OPTION_PATTERN.match(arg.flags[0]) is None, f"{arg.flags[0]} is not match"

    def test_arg_option_mix_short_long(self):
        """
        Test if the name of cli.args mix option (-s, --long) valid
        """
        optional_mix = [
            arg for arg in cli_args.values() if len(arg.flags) == 2 and arg.flags[0].startswith("-")
        ]
        for arg in optional_mix:
            assert LEGAL_SHORT_OPTION_PATTERN.match(arg.flags[0]) is not None, f"{arg.flags[0]} is not match"
            assert ILLEGAL_LONG_OPTION_PATTERN.match(arg.flags[1]) is None, f"{arg.flags[1]} is not match"

    def test_subcommand_conflict(self):
        """
        Test if each of cli.*_COMMANDS without conflict subcommand
        """
        subcommand = {
            var: cli_parser.__dict__.get(var)
            for var in cli_parser.__dict__
            if var.isupper() and var.startswith("COMMANDS")
        }
        for group_name, sub in subcommand.items():
            name = [command.name.lower() for command in sub]
            assert len(name) == len(set(name)), f"Command group {group_name} have conflict subcommand"

    def test_subcommand_arg_name_conflict(self):
        """
        Test if each of cli.*_COMMANDS.arg name without conflict
        """
        subcommand = {
            var: cli_parser.__dict__.get(var)
            for var in cli_parser.__dict__
            if var.isupper() and var.startswith("COMMANDS")
        }
        for group, command in subcommand.items():
            for com in command:
                conflict_arg = [arg for arg, count in Counter(com.args).items() if count > 1]
                assert (
                    [] == conflict_arg
                ), f"Command group {group} function {com.name} have conflict args name {conflict_arg}"

    def test_subcommand_arg_flag_conflict(self):
        """
        Test if each of cli.*_COMMANDS.arg flags without conflict
        """
        subcommand = {
            key: val
            for key, val in cli_parser.__dict__.items()
            if key.isupper() and key.startswith("COMMANDS")
        }
        for group, command in subcommand.items():
            for com in command:
                position = [
                    a.flags[0] for a in com.args if (len(a.flags) == 1 and not a.flags[0].startswith("-"))
                ]
                conflict_position = [arg for arg, count in Counter(position).items() if count > 1]
                assert [] == conflict_position, (
                    f"Command group {group} function {com.name} have conflict "
                    f"position flags {conflict_position}"
                )

                long_option = [
                    a.flags[0] for a in com.args if (len(a.flags) == 1 and a.flags[0].startswith("-"))
                ] + [a.flags[1] for a in com.args if len(a.flags) == 2]
                conflict_long_option = [arg for arg, count in Counter(long_option).items() if count > 1]
                assert [] == conflict_long_option, (
                    f"Command group {group} function {com.name} have conflict "
                    f"long option flags {conflict_long_option}"
                )

                short_option = [a.flags[0] for a in com.args if len(a.flags) == 2]
                conflict_short_option = [arg for arg, count in Counter(short_option).items() if count > 1]
                assert [] == conflict_short_option, (
                    f"Command group {group} function {com.name} have conflict "
                    f"short option flags {conflict_short_option}"
                )

    @pytest.mark.db_test
    @patch.object(LocalExecutor, "get_cli_commands")
    def test_dynamic_conflict_detection(self, cli_commands_mock: MagicMock):
        core_commands.append(
            ActionCommand(
                name="test_command",
                help="does nothing",
                func=lambda: None,
                args=[],
            )
        )
        cli_commands_mock.return_value = [
            ActionCommand(
                name="test_command",
                help="just a command that'll conflict with one defined in core",
                func=lambda: None,
                args=[],
            )
        ]
        with pytest.raises(CliConflictError, match="test_command"):
            # force re-evaluation of cli commands (done in top level code)
            reload(cli_parser)

    def test_falsy_default_value(self):
        arg = cli_config.Arg(("--test",), default=0, type=int)
        parser = argparse.ArgumentParser()
        arg.add_to_parser(parser)

        args = parser.parse_args(["--test", "10"])
        assert args.test == 10

        args = parser.parse_args([])
        assert args.test == 0

    def test_commands_and_command_group_sections(self):
        parser = cli_parser.get_parser()

        with contextlib.redirect_stdout(StringIO()) as stdout:
            with pytest.raises(SystemExit):
                parser.parse_args(["--help"])
            stdout = stdout.getvalue()
        assert "Commands" in stdout
        assert "Groups" in stdout

    def test_dag_parser_commands_and_comamnd_group_sections(self):
        parser = cli_parser.get_parser(dag_parser=True)

        with contextlib.redirect_stdout(StringIO()) as stdout:
            with pytest.raises(SystemExit):
                parser.parse_args(["--help"])
            stdout = stdout.getvalue()
        assert "Commands" in stdout
        assert "Groups" in stdout

    def test_should_display_help(self):
        parser = cli_parser.get_parser()

        all_command_as_args = [
            command_as_args
            for top_command in cli_parser.airflow_commands
            for command_as_args in (
                [[top_command.name]]
                if isinstance(top_command, cli_parser.ActionCommand)
                else [[top_command.name, nested_command.name] for nested_command in top_command.subcommands]
            )
        ]
        for cmd_args in all_command_as_args:
            with pytest.raises(SystemExit):
                parser.parse_args([*cmd_args, "--help"])

    def test_dag_cli_should_display_help(self):
        parser = cli_parser.get_parser(dag_parser=True)

        all_command_as_args = [
            command_as_args
            for top_command in cli_config.dag_cli_commands
            for command_as_args in (
                [[top_command.name]]
                if isinstance(top_command, cli_parser.ActionCommand)
                else [[top_command.name, nested_command.name] for nested_command in top_command.subcommands]
            )
        ]
        for cmd_args in all_command_as_args:
            with pytest.raises(SystemExit):
                parser.parse_args([*cmd_args, "--help"])

    def test_positive_int(self):
        assert 1 == cli_config.positive_int(allow_zero=True)("1")
        assert 0 == cli_config.positive_int(allow_zero=True)("0")

        with pytest.raises(argparse.ArgumentTypeError):
            cli_config.positive_int(allow_zero=False)("0")
            cli_config.positive_int(allow_zero=True)("-1")

    @pytest.mark.parametrize(
        "command",
        [
            "celery",
            "kubernetes",
        ],
    )
    def test_executor_specific_commands_not_accessible(self, command):
        with conf_vars({("core", "executor"): "SequentialExecutor"}), contextlib.redirect_stderr(
            StringIO()
        ) as stderr:
            reload(cli_parser)
            parser = cli_parser.get_parser()
            with pytest.raises(SystemExit):
                parser.parse_args([command])
            stderr = stderr.getvalue()
        assert (f"airflow command error: argument GROUP_OR_COMMAND: invalid choice: '{command}'") in stderr

    @pytest.mark.parametrize(
        "executor,expected_args",
        [
            ("CeleryExecutor", ["celery"]),
            ("CeleryKubernetesExecutor", ["celery", "kubernetes"]),
            ("KubernetesExecutor", ["kubernetes"]),
            ("LocalExecutor", []),
            ("LocalKubernetesExecutor", ["kubernetes"]),
            ("SequentialExecutor", []),
            # custom executors are mapped to the regular ones in `conftest.py`
            ("custom_executor.CustomLocalExecutor", []),
            ("custom_executor.CustomLocalKubernetesExecutor", ["kubernetes"]),
            ("custom_executor.CustomCeleryExecutor", ["celery"]),
            ("custom_executor.CustomCeleryKubernetesExecutor", ["celery", "kubernetes"]),
            ("custom_executor.CustomKubernetesExecutor", ["kubernetes"]),
        ],
    )
    def test_cli_parser_executors(self, executor, expected_args):
        """Test that CLI commands for the configured executor are present"""
        for expected_arg in expected_args:
            with conf_vars({("core", "executor"): executor}), contextlib.redirect_stderr(
                StringIO()
            ) as stderr:
                reload(cli_parser)
                parser = cli_parser.get_parser()
                with pytest.raises(SystemExit) as e:  # running the help command exits, so we prevent that
                    parser.parse_args([expected_arg, "--help"])
                assert e.value.code == 0, stderr.getvalue()  # return code 0 == no problem
                stderr = stderr.getvalue()
                assert "airflow command error" not in stderr

    def test_non_existing_directory_raises_when_metavar_is_dir_for_db_export_cleaned(self):
        """Test that the error message is correct when the directory does not exist."""
        with contextlib.redirect_stderr(StringIO()) as stderr:
            with pytest.raises(SystemExit):
                parser = cli_parser.get_parser()
                parser.parse_args(["db", "export-archived", "--output-path", "/non/existing/directory"])
            error_msg = stderr.getvalue()

        assert error_msg == (
            "\nairflow db export-archived command error: The directory "
            "'/non/existing/directory' does not exist!, see help above.\n"
        )

    @pytest.mark.parametrize("export_format", ["json", "yaml", "unknown"])
    @patch("airflow.cli.cli_config.os.path.isdir", return_value=True)
    def test_invalid_choice_raises_for_export_format_in_db_export_archived_command(
        self, mock_isdir, export_format
    ):
        """Test that invalid choice raises for export-format in db export-cleaned command."""
        with contextlib.redirect_stderr(StringIO()) as stderr:
            with pytest.raises(SystemExit):
                parser = cli_parser.get_parser()
                parser.parse_args(
                    ["db", "export-archived", "--export-format", export_format, "--output-path", "mydir"]
                )
            error_msg = stderr.getvalue()
        assert error_msg == (
            "\nairflow db export-archived command error: argument "
            f"--export-format: invalid choice: '{export_format}' "
            "(choose from 'csv'), see help above.\n"
        )

    @pytest.mark.parametrize(
        "action_cmd",
        [
            ActionCommand(name="name", help="help", func=lazy_load_command(""), args=(), hide=True),
            ActionCommand(name="name", help="help", func=lazy_load_command(""), args=(), hide=False),
        ],
    )
    @patch("argparse._SubParsersAction")
    def test_add_command_with_hide(self, mock_subparser_actions, action_cmd):
        cli_parser._add_command(mock_subparser_actions, action_cmd)
        if action_cmd.hide:
            mock_subparser_actions.add_parser.assert_called_once_with(
                action_cmd.name, epilog=action_cmd.epilog
            )
        else:
            mock_subparser_actions.add_parser.assert_called_once_with(
                action_cmd.name, help=action_cmd.help, description=action_cmd.help, epilog=action_cmd.epilog
            )


# We need to run it from sources with PYTHONPATH, not command line tool,
# because we need to make sure that we have providers configured from source provider.yaml files

CONFIG_FILE = Path(AIRFLOW_HOME) / "airflow.cfg"


class TestCliSubprocess:
    """
    We need to run it from sources using "__main__" and setting the PYTHONPATH, not command line tool,
    because we need to make sure that we have providers loaded from source provider.yaml files rather
    than from provider packages which might not be installed in the test environment.
    """

    @pytest.mark.quarantined
    def test_cli_run_time(self):
        setup_code = "import subprocess"
        command = [sys.executable, "-m", "airflow", "--help"]
        env = {"PYTHONPATH": os.pathsep.join(sys.path)}
        timing_code = f"subprocess.run({command},env={env})"
        # Limit the number of samples otherwise the test will take a very long time
        num_samples = 3
        threshold = 3.5
        timing_result = timeit.timeit(stmt=timing_code, number=num_samples, setup=setup_code) / num_samples
        # Average run time of Airflow CLI should at least be within 3.5s
        assert timing_result < threshold

    def test_cli_parsing_does_not_initialize_providers_manager(self):
        """Test that CLI parsing does not initialize providers manager.

        This test is here to make sure that we do not initialize providers manager - it is run as a
        separate subprocess, to make sure we do not have providers manager initialized in the main
        process from other tests.
        """
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.touch(exist_ok=True)
        result = subprocess.run(
            [sys.executable, "-m", "airflow", "providers", "lazy-loaded"],
            env={"PYTHONPATH": os.pathsep.join(sys.path)},
            check=False,
            text=True,
        )
        assert result.returncode == 0

    def test_airflow_config_contains_providers(self):
        """Test that airflow config has providers included by default.

        This test is run as a separate subprocess, to make sure we do not have providers manager
        initialized in the main process from other tests.
        """
        CONFIG_FILE.unlink(missing_ok=True)
        result = subprocess.run(
            [sys.executable, "-m", "airflow", "config", "list"],
            env={"PYTHONPATH": os.pathsep.join(sys.path)},
            check=False,
            text=True,
        )
        assert result.returncode == 0
        assert CONFIG_FILE.exists()
        assert "celery_config_options" in CONFIG_FILE.read_text()

    def test_airflow_config_output_contains_providers_by_default(self):
        """Test that airflow config has providers excluded in config list when asked for it."""
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.touch(exist_ok=True)

        result = subprocess.run(
            [sys.executable, "-m", "airflow", "config", "list"],
            env={"PYTHONPATH": os.pathsep.join(sys.path)},
            check=False,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0
        assert "celery_config_options" in result.stdout

    def test_airflow_config_output_does_not_contain_providers_when_excluded(self):
        """Test that airflow config has providers excluded in config list when asked for it."""
        CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.unlink(missing_ok=True)
        CONFIG_FILE.touch(exist_ok=True)
        result = subprocess.run(
            [sys.executable, "-m", "airflow", "config", "list", "--exclude-providers"],
            env={"PYTHONPATH": os.pathsep.join(sys.path)},
            check=False,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0
        assert "celery_config_options" not in result.stdout
