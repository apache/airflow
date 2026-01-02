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
import logging
import os
import re
import subprocess
import sys
import timeit
from collections import Counter
from collections.abc import Callable
from importlib import reload
from io import StringIO
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from airflow.cli import cli_config, cli_parser
from airflow.cli.cli_config import ActionCommand, core_commands, lazy_load_command
from airflow.cli.utils import CliConflictError
from airflow.configuration import AIRFLOW_HOME
from airflow.executors import executor_loader

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

# Can not be `--snake_case` or contain uppercase letter
ILLEGAL_LONG_OPTION_PATTERN = re.compile("^--[a-z]+_[a-z]+|^--.*[A-Z].*")
# Only can be `-[a-z]` or `-[A-Z]`
LEGAL_SHORT_OPTION_PATTERN = re.compile("^-[a-zA-Z]$")

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
                assert conflict_arg == [], (
                    f"Command group {group} function {com.name} have conflict args name {conflict_arg}"
                )

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
                assert conflict_position == [], (
                    f"Command group {group} function {com.name} have conflict "
                    f"position flags {conflict_position}"
                )

                long_option = [
                    a.flags[0] for a in com.args if (len(a.flags) == 1 and a.flags[0].startswith("-"))
                ] + [a.flags[1] for a in com.args if len(a.flags) == 2]
                conflict_long_option = [arg for arg, count in Counter(long_option).items() if count > 1]
                assert conflict_long_option == [], (
                    f"Command group {group} function {com.name} have conflict "
                    f"long option flags {conflict_long_option}"
                )

                short_option = [a.flags[0] for a in com.args if len(a.flags) == 2]
                conflict_short_option = [arg for arg, count in Counter(short_option).items() if count > 1]
                assert conflict_short_option == [], (
                    f"Command group {group} function {com.name} have conflict "
                    f"short option flags {conflict_short_option}"
                )

    @staticmethod
    def mock_duplicate_command():
        return [
            ActionCommand(
                name="test_command",
                help="does nothing",
                func=lambda: None,
                args=[],
            ),
            ActionCommand(
                name="test_command",
                help="just a command that'll conflict with the other one",
                func=lambda: None,
                args=[],
            ),
        ]

    @patch(
        "airflow.providers_manager.ProvidersManager.cli_command_functions",
        new_callable=mock.PropertyMock,
    )
    def test_dynamic_conflict_detection(self, mock_cli_command_functions: MagicMock):
        mock_cli_command_functions.return_value = [self.mock_duplicate_command]

        test_command = ActionCommand(
            name="test_command",
            help="does nothing",
            func=lambda: None,
            args=[],
        )
        core_commands.append(test_command)

        with pytest.raises(CliConflictError, match="test_command"):
            # force re-evaluation of cli commands (done in top level code)
            reload(cli_parser)

    @pytest.mark.parametrize(
        "module_pattern",
        ["airflow.auth.managers", "airflow.executors.executor_loader"],
    )
    def test_should_not_import_in_cli_parser(self, module_pattern: str):
        """Test that cli_parser does not import auth_managers or executor_loader at import time."""
        # Remove the module from sys.modules if present to force a fresh import
        import sys

        modules_to_remove = [mod for mod in sys.modules.keys() if module_pattern in mod]
        removed_modules = {}
        for mod in modules_to_remove:
            removed_modules[mod] = sys.modules.pop(mod)

        try:
            reload(cli_parser)
            # Check that the module pattern is not in sys.modules after reload
            loaded_modules = list(sys.modules.keys())
            matching_modules = [mod for mod in loaded_modules if module_pattern in mod]
            assert not matching_modules, (
                f"Module pattern '{module_pattern}' found in sys.modules: {matching_modules}"
            )
        finally:
            # Restore removed modules
            sys.modules.update(removed_modules)

    def test_hybrid_executor_get_cli_commands(self):
        """Test that if multiple executors are configured, then every executor loads its commands."""

        expected_commands = ["celery", "kubernetes", "edge"]
        reload(cli_parser)
        commands = [command.name for command in cli_parser.airflow_commands]
        for executor_command in expected_commands:
            assert executor_command in commands

    @pytest.mark.parametrize(
        (
            "cli_command_functions",
            "cli_command_providers",
            "executor_without_check",
            "expected_loaded_executors",
        ),
        [
            pytest.param(
                [],
                set(),
                {
                    ("path.to.KubernetesExecutor", "apache-airflow-providers-cncf-kubernetes"),
                },
                ["path.to.KubernetesExecutor"],
                id="empty cli section should load all the executors by ExecutorLoader",
            ),
            pytest.param(
                [lambda: [ActionCommand(name="celery", help="", func=lambda: None, args=[])]],
                {"apache-airflow-providers-celery"},
                {
                    ("path.to.CeleryExecutor", "apache-airflow-providers-celery"),
                    ("path.to.KubernetesExecutor", "apache-airflow-providers-cncf-kubernetes"),
                },
                ["path.to.KubernetesExecutor"],
                id="only partial executor define cli section in provider info, should load the rest by ExecutorLoader",
            ),
            pytest.param(
                [
                    lambda: [ActionCommand(name="celery", help="", func=lambda: None, args=[])],
                    lambda: [ActionCommand(name="kubernetes", help="", func=lambda: None, args=[])],
                ],
                {"apache-airflow-providers-celery", "apache-airflow-providers-cncf-kubernetes"},
                {
                    ("path.to.CeleryExecutor", "apache-airflow-providers-celery"),
                    ("path.to.KubernetesExecutor", "apache-airflow-providers-cncf-kubernetes"),
                },
                [],
                id="all executors define cli section in provider info, should not load any by ExecutorLoader",
            ),
        ],
    )
    @patch("airflow.executors.executor_loader.ExecutorLoader.import_executor_cls")
    @patch(
        "airflow.executors.executor_loader.ExecutorLoader.get_executor_names",
    )
    @patch(
        "airflow.providers_manager.ProvidersManager.executor_without_check",
        new_callable=mock.PropertyMock,
    )
    @patch(
        "airflow.providers_manager.ProvidersManager.cli_command_providers",
        new_callable=mock.PropertyMock,
    )
    @patch(
        "airflow.providers_manager.ProvidersManager.cli_command_functions",
        new_callable=mock.PropertyMock,
    )
    def test_compat_cli_loading_for_executors_commands(
        self,
        mock_cli_command_functions: MagicMock,
        mock_cli_command_providers: MagicMock,
        mock_executor_without_check: MagicMock,
        mock_get_executor_names: MagicMock,
        mock_import_executor_cls: MagicMock,
        cli_command_functions: list[Callable[[], list[ActionCommand | cli_parser.GroupCommand]]],
        cli_command_providers: set[str],
        executor_without_check: set[tuple[str, str]],
        expected_loaded_executors: list[str],
        caplog,
    ):
        # Create mock ExecutorName objects
        mock_executor_names = [
            MagicMock(name=executor_name.split(".")[-1], module_path=executor_name)
            for executor_name, _ in executor_without_check
        ]

        # Create mock executor classes that return empty command lists
        mock_executor_instance = MagicMock()
        mock_executor_instance.get_cli_commands.return_value = []
        mock_import_executor_cls.return_value = (mock_executor_instance, None)

        # mock
        mock_cli_command_functions.return_value = cli_command_functions
        mock_cli_command_providers.return_value = cli_command_providers
        mock_executor_without_check.return_value = executor_without_check
        mock_get_executor_names.return_value = mock_executor_names

        # act
        with caplog.at_level(logging.WARNING, logger="airflow.cli.cli_parser"):
            reload(cli_parser)

        # assert
        expected_warning = "Please define the 'cli' section in the 'get_provider_info' for custom executors to avoid this warning."
        if expected_loaded_executors:
            assert expected_warning in caplog.text
            for executor_path in expected_loaded_executors:
                assert executor_path in caplog.text
        else:
            assert expected_warning not in caplog.text

        # Verify import_executor_cls was called with correct ExecutorName objects
        if expected_loaded_executors:
            expected_calls = [
                mock.call(executor_name)
                for executor_name in mock_executor_names
                if executor_name.module_path in expected_loaded_executors
            ]
            mock_import_executor_cls.assert_has_calls(expected_calls, any_order=True)
        else:
            mock_import_executor_cls.assert_not_called()

    @pytest.mark.parametrize(
        (
            "cli_command_functions",
            "cli_command_providers",
            "auth_manager_without_check",
            "auth_manager_cls_path",
            "expected_loaded",
        ),
        [
            pytest.param(
                [],
                set(),
                {
                    (
                        "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
                        "apache-airflow-providers-fab",
                    ),
                },
                "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
                True,
                id="empty cli section should load auth manager",
            ),
            pytest.param(
                [lambda: [ActionCommand(name="fab", help="", func=lambda: None, args=[])]],
                {"apache-airflow-providers-fab"},
                {
                    (
                        "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
                        "apache-airflow-providers-fab",
                    ),
                },
                "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
                False,
                id="auth manager with cli section should not load by import_string",
            ),
            pytest.param(
                [],
                set(),
                {
                    (
                        "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
                        "apache-airflow-providers-amazon",
                    ),
                    (
                        "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
                        "apache-airflow-providers-fab",
                    ),
                },
                "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
                True,
                id="only configured auth manager should be loaded",
            ),
        ],
    )
    @patch(
        "airflow.providers_manager.ProvidersManager.cli_command_functions",
        new_callable=mock.PropertyMock,
    )
    @patch(
        "airflow.providers_manager.ProvidersManager.cli_command_providers",
        new_callable=mock.PropertyMock,
    )
    @patch(
        "airflow.providers_manager.ProvidersManager.auth_manager_without_check",
        new_callable=mock.PropertyMock,
    )
    @patch("airflow.configuration.conf.get")
    @patch("airflow._shared.module_loading.import_string")
    def test_compat_cli_loading_for_auth_manager_commands(
        self,
        mock_import_string: MagicMock,
        mock_conf_get: MagicMock,
        mock_auth_manager_without_check: MagicMock,
        mock_cli_command_providers: MagicMock,
        mock_cli_command_functions: MagicMock,
        cli_command_functions: list[Callable[[], list[ActionCommand | cli_parser.GroupCommand]]],
        cli_command_providers: set[str],
        auth_manager_without_check: set[tuple[str, str]],
        auth_manager_cls_path: str,
        expected_loaded: bool,
        caplog,
    ):
        # Create mock auth manager instance that returns empty command lists
        mock_auth_manager_instance = MagicMock()
        mock_auth_manager_instance.get_cli_commands.return_value = []
        mock_auth_manager_cls = MagicMock(return_value=mock_auth_manager_instance)
        mock_import_string.return_value = mock_auth_manager_cls

        # Mock configuration
        mock_conf_get.return_value = auth_manager_cls_path

        # mock providers manager
        mock_cli_command_functions.return_value = cli_command_functions
        mock_cli_command_providers.return_value = cli_command_providers
        mock_auth_manager_without_check.return_value = auth_manager_without_check

        # act
        with caplog.at_level(logging.WARNING, logger="airflow.cli.cli_parser"):
            reload(cli_parser)

        # assert
        expected_warning = "Please define the 'cli' section in the provider.yaml for custom auth managers to avoid this warning."
        if expected_loaded:
            assert expected_warning in caplog.text
            assert auth_manager_cls_path in caplog.text
            mock_import_string.assert_called_once_with(auth_manager_cls_path)
            mock_auth_manager_cls.assert_called_once()
            mock_auth_manager_instance.get_cli_commands.assert_called_once()
        else:
            if auth_manager_cls_path in [path for path, _ in auth_manager_without_check]:
                # Auth manager is in the without_check but also in cli_providers, so warning should appear
                # but import_string should NOT be called
                assert expected_warning not in caplog.text
                mock_import_string.assert_not_called()
            else:
                # Auth manager is not in the without_check, no warning
                mock_import_string.assert_not_called()

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
            stdout_val = stdout.getvalue()
        assert "Commands" in stdout_val
        assert "Groups" in stdout_val

    def test_dag_parser_commands_and_comamnd_group_sections(self):
        parser = cli_parser.get_parser(dag_parser=True)

        with contextlib.redirect_stdout(StringIO()) as stdout:
            with pytest.raises(SystemExit):
                parser.parse_args(["--help"])
            stdout_val = stdout.getvalue()
        assert "Commands" in stdout_val
        assert "Groups" in stdout_val

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
        assert cli_config.positive_int(allow_zero=True)("1") == 1
        assert cli_config.positive_int(allow_zero=True)("0") == 0

        with pytest.raises(argparse.ArgumentTypeError):
            cli_config.positive_int(allow_zero=False)("0")
        with pytest.raises(argparse.ArgumentTypeError):
            cli_config.positive_int(allow_zero=True)("-1")

    def test_variables_import_help_message_consistency(self):
        """
        Test that ARG_VAR_IMPORT help message accurately reflects supported file formats.

        This test ensures that when new file formats are added to FILE_PARSERS,
        developers remember to update the CLI help message accordingly.
        """
        from airflow.cli.cli_config import ARG_VAR_IMPORT
        from airflow.secrets.local_filesystem import FILE_PARSERS

        # Get actually supported formats
        supported_formats = set(FILE_PARSERS.keys())

        # Check each supported format is mentioned in help as .ext
        help_text = ARG_VAR_IMPORT.kwargs["help"].lower()
        missing_in_help = {fmt for fmt in supported_formats if f".{fmt}" not in help_text}

        assert not missing_in_help, (
            f"CLI help message for 'airflow variables import' is missing supported formats.\n"
            f"Supported formats: {sorted(supported_formats)}\n"
            f"Missing from help: {sorted(missing_in_help)}\n"
            f"Help message: '{ARG_VAR_IMPORT.kwargs['help']}'\n"
            f"Please update ARG_VAR_IMPORT help message in cli_config.py to include: {', '.join([f'.{fmt}' for fmt in sorted(missing_in_help)])}"
        )

    @pytest.mark.parametrize(
        ("executor", "expected_args"),
        [
            ("CeleryExecutor", ["celery"]),
            ("KubernetesExecutor", ["kubernetes"]),
            ("LocalExecutor", []),
            # custom executors are mapped to the regular ones in `conftest.py`
            ("custom_executor.CustomLocalExecutor", []),
            ("custom_executor.CustomCeleryExecutor", ["celery"]),
            ("custom_executor.CustomKubernetesExecutor", ["kubernetes"]),
        ],
    )
    def test_cli_parser_executors(self, executor, expected_args):
        """Test that CLI commands for the configured executor are present"""
        for expected_arg in expected_args:
            with (
                conf_vars({("core", "executor"): executor}),
                contextlib.redirect_stderr(StringIO()) as stderr,
            ):
                reload(executor_loader)
                reload(cli_parser)
                parser = cli_parser.get_parser()
                with pytest.raises(SystemExit) as e:  # running the help command exits, so we prevent that
                    parser.parse_args([expected_arg, "--help"])
                assert e.value.code == 0, stderr.getvalue()  # return code 0 == no problem
                stderr_val = stderr.getvalue()
                assert "airflow command error" not in stderr_val

    def test_non_existing_directory_raises_when_metavar_is_dir_for_db_export_cleaned(self):
        """Test that the error message is correct when the directory does not exist."""
        with contextlib.redirect_stderr(StringIO()) as stderr:
            parser = cli_parser.get_parser()
            with pytest.raises(SystemExit):
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
            parser = cli_parser.get_parser()
            with pytest.raises(SystemExit):
                parser.parse_args(
                    ["db", "export-archived", "--export-format", export_format, "--output-path", "mydir"]
                )
            error_msg = stderr.getvalue()
        assert (
            "airflow db export-archived command error: argument --export-format: invalid choice" in error_msg
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
    than from provider distributions which might not be installed in the test environment.
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

    def test_airflow_config_contains_providers(self):
        """
        Test that airflow config has providers included by default.

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
