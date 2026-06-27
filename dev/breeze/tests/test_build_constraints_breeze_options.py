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
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from click.testing import CliRunner

import airflow_breeze.commands.ci_image_commands as ci_image_commands
import airflow_breeze.commands.developer_commands as developer_commands
import airflow_breeze.commands.production_image_commands as production_image_commands
import airflow_breeze.commands.release_management_commands as release_management_commands
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.shell_params import ShellParams

if TYPE_CHECKING:
    import click

BUILD_CONSTRAINTS_LOCATION = "/tmp/build-constraints.txt"


def _has_build_arg(args: list[str], name: str, value: str) -> bool:
    return ["--build-arg", f"{name}={value}"] in [args[index : index + 2] for index in range(len(args))]


def test_shell_params_exports_build_constraints_location_only_when_configured(monkeypatch):
    monkeypatch.setattr(os, "environ", {})
    assert "AIRFLOW_BUILD_CONSTRAINTS_LOCATION" not in ShellParams().env_variables_for_docker_commands

    env = ShellParams(
        airflow_build_constraints_location=BUILD_CONSTRAINTS_LOCATION
    ).env_variables_for_docker_commands

    assert env["AIRFLOW_BUILD_CONSTRAINTS_LOCATION"] == BUILD_CONSTRAINTS_LOCATION


def test_shell_params_reads_build_constraints_location_from_environment(monkeypatch):
    monkeypatch.setattr(os, "environ", {"AIRFLOW_BUILD_CONSTRAINTS_LOCATION": BUILD_CONSTRAINTS_LOCATION})
    env = ShellParams().env_variables_for_docker_commands

    assert env["AIRFLOW_BUILD_CONSTRAINTS_LOCATION"] == BUILD_CONSTRAINTS_LOCATION


@pytest.mark.parametrize("params_class", [BuildCiParams, BuildProdParams])
def test_common_build_params_emit_build_constraints_location_when_configured(params_class):
    args = params_class(
        airflow_build_constraints_location=BUILD_CONSTRAINTS_LOCATION
    ).prepare_arguments_for_docker_build_command()

    assert _has_build_arg(args, "AIRFLOW_BUILD_CONSTRAINTS_LOCATION", BUILD_CONSTRAINTS_LOCATION)


@pytest.mark.parametrize("params_class", [BuildCiParams, BuildProdParams])
def test_common_build_params_omit_build_constraints_location_when_unset(params_class):
    args = params_class().prepare_arguments_for_docker_build_command()

    assert not any(arg.startswith("AIRFLOW_BUILD_CONSTRAINTS_LOCATION=") for arg in args)


def test_existing_airflow_constraints_location_build_arg_is_unchanged():
    args = BuildCiParams(
        airflow_build_constraints_location=BUILD_CONSTRAINTS_LOCATION,
        airflow_constraints_location="/tmp/runtime-constraints.txt",
    ).prepare_arguments_for_docker_build_command()

    assert _has_build_arg(args, "AIRFLOW_BUILD_CONSTRAINTS_LOCATION", BUILD_CONSTRAINTS_LOCATION)
    assert _has_build_arg(args, "AIRFLOW_CONSTRAINTS_LOCATION", "/tmp/runtime-constraints.txt")


@pytest.mark.parametrize(
    "command",
    [
        ci_image_commands.build,
        production_image_commands.build,
        developer_commands.shell,
        developer_commands.start_airflow,
        release_management_commands.install_provider_distributions,
        release_management_commands.verify_provider_distributions,
    ],
)
def test_commands_expose_build_constraints_location_option(command: click.Command):
    options = {option for parameter in command.params for option in getattr(parameter, "opts", ())}

    assert "--airflow-build-constraints-location" in options


@mock.patch("airflow_breeze.commands.ci_image_commands.run_build_ci_image", autospec=True)
@mock.patch("airflow_breeze.commands.ci_image_commands.prepare_for_building_ci_image", autospec=True)
@mock.patch("airflow_breeze.commands.ci_image_commands.fix_group_permissions", autospec=True)
@mock.patch("airflow_breeze.commands.ci_image_commands.check_remote_ghcr_io_commands", autospec=True)
@mock.patch("airflow_breeze.commands.ci_image_commands.perform_environment_checks", autospec=True)
def test_ci_image_build_forwards_build_constraints_location(
    perform_environment_checks,
    check_remote_ghcr_io_commands,
    fix_group_permissions,
    prepare_for_building_ci_image,
    run_build_ci_image,
):
    run_build_ci_image.return_value = (0, "")

    result = CliRunner().invoke(
        ci_image_commands.build,
        ["--airflow-build-constraints-location", BUILD_CONSTRAINTS_LOCATION],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    build_params = run_build_ci_image.call_args.kwargs["ci_image_params"]
    assert build_params.airflow_build_constraints_location == BUILD_CONSTRAINTS_LOCATION


@mock.patch("airflow_breeze.commands.production_image_commands.run_build_production_image", autospec=True)
@mock.patch(
    "airflow_breeze.commands.production_image_commands.prepare_for_building_prod_image", autospec=True
)
@mock.patch("airflow_breeze.commands.production_image_commands.fix_group_permissions", autospec=True)
@mock.patch("airflow_breeze.commands.production_image_commands.check_remote_ghcr_io_commands", autospec=True)
@mock.patch("airflow_breeze.commands.production_image_commands.perform_environment_checks", autospec=True)
def test_prod_image_build_forwards_build_constraints_location(
    perform_environment_checks,
    check_remote_ghcr_io_commands,
    fix_group_permissions,
    prepare_for_building_prod_image,
    run_build_production_image,
):
    run_build_production_image.return_value = (0, "")

    result = CliRunner().invoke(
        production_image_commands.build,
        [
            "--airflow-build-constraints-location",
            BUILD_CONSTRAINTS_LOCATION,
            "--installation-method",
            "apache-airflow",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    build_params = run_build_production_image.call_args.kwargs["prod_image_params"]
    assert build_params.airflow_build_constraints_location == BUILD_CONSTRAINTS_LOCATION


@pytest.mark.parametrize(
    ("command", "extra_args"),
    [
        (developer_commands.shell, []),
        (developer_commands.start_airflow, ["--skip-assets-compilation"]),
    ],
)
def test_developer_commands_forward_build_constraints_location(
    monkeypatch, command: click.Command, extra_args: list[str]
):
    entered_shell = mock.create_autospec(
        developer_commands.enter_shell, return_value=SimpleNamespace(returncode=0)
    )
    rebuild_or_pull = mock.create_autospec(developer_commands.rebuild_or_pull_ci_image_if_needed)
    monkeypatch.setattr(
        developer_commands,
        "perform_environment_checks",
        mock.create_autospec(developer_commands.perform_environment_checks),
    )
    monkeypatch.setattr(developer_commands, "rebuild_or_pull_ci_image_if_needed", rebuild_or_pull)
    monkeypatch.setattr(developer_commands, "enter_shell", entered_shell)
    monkeypatch.setattr(
        developer_commands,
        "fix_ownership_using_docker",
        mock.create_autospec(developer_commands.fix_ownership_using_docker),
    )
    monkeypatch.setattr(
        developer_commands,
        "assert_prek_installed",
        mock.create_autospec(developer_commands.assert_prek_installed),
    )
    monkeypatch.setattr(
        developer_commands,
        "run_compile_ui_assets",
        mock.create_autospec(developer_commands.run_compile_ui_assets),
    )

    result = CliRunner().invoke(
        command,
        ["--airflow-build-constraints-location", BUILD_CONSTRAINTS_LOCATION, *extra_args],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    shell_params = rebuild_or_pull.call_args.kwargs["command_params"]
    assert shell_params.airflow_build_constraints_location == BUILD_CONSTRAINTS_LOCATION


@pytest.mark.parametrize(
    "command",
    [
        release_management_commands.install_provider_distributions,
        release_management_commands.verify_provider_distributions,
    ],
)
def test_release_management_commands_forward_build_constraints_location(monkeypatch, command: click.Command):
    execute_command = mock.create_autospec(
        release_management_commands.execute_command_in_shell, return_value=SimpleNamespace(returncode=0)
    )
    monkeypatch.setattr(
        release_management_commands,
        "perform_environment_checks",
        mock.create_autospec(release_management_commands.perform_environment_checks),
    )
    monkeypatch.setattr(
        release_management_commands,
        "fix_ownership_using_docker",
        mock.create_autospec(release_management_commands.fix_ownership_using_docker),
    )
    monkeypatch.setattr(
        release_management_commands,
        "cleanup_python_generated_files",
        mock.create_autospec(release_management_commands.cleanup_python_generated_files),
    )
    monkeypatch.setattr(
        release_management_commands,
        "rebuild_or_pull_ci_image_if_needed",
        mock.create_autospec(release_management_commands.rebuild_or_pull_ci_image_if_needed),
    )
    monkeypatch.setattr(release_management_commands, "execute_command_in_shell", execute_command)

    result = CliRunner().invoke(
        command,
        ["--airflow-build-constraints-location", BUILD_CONSTRAINTS_LOCATION],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    shell_params = execute_command.call_args.args[0]
    assert shell_params.airflow_build_constraints_location == BUILD_CONSTRAINTS_LOCATION
