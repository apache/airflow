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
"""Unit tests for the registry extract-data command."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.registry_commands import DEV_REGISTRY_DIR, extract_data

MODULE = "airflow_breeze.commands.registry_commands"
METADATA_CMD = [
    "uv",
    "run",
    "--project",
    str(DEV_REGISTRY_DIR),
    "python",
    str(DEV_REGISTRY_DIR / "extract_metadata.py"),
]


@pytest.fixture
def mocks():
    with (
        patch(
            f"{MODULE}.run_command",
            autospec=True,
            return_value=subprocess.CompletedProcess(args=[], returncode=0),
        ) as run_command,
        patch(f"{MODULE}.build_ci_image_if_needed", autospec=True),
        patch(f"{MODULE}._get_suspended_provider_packages", autospec=True, return_value=[]),
        patch(
            f"{MODULE}.execute_command_in_shell",
            autospec=True,
            return_value=subprocess.CompletedProcess(args=[], returncode=0),
        ) as execute_command_in_shell,
        patch(f"{MODULE}.bring_compose_project_down", autospec=True),
        patch(f"{MODULE}.remove_docker_networks", autospec=True),
        patch(f"{MODULE}.fix_ownership_using_docker", autospec=True),
    ):
        yield run_command, execute_command_in_shell


def test_runs_extract_metadata_on_the_host_where_git_tags_are_visible(mocks):
    run_command, execute_command_in_shell = mocks

    result = CliRunner().invoke(extract_data, ["--provider", "amazon google"])

    assert result.exit_code == 0, result.output
    assert run_command.call_args.args[0] == [*METADATA_CMD, "--provider", "amazon google"]
    container_command = execute_command_in_shell.call_args.kwargs["command"]
    assert "extract_metadata.py" not in container_command
    assert "python dev/registry/extract_parameters.py --provider 'amazon google' && " in container_command


def test_allow_unreleased_is_forwarded_to_extract_metadata(mocks):
    run_command, _ = mocks

    result = CliRunner().invoke(extract_data, ["--allow-unreleased"])

    assert result.exit_code == 0, result.output
    assert run_command.call_args.args[0] == [*METADATA_CMD, "--allow-unreleased"]


def test_stops_before_the_container_when_extract_metadata_fails(mocks):
    run_command, execute_command_in_shell = mocks
    run_command.return_value = subprocess.CompletedProcess(args=[], returncode=3)

    result = CliRunner().invoke(extract_data, [])

    assert result.exit_code == 3
    execute_command_in_shell.assert_not_called()
