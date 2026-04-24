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

from unittest import mock

import pytest

from airflow_breeze.utils.environment_check import check_uv_version

# Kept in sync with `[tool.uv] required-version` in the root pyproject.toml by the
# `sync-uv-min-version-markers` prek hook. Any line tagged with the
# `# sync-uv-min-version` marker is auto-rewritten when required-version is bumped
# manually. Tests that exercise the success path set both the mocked floor AND the
# mocked "actual uv" to this value so the ok-case keeps passing after a bump.
_MIN_UV = "0.9.17"  # sync-uv-min-version


@mock.patch("airflow_breeze.utils.environment_check._read_required_uv_version")
@mock.patch("airflow_breeze.utils.environment_check.run_command")
@mock.patch("airflow_breeze.utils.environment_check.console_print")
def test_check_uv_version_ok(mock_console_print, mock_run_command, mock_required_version):
    mock_required_version.return_value = _MIN_UV
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = f"uv {_MIN_UV} (abc 2026-03-26 aarch64-apple-darwin)"
    check_uv_version()
    mock_run_command.assert_called_with(
        ["uv", "--version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_console_print.assert_called_with(f"[success]Good version of uv: {_MIN_UV}.[/]")


@mock.patch("airflow_breeze.utils.environment_check._read_required_uv_version")
@mock.patch("airflow_breeze.utils.environment_check.run_command")
@mock.patch("airflow_breeze.utils.environment_check.console_print")
def test_check_uv_version_too_low(mock_console_print, mock_run_command, mock_required_version):
    mock_required_version.return_value = _MIN_UV
    mock_run_command.return_value.returncode = 0
    # Deliberately far below any plausible `required-version` so this test stays valid
    # without needing the sync marker.
    mock_run_command.return_value.stdout = "uv 0.0.1 (fake)"
    with pytest.raises(SystemExit) as e:
        check_uv_version()
    assert e.value.code == 1


@mock.patch("airflow_breeze.utils.environment_check._read_required_uv_version")
@mock.patch("airflow_breeze.utils.environment_check.run_command")
@mock.patch("airflow_breeze.utils.environment_check.console_print")
def test_check_uv_version_not_installed(mock_console_print, mock_run_command, mock_required_version):
    mock_required_version.return_value = _MIN_UV
    mock_run_command.return_value.returncode = 1
    mock_run_command.return_value.stdout = ""
    with pytest.raises(SystemExit) as e:
        check_uv_version()
    assert e.value.code == 1


@mock.patch("airflow_breeze.utils.environment_check._read_required_uv_version")
@mock.patch("airflow_breeze.utils.environment_check.run_command")
@mock.patch("airflow_breeze.utils.environment_check.console_print")
def test_check_uv_version_missing_declaration_skips(
    mock_console_print, mock_run_command, mock_required_version
):
    mock_required_version.return_value = None
    check_uv_version()
    mock_run_command.assert_not_called()
