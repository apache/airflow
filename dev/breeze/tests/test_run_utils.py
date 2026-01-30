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

import stat
from unittest import mock

from airflow_breeze.utils.run_utils import (
    change_directory_permission,
    change_file_permission,
    check_if_buildx_plugin_installed,
)


def test_change_file_permission(tmp_path):
    tmpfile = tmp_path / "test.config"
    tmpfile.write_text("content")
    change_file_permission(tmpfile)
    mode = tmpfile.stat().st_mode
    assert not (mode & stat.S_IWGRP)
    assert not (mode & stat.S_IWOTH)


def test_change_directory_permission(tmp_path):
    subdir = tmp_path / "testdir"
    subdir.mkdir()
    change_directory_permission(subdir)
    mode = subdir.stat().st_mode
    assert not (mode & stat.S_IWGRP)
    assert not (mode & stat.S_IWOTH)
    assert mode & stat.S_IXGRP
    assert mode & stat.S_IXOTH


@mock.patch("airflow_breeze.utils.run_utils.run_command")
@mock.patch("airflow_breeze.utils.run_utils.get_console")
def test_check_buildah_is_installed(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "buildah 1.33.7"
    assert check_if_buildx_plugin_installed() is False
    mock_run_command.assert_called_with(
        ["docker", "buildx", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_get_console.return_value.print.assert_called_with(
        "[warning]Detected buildah installation.[/]\n"
        "[warning]The Dockerfiles are only compatible with BuildKit.[/]\n"
        "[warning]Please see the syntax declaration at the top of the Dockerfiles for BuildKit version\n"
    )


@mock.patch("airflow_breeze.utils.run_utils.run_command")
@mock.patch("airflow_breeze.utils.run_utils.get_console")
def test_check_buildkit_is_installed(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "github.com/docker/buildx v0.29.1-desktop.1"
    assert check_if_buildx_plugin_installed() is True
    mock_run_command.assert_called_with(
        ["docker", "buildx", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_get_console.return_value.print.assert_called_with(
        "[success]Docker BuildKit is installed and will be used for the image build.[/]\n"
    )


@mock.patch("airflow_breeze.utils.run_utils.run_command")
@mock.patch("airflow_breeze.utils.run_utils.get_console")
def test_check_buildx_not_detected(mock_get_console, mock_run_command):
    mock_run_command.return_value.returncode = 1
    assert check_if_buildx_plugin_installed() is False
    mock_run_command.assert_called_with(
        ["docker", "buildx", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    mock_get_console.return_value.print.assert_not_called()
