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
from unittest import mock

import pytest
from rich.console import Console

from docs import build_docs


@pytest.fixture(autouse=True)
def isolated_environ():
    with mock.patch.dict(os.environ):
        os.environ.pop("PUPPETEER_EXECUTABLE_PATH", None)
        yield


@pytest.fixture
def chromium_path(tmp_path, monkeypatch):
    path = tmp_path / "chromium"
    path.touch()
    monkeypatch.setattr(build_docs, "CHROMIUM_PATH", path)
    return path


def test_uses_mermaid_finds_diagrams_only_in_packages_that_have_them():
    assert build_docs._uses_mermaid("apache-airflow-providers-common-ai")
    assert not build_docs._uses_mermaid("apache-airflow-providers-sqlite")


@mock.patch.object(subprocess, "run", autospec=True)
@mock.patch.object(build_docs, "is_command_available", autospec=True, return_value=False)
@mock.patch.object(build_docs, "_uses_mermaid", autospec=True, return_value=False)
def test_ensure_mermaid_cli_skips_install_without_diagrams(
    mock_uses_mermaid, mock_is_command_available, mock_run, monkeypatch
):
    monkeypatch.setenv("BREEZE", "true")

    build_docs.ensure_mermaid_cli(["apache-airflow-providers-sqlite"])

    mock_run.assert_not_called()


@mock.patch.object(subprocess, "run", autospec=True)
@mock.patch.object(build_docs, "is_command_available", autospec=True, return_value=False)
@mock.patch.object(build_docs, "_uses_mermaid", autospec=True, return_value=True)
def test_ensure_mermaid_cli_installs_in_breeze_when_mmdc_missing(
    mock_uses_mermaid, mock_is_command_available, mock_run, monkeypatch, chromium_path
):
    monkeypatch.setenv("BREEZE", "true")

    build_docs.ensure_mermaid_cli(["apache-airflow-providers-common-ai"])

    mock_run.assert_called_once_with([build_docs.INSTALL_MERMAID_CLI_SCRIPT.as_posix()], check=True)
    assert os.environ["PUPPETEER_EXECUTABLE_PATH"] == chromium_path.as_posix()


@mock.patch.object(subprocess, "run", autospec=True)
@mock.patch.object(build_docs, "is_command_available", autospec=True, return_value=True)
@mock.patch.object(build_docs, "_uses_mermaid", autospec=True, return_value=True)
def test_ensure_mermaid_cli_does_not_reinstall_when_mmdc_present(
    mock_uses_mermaid, mock_is_command_available, mock_run, monkeypatch, chromium_path
):
    monkeypatch.setenv("BREEZE", "true")

    build_docs.ensure_mermaid_cli(["apache-airflow-providers-common-ai"])

    mock_run.assert_not_called()
    assert os.environ["PUPPETEER_EXECUTABLE_PATH"] == chromium_path.as_posix()


@mock.patch.object(subprocess, "run", autospec=True)
@mock.patch.object(build_docs, "is_command_available", autospec=True, return_value=False)
@mock.patch.object(build_docs, "_uses_mermaid", autospec=True, return_value=True)
def test_ensure_mermaid_cli_does_not_install_outside_breeze(
    mock_uses_mermaid, mock_is_command_available, mock_run, monkeypatch
):
    monkeypatch.delenv("BREEZE", raising=False)
    mock_console = mock.create_autospec(Console, instance=True)
    monkeypatch.setattr(build_docs, "console", mock_console)

    build_docs.ensure_mermaid_cli(["apache-airflow-providers-common-ai"])

    mock_run.assert_not_called()
    (message,), _ = mock_console.print.call_args
    assert "npm install --global @mermaid-js/mermaid-cli" in message
