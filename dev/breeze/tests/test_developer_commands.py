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

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.developer_commands import build_docs, run
from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION


@pytest.fixture
def runner():
    return CliRunner()


class TestBuildDocsPythonVersion:
    """`breeze build-docs` always documents on the default Python.

    The Sphinx config mocks third-party modules and what that mocking does depends on the
    interpreter, so the docs build must not follow whatever Python the caller happens to have
    selected - see the comment in ``_build_python_docs``.
    """

    @pytest.fixture(autouse=True)
    def _no_docker(self, monkeypatch):
        monkeypatch.setenv("SKIP_SAVING_CHOICES", "true")
        for name in (
            "perform_environment_checks",
            "fix_ownership_using_docker",
            "cleanup_python_generated_files",
        ):
            monkeypatch.setattr(f"airflow_breeze.commands.developer_commands.{name}", lambda *a, **kw: None)

    def _invoke(self, runner: CliRunner, args: list[str], env: dict[str, str] | None = None):
        with (
            patch("airflow_breeze.commands.developer_commands.build_ci_image_if_needed") as mock_build,
            patch("airflow_breeze.commands.developer_commands.execute_command_in_shell") as mock_shell,
        ):
            mock_shell.return_value.returncode = 0
            runner.invoke(build_docs, args, env=env, catch_exceptions=False)
        return mock_build, mock_shell

    def test_environment_python_does_not_change_the_docs_build(self, runner):
        # PYTHON_MAJOR_MINOR_VERSION is set on every job of the docs publishing workflow, so an
        # option reading it silently decided what the docs were built with.
        mock_build, mock_shell = self._invoke(
            runner, ["--docs-only"], env={"PYTHON_MAJOR_MINOR_VERSION": "3.12"}
        )

        assert mock_build.call_args.kwargs["command_params"].python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION
        assert mock_shell.call_args.args[0].python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION

    def test_python_option_is_rejected(self, runner):
        result = runner.invoke(build_docs, ["--python", "3.12", "--docs-only"])

        assert result.exit_code != 0
        assert "no such option" in result.output.lower()


class TestRunIncludeMypyVolume:
    @pytest.fixture(autouse=True)
    def _no_docker(self, monkeypatch):
        monkeypatch.setenv("SKIP_SAVING_CHOICES", "true")
        monkeypatch.delenv("INCLUDE_MYPY_VOLUME", raising=False)
        monkeypatch.setattr(
            "airflow_breeze.commands.developer_commands.bring_compose_project_down", lambda *a, **kw: None
        )
        for name in ("fix_ownership_using_docker", "remove_docker_networks"):
            monkeypatch.setattr(f"airflow_breeze.utils.docker_command_utils.{name}", lambda *a, **kw: None)

    def _invoke(self, runner: CliRunner, args: list[str], env: dict[str, str] | None = None):
        with (
            patch("airflow_breeze.commands.ci_image_commands.build_ci_image_if_needed"),
            patch("airflow_breeze.utils.docker_command_utils.execute_command_in_shell") as mock_shell,
        ):
            mock_shell.return_value.returncode = 0
            runner.invoke(run, [*args, "true"], env=env, catch_exceptions=False)
        return mock_shell.call_args.kwargs["shell_params"]

    @pytest.mark.parametrize(
        ("args", "env", "expected"),
        [
            pytest.param([], None, False, id="default"),
            pytest.param(["--include-mypy-volume"], None, True, id="flag"),
            pytest.param([], {"INCLUDE_MYPY_VOLUME": "true"}, True, id="env"),
        ],
    )
    def test_include_mypy_volume_is_passed_to_shell_params(self, runner, args, env, expected):
        assert self._invoke(runner, args, env=env).include_mypy_volume is expected
