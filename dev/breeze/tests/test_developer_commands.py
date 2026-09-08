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

from airflow_breeze.commands.developer_commands import build_docs


@pytest.fixture
def runner():
    return CliRunner()


class TestBuildDocsPythonVersion:
    """`breeze build-docs` must document with the Python version it was asked for."""

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
            patch(
                "airflow_breeze.commands.developer_commands.rebuild_or_pull_ci_image_if_needed"
            ) as mock_rebuild,
            patch("airflow_breeze.commands.developer_commands.execute_command_in_shell") as mock_shell,
        ):
            mock_shell.return_value.returncode = 0
            runner.invoke(build_docs, args, env=env, catch_exceptions=False)
        return mock_rebuild, mock_shell

    @pytest.mark.parametrize(
        ("args", "env"),
        [
            pytest.param(["--python", "3.12"], None, id="python-option"),
            pytest.param([], {"PYTHON_MAJOR_MINOR_VERSION": "3.12"}, id="python-env-var"),
        ],
    )
    def test_selected_python_is_used_for_image_and_shell(self, runner, args, env):
        mock_rebuild, mock_shell = self._invoke(runner, [*args, "--docs-only"], env=env)

        assert mock_rebuild.call_args.kwargs["command_params"].python == "3.12"
        assert mock_shell.call_args.args[0].python == "3.12"
