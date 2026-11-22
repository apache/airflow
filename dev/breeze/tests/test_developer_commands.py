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

from subprocess import CompletedProcess
from unittest.mock import create_autospec, patch

import click
import pytest
from click import UsageError
from click.testing import CliRunner

from airflow_breeze.commands import developer_commands
from airflow_breeze.commands.developer_commands import build_docs, down, run
from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION


@pytest.fixture
def runner():
    return CliRunner()


def test_down_rejects_conflicting_project_selectors(runner):
    with pytest.raises(UsageError, match="--all-worktrees and --project-name cannot be used together"):
        runner.invoke(
            down,
            ["--all-worktrees", "--project-name", "foobar"],
            standalone_mode=False,
            catch_exceptions=False,
        )


@pytest.mark.parametrize("isolation", ["true", "false"])
@pytest.mark.parametrize("linked", [False, True])
def test_down_preserves_volumes_without_startup_cleanup(runner, tmp_path, monkeypatch, linked, isolation):
    monkeypatch.setenv("BREEZE_WORKTREE_ISOLATION", isolation)
    with (
        patch(
            "airflow_breeze.commands.developer_commands.perform_environment_checks", autospec=True
        ) as checks,
        patch(
            "airflow_breeze.commands.developer_commands.bring_compose_projects_down",
            autospec=True,
            return_value=[],
        ) as teardown,
        patch(
            "airflow_breeze.commands.developer_commands.find_other_worktree_projects",
            autospec=True,
            return_value={},
        ),
        patch("airflow_breeze.commands.developer_commands.AIRFLOW_ROOT_PATH", tmp_path),
        patch(
            "airflow_breeze.commands.developer_commands.get_main_git_dir_for_worktree",
            autospec=True,
            return_value=tmp_path / ".git" if linked else None,
        ),
    ):
        result = runner.invoke(down, ["--preserve-volumes"])
    assert result.exit_code == 0
    checks.assert_called_once_with(cleanup_stale_worktrees=False)
    assert teardown.call_args.kwargs["preserve_volumes"] is True
    assert teardown.call_args.kwargs["current_worktree"] == (str(tmp_path.resolve()) if linked else "")


@pytest.mark.parametrize(
    ("args", "listed"),
    [
        pytest.param([], True, id="current-checkout"),
        pytest.param(["--all-worktrees"], False, id="all-worktrees"),
        pytest.param(["--project-name", "foobar"], False, id="explicit-project"),
    ],
)
def test_down_lists_projects_left_in_other_worktrees(runner, tmp_path, args, listed):
    with (
        patch("airflow_breeze.commands.developer_commands.perform_environment_checks", autospec=True),
        patch(
            "airflow_breeze.commands.developer_commands.bring_compose_projects_down",
            autospec=True,
            return_value=[],
        ),
        patch(
            "airflow_breeze.commands.developer_commands.find_other_worktree_projects",
            autospec=True,
            return_value={"/work/other": ["breeze-other-abc123", "breeze-other-abc123-tests"]},
        ) as find_others,
        patch("airflow_breeze.commands.developer_commands.AIRFLOW_ROOT_PATH", tmp_path),
        patch(
            "airflow_breeze.commands.developer_commands.get_main_git_dir_for_worktree",
            autospec=True,
            return_value=tmp_path / ".git",
        ),
    ):
        result = runner.invoke(down, args)
    assert result.exit_code == 0
    output = " ".join(click.unstyle(result.output).split())
    if listed:
        find_others.assert_called_once_with(str(tmp_path.resolve()))
        assert "/work/other: breeze-other-abc123, breeze-other-abc123-tests" in output
        assert "breeze down --all-worktrees" in output
    else:
        find_others.assert_not_called()
        assert "/work/other" not in output


@pytest.mark.parametrize(
    ("stdin_is_terminal", "stdout_is_terminal", "docker_exec_flag"),
    [
        pytest.param(True, True, "-it", id="terminal"),
        pytest.param(False, True, "-i", id="input-not-a-terminal"),
        pytest.param(True, False, "-i", id="output-not-a-terminal"),
    ],
)
def test_exec_selects_the_project_and_allocates_a_terminal_only_when_attached(
    stdin_is_terminal, stdout_is_terminal, docker_exec_flag
):
    with (
        patch("airflow_breeze.commands.developer_commands.perform_environment_checks", autospec=True),
        patch("airflow_breeze.commands.developer_commands.check_docker_resources", autospec=True),
        patch("airflow_breeze.commands.developer_commands.run_command", autospec=True) as run_command,
        patch("sys.stdin", autospec=True) as stdin,
        patch("sys.stdout", autospec=True) as stdout,
    ):
        stdin.isatty.return_value = stdin_is_terminal
        stdout.isatty.return_value = stdout_is_terminal
        run_command.side_effect = [
            CompletedProcess(args=[], returncode=0, stdout="feedc0de\n0123abcd\n"),
            CompletedProcess(args=[], returncode=3),
        ]
        with pytest.raises(SystemExit) as exit_info:
            developer_commands.exec.main(
                ["--project-name", "foobar", "--", "-c", "airflow version"], standalone_mode=False
            )
    assert exit_info.value.code == 3
    find_container, run_in_container = run_command.call_args_list
    assert find_container.args[0] == [
        "docker",
        "compose",
        "--project-name",
        "foobar",
        "ps",
        "--all",
        "--quiet",
        "--filter",
        "status=running",
        "airflow",
    ]
    assert run_in_container.args[0] == [
        "docker",
        "exec",
        docker_exec_flag,
        "0123abcd",
        "/opt/airflow/scripts/docker/entrypoint_exec.sh",
        "-c",
        "airflow version",
    ]


def test_exec_reports_a_project_without_a_running_container(runner):
    with (
        patch("airflow_breeze.commands.developer_commands.perform_environment_checks", autospec=True),
        patch("airflow_breeze.commands.developer_commands.check_docker_resources", autospec=True),
        patch(
            "airflow_breeze.commands.developer_commands.run_command",
            autospec=True,
            return_value=CompletedProcess(args=[], returncode=0, stdout=""),
        ) as run_command,
    ):
        result = runner.invoke(developer_commands.exec, ["--project-name", "foobar"])
    assert result.exit_code == 1
    run_command.assert_called_once()
    output = " ".join(click.unstyle(result.output).split())
    assert "No airflow container is running in the foobar compose project" in output


@pytest.fixture
def docker_mocks(monkeypatch, tmp_path):
    monkeypatch.setenv("SKIP_SAVING_CHOICES", "true")
    for name in ("BREEZE_SDK", "DISTRIBUTIONS_LIST"):
        monkeypatch.delenv(name, raising=False)
    # The real _build_python_docs deletes generated/**/apis under AIRFLOW_ROOT_PATH.
    monkeypatch.setattr(developer_commands, "AIRFLOW_ROOT_PATH", tmp_path)
    mocks = {}
    for name in (
        "perform_environment_checks",
        "fix_ownership_using_docker",
        "cleanup_python_generated_files",
        "build_ci_image_if_needed",
        "execute_command_in_shell",
    ):
        mocks[name] = create_autospec(getattr(developer_commands, name))
        monkeypatch.setattr(developer_commands, name, mocks[name])
    mocks["execute_command_in_shell"].return_value.returncode = 0
    return mocks


@pytest.mark.usefixtures("docker_mocks")
class TestBuildDocsPythonVersion:
    """`breeze build-docs` always documents on the default Python.

    The Sphinx config mocks third-party modules and what that mocking does depends on the
    interpreter, so the docs build must not follow whatever Python the caller happens to have
    selected - see the comment in ``_build_python_docs``.
    """

    def test_environment_python_does_not_change_the_docs_build(self, runner, docker_mocks):
        # PYTHON_MAJOR_MINOR_VERSION is set on every job of the docs publishing workflow, so an
        # option reading it silently decided what the docs were built with.
        result = runner.invoke(
            build_docs, [], env={"PYTHON_MAJOR_MINOR_VERSION": "3.12"}, catch_exceptions=False
        )

        assert result.exit_code == 0
        mock_build = docker_mocks["build_ci_image_if_needed"]
        mock_shell = docker_mocks["execute_command_in_shell"]
        assert mock_build.call_args.kwargs["command_params"].python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION
        assert mock_shell.call_args.args[0].python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION

    def test_python_option_is_rejected(self, runner):
        result = runner.invoke(build_docs, ["--python", "3.12"])

        assert result.exit_code != 0
        assert "no such option" in result.output.lower()


class TestBuildDocsSphinxAirflowTheme:
    @pytest.mark.parametrize("pass_option", [True, False])
    def test_theme_path_is_passed_to_shell_params(
        self, runner, docker_mocks, tmp_path, monkeypatch, pass_option
    ):
        (tmp_path / "theme" / "sphinx_airflow_theme" / "static" / "_gen").mkdir(parents=True)
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(
            build_docs,
            ["--sphinx-airflow-theme-path", "theme"] if pass_option else [],
            catch_exceptions=False,
        )

        assert result.exit_code == 0
        shell_params = docker_mocks["execute_command_in_shell"].call_args.args[0]
        assert shell_params.sphinx_airflow_theme_path == (str(tmp_path / "theme") if pass_option else "")

    def test_theme_without_generated_assets_is_rejected(self, runner, docker_mocks, tmp_path):
        theme_checkout = tmp_path / "airflow-site"
        (theme_checkout / "sphinx_airflow_theme").mkdir(parents=True)

        result = runner.invoke(build_docs, ["--sphinx-airflow-theme-path", str(theme_checkout)])

        assert result.exit_code == 2
        assert "sphinx_airflow_theme/static/_gen" in " ".join(click.unstyle(result.output).split())
        for mock in docker_mocks.values():
            mock.assert_not_called()

    def test_theme_with_sdk_docs_only_is_rejected(self, runner, docker_mocks, tmp_path):
        theme_checkout = tmp_path / "airflow-site"
        (theme_checkout / "sphinx_airflow_theme" / "static" / "_gen").mkdir(parents=True)

        result = runner.invoke(
            build_docs, ["--sdk-docs-only", "--sphinx-airflow-theme-path", str(theme_checkout)]
        )

        assert result.exit_code == 2
        assert "--sdk-docs-only:" in click.unstyle(result.output)
        for mock in docker_mocks.values():
            mock.assert_not_called()


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
