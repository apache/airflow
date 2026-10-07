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

import json
import subprocess
from contextlib import ExitStack
from pathlib import Path
from unittest import mock
from unittest.mock import call

import pytest

from airflow_breeze.global_constants import (
    ALLOWED_POSTGRES_VERSIONS,
    CI_IMAGE_SOURCES_HASH_LABEL,
    CURRENT_POSTGRES_VERSIONS,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils import docker_command_utils
from airflow_breeze.utils.docker_command_utils import (
    autodetect_docker_context,
    check_docker_compose_version,
    check_docker_is_running,
    check_docker_permission_denied,
    check_docker_version,
    enter_shell,
    fix_ownership_using_docker,
    get_images_to_pull,
    prepare_docker_build_command,
    pull_images_with_retries,
)


@pytest.mark.parametrize("listing_failed", [False, True])
def test_stale_worktree_cleanup_removes_only_containers_with_missing_absolute_paths(tmp_path, listing_failed):
    existing = tmp_path / "existing worktree"
    existing.mkdir()
    missing = tmp_path / "deleted worktree"
    listing = "\n".join(
        [f"stale\t{missing}", f"active\t{existing}", "primary\t", "legacy\t<no value>", "relative\trelative"]
    )
    with mock.patch("airflow_breeze.utils.docker_command_utils.run_command", autospec=True) as run:
        run.return_value = subprocess.CompletedProcess([], int(listing_failed), stdout=listing, stderr="")
        docker_command_utils.remove_stale_worktree_containers()

    assert run.call_args_list[0].args[0] == [
        "docker",
        "ps",
        "--all",
        "--filter",
        "label=org.apache.airflow.breeze=true",
        "--format",
        '{{.ID}}\t{{.Label "org.apache.airflow.breeze.worktree"}}',
    ]
    removals = [c.args[0] for c in run.call_args_list if c.args[0][:2] == ["docker", "rm"]]
    assert removals == ([] if listing_failed else [["docker", "rm", "--force", "--volumes", "stale"]])


def test_stale_worktree_cleanup_keeps_containers_when_path_cannot_be_checked(tmp_path):
    original_stat = Path.stat

    def stat(path, **kwargs):
        if path == tmp_path:
            raise PermissionError("unreadable")
        return original_stat(path, **kwargs)

    with (
        mock.patch("airflow_breeze.utils.docker_command_utils.run_command", autospec=True) as run,
        mock.patch.object(Path, "stat", autospec=True, side_effect=stat),
    ):
        run.return_value = subprocess.CompletedProcess([], 0, stdout=f"container\t{tmp_path}\n", stderr="")
        docker_command_utils.remove_stale_worktree_containers()

    assert run.call_count == 1


@pytest.mark.parametrize(
    ("exception", "expected_message"),
    [
        pytest.param(
            subprocess.TimeoutExpired(["docker", "info"], 30),
            "[error]Docker did not respond within 30 seconds.[/]\n"
            "[warning]Please make sure Docker is running and responsive.[/]",
            id="timeout",
        ),
        pytest.param(
            FileNotFoundError(2, "No such file or directory", "docker"),
            "[error]Docker executable was not found.[/]\n"
            "[warning]Please install Docker and ensure `docker` is available on PATH.[/]",
            id="missing-executable",
        ),
    ],
)
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
def test_check_docker_is_running_reports_unavailable_docker(
    mock_run_command, mock_console_print, exception, expected_message
):
    mock_run_command.side_effect = exception

    with pytest.raises(SystemExit) as error:
        check_docker_is_running()

    assert error.value.code == 1
    mock_run_command.assert_called_once_with(
        ["docker", "info"],
        no_output_dump_on_exception=True,
        text=True,
        capture_output=True,
        check=False,
        timeout=30,
    )
    mock_console_print.assert_called_once_with(expected_message)


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
def test_check_docker_permission_denied_uses_bounded_info_probe(mock_run_command):
    mock_run_command.return_value.returncode = 0

    assert check_docker_permission_denied() is False

    mock_run_command.assert_called_once_with(
        ["docker", "info"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_version_unknown(
    mock_console_print, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    with pytest.raises(SystemExit) as e:
        check_docker_version()
    assert e.value.code == 1
    expected_run_command_calls = [
        call(
            ["docker", "version", "--format", "{{.Client.Version}}"],
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            check=False,
            dry_run_override=False,
        ),
    ]
    mock_run_command.assert_has_calls(expected_run_command_calls)
    mock_console_print.assert_called_with(
        """
[warning]Your version of docker is unknown. If the scripts fail, please make sure to[/]
[warning]install docker at least: 25.0.0 version.[/]
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_version_too_low(
    mock_console_print, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "0.9"
    with pytest.raises(SystemExit) as e:
        check_docker_version()
    assert e.value.code == 1
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_console_print.assert_called_with(
        """
[error]Your version of docker is too old: 0.9.\n[/]\n[warning]Please upgrade to at least 25.0.0.\n[/]\n\
You can find installation instructions here: https://docs.docker.com/engine/install/
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_version_ok(mock_console_print, mock_run_command, mock_check_docker_permission_denied):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "25.0.0"
    check_docker_version()
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_console_print.assert_called_with("[success]Good version of Docker: 25.0.0.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_version_higher(
    mock_console_print, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "25.0.0"
    check_docker_version()
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_console_print.assert_called_with("[success]Good version of Docker: 25.0.0.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.check_docker_permission_denied")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_version_higher_rancher_desktop(
    mock_console_print, mock_run_command, mock_check_docker_permission_denied
):
    mock_check_docker_permission_denied.return_value = False
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "25.0.0-rd"
    check_docker_version()
    mock_check_docker_permission_denied.assert_called()
    mock_run_command.assert_called_with(
        ["docker", "version", "--format", "{{.Client.Version}}"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    mock_console_print.assert_called_with("[success]Good version of Docker: 25.0.0-r.[/]")


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_compose_version_unknown(mock_console_print, mock_run_command):
    with pytest.raises(SystemExit) as e:
        check_docker_compose_version()
    assert e.value.code == 1
    expected_run_command_calls = [
        call(
            ["docker", "compose", "version"],
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            dry_run_override=False,
        ),
    ]
    mock_run_command.assert_has_calls(expected_run_command_calls)
    mock_console_print.assert_called_with(
        """
[error]Unknown docker-compose version.[/]\n[warning]At least 2.20.2 needed! Please upgrade!\n[/]
See https://docs.docker.com/compose/install/ for installation instructions.\n
Make sure docker-compose you install is first on the PATH variable of yours.\n
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_compose_version_low(mock_console_print, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "1.28.5"
    with pytest.raises(SystemExit) as e:
        check_docker_compose_version()
    assert e.value.code == 1
    mock_run_command.assert_called_with(
        ["docker", "compose", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    mock_console_print.assert_called_with(
        """
[error]You have too old version of docker-compose: 1.28.5!\n[/]
[warning]At least 2.20.2 needed! Please upgrade!\n[/]
See https://docs.docker.com/compose/install/ for installation instructions.\n
Make sure docker-compose you install is first on the PATH variable of yours.\n
"""
    )


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
def test_check_docker_compose_version_ok(mock_console_print, mock_run_command):
    mock_run_command.return_value.returncode = 0
    mock_run_command.return_value.stdout = "2.20.2"
    check_docker_compose_version()
    mock_run_command.assert_called_with(
        ["docker", "compose", "version"],
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    mock_console_print.assert_called_with("[success]Good version of docker-compose: 2.20.2[/]")


def _fake_ctx_output(*names: str) -> str:
    return "\n".join(json.dumps({"Name": name, "DockerEndpoint": f"unix://{name}"}) for name in names)


@pytest.mark.parametrize(
    ("context_output", "selected_context", "console_output"),
    [
        (
            _fake_ctx_output("default"),
            "default",
            "[info]Using 'default' as context",
        ),
        ("\n", "default", "[warning]Could not detect docker builder"),
        (
            _fake_ctx_output("a", "b"),
            "a",
            "[warning]Could not use any of the preferred docker contexts",
        ),
        (
            _fake_ctx_output("a", "desktop-linux"),
            "desktop-linux",
            "[info]Using 'desktop-linux' as context",
        ),
        (
            _fake_ctx_output("a", "default"),
            "default",
            "[info]Using 'default' as context",
        ),
        (
            _fake_ctx_output("a", "default", "desktop-linux"),
            "desktop-linux",
            "[info]Using 'desktop-linux' as context",
        ),
        (
            '[{"Name": "desktop-linux", "DockerEndpoint": "unix://desktop-linux"}]',
            "desktop-linux",
            "[info]Using 'desktop-linux' as context",
        ),
    ],
)
def test_autodetect_docker_context(context_output: str, selected_context: str, console_output: str):
    with mock.patch("airflow_breeze.utils.docker_command_utils.run_command") as mock_run_command:
        mock_run_command.return_value.returncode = 0
        mock_run_command.return_value.stdout = context_output
        with mock.patch("airflow_breeze.utils.docker_command_utils.console_print") as mock_console_print:
            assert autodetect_docker_context() == selected_context
            mock_console_print.assert_called_once()
            assert console_output in mock_console_print.call_args[0][0]


SOCKET_INFO = json.dumps(
    [
        {
            "Name": "default",
            "Metadata": {},
            "Endpoints": {"docker": {"Host": "unix:///not-standard/docker.sock", "SkipTLSVerify": False}},
            "TLSMaterial": {},
            "Storage": {"MetadataPath": "\u003cIN MEMORY\u003e", "TLSPath": "\u003cIN MEMORY\u003e"},
        }
    ]
)

SOCKET_INFO_DESKTOP_LINUX = json.dumps(
    [
        {
            "Name": "desktop-linux",
            "Metadata": {},
            "Endpoints": {
                "docker": {"Host": "unix:///VERY_NON_STANDARD/docker.sock", "SkipTLSVerify": False}
            },
            "TLSMaterial": {},
            "Storage": {"MetadataPath": "\u003cIN MEMORY\u003e", "TLSPath": "\u003cIN MEMORY\u003e"},
        }
    ]
)


@pytest.fixture
def docker_resources():
    resources = {"container": [], "network": [], "volume": []}

    def docker(cmd, **kwargs):
        kind, action = cmd[1:3]
        if action == "ls":
            assert "label=com.docker.compose.project" in cmd
            if kind == "container":
                assert "--all" in cmd
            output = "\n".join(item.get("Id", item.get("Name")) for item in resources[kind])
        elif action == "inspect":
            output = json.dumps(resources[kind])
        else:
            assert action in ("stop", "wait", "rm")
            output = ""
        return subprocess.CompletedProcess(cmd, 0, stdout=output, stderr="")

    with mock.patch(
        "airflow_breeze.utils.docker_command_utils.run_command", autospec=True, side_effect=docker
    ) as run:
        yield resources, run


@pytest.mark.parametrize(
    ("kwargs", "linked", "expected"),
    [
        ({}, False, {"breeze", "breeze-docs", "main-tests", "stale"}),
        ({}, True, {"breeze", "breeze-docs", "main-tests", "foobar", "foobar-tests", "stale"}),
        (
            {"all_worktrees": True},
            True,
            {"breeze", "breeze-docs", "main-tests", "stale", "foobar", "foobar-tests", "relative", "other"},
        ),
        ({"only_project": "foobar"}, True, {"foobar"}),
        ({"only_project": "breeze-thirdparty"}, True, {"breeze-thirdparty"}),
        ({"only_project": "absent"}, True, set()),
        ({"stale_only": True}, True, {"stale"}),
    ],
)
@pytest.mark.parametrize("preserve_volumes", [False, True])
def test_down_selects_checkout_stale_or_explicit_projects(
    docker_resources, tmp_path, kwargs, linked, expected, preserve_volumes
):
    resources, run = docker_resources
    other = tmp_path / "other"
    other.mkdir()
    projects = {
        "breeze": {},
        "breeze-docs": {},
        "unrelated": {},
        "breeze-excluded": {"org.apache.airflow.breeze": "false"},
        "main-tests": {"org.apache.airflow.breeze": "true", "org.apache.airflow.breeze.worktree": ""},
        "foobar": {"org.apache.airflow.breeze": "true", "org.apache.airflow.breeze.worktree": str(tmp_path)},
        "foobar-tests": {
            "org.apache.airflow.breeze": "true",
            "org.apache.airflow.breeze.worktree": str(tmp_path),
        },
        "other": {"org.apache.airflow.breeze": "true", "org.apache.airflow.breeze.worktree": str(other)},
        "thirdparty": {"org.apache.airflow.breeze.worktree": str(tmp_path)},
        "stale": {
            "org.apache.airflow.breeze": "true",
            "org.apache.airflow.breeze.worktree": str(tmp_path / "deleted"),
        },
        "relative": {
            "org.apache.airflow.breeze": "true",
            "org.apache.airflow.breeze.worktree": "relative/path",
        },
        "breeze-thirdparty": {"org.apache.airflow.breeze.worktree": str(tmp_path / "deleted")},
    }
    for project, extra_labels in projects.items():
        labels = {"com.docker.compose.project": project, **extra_labels}
        resources["container"].append({"Id": f"{project}-container", "Config": {"Labels": labels}})
        resources["network"].append({"Id": f"{project}-network", "Labels": labels})
        resources["volume"].append({"Name": f"{project}-volume", "Labels": labels})

    assert docker_command_utils.bring_compose_projects_down(
        preserve_volumes=preserve_volumes, current_worktree=str(tmp_path) if linked else "", **kwargs
    ) == sorted(expected)

    commands = [c.args[0] for c in run.call_args_list]
    for kind in ("container", "network", "volume"):
        removed = {
            arg for cmd in commands if cmd[1:3] == [kind, "rm"] for arg in cmd[3:] if not arg.startswith("--")
        }
        assert removed == (
            set() if kind == "volume" and preserve_volumes else {f"{project}-{kind}" for project in expected}
        )
    if expected:
        stops = [cmd for cmd in commands if cmd[1:3] == ["container", "stop"]]
        assert {arg for cmd in stops for arg in cmd[3:]} == {f"{project}-container" for project in expected}
        first_removal = next(i for i, cmd in enumerate(commands) if cmd[2] == "rm")
        assert all(cmd[2] in ("ls", "inspect", "stop", "wait") for cmd in commands[:first_removal])
        removal = next(cmd for cmd in commands if cmd[1:3] == ["container", "rm"])
        assert ("--volumes" in removal) is not preserve_volumes


@pytest.mark.parametrize("cleanup_stale_worktrees", [False, True])
@pytest.mark.parametrize("docker_available", [False, True])
def test_environment_checks_reap_stale_resources(
    docker_resources, tmp_path, cleanup_stale_worktrees, docker_available
):
    resources, run = docker_resources
    resources["volume"] = [
        {
            "Name": "stale-db",
            "Labels": {
                "com.docker.compose.project": "stale",
                "org.apache.airflow.breeze": "true",
                "org.apache.airflow.breeze.worktree": str(tmp_path / "deleted"),
            },
        },
        {"Name": "live-db", "Labels": {"com.docker.compose.project": "breeze"}},
    ]
    with ExitStack() as stack:
        for name in (
            "check_docker_is_running",
            "check_container_engine_is_docker",
            "check_docker_version",
            "check_docker_compose_version",
            "check_windows_filesystem_mount",
            "check_executable_entrypoint_permissions",
            "check_uv_version",
        ):
            check = stack.enter_context(
                mock.patch.object(docker_command_utils, name, autospec=True, return_value=True)
            )
            if name == "check_docker_is_running" and not docker_available:
                check.side_effect = SystemExit(1)
        if docker_available:
            docker_command_utils.perform_environment_checks.__wrapped__(
                cleanup_stale_worktrees=cleanup_stale_worktrees
            )
        else:
            with pytest.raises(SystemExit):
                docker_command_utils.perform_environment_checks.__wrapped__(
                    cleanup_stale_worktrees=cleanup_stale_worktrees
                )
    if docker_available and cleanup_stale_worktrees:
        assert (
            call(["docker", "volume", "rm", "stale-db"], check=False, capture_output=True)
            in run.call_args_list
        )
    else:
        run.assert_not_called()


@pytest.mark.parametrize("all_worktrees", [False, True])
@pytest.mark.parametrize("preserve_volumes", [False, True])
def test_down_finds_volumes_without_containers(docker_resources, tmp_path, preserve_volumes, all_worktrees):
    resources, run = docker_resources
    resources["volume"] = [
        {
            "Name": "foobar-postgres14-db-volume",
            "Labels": {
                "com.docker.compose.project": "foobar",
                "org.apache.airflow.breeze": "true",
                "org.apache.airflow.breeze.worktree": str(
                    tmp_path if all_worktrees else tmp_path / "deleted"
                ),
            },
        }
    ]

    projects = docker_command_utils.bring_compose_projects_down(
        preserve_volumes=preserve_volumes, all_worktrees=all_worktrees
    )

    removals = [c.args[0] for c in run.call_args_list if c.args[0][2] == "rm"]
    assert projects == ([] if preserve_volumes else ["foobar"])
    assert removals == (
        [] if preserve_volumes else [["docker", "volume", "rm", "foobar-postgres14-db-volume"]]
    )


@pytest.mark.parametrize("failed_action", ["ls", "inspect", "stop", "rm"])
def test_startup_cleanup_continues_after_docker_failures(docker_resources, tmp_path, capsys, failed_action):
    resources, run = docker_resources
    labels = {
        "com.docker.compose.project": "stale",
        "org.apache.airflow.breeze": "true",
        "org.apache.airflow.breeze.worktree": str(tmp_path / "deleted"),
    }
    resources["container"] = [{"Id": "remaining", "Config": {"Labels": labels}}]
    resources["volume"] = [{"Name": "database", "Labels": labels}]
    docker = run.side_effect

    def fail(cmd, **kwargs):
        result = docker(cmd, **kwargs)
        if cmd[2] == "ls":
            assert "label=org.apache.airflow.breeze=true" in cmd
            assert "label=org.apache.airflow.breeze.worktree" in cmd
            if cmd[1] == "container":
                result.stdout += "\nvanished"
        if cmd[2] == failed_action:
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, cmd)
            result.returncode = 1
            result.stderr = "resource disappeared or is still in use"
        return result

    run.side_effect = fail
    stop_failed = failed_action == "stop"
    assert docker_command_utils.bring_compose_projects_down(stale_only=True) == (
        ["stale"] if stop_failed else []
    )
    assert capsys.readouterr().out.count("Unable to clean up some deleted-worktree resources") == (
        0 if stop_failed else 1
    )
    commands = [c.args[0] for c in run.call_args_list]
    if failed_action == "ls":
        assert all(cmd[2] == "ls" for cmd in commands)
    else:
        assert ["docker", "volume", "rm", "database"] in commands
        assert ["docker", "container", "rm", "--force", "--volumes", "remaining"] in commands
        assert not any("vanished" in cmd for cmd in commands if cmd[2] in ("stop", "rm"))


def test_down_empty_discovery_is_not_an_error(docker_resources, capsys):
    assert docker_command_utils.bring_compose_projects_down(all_worktrees=True) == []
    assert "error" not in capsys.readouterr().out.lower()


@pytest.mark.parametrize("failed_kind", ["container", "volume"])
def test_down_does_not_remove_resources_when_discovery_fails(docker_resources, failed_kind):
    resources, run = docker_resources
    resources["container"] = [
        {"Id": "container", "Config": {"Labels": {"com.docker.compose.project": "breeze"}}}
    ]
    docker = run.side_effect

    def fail(cmd, **kwargs):
        if cmd[1:3] == [failed_kind, "ls"]:
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, cmd)
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="unavailable")
        return docker(cmd, **kwargs)

    run.side_effect = fail

    with pytest.raises(subprocess.CalledProcessError):
        docker_command_utils.bring_compose_projects_down()

    assert all(c.args[0][2] in ("ls", "inspect") for c in run.call_args_list)


def test_down_stops_after_container_removal_failure(docker_resources):
    resources, run = docker_resources
    labels = {"com.docker.compose.project": "breeze"}
    resources["container"] = [{"Id": "container", "Config": {"Labels": labels}}]
    resources["volume"] = [{"Name": "database", "Labels": labels}]
    docker = run.side_effect

    def fail(cmd, **kwargs):
        if cmd[1:3] == ["container", "rm"]:
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, cmd)
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="in use")
        return docker(cmd, **kwargs)

    run.side_effect = fail
    with pytest.raises(subprocess.CalledProcessError):
        docker_command_utils.bring_compose_projects_down()

    assert not any(c.args[0][1:3] == ["volume", "rm"] for c in run.call_args_list)


def test_down_force_removes_containers_that_outlive_stop(docker_resources, capsys):
    resources, run = docker_resources
    labels = {"com.docker.compose.project": "breeze"}
    resources["container"] = [{"Id": "container", "Config": {"Labels": labels}}]
    docker = run.side_effect

    def fail(cmd, **kwargs):
        if cmd[1:3] == ["container", "stop"]:
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, cmd)
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="did not receive an exit event")
        return docker(cmd, **kwargs)

    run.side_effect = fail
    assert docker_command_utils.bring_compose_projects_down() == ["breeze"]

    assert "Stopping Breeze containers" in capsys.readouterr().out
    assert [c.args[0] for c in run.call_args_list if c.args[0][2] in ("stop", "wait", "rm")] == [
        ["docker", "container", "stop", "container"],
        ["docker", "container", "wait", "container"],
        ["docker", "container", "rm", "--force", "--volumes", "container"],
    ]


def test_down_skips_removal_of_containers_that_removed_themselves(docker_resources):
    resources, run = docker_resources
    labels = {"com.docker.compose.project": "breeze"}
    resources["container"] = [{"Id": "container", "Config": {"Labels": labels}}]
    resources["volume"] = [{"Name": "database", "Labels": labels}]
    docker = run.side_effect

    def remove_on_stop(cmd, **kwargs):
        if cmd[1:3] == ["container", "stop"]:
            resources["container"].clear()
        return docker(cmd, **kwargs)

    run.side_effect = remove_on_stop
    assert docker_command_utils.bring_compose_projects_down() == ["breeze"]

    assert [c.args[0] for c in run.call_args_list if c.args[0][2] == "rm"] == [
        ["docker", "volume", "rm", "database"]
    ]


def test_startup_cleanup_reports_containers_that_cannot_be_listed_after_stop(
    docker_resources, tmp_path, capsys
):
    resources, run = docker_resources
    labels = {
        "com.docker.compose.project": "stale",
        "org.apache.airflow.breeze": "true",
        "org.apache.airflow.breeze.worktree": str(tmp_path / "deleted"),
    }
    resources["container"] = [{"Id": "remaining", "Config": {"Labels": labels}}]
    docker = run.side_effect
    stopped = False

    def fail_listing_after_stop(cmd, **kwargs):
        nonlocal stopped
        result = docker(cmd, **kwargs)
        if cmd[1:3] == ["container", "stop"]:
            stopped = True
        if stopped and cmd[1:3] == ["container", "ls"]:
            result.returncode = 1
        return result

    run.side_effect = fail_listing_after_stop
    assert docker_command_utils.bring_compose_projects_down(stale_only=True) == []
    assert capsys.readouterr().out.count("Unable to clean up some deleted-worktree resources") == 1


def test_down_removes_volumes_even_when_a_shared_network_is_still_in_use(docker_resources):
    resources, run = docker_resources
    labels = {"com.docker.compose.project": "breeze"}
    resources["network"] = [{"Id": "shared-network", "Labels": labels}]
    resources["volume"] = [{"Name": "database", "Labels": labels}]
    docker = run.side_effect

    def fail(cmd, **kwargs):
        if cmd[1:3] == ["network", "rm"]:
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, cmd)
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="active endpoints")
        return docker(cmd, **kwargs)

    run.side_effect = fail
    with pytest.raises(subprocess.CalledProcessError) as error:
        docker_command_utils.bring_compose_projects_down()

    assert error.value.cmd == ["docker", "network", "rm", "shared-network"]
    assert any(c.args[0] == ["docker", "volume", "rm", "database"] for c in run.call_args_list)


def test_down_dry_run_reads_metadata_without_removing_resources():
    def docker(cmd, **kwargs):
        assert cmd[2] in ("ls", "inspect")
        if cmd[1:3] == ["volume", "ls"]:
            output = "database\n"
        elif cmd[2] == "inspect":
            output = json.dumps([{"Name": "database", "Labels": {"com.docker.compose.project": "breeze"}}])
        else:
            output = ""
        return subprocess.CompletedProcess(cmd, 0, stdout=output, stderr="")

    with (
        mock.patch("airflow_breeze.utils.run_utils.subprocess.run", autospec=True, side_effect=docker),
        mock.patch(
            "airflow_breeze.utils.run_utils.get_dry_run",
            autospec=True,
            side_effect=lambda override: True if override is None else override,
        ),
    ):
        assert docker_command_utils.bring_compose_projects_down() == ["breeze"]


def _shell_params_for_openlineage(
    backend: str, postgres_version: str, integration: tuple[str, ...] = ("openlineage",)
) -> mock.MagicMock:
    shell_params = mock.MagicMock()
    shell_params.use_airflow_version = None
    shell_params.restart = False
    shell_params.include_mypy_volume = False
    shell_params.include_pycache_volume = False
    shell_params.quiet = True
    shell_params.project_name = None
    shell_params.tty = "disabled"
    shell_params.command_passed = None
    shell_params.integration = integration
    shell_params.backend = backend
    shell_params.postgres_version = postgres_version
    return shell_params


@mock.patch("airflow_breeze.utils.docker_command_utils.fix_ownership_using_docker")
@mock.patch("airflow_breeze.utils.docker_command_utils.cleanup_python_generated_files")
@mock.patch("airflow_breeze.utils.docker_command_utils.read_from_cache_file", return_value="1")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@pytest.mark.parametrize("integration", [("openlineage",), ("all",)])
@pytest.mark.parametrize("postgres_version", CURRENT_POSTGRES_VERSIONS)
def test_enter_shell_openlineage_allows_current_postgres_versions(
    mock_run_command,
    mock_console_print,
    _mock_read_cache,
    _mock_cleanup,
    _mock_fix_ownership,
    postgres_version,
    integration,
):
    mock_run_command.return_value.returncode = 0
    shell_params = _shell_params_for_openlineage("postgres", postgres_version, integration)
    enter_shell(shell_params)
    mock_run_command.assert_called_once()


@mock.patch("airflow_breeze.utils.docker_command_utils.fix_ownership_using_docker")
@mock.patch("airflow_breeze.utils.docker_command_utils.cleanup_python_generated_files")
@mock.patch("airflow_breeze.utils.docker_command_utils.read_from_cache_file", return_value="1")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@pytest.mark.parametrize("integration", [("openlineage",), ("all",)])
@pytest.mark.parametrize("postgres_version", set(ALLOWED_POSTGRES_VERSIONS) - set(CURRENT_POSTGRES_VERSIONS))
def test_enter_shell_openlineage_rejects_stale_postgres_versions(
    mock_run_command,
    mock_console_print,
    _mock_read_cache,
    _mock_cleanup,
    _mock_fix_ownership,
    postgres_version,
    integration,
):
    shell_params = _shell_params_for_openlineage("postgres", postgres_version, integration)
    with pytest.raises(SystemExit) as exc_info:
        enter_shell(shell_params)
    assert exc_info.value.code == 1
    error_message = mock_console_print.call_args[0][0]
    assert all(version in error_message for version in CURRENT_POSTGRES_VERSIONS)
    mock_run_command.assert_not_called()


@mock.patch("airflow_breeze.utils.docker_command_utils.fix_ownership_using_docker")
@mock.patch("airflow_breeze.utils.docker_command_utils.cleanup_python_generated_files")
@mock.patch("airflow_breeze.utils.docker_command_utils.read_from_cache_file", return_value="1")
@mock.patch("airflow_breeze.utils.docker_command_utils.console_print")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@pytest.mark.parametrize("integration", [("openlineage",), ("all",)])
def test_enter_shell_openlineage_rejects_non_postgres_backend(
    mock_run_command, mock_console_print, _mock_read_cache, _mock_cleanup, _mock_fix_ownership, integration
):
    shell_params = _shell_params_for_openlineage("mysql", CURRENT_POSTGRES_VERSIONS[0], integration)
    with pytest.raises(SystemExit) as exc_info:
        enter_shell(shell_params)
    assert exc_info.value.code == 1
    mock_run_command.assert_not_called()


CI_IMAGE = "ghcr.io/apache/airflow/main/ci/python3.11:latest"


def _fake_docker_calls(present_images: set[str], failing_pulls: dict[str, int]):
    """
    Builds a run_command side effect emulating docker for the pull helpers.

    :param present_images: images `docker image inspect` reports as already available
    :param failing_pulls: how many times `docker pull` fails for a given image before succeeding
    """
    remaining_failures = dict(failing_pulls)

    def _run_command(cmd, **kwargs):
        result = mock.MagicMock()
        result.returncode = 0
        if cmd[:2] == ["docker", "compose"]:
            result.stdout = f"{CI_IMAGE}\npostgres:17\notel/opentelemetry-collector-contrib:0.155.0\n"
        elif cmd[:3] == ["docker", "image", "inspect"]:
            result.returncode = 0 if cmd[3] in present_images else 1
        elif cmd[:2] == ["docker", "pull"]:
            if remaining_failures.get(cmd[2], 0) > 0:
                remaining_failures[cmd[2]] -= 1
                result.returncode = 1
        return result

    return _run_command


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
def test_get_images_to_pull_skips_present_and_skipped_images(mock_run_command):
    mock_run_command.side_effect = _fake_docker_calls(present_images={"postgres:17"}, failing_pulls={})

    images = get_images_to_pull("breeze-test", env={}, skip_images={CI_IMAGE})

    assert images == ["otel/opentelemetry-collector-contrib:0.155.0"]


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
def test_get_images_to_pull_returns_nothing_when_compose_config_fails(mock_run_command):
    mock_run_command.return_value = mock.MagicMock(returncode=1, stdout="")

    assert get_images_to_pull("breeze-test", env={}, skip_images=set()) == []


@mock.patch("airflow_breeze.utils.docker_command_utils.time.sleep")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
def test_pull_images_with_retries_recovers_from_transient_failures(mock_run_command, mock_sleep):
    otel_image = "otel/opentelemetry-collector-contrib:0.155.0"
    mock_run_command.side_effect = _fake_docker_calls(
        present_images={"postgres:17"}, failing_pulls={otel_image: 2}
    )

    assert pull_images_with_retries("breeze-test", env={}, skip_images={CI_IMAGE}) is True
    assert mock_sleep.call_args_list == [call(15), call(30)]


@mock.patch("airflow_breeze.utils.docker_command_utils.time.sleep")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
def test_pull_images_with_retries_gives_up_after_all_attempts(mock_run_command, mock_sleep):
    otel_image = "otel/opentelemetry-collector-contrib:0.155.0"
    mock_run_command.side_effect = _fake_docker_calls(
        present_images={"postgres:17"}, failing_pulls={otel_image: 99}
    )

    assert pull_images_with_retries("breeze-test", env={}, skip_images={CI_IMAGE}, attempts=3) is False
    assert mock_sleep.call_args_list == [call(15), call(30)]


@mock.patch("airflow_breeze.utils.docker_command_utils.time.sleep")
@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
def test_pull_images_with_retries_does_not_pull_when_all_images_are_present(mock_run_command, mock_sleep):
    mock_run_command.side_effect = _fake_docker_calls(
        present_images={"postgres:17", "otel/opentelemetry-collector-contrib:0.155.0"}, failing_pulls={}
    )

    assert pull_images_with_retries("breeze-test", env={}, skip_images={CI_IMAGE}) is True
    assert not any(c.args[0][:2] == ["docker", "pull"] for c in mock_run_command.call_args_list)
    mock_sleep.assert_not_called()


@mock.patch("airflow_breeze.utils.docker_command_utils.calculate_ci_sources_hash")
@mock.patch("airflow_breeze.utils.docker_command_utils.check_if_buildx_plugin_installed")
def test_prepare_docker_build_command_labels_ci_image_with_sources_hash(
    mock_check_if_buildx_plugin_installed, mock_calculate_ci_sources_hash
):
    mock_check_if_buildx_plugin_installed.return_value = False
    mock_calculate_ci_sources_hash.return_value = "hash-of-sources"
    command = prepare_docker_build_command(BuildCiParams())
    label_index = command.index("--label")
    assert command[label_index + 1] == f"{CI_IMAGE_SOURCES_HASH_LABEL}=hash-of-sources"


@mock.patch("airflow_breeze.utils.docker_command_utils.check_if_buildx_plugin_installed")
def test_prepare_docker_build_command_does_not_add_sources_hash_label_to_prod_image(
    mock_check_if_buildx_plugin_installed,
):
    mock_check_if_buildx_plugin_installed.return_value = False
    command = prepare_docker_build_command(BuildProdParams())
    assert not any(flag.startswith(CI_IMAGE_SOURCES_HASH_LABEL) for flag in command)


@mock.patch("airflow_breeze.utils.docker_command_utils.run_command")
@mock.patch("airflow_breeze.utils.docker_command_utils.get_main_git_dir_for_worktree", return_value=None)
@mock.patch("airflow_breeze.utils.docker_command_utils.get_host_group_id", return_value=1000)
@mock.patch("airflow_breeze.utils.docker_command_utils.get_host_user_id", return_value=1000)
@mock.patch("airflow_breeze.utils.docker_command_utils.get_host_os", return_value="linux")
@mock.patch("airflow_breeze.utils.docker_command_utils.is_docker_rootless")
@pytest.mark.parametrize(
    ("rootless", "expected"),
    [(True, "DOCKER_IS_ROOTLESS=true"), (False, "DOCKER_IS_ROOTLESS=false")],
)
def test_fix_ownership_using_docker_passes_lowercase_rootless_flag(
    mock_is_docker_rootless,
    _mock_get_host_os,
    _mock_get_host_user_id,
    _mock_get_host_group_id,
    _mock_get_main_git_dir,
    mock_run_command,
    rootless,
    expected,
):
    """The in-container script compares the flag with lowercase ``true``, so ``True`` would never skip."""
    mock_is_docker_rootless.return_value = rootless
    fix_ownership_using_docker()
    docker_command = mock_run_command.call_args[0][0]
    assert expected in docker_command
