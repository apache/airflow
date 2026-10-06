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

import hashlib
import os
from unittest.mock import patch

import click
import pytest
import yaml
from click.testing import CliRunner
from rich.console import Console

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.commands.common_options import option_project_name
from airflow_breeze.global_constants import MOUNT_SELECTED, PYCACHE_PREFIX_IN_CONTAINER
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.path_utils import (
    SCRIPTS_CI_DOCKER_COMPOSE_BASE_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_YAML_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_MOUNT_UV_LOCK_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_PYCACHE_PATH,
)

console = Console(width=400, color_system="standard")


@pytest.mark.parametrize("isolation", [None, "false"])
@pytest.mark.parametrize("linked_worktree", [False, True])
@pytest.mark.parametrize("explicit_project", [None, "foobar", "breeze"])
def test_worktree_project_default_and_override(
    tmp_path, monkeypatch, linked_worktree, explicit_project, isolation
):
    if isolation:
        monkeypatch.setenv("BREEZE_WORKTREE_ISOLATION", isolation)
    else:
        monkeypatch.delenv("BREEZE_WORKTREE_ISOLATION", raising=False)
    root = tmp_path / "My Worktree"
    with (
        patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", root),
        patch(
            "airflow_breeze.utils.path_utils.get_main_git_dir_for_worktree",
            autospec=True,
            return_value=tmp_path / ".git" if linked_worktree else None,
        ),
    ):
        params = ShellParams(**({"project_name": explicit_project} if explicit_project else {}))
        sqlite_file = params.get_backend_compose_files("sqlite")[0].name

        @click.command()
        @option_project_name
        def command(project_name):
            click.echo(project_name)

        result = CliRunner().invoke(
            command,
            ["--project-name", explicit_project] if explicit_project else [],
            env={"PROJECT_NAME": None},
        )
    path_hash = hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:6]
    isolated = linked_worktree and not isolation
    assert params.project_name == (
        explicit_project or (f"breeze-my-worktree-{path_hash}" if isolated else "breeze")
    )
    assert result.exit_code == 0
    assert result.output.strip() == params.project_name
    assert sqlite_file == (
        "backend-sqlite-no-volume.yml" if explicit_project == "foobar" else "backend-sqlite.yml"
    )


@pytest.mark.parametrize("isolation", [None, "false"])
@pytest.mark.parametrize("linked_worktree", [False, True])
def test_worktree_labels_are_derived_from_checkout(tmp_path, monkeypatch, linked_worktree, isolation):
    monkeypatch.setenv("BREEZE_WORKTREE_PATH", "/another/checkout")
    monkeypatch.setenv("BREEZE_HOST_ID", "another-host")
    if isolation:
        monkeypatch.setenv("BREEZE_WORKTREE_ISOLATION", isolation)
    else:
        monkeypatch.delenv("BREEZE_WORKTREE_ISOLATION", raising=False)
    with (
        patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", tmp_path),
        patch(
            "airflow_breeze.utils.path_utils.get_main_git_dir_for_worktree",
            autospec=True,
            return_value=tmp_path / ".git" if linked_worktree else None,
        ),
        patch("airflow_breeze.utils.path_utils.socket.gethostname", autospec=True, return_value="this-host"),
    ):
        env = ShellParams().env_variables_for_docker_commands

    isolated = linked_worktree and not isolation
    assert env["BREEZE_WORKTREE_PATH"] == (str(tmp_path.resolve()) if isolated else "")
    assert env["BREEZE_HOST_ID"] == "this-host"


@pytest.mark.parametrize(
    ("env_vars", "kwargs", "expected_vars"),
    [
        pytest.param(
            {},
            {"python": "3.13"},
            {
                "DEFAULT_BRANCH": AIRFLOW_BRANCH,
                "AIRFLOW_CI_IMAGE": f"ghcr.io/apache/airflow/{AIRFLOW_BRANCH}/ci/python3.13",
                "PYTHON_MAJOR_MINOR_VERSION": "3.13",
            },
            id="python3.13",
        ),
        pytest.param(
            {},
            {"python": "3.10"},
            {
                "AIRFLOW_CI_IMAGE": f"ghcr.io/apache/airflow/{AIRFLOW_BRANCH}/ci/python3.10",
                "PYTHON_MAJOR_MINOR_VERSION": "3.10",
            },
            id="python3.10",
        ),
        pytest.param(
            {},
            {"airflow_branch": "v3-0-test"},
            {
                "DEFAULT_BRANCH": "v3-0-test",
                "AIRFLOW_CI_IMAGE": "ghcr.io/apache/airflow/v3-0-test/ci/python3.10",
                "PYTHON_MAJOR_MINOR_VERSION": "3.10",
            },
            id="With release branch",
        ),
        pytest.param(
            {"DEFAULT_BRANCH": "v3-0-test"},
            {},
            {
                "DEFAULT_BRANCH": AIRFLOW_BRANCH,  # DEFAULT_BRANCH is overridden from sources
                "AIRFLOW_CI_IMAGE": f"ghcr.io/apache/airflow/{AIRFLOW_BRANCH}/ci/python3.10",
                "PYTHON_MAJOR_MINOR_VERSION": "3.10",
            },
            id="Branch variable from sources not from original env",
        ),
        pytest.param(
            {},
            {},
            {
                "FLOWER_HOST_PORT": "25555",
            },
            id="Default flower port",
        ),
        pytest.param(
            {"FLOWER_HOST_PORT": "1234"},
            {},
            {
                "FLOWER_HOST_PORT": "1234",
            },
            id="Overridden flower host",
        ),
        pytest.param(
            {},
            {"celery_broker": "redis"},
            {
                "AIRFLOW__CELERY__BROKER_URL": "redis://redis:6379/0",
            },
            id="Celery executor with redis broker",
        ),
        pytest.param(
            {},
            {"celery_broker": "unknown"},
            {
                "AIRFLOW__CELERY__BROKER_URL": "",
            },
            id="No URL for celery if bad broker specified",
        ),
        pytest.param(
            {},
            {},
            {
                "CI_EVENT_TYPE": "pull_request",
            },
            id="Default CI event type",
        ),
        pytest.param(
            {"CI_EVENT_TYPE": "push"},
            {},
            {
                "CI_EVENT_TYPE": "push",
            },
            id="Override CI event type by variable",
        ),
        pytest.param(
            {},
            {},
            {
                "INIT_SCRIPT_FILE": "init.sh",
            },
            id="Default init script file",
        ),
        pytest.param(
            {"INIT_SCRIPT_FILE": "my_init.sh"},
            {},
            {
                "INIT_SCRIPT_FILE": "my_init.sh",
            },
            id="Override init script file by variable",
        ),
        pytest.param(
            {},
            {},
            {
                "CI": "false",
            },
            id="CI false by default",
        ),
        pytest.param(
            {"CI": "true"},
            {},
            {
                "CI": "true",
            },
            id="Unless it's overridden by environment variable",
        ),
        pytest.param(
            {},
            {},
            {
                "PYTHONWARNINGS": None,
            },
            id="PYTHONWARNINGS should not be set by default",
        ),
        pytest.param(
            {"PYTHONWARNINGS": "default"},
            {},
            {
                "PYTHONWARNINGS": "default",
            },
            id="PYTHONWARNINGS should be set when specified in environment",
        ),
        pytest.param(
            {},
            {},
            {"POSTGRES_DRIVER": "psycopg"},
            id="POSTGRES_DRIVER defaults to psycopg (v3)",
        ),
        pytest.param(
            {},
            {"use_airflow_version": "2.11.0"},
            {"POSTGRES_DRIVER": "psycopg2"},
            id="POSTGRES_DRIVER falls back to psycopg2 on Airflow 2.x (SQLAlchemy 1.4)",
        ),
        pytest.param(
            {},
            {"use_airflow_version": "3.1.0"},
            {"POSTGRES_DRIVER": "psycopg2"},
            id="POSTGRES_DRIVER falls back to psycopg2 on released Airflow 3.x",
        ),
        pytest.param(
            {},
            {"use_airflow_version": "wheel"},
            {"POSTGRES_DRIVER": "psycopg"},
            id="POSTGRES_DRIVER stays psycopg when installing from sources",
        ),
        pytest.param(
            {},
            {"use_airflow_version": "70496"},
            {"POSTGRES_DRIVER": "psycopg"},
            id="POSTGRES_DRIVER stays psycopg when installing from a PR number",
        ),
        pytest.param(
            {},
            {"use_airflow_version": "apache/airflow:main"},
            {"POSTGRES_DRIVER": "psycopg"},
            id="POSTGRES_DRIVER stays psycopg when installing from a GitHub branch",
        ),
        pytest.param(
            {},
            {"backend": "postgres", "postgres_version": "17"},
            {"POSTGRES_DATA_VOLUME_PATH": "/var/lib/postgresql/data"},
            id="POSTGRES_DATA_VOLUME_PATH is the data directory up to Postgres 17",
        ),
        pytest.param(
            {},
            {"backend": "postgres", "postgres_version": "18"},
            {"POSTGRES_DATA_VOLUME_PATH": "/var/lib/postgresql"},
            id="POSTGRES_DATA_VOLUME_PATH is its parent from Postgres 18",
        ),
        pytest.param(
            {},
            {"backend": "none", "postgres_version": ""},
            {"POSTGRES_DATA_VOLUME_PATH": "/var/lib/postgresql/data"},
            id="POSTGRES_DATA_VOLUME_PATH ignores the empty version of non-postgres backends",
        ),
    ],
)
def test_shell_params_to_env_var_conversion(
    env_vars: dict[str, str], kwargs: dict[str, str | bool], expected_vars: dict[str, str]
):
    with patch("os.environ", env_vars):
        shell_params = ShellParams(**kwargs)
        env_vars = shell_params.env_variables_for_docker_commands
        error = False
        for expected_key, expected_value in expected_vars.items():
            if expected_key not in env_vars:
                if expected_value is not None:
                    console.print(f"[red] Expected variable {expected_key} missing.[/]\nVariables retrieved:")
                    console.print(env_vars)
                    error = True
            elif expected_key is None:
                console.print(f"[red] The variable {expected_key} is not expected.[/]\nVariables retrieved:")
                console.print(env_vars)
                error = True
            elif env_vars[expected_key] != expected_value:
                console.print(
                    f"[red] The expected variable {expected_key} value '{env_vars[expected_key]}' is different than expected {expected_value}[/]\n"
                    f"Variables retrieved:"
                )
                console.print(env_vars)
                error = True
        assert not error, "Some values are not as expected."


def test_generated_env_files_do_not_change_when_pythonwarnings_is_set(tmp_path, monkeypatch):
    docker_env_path = tmp_path / "_generated_docker.env"
    compose_env_path = tmp_path / "_generated_docker_compose.env"
    with (
        patch("airflow_breeze.params.shell_params.GENERATED_DOCKER_ENV_PATH", docker_env_path),
        patch("airflow_breeze.params.shell_params.GENERATED_DOCKER_COMPOSE_ENV_PATH", compose_env_path),
        patch("airflow_breeze.params.shell_params.GENERATED_DOCKER_LOCK_PATH", tmp_path / "_generated.lock"),
    ):
        monkeypatch.delenv("PYTHONWARNINGS", raising=False)
        _ = ShellParams().env_variables_for_docker_commands
        docker_env = docker_env_path.read_text()
        compose_env = compose_env_path.read_text()
        monkeypatch.setenv("PYTHONWARNINGS", "default")
        _ = ShellParams().env_variables_for_docker_commands
        assert docker_env_path.read_text() == docker_env
        assert compose_env_path.read_text() == compose_env


def test_pythonwarnings_is_forwarded_by_the_compose_base_file():
    base_compose_file = yaml.safe_load(SCRIPTS_CI_DOCKER_COMPOSE_BASE_PATH.read_text())
    assert "PYTHONWARNINGS" in base_compose_file["services"]["airflow"]["environment"]


def test_postgres_data_volume_is_mounted_at_the_image_volume_path():
    backend_compose_file = yaml.safe_load(
        (SCRIPTS_CI_DOCKER_COMPOSE_PATH / "backend-postgres.yml").read_text()
    )
    assert (
        "postgres-data-volume:${POSTGRES_DATA_VOLUME_PATH:-/var/lib/postgresql/data}"
        in backend_compose_file["services"]["postgres"]["volumes"]
    )


@pytest.mark.parametrize(
    ("include_pycache_volume", "expected_prefix", "expected_dont_write"),
    [(True, PYCACHE_PREFIX_IN_CONTAINER, ""), (False, "", "true")],
)
def test_bytecode_cache_is_enabled_only_together_with_its_volume(
    include_pycache_volume: bool, expected_prefix: str, expected_dont_write: str
):
    env_vars = ShellParams(include_pycache_volume=include_pycache_volume).env_variables_for_docker_commands
    assert env_vars["PYTHONPYCACHEPREFIX"] == expected_prefix
    assert env_vars["PYTHONDONTWRITEBYTECODE"] == expected_dont_write


@pytest.mark.parametrize(("include_pycache_volume", "expected_count"), [(True, 1), (False, 0)])
def test_pycache_volume_compose_file_is_included_only_when_requested(
    include_pycache_volume: bool, expected_count: int
):
    compose_files = ShellParams(include_pycache_volume=include_pycache_volume).compose_file.split(os.pathsep)
    assert compose_files.count(str(SCRIPTS_CI_DOCKER_COMPOSE_PYCACHE_PATH)) == expected_count


def test_include_mypy_volume_adds_mypy_compose_file():
    compose_files = ShellParams(include_mypy_volume=True).compose_file.split(":")
    assert str(SCRIPTS_CI_DOCKER_COMPOSE_PATH / "mypy.yml") in compose_files


@pytest.mark.parametrize(("force_lowest_dependencies", "expected_count"), [(True, 0), (False, 1)])
def test_uv_lock_is_not_mounted_for_lowest_dependencies(force_lowest_dependencies: bool, expected_count: int):
    """The lowest-direct ``uv sync`` rewrites uv.lock, so its mount is dropped for that run."""
    compose_files = ShellParams(
        mount_sources=MOUNT_SELECTED, force_lowest_dependencies=force_lowest_dependencies
    ).compose_file.split(os.pathsep)
    assert compose_files.count(str(SCRIPTS_CI_DOCKER_COMPOSE_MOUNT_UV_LOCK_PATH)) == expected_count


def test_uv_lock_is_mounted_only_by_its_own_compose_file():
    """Guards the split: a stray uv.lock bind in local.yml would defeat the skip above."""
    local_compose_file = yaml.safe_load(SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_YAML_PATH.read_text())
    # local.yml mixes named volumes (plain "source:target" strings) with bind mappings.
    local_volumes = local_compose_file["services"]["airflow"]["volumes"]
    targets = [volume["target"] if isinstance(volume, dict) else volume for volume in local_volumes]
    assert not any("uv.lock" in target for target in targets)

    uv_lock_compose_file = yaml.safe_load(SCRIPTS_CI_DOCKER_COMPOSE_MOUNT_UV_LOCK_PATH.read_text())
    assert uv_lock_compose_file["services"]["airflow"]["volumes"] == [
        {"type": "bind", "source": "../../../uv.lock", "target": "/opt/airflow/uv.lock"}
    ]
