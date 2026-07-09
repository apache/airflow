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
import platform
import re
import subprocess
import sys
import time

import pytest
import requests
from constants import (
    API_BASE_URL,
    DOCKER_COMPOSE_FILE_PATH,
    DOCKER_IMAGE,
    ENV_FILE,
    LOCAL_DOCKER_COMPOSE_FILE_PATH,
    MIN_DOCKER_COMPOSE_VERSION,
    MIN_DOCKER_VERSION,
    OPENLINEAGE_E2E_TESTS_ROOT,
    console,
)
from packaging import version

from tests_common.test_utils.fernet import generate_fernet_key_string

COMPOSE_PROJECT_NAME = "breeze-openlineage-e2e"
FALSEY_VALUES = ("false", "0")


def _env_flag(name: str) -> bool:
    """True when the environment variable is set to a non-falsey value."""
    value = os.environ.get(name, "")
    return bool(value) and value.lower() not in FALSEY_VALUES


def _compare_gte(a: str, b: str) -> bool:
    try:
        return version.parse(a) >= version.parse(b)
    except Exception:
        return False


def _docker_is_running() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, check=False, timeout=10).returncode == 0
        )
    except Exception:
        return False


def _tool_version(command: list[str]) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, timeout=10)
        if result.returncode != 0:
            return "unknown"
        match = re.search(r"(\d+)\.(\d+)\.(\d+)", result.stdout.strip())
        return ".".join(match.groups()) if match else "unknown"
    except Exception:
        return "unknown"


def check_docker_requirements() -> None:
    """Verify Docker is running and Docker / Docker Compose meet the minimum versions."""
    console.print("[yellow]Checking Docker requirements...[/]")
    if not _docker_is_running():
        console.print("[red]Docker is not running. Please start Docker and try again.[/]")
        sys.exit(1)

    docker_version = _tool_version(["docker", "version", "--format", "{{.Client.Version}}"])
    if not _compare_gte(docker_version, MIN_DOCKER_VERSION):
        console.print(f"[red]Docker version {docker_version} is too old (min {MIN_DOCKER_VERSION}).[/]")
        sys.exit(1)
    console.print(f"[green]Docker version {docker_version} meets requirements[/]")

    compose_version = _tool_version(["docker", "compose", "version"])
    if not _compare_gte(compose_version, MIN_DOCKER_COMPOSE_VERSION):
        console.print(
            f"[red]Docker Compose version {compose_version} is too old (min {MIN_DOCKER_COMPOSE_VERSION}).[/]"
        )
        sys.exit(1)
    console.print(f"[green]Docker Compose version {compose_version} meets requirements[/]")


def _print_diagnostics(compose) -> None:
    console.print("[red]=== DIAGNOSTIC INFORMATION ===[/]")
    try:
        for container in compose.compose.ps():
            console.print(f"  {container.name}: {container.state}")
        console.print("\n[yellow]Container Logs:[/]")
        console.print(compose.compose.logs())
    except Exception as exc:
        console.print(f"  Error collecting diagnostics: {exc}")


def pytest_sessionstart(session):
    """Check Docker and source the OpenLineage DAGs into the dags folder before the stack starts."""
    check_docker_requirements()

    # prepare_dags lives at the project root (next to docker-compose.yaml), not in the test package.
    sys.path.insert(0, OPENLINEAGE_E2E_TESTS_ROOT.as_posix())
    from prepare_dags import prepare_dags

    console.print("[yellow]Preparing OpenLineage DAGs from the provider system tests...[/]")
    dags_folder = prepare_dags()
    console.print(f"[green]Prepared dags in {dags_folder}[/]")


@pytest.fixture(scope="session")
def docker_compose_setup():
    """Start docker-compose once per session and tear it down at the end."""
    from python_on_whales import DockerClient

    debugging_on = _env_flag("VERBOSE")
    mount_volumes = not _env_flag("SKIP_MOUNTING_LOCAL_VOLUMES")
    delete_compose = not _env_flag("SKIP_DOCKER_COMPOSE_DELETION")
    if mount_volumes:
        delete_compose = False

    with open(ENV_FILE, "w") as f:
        print(f"AIRFLOW_IMAGE_NAME={DOCKER_IMAGE}", file=f)
        print(f"AIRFLOW_UID={os.getuid()}", file=f)
        print(f"HOST_OS={platform.system().lower()}", file=f)
        # Please do not reuse this Fernet key in any deployment — it is generated only for these tests.
        print(f"FERNET_KEY={generate_fernet_key_string()}", file=f)

    docker_compose_files = [DOCKER_COMPOSE_FILE_PATH.as_posix()]
    if mount_volumes:
        docker_compose_files.append(LOCAL_DOCKER_COMPOSE_FILE_PATH.as_posix())
    log_level = "debug" if debugging_on else "info"

    compose = DockerClient(
        compose_files=docker_compose_files,
        debug=debugging_on,
        log_level=log_level,
        compose_project_name=COMPOSE_PROJECT_NAME,
    )

    start_new_compose = True
    processes = compose.compose.ps(["airflow-apiserver"])
    if processes and processes[0].state.status == "running":
        if mount_volumes:
            console.print(
                "[yellow]Docker compose already running. Reusing it. Run `docker compose down` to reset.[/]"
            )
            start_new_compose = False
        else:
            compose.compose.down(remove_orphans=True, volumes=True, quiet=True)

    try:
        if start_new_compose:
            console.print("[yellow]Starting docker-compose for session...[/]")
            files = " ".join(f'-f "{file}"' for file in docker_compose_files)
            console.print(f"[info]Equivalent manual command: docker compose {files} up --detach[/]")
            compose.compose.up(detach=True, wait=True)
            console.print("[green]Docker compose started successfully![/]")
        yield compose
    except Exception as exc:
        console.print(f"[red]Docker compose failed to start: {exc}[/]")
        _print_diagnostics(compose)
        raise
    finally:
        if delete_compose:
            console.print("[yellow]Cleaning up docker-compose...[/]")
            compose.compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose cleaned up[/]")


@pytest.fixture(scope="session")
def auth_headers(docker_compose_setup) -> dict[str, str]:
    """Wait for Airflow and return REST API auth headers (SimpleAuthManager grants an admin token)."""
    time.sleep(15)
    auth_response = requests.get(f"{API_BASE_URL}/auth/token", timeout=30)
    auth_response.raise_for_status()
    token = auth_response.json()["access_token"]
    console.print("[green]✅ Got Airflow auth token[/]")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
