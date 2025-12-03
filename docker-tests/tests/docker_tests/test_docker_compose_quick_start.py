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
import os
import shlex
from shutil import copyfile
from time import sleep

import pytest
import requests
from python_on_whales import DockerClient, docker
from python_on_whales.exceptions import DockerException
from rich.console import Console

from docker_tests.command_utils import run_command
from docker_tests.constants import AIRFLOW_ROOT_PATH

from tests_common.test_utils.api_client_helpers import generate_access_token

console = Console(width=400, color_system="standard")

DOCKER_COMPOSE_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")
AIRFLOW_WWW_USER_USERNAME = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_WWW_USER_PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")
DAG_ID = "example_simplest_dag"
DAG_RUN_ID = "test_dag_run_id"


def api_request(
    method: str, path: str, base_url: str = f"http://{DOCKER_COMPOSE_HOST_PORT}/api/v2", **kwargs
) -> dict:
    access_token = generate_access_token(
        AIRFLOW_WWW_USER_USERNAME, AIRFLOW_WWW_USER_PASSWORD, DOCKER_COMPOSE_HOST_PORT
    )
    response = requests.request(
        method=method,
        url=f"{base_url}/{path}",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def wait_for_terminal_dag_state(dag_id, dag_run_id):
    console.print(f"[bright_blue]Simplified representation of DAG {dag_id} ".center(72, "="))
    console.print(api_request("GET", f"dags/{DAG_ID}/details"))

    # Wait 400 seconds
    for _ in range(400):
        dag_state = api_request("GET", f"dags/{dag_id}/dagRuns/{dag_run_id}").get("state")
        console.print(f"Waiting for DAG Run: dag_state={dag_state}")
        sleep(1)
        if dag_state in ("success", "failed"):
            break


def test_trigger_dag_and_wait_for_result(default_docker_image, tmp_path_factory, monkeypatch):
    """Simple test which reproduce setup docker-compose environment and trigger example dag."""
    tmp_dir = tmp_path_factory.mktemp("airflow-quick-start")
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", default_docker_image)
    console.print(f"[yellow]Tests are run in {tmp_dir}")

    compose_file_path = (
        AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "howto" / "docker-compose" / "docker-compose.yaml"
    )
    copyfile(compose_file_path, tmp_dir / "docker-compose.yaml")

    subfolders = ("dags", "logs", "plugins", "config")
    console.print(f"[yellow]Creating subfolders:{subfolders}[/]")
    # Create required directories for docker compose quick start howto
    for subdir in subfolders:
        (tmp_dir / subdir).mkdir()

    dot_env_file = tmp_dir / ".env"
    console.print(f"[yellow]Creating .env file :{dot_env_file}[/]")
    dot_env_file.write_text(f"AIRFLOW_UID={os.getuid()}\n")
    console.print(" .env file content ".center(72, "="))
    console.print(dot_env_file.read_text())

    compose_version = None
    try:
        compose_version = docker.compose.version()
    except DockerException:
        pytest.fail("`docker compose` not available. Make sure compose plugin is installed")
    try:
        docker_version = docker.version()
    except NotImplementedError:
        docker_version = run_command(["docker", "version"], return_output=True)

    console.print("[yellow] Shutting down previous instances of quick-start docker compose")
    compose = DockerClient(compose_project_name="quick-start", compose_project_directory=tmp_dir).compose
    compose.down(remove_orphans=True, volumes=True, quiet=True)
    try:
        console.print("[yellow] Starting docker compose")
        compose.up(detach=True, wait=True, color=not os.environ.get("NO_COLOR"))
        console.print("[green] Docker compose started")

        # Before we proceed, let's make sure our DAG has been parsed
        compose.execute(service="airflow-dag-processor", command=["airflow", "dags", "reserialize"])

        # Verify API server health endpoint is accessible and returns valid response
        health_status = api_request("GET", path="monitor/health").get("metadatabase").get("status")
        assert health_status == "healthy"

        api_request("PATCH", path=f"dags/{DAG_ID}", json={"is_paused": False})
        api_request(
            "POST",
            path=f"dags/{DAG_ID}/dagRuns",
            json={"dag_run_id": DAG_RUN_ID, "logical_date": "2020-06-11T18:00:00+00:00"},
        )

        wait_for_terminal_dag_state(dag_id=DAG_ID, dag_run_id=DAG_RUN_ID)
        dag_state = api_request("GET", f"dags/{DAG_ID}/dagRuns/{DAG_RUN_ID}").get("state")
        assert dag_state == "success"
        if os.environ.get("INCLUDE_SUCCESS_OUTPUTS", "") == "true":
            print_diagnostics(compose, compose_version, docker_version)
    except Exception:
        print_diagnostics(compose, compose_version, docker_version)
        raise
    finally:
        if not os.environ.get("SKIP_DOCKER_COMPOSE_DELETION"):
            console.print(
                "[yellow] Deleting docker compose instance (you can avoid that by passing "
                "--skip-docker-compose-deletion flag in `breeze testing docker-compose` or "
                'by setting SKIP_DOCKER_COMPOSE_DELETION environment variable to "true")'
            )
            compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose instance deleted")
        else:
            console.print("[yellow]Skipping docker-compose deletion")
            console.print()
            console.print(
                "[yellow]You can run inspect your docker-compose by running commands starting with:"
            )
            console.print()
            quoted_command = map(shlex.quote, map(str, compose.docker_compose_cmd))
            console.print(" ".join(quoted_command))


def print_diagnostics(compose: DockerClient, compose_version: str, docker_version: str):
    console.print("HTTP: GET health")
    try:
        console.print(api_request("GET", "monitor/health"))
        console.print(f"HTTP: GET dags/{DAG_ID}/dagRuns")
        console.print(api_request("GET", f"dags/{DAG_ID}/dagRuns"))
        console.print(f"HTTP: GET dags/{DAG_ID}/dagRuns/{DAG_RUN_ID}/taskInstances")
        console.print(api_request("GET", f"dags/{DAG_ID}/dagRuns/{DAG_RUN_ID}/taskInstances"))
    except Exception as e:
        console.print(f"Failed to get health check: {e}")
    console.print(" Docker Version ".center(72, "="))
    console.print(docker_version)
    console.print(" Docker Compose Version ".center(72, "="))
    console.print(compose_version)
    console.print(" Compose Config ".center(72, "="))
    console.print(json.dumps(compose.config(return_json=True), indent=4))
    for service in compose.ps(all=True):
        console.print(f"Service: {service.name} ".center(72, "="))
        console.print(f" Service State {service.name}".center(50, "."))
        console.print(service.state)
        console.print(f" Service Config {service.name}".center(50, "."))
        console.print(service.config)
        console.print(f" Service Logs {service.name}".center(50, "."))
        console.print(service.logs())
        console.print(f"End of service: {service.name} ".center(72, "="))
