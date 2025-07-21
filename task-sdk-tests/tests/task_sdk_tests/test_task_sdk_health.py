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
import sys
from pathlib import Path
from shutil import copyfile

from python_on_whales import DockerClient, docker
from rich.console import Console

from task_sdk_tests.constants import AIRFLOW_ROOT_PATH, TASK_SDK_API_VERSION, TASK_SDK_HOST_PORT

console = Console(width=400, color_system="standard")

TASK_SDK_TESTS_ROOT = Path(__file__).parent.parent.parent


def print_diagnostics(compose, compose_version, docker_version):
    """Print diagnostic information when test fails."""
    console.print("[red]=== DIAGNOSTIC INFORMATION ===[/]")
    console.print(f"Docker version: {docker_version}")
    console.print(f"Docker Compose version: {compose_version}")
    console.print("\n[yellow]Container Status:[/]")
    try:
        containers = compose.compose.ps()
        for container in containers:
            console.print(f"  {container.name}: {container.state}")
    except Exception as e:
        console.print(f"  Error getting container status: {e}")

    console.print("\n[yellow]Container Logs:[/]")
    try:
        logs = compose.compose.logs()
        console.print(logs)
    except Exception as e:
        console.print(f"  Error getting logs: {e}")


def test_task_sdk_health(tmp_path_factory, monkeypatch):
    """Test Task SDK health check using docker-compose environment."""
    tmp_dir = tmp_path_factory.mktemp("airflow-task-sdk-test")
    console.print(f"[yellow]Tests are run in {tmp_dir}")

    # Copy docker-compose.yaml to temp directory
    docker_compose_file = TASK_SDK_TESTS_ROOT / "docker" / "docker-compose.yaml"
    tmp_docker_compose_file = tmp_dir / "docker-compose.yaml"
    copyfile(docker_compose_file, tmp_docker_compose_file)

    # Set environment variables for the test
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", os.environ.get("DOCKER_IMAGE", "apache/airflow:3.0.3"))
    monkeypatch.setenv("TASK_SDK_VERSION", os.environ.get("TASK_SDK_VERSION", "1.0.3"))

    # Initialize Docker client
    compose = DockerClient(compose_files=[str(tmp_docker_compose_file)])

    try:
        compose.compose.up(detach=True, wait=True)
        console.print("[green]Docker compose started for task SDK test")

        task_sdk_version = os.environ.get("TASK_SDK_VERSION", "1.1.0")
        console.print(f"[yellow]Installing apache-airflow-task-sdk=={task_sdk_version}...")

        # Right now 1.1.0 is not yet released, and attempting to install 1.0.3 with 3.1.0 with fail due to an imcompat.
        # For now, installing from local path, will update once the compatibility is regained in 3.1.0

        task_sdk_path = AIRFLOW_ROOT_PATH / "task-sdk"
        console.print(f"[blue]Installing from: {task_sdk_path}")

        try:
            subprocess.check_call(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    "--no-deps",
                    str(task_sdk_path),
                ]
            )
            console.print("[green]Task SDK installed successfully!")
        except subprocess.CalledProcessError as e:
            console.print(f"[red]Failed to install Task SDK: {e}")
            console.print(f"[red]Command: {e.cmd}")
            raise

        # Now import the client after installation
        from airflow.sdk.api.client import Client

        client = Client(base_url=f"http://{TASK_SDK_HOST_PORT}/execution", token="not-a-token")

        console.print("[yellow]Making health check request...")
        response = client.get("health/ping", headers={"Airflow-API-Version": TASK_SDK_API_VERSION})

        console.print(" Health Check Response ".center(72, "="))
        console.print(f"[bright_blue]Status Code:[/] {response.status_code}")
        console.print("[bright_blue]Response Headers:[/]")
        for key, value in response.headers.items():
            console.print(f"  {key}: {value}")
        console.print("[bright_blue]Response Body:[/]")
        console.print(response.json())
        console.print("=" * 72)

        assert response.status_code == 200
        assert response.json() == {"ok": ["airflow.api_fastapi.auth.tokens.JWTValidator"], "failing": {}}

    except Exception:
        print_diagnostics(compose, compose.version(), docker.version())
        raise
    finally:
        if not os.environ.get("SKIP_DOCKER_COMPOSE_DELETION"):
            compose.compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose instance deleted")
