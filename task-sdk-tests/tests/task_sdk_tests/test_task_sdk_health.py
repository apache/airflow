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
from pathlib import Path
from shutil import copyfile

from python_on_whales import DockerClient, docker
from rich.console import Console

from task_sdk_tests.constants import (
    DOCKER_COMPOSE_FILE_PATH,
    DOCKER_IMAGE,
    TASK_SDK_API_VERSION,
    TASK_SDK_HOST_PORT,
)

console = Console(width=400, color_system="standard")


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


def debug_environment():
    """Debug the Python environment setup in CI."""

    import os
    import subprocess
    import sys

    console.print("[yellow]===== CI ENVIRONMENT DEBUG =====")
    console.print(f"[blue]Python executable: {sys.executable}")
    console.print(f"[blue]Python version: {sys.version}")
    console.print(f"[blue]Working directory: {os.getcwd()}")
    console.print(f"[blue]VIRTUAL_ENV: {os.environ.get('VIRTUAL_ENV', 'Not set')}")
    console.print(f"[blue]PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")

    console.print(f"[blue]Python executable exists: {Path(sys.executable).exists()}")
    if Path(sys.executable).is_symlink():
        console.print(f"[blue]Python executable is symlink to: {Path(sys.executable).readlink()}")

    try:
        uv_python = subprocess.check_output(["uv", "python", "find"], text=True).strip()
        console.print(f"[cyan]UV Python: {uv_python}")
        console.print(f"[green]Match: {uv_python == sys.executable}")

        console.print(f"[cyan]UV Python exists: {Path(uv_python).exists()}")
        if Path(uv_python).is_symlink():
            console.print(f"[cyan]UV Python is symlink to: {Path(uv_python).readlink()}")
    except Exception as e:
        console.print(f"[red]UV Python error: {e}")

    # Check what's installed in current environment
    try:
        import airflow

        console.print(f"[green]✅ airflow already available: {airflow.__file__}")
    except ImportError:
        console.print("[red]❌ airflow not available in current environment")

    console.print("[yellow]================================")


def test_task_sdk_health(tmp_path_factory, monkeypatch):
    """Test Task SDK health check using docker-compose environment."""
    tmp_dir = tmp_path_factory.mktemp("airflow-task-sdk-test")
    console.print(f"[yellow]Tests are run in {tmp_dir}")

    # Copy docker-compose.yaml to temp directory
    tmp_docker_compose_file = tmp_dir / "docker-compose.yaml"
    copyfile(DOCKER_COMPOSE_FILE_PATH, tmp_docker_compose_file)

    # Set environment variables for the test
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", DOCKER_IMAGE)
    monkeypatch.setenv("TASK_SDK_VERSION", os.environ.get("TASK_SDK_VERSION", "1.0.3"))

    # Initialize Docker client
    compose = DockerClient(compose_files=[str(tmp_docker_compose_file)])

    try:
        compose.compose.up(detach=True, wait=True)
        console.print("[green]Docker compose started for task SDK test\n")

        try:
            from airflow.sdk.api.client import Client

            console.print("[green]✅ Task SDK client imported successfully!")
        except ImportError as e:
            console.print(f"[red]❌ Failed to import Task SDK client: {e}")
            raise

        client = Client(base_url=f"http://{TASK_SDK_HOST_PORT}/execution", token="not-a-token")

        console.print("[yellow]Making health check request...")
        response = client.get("health/ping", headers={"Airflow-API-Version": TASK_SDK_API_VERSION})

        console.print(" Health Check Response ".center(72, "="))
        console.print(f"[bright_blue]Status Code:[/] {response.status_code}")
        console.print(f"[bright_blue]Response:[/] {response.json()}")
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
