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
from shutil import copyfile

from python_on_whales import DockerClient, docker
from rich.console import Console

from task_sdk_tests.constants import AIRFLOW_ROOT_PATH, TASK_SDK_API_VERSION, TASK_SDK_HOST_PORT

console = Console(width=400, color_system="standard")

DOCKER_COMPOSE_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")
AIRFLOW_WWW_USER_USERNAME = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_WWW_USER_PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")


def print_diagnostics(compose: DockerClient, compose_version: str, docker_version: str):
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


def test_task_sdk_health(default_docker_image, tmp_path_factory, monkeypatch):
    """Test Task SDK health check using docker-compose environment."""
    tmp_dir = tmp_path_factory.mktemp("airflow-task-sdk-test")
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", default_docker_image)
    console.print(f"[yellow]Tests are run in {tmp_dir}")

    compose_file_path = (
        AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "howto" / "docker-compose" / "docker-compose.yaml"
    )
    copyfile(compose_file_path, tmp_dir / "docker-compose.yaml")

    # Replace |version| placeholder with latest
    with open(tmp_dir / "docker-compose.yaml") as f:
        compose_config = f.read()
    compose_config = compose_config.replace("apache/airflow:|version|", default_docker_image)
    with open(tmp_dir / "docker-compose.yaml", "w") as f:
        f.write(compose_config)

    # Create required directories
    subfolders = ("dags", "logs", "plugins", "config")
    console.print(f"[yellow]Creating subfolders:[/ {subfolders}")
    for subdir in subfolders:
        (tmp_dir / subdir).mkdir()

    # Create .env file with proper UID
    dot_env_file = tmp_dir / ".env"
    console.print(f"[yellow]Creating .env file:[/ {dot_env_file}")
    dot_env_file.write_text(f"AIRFLOW_UID={os.getuid()}\n")
    console.print(" .env file content ".center(72, "="))
    console.print(dot_env_file.read_text())

    compose = DockerClient(compose_project_name="task-sdk-test", compose_project_directory=tmp_dir).compose
    compose.down(remove_orphans=True, volumes=True, quiet=True)

    try:
        compose.up(detach=True, wait=True)
        console.print("[green]Docker compose started for task SDK test")

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
            compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose instance deleted")
