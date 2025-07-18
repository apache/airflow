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
from pathlib import Path
from shutil import copyfile

from python_on_whales import DockerClient, docker
from rich.console import Console

console = Console(width=400, color_system="standard")

DOCKER_COMPOSE_HOST_PORT = os.environ.get("HOST_PORT", "localhost:8080")
AIRFLOW_WWW_USER_USERNAME = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_WWW_USER_PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")
TASK_SDK_TESTS_ROOT = Path(__file__).parent.parent.parent


def test_task_sdk_health(tmp_path_factory, monkeypatch):
    """Test Task SDK health check using docker-compose environment."""
    tmp_dir = tmp_path_factory.mktemp("airflow-task-sdk-test")
    console.print(f"[yellow]Tests are run in {tmp_dir}")

    # Copy docker-compose.yaml to temp directory
    docker_compose_file = TASK_SDK_TESTS_ROOT / "docker" / "docker-compose.yaml"
    tmp_docker_compose_file = tmp_dir / "docker-compose.yaml"
    copyfile(docker_compose_file, tmp_docker_compose_file)

    # Set environment variables for the test
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", os.environ.get("DOCKER_IMAGE", "apache/airflow:2.8.1"))
    monkeypatch.setenv(
        "_PIP_ADDITIONAL_REQUIREMENTS",
        f"apache-airflow-task-sdk=={os.environ.get('TASK_SDK_VERSION', '1.0.1a1')}",
    )

    # Initialize Docker client
    compose = DockerClient(compose_files=[str(tmp_docker_compose_file)])

    try:
        # Start the services
        compose.compose.up(detach=True)
        console.print("[green]Docker Compose environment is up")

        # Wait for services to be healthy
        compose.compose.ps()
        console.print("[green]Services are running")

        # Test the API server
        response = docker.container.execute(
            compose.compose.ps(services=["airflow-apiserver"])[0],
            [
                "curl",
                "-s",
                "-u",
                f"{AIRFLOW_WWW_USER_USERNAME}:{AIRFLOW_WWW_USER_PASSWORD}",
                "http://localhost:8080/api/v1/health",
            ],
        )
        health_check = json.loads(response)
        assert health_check["metadatabase"]["status"] == "healthy"
        console.print("[green]API server is healthy")

        # Test task-sdk installation
        response = docker.container.execute(
            compose.compose.ps(services=["airflow-apiserver"])[0], ["pip", "freeze"]
        )
        assert "apache-airflow-task-sdk" in response
        console.print("[green]Task SDK is installed")

        # Test task-sdk API
        response = docker.container.execute(
            compose.compose.ps(services=["airflow-apiserver"])[0],
            [
                "curl",
                "-s",
                "-u",
                f"{AIRFLOW_WWW_USER_USERNAME}:{AIRFLOW_WWW_USER_PASSWORD}",
                "http://localhost:8080/execution/api/v1/health",
            ],
        )
        task_sdk_health = json.loads(response)
        assert task_sdk_health["status"] == "healthy"
        console.print("[green]Task SDK API is healthy")

    finally:
        # Clean up
        compose.compose.down(volumes=True)
        console.print("[yellow]Docker Compose environment is down")
