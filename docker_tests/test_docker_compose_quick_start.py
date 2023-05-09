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
from pprint import pprint
from shutil import copyfile
from time import monotonic, sleep

import requests

# isort:off (needed to workaround isort bug)
from docker_tests.command_utils import run_command
from docker_tests.constants import SOURCE_ROOT
from docker_tests.docker_tests_utils import docker_image

# isort:on (needed to workaround isort bug)

AIRFLOW_WWW_USER_USERNAME = os.environ.get("_AIRFLOW_WWW_USER_USERNAME", "airflow")
AIRFLOW_WWW_USER_PASSWORD = os.environ.get("_AIRFLOW_WWW_USER_PASSWORD", "airflow")
DAG_ID = "example_bash_operator"
DAG_RUN_ID = "test_dag_run_id"


def api_request(method: str, path: str, base_url: str = "http://localhost:8080/api/v1", **kwargs) -> dict:
    response = requests.request(
        method=method,
        url=f"{base_url}/{path}",
        auth=(AIRFLOW_WWW_USER_USERNAME, AIRFLOW_WWW_USER_PASSWORD),
        headers={"Content-Type": "application/json"},
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def wait_for_container(container_id: str, timeout: int = 300):
    container_name = (
        subprocess.check_output(["docker", "inspect", container_id, "--format", "{{ .Name }}"])
        .decode()
        .strip()
    )
    print(f"Waiting for container: {container_name} [{container_id}]")
    waiting_done = False
    start_time = monotonic()
    while not waiting_done:
        container_state = (
            subprocess.check_output(["docker", "inspect", container_id, "--format", "{{ .State.Status }}"])
            .decode()
            .strip()
        )
        if container_state in ("running", "restarting"):
            health_status = (
                subprocess.check_output(
                    [
                        "docker",
                        "inspect",
                        container_id,
                        "--format",
                        "{{ if .State.Health }}{{ .State.Health.Status }}{{ else }}no-check{{ end }}",
                    ]
                )
                .decode()
                .strip()
            )
            print(f"{container_name}: container_state={container_state}, health_status={health_status}")

            if health_status == "healthy" or health_status == "no-check":
                waiting_done = True
        else:
            print(f"{container_name}: container_state={container_state}")
            waiting_done = True
        if timeout != 0 and monotonic() - start_time > timeout:
            raise Exception(f"Timeout. The operation takes longer than the maximum waiting time ({timeout}s)")
        sleep(1)


def wait_for_terminal_dag_state(dag_id, dag_run_id):
    print(f" Simplified representation of DAG {dag_id} ".center(72, "="))
    pprint(api_request("GET", f"dags/{DAG_ID}/details"))

    # Wait 80 seconds
    for _ in range(80):
        dag_state = api_request("GET", f"dags/{dag_id}/dagRuns/{dag_run_id}").get("state")
        print(f"Waiting for DAG Run: dag_state={dag_state}")
        sleep(1)
        if dag_state in ("success", "failed"):
            break


def test_trigger_dag_and_wait_for_result(tmp_path_factory, monkeypatch):
    """Simple test which reproduce setup docker-compose environment and trigger example dag."""
    tmp_dir = tmp_path_factory.mktemp("airflow-quick-start")
    monkeypatch.chdir(tmp_dir)
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", docker_image)
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "quick-start")

    compose_file_path = (
        SOURCE_ROOT / "docs" / "apache-airflow" / "howto" / "docker-compose" / "docker-compose.yaml"
    )
    copyfile(compose_file_path, tmp_dir / "docker-compose.yaml")

    # Create required directories for docker compose quick start howto
    for subdir in ("dags", "logs", "plugins"):
        (tmp_dir / subdir).mkdir()

    dot_env_file = tmp_dir / ".env"
    dot_env_file.write_text(f"AIRFLOW_UID={os.getuid()}\n")
    print(" .env file content ".center(72, "="))
    print(dot_env_file.read_text())

    run_command(["docker-compose", "config"])
    run_command(["docker-compose", "down", "--volumes", "--remove-orphans"])
    try:
        run_command(["docker-compose", "up", "-d"])
        # The --wait condition was released in docker-compose v2.1.1, but we want to support
        # docker-compose v1 yet.
        # See:
        # https://github.com/docker/compose/releases/tag/v2.1.1
        # https://github.com/docker/compose/pull/8777
        for container_id in (
            subprocess.check_output(["docker-compose", "ps", "-q"]).decode().strip().splitlines()
        ):
            wait_for_container(container_id)
        api_request("PATCH", path=f"dags/{DAG_ID}", json={"is_paused": False})
        api_request("POST", path=f"dags/{DAG_ID}/dagRuns", json={"dag_run_id": DAG_RUN_ID})
        try:
            wait_for_terminal_dag_state(dag_id=DAG_ID, dag_run_id=DAG_RUN_ID)
            dag_state = api_request("GET", f"dags/{DAG_ID}/dagRuns/{DAG_RUN_ID}").get("state")
            assert dag_state == "success"
        except Exception:
            print("HTTP: GET health")
            pprint(api_request("GET", "health"))
            print(f"HTTP: GET dags/{DAG_ID}/dagRuns")
            pprint(api_request("GET", f"dags/{DAG_ID}/dagRuns"))
            print(f"HTTP: GET dags/{DAG_ID}/dagRuns/{DAG_RUN_ID}/taskInstances")
            pprint(api_request("GET", f"dags/{DAG_ID}/dagRuns/{DAG_RUN_ID}/taskInstances"))
            raise
    except Exception:
        print(f"Current working directory: {os.getcwd()}")
        run_command(["docker", "version"])
        run_command(["docker-compose", "version"])
        run_command(["docker", "ps"])
        run_command(["docker-compose", "logs"])
        raise
    finally:
        run_command(["docker-compose", "down", "--volumes"])
