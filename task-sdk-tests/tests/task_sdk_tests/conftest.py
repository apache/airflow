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

import pytest

from task_sdk_tests import console
from task_sdk_tests.constants import (
    AIRFLOW_ROOT_PATH,
    DOCKER_COMPOSE_FILE_PATH,
    DOCKER_IMAGE,
    TASK_SDK_HOST_PORT,
)


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


@pytest.fixture(scope="session")
def docker_compose_setup(tmp_path_factory):
    """Start docker-compose once per session."""
    import os
    from shutil import copyfile, copytree

    from python_on_whales import DockerClient, docker

    # Create temp directory for docker-compose
    tmp_dir = tmp_path_factory.mktemp("airflow-task-sdk-test")
    tmp_docker_compose_file = tmp_dir / "docker-compose.yaml"
    copyfile(DOCKER_COMPOSE_FILE_PATH, tmp_docker_compose_file)

    # Copy the DAGs folder to the temp directory so docker-compose can find it
    from task_sdk_tests.constants import TASK_SDK_TESTS_ROOT

    TASK_SDK_DAGS_FOLDER = TASK_SDK_TESTS_ROOT / "dags"
    copytree(TASK_SDK_DAGS_FOLDER, tmp_dir / "dags", dirs_exist_ok=True)

    # Set environment variables
    os.environ["AIRFLOW_IMAGE_NAME"] = DOCKER_IMAGE
    os.environ["TASK_SDK_VERSION"] = os.environ.get("TASK_SDK_VERSION", "1.1.0")

    compose = DockerClient(compose_files=[str(tmp_docker_compose_file)])

    try:
        console.print("[yellow]Starting docker-compose for session...")
        compose.compose.up(detach=True, wait=True)
        console.print("[green]Docker compose started successfully!\n")

        yield compose
    except Exception as e:
        console.print(f"[red]❌ Docker compose failed to start: {e}")

        debug_environment()
        print_diagnostics(compose, compose.version(), docker.version())

        raise
    finally:
        if not os.environ.get("SKIP_DOCKER_COMPOSE_DELETION"):
            console.print("[yellow]Cleaning up docker-compose...")
            compose.compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose cleaned up")


def pytest_sessionstart(session):
    """Install Task SDK at the very start of the pytest session."""
    task_sdk_version = os.environ.get("TASK_SDK_VERSION", "1.1.0")
    console.print(
        f"[yellow]Installing apache-airflow-task-sdk=={task_sdk_version} via pytest_sessionstart..."
    )

    task_sdk_path = AIRFLOW_ROOT_PATH / "task-sdk"
    console.print(f"[blue]Installing from: {task_sdk_path}")

    # Install directly to current UV environment
    console.print("[blue]Installing to current UV environment...")
    console.print(f"[blue]Current Python: {sys.executable}")

    try:
        cmd = ["uv", "pip", "install", str(task_sdk_path)]
        console.print(f"[cyan]Running command: {' '.join(cmd)}")
        subprocess.check_call(cmd)
        console.print("[green]Task SDK installed successfully to UV environment via pytest_sessionstart!")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        console.print(f"[yellow]UV installation failed: {e}")
        raise

    console.print("[yellow]Verifying task Task installation via pytest_sessionstart...")
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import airflow.sdk.api.client; print('✅ Task SDK import successful via pytest_sessionstart')",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"[green]{result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        console.print("[red]❌ Task SDK import verification failed via pytest_sessionstart:")
        console.print(f"[red]Return code: {e.returncode}")
        console.print(f"[red]Stdout: {e.stdout}")
        console.print(f"[red]Stderr: {e.stderr}")
        raise


@pytest.fixture(scope="session")
def airflow_test_setup(docker_compose_setup):
    """Fixed session-scoped fixture that matches UI behavior."""
    import time

    import requests

    from airflow.sdk.api.client import Client
    from airflow.sdk.timezone import utcnow
    from task_sdk_tests.jwt_plugin import generate_jwt_token

    time.sleep(15)

    # Step 1: Get auth token
    auth_url = "http://localhost:8080/auth/token"
    try:
        auth_response = requests.get(auth_url, timeout=10)
        auth_response.raise_for_status()
        auth_token = auth_response.json()["access_token"]
        console.print("[green]✅ Got auth token")
    except Exception as e:
        raise e

    # Step 2: Check and unpause DAG
    headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}

    console.print("[yellow]Checking DAG status...")
    dag_response = requests.get("http://localhost:8080/api/v2/dags/test_dag", headers=headers)
    dag_response.raise_for_status()
    dag_data = dag_response.json()

    if dag_data.get("is_paused", True):
        console.print("[yellow]Unpausing DAG...")
        unpause_response = requests.patch(
            "http://localhost:8080/api/v2/dags/test_dag", json={"is_paused": False}, headers=headers
        )
        unpause_response.raise_for_status()
        console.print("[green]✅ DAG unpaused")
    logical_date = utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
    payload = {"conf": {}, "logical_date": logical_date}

    trigger_response = requests.post(
        "http://localhost:8080/api/v2/dags/test_dag/dagRuns", json=payload, headers=headers, timeout=30
    )

    console.print(f"[blue]Trigger DAG Run response status: {trigger_response.status_code}")
    console.print(f"[blue]Trigger DAG Run response: {trigger_response.text}")

    trigger_response.raise_for_status()
    dag_run_data = trigger_response.json()
    dag_run_id = dag_run_data["dag_run_id"]

    console.print(f"[green]✅ DAG triggered: {dag_run_id}")

    # Step 4: Get task instance for testing
    console.print("[yellow]Waiting for any task instance...")
    ti_id = None

    for attempt in range(20):
        try:
            ti_url = f"http://localhost:8080/api/v2/dags/test_dag/dagRuns/{dag_run_id}/taskInstances"
            ti_response = requests.get(ti_url, headers=headers, timeout=10)
            ti_response.raise_for_status()

            task_instances = ti_response.json().get("task_instances", [])

            if task_instances:
                first_ti = task_instances[0]
                ti_id = first_ti.get("id")

                if ti_id:
                    console.print(f"[green]✅ Using task instance from '{first_ti.get('task_id')}'")
                    console.print(f"[green]    State: {first_ti.get('state')}")
                    console.print(f"[green]    Instance ID: {ti_id}")
                    break
            else:
                console.print(f"[blue]Waiting for tasks (attempt {attempt + 1}/20)")

        except Exception as e:
            console.print(f"[yellow]Task check failed: {e}")

        time.sleep(2)

    if not ti_id:
        console.print("[red]❌ Task instances never appeared. Final debug info:")
        raise TimeoutError("No task instance found within timeout period")

    # Step 5: Create SDK client
    jwt_token = generate_jwt_token(ti_id)
    sdk_client = Client(base_url=f"http://{TASK_SDK_HOST_PORT}/execution", token=jwt_token)

    return {
        "auth_token": auth_token,
        "dag_info": {"dag_id": "test_dag", "dag_run_id": dag_run_id, "logical_date": logical_date},
        "task_instance_id": ti_id,
        "sdk_client": sdk_client,
        "core_api_headers": headers,
    }


@pytest.fixture(scope="session")
def auth_token(airflow_test_setup):
    """Get the auth token from setup."""
    return airflow_test_setup["auth_token"]


@pytest.fixture(scope="session")
def dag_info(airflow_test_setup):
    """Get DAG information from setup."""
    return airflow_test_setup["dag_info"]


@pytest.fixture(scope="session")
def task_instance_id(airflow_test_setup):
    """Get task instance ID from setup."""
    return airflow_test_setup["task_instance_id"]


@pytest.fixture(scope="session")
def sdk_client(airflow_test_setup):
    """Get authenticated Task SDK client from setup."""
    return airflow_test_setup["sdk_client"]


@pytest.fixture(scope="session")
def core_api_headers(airflow_test_setup):
    """Get Core API headers from setup."""
    return airflow_test_setup["core_api_headers"]
