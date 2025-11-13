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
from collections.abc import Callable
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


def setup_dag_and_get_client(
    *,
    dag_id: str,
    headers: dict[str, str],
    auth_token: str | None = None,
    task_id_filter: str | Callable[[dict], bool] | None = None,
    wait_for_task_state: str | None = None,
    wait_for_dag_state: str | None = None,
    wait_timeout: int = 60,
    additional_metadata: dict | None = None,
) -> dict:
    """
    Utility to set up a DAG run and create an SDK client.

    This function handles the common pattern of:
    1. Getting DAG status
    2. Unpausing DAG (if needed)
    3. Triggering a DAG run
    4. Waiting for task instances or DAG run state
    5. Acquiring the task instance ID for the triggered DAG run
    6. Generating JWT token for that task instance
    7. Creating a task SDK client with that JWT token

    Args:
        dag_id: The DAG ID to set up
        headers: Headers for API requests (must include Authorization)
        auth_token: Auth token string (extracted from headers if not provided)
        task_id_filter: Task ID to filter for, or callable that takes TI dict and returns bool.
                       If None, uses first available task instance.
        wait_for_task_state: If provided, wait for task instance to reach this state.
                             Mutually exclusive with wait_for_dag_state.
        wait_for_dag_state: If provided, wait for DAG run to reach this state.
                            Mutually exclusive with wait_for_task_state.
        wait_timeout: Maximum number of attempts to wait (each attempt is ~2 seconds)
        additional_metadata: Additional metadata to include in return dict

    Returns:
        A dict which contains:
        - dag_info: dict with dag_id, dag_run_id, logical_date
        - task_instance_id: UUID string of task instance
        - sdk_client: Authenticated SDK client
        - core_api_headers: Headers for Core API requests
        - auth_token: Auth token string
        - Any additional metadata from additional_metadata

    Raises:
        TimeoutError: If waiting for the DAG run or task instance times out
        RuntimeError: If task instance is not found or DAG run fails
    """
    import time

    import requests

    from airflow.sdk.api.client import Client
    from airflow.sdk.timezone import utcnow
    from task_sdk_tests.jwt_plugin import generate_jwt_token

    if wait_for_task_state and wait_for_dag_state:
        raise ValueError("Cannot specify both wait_for_task_state and wait_for_dag_state")

    # Step 1: Get DAG status
    console.print(f"[yellow]Checking {dag_id} status...")
    dag_response = requests.get(f"http://localhost:8080/api/v2/dags/{dag_id}", headers=headers)
    dag_response.raise_for_status()
    dag_data = dag_response.json()

    # Step 2: Unpause DAG if needed
    if dag_data.get("is_paused", True):
        console.print(f"[yellow]Unpausing {dag_id}...")
        unpause_response = requests.patch(
            f"http://localhost:8080/api/v2/dags/{dag_id}", json={"is_paused": False}, headers=headers
        )
        unpause_response.raise_for_status()
        console.print(f"[green]✅ {dag_id} unpaused")

    # Step 3: Trigger DAG run
    logical_date = utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
    payload = {"conf": {}, "logical_date": logical_date}

    console.print(f"[yellow]Triggering {dag_id}...")
    trigger_response = requests.post(
        f"http://localhost:8080/api/v2/dags/{dag_id}/dagRuns", json=payload, headers=headers, timeout=30
    )
    trigger_response.raise_for_status()
    dag_run_data = trigger_response.json()
    dag_run_id = dag_run_data["dag_run_id"]

    console.print(f"[green]✅ {dag_id} triggered: {dag_run_id}")

    # Step 4: Wait for condition
    if wait_for_dag_state:
        # Wait for DAG run to reach specific state
        console.print(f"[yellow]Waiting for {dag_id} to reach state '{wait_for_dag_state}'...")
        final_state = None
        for attempt in range(wait_timeout):
            try:
                dr_response = requests.get(
                    f"http://localhost:8080/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}", headers=headers
                )
                dr_response.raise_for_status()
                dr_data = dr_response.json()
                state = dr_data.get("state")

                if state == wait_for_dag_state:
                    console.print(f"[green]✅ {dag_id} reached state '{wait_for_dag_state}'!")
                    final_state = state
                    break
                if state in ["failed", "skipped"]:
                    raise RuntimeError(f"{dag_id} ended in state: {state}")
                console.print(
                    f"[blue]Waiting for {dag_id} to reach '{wait_for_dag_state}' "
                    f"(attempt {attempt + 1}/{wait_timeout}, current state: {state})"
                )

            except Exception as e:
                console.print(f"[yellow]DAG run check failed: {e}")

            time.sleep(2)

        if final_state != wait_for_dag_state:
            raise TimeoutError(f"{dag_id} did not reach state '{wait_for_dag_state}' within timeout period")

    # Step 5: Get task instance ID
    console.print(f"[yellow]Getting task instance ID from {dag_id}...")
    ti_url = f"http://localhost:8080/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    ti_response = requests.get(ti_url, headers=headers, timeout=10)
    ti_response.raise_for_status()

    task_instances = ti_response.json().get("task_instances", [])

    # Filter task instances
    if wait_for_task_state:
        # Wait for specific task to reach specific state
        console.print(f"[yellow]Waiting for task instance to reach state '{wait_for_task_state}'...")
        ti_id = None

        for attempt in range(wait_timeout):
            try:
                ti_response = requests.get(ti_url, headers=headers, timeout=10)
                ti_response.raise_for_status()
                task_instances = ti_response.json().get("task_instances", [])

                # Filter task instances
                for ti in task_instances:
                    if callable(task_id_filter):
                        matches_filter = task_id_filter(ti)
                    elif task_id_filter:
                        matches_filter = ti.get("task_id") == task_id_filter
                    else:
                        matches_filter = True

                    if matches_filter and ti.get("state") == wait_for_task_state:
                        ti_id = ti.get("id")
                        if ti_id:
                            console.print(f"[green]✅ Found task in '{wait_for_task_state}' state")
                            console.print(f"[green]    Task ID: {ti.get('task_id')}")
                            console.print(f"[green]    Instance ID: {ti_id}")
                            break

                if ti_id:
                    break
                console.print(f"[blue]Waiting for task instance (attempt {attempt + 1}/{wait_timeout})")

            except Exception as e:
                console.print(f"[yellow]Task check failed: {e}")

            time.sleep(2)

        if not ti_id:
            raise TimeoutError(
                f"Task instance did not reach '{wait_for_task_state}' state within timeout period"
            )
    else:
        ti_id = None
        for ti in task_instances:
            if callable(task_id_filter):
                matches_filter = task_id_filter(ti)
            elif task_id_filter:
                matches_filter = ti.get("task_id") == task_id_filter
            else:
                matches_filter = True

            if matches_filter:
                ti_id = ti.get("id")
                break

        if not ti_id:
            raise RuntimeError(f"Could not find task instance ID from {dag_id}")

        console.print(f"[green]✅ Found task instance ID: {ti_id}")

    # Step 6: Generate JWT token and create SDK client
    jwt_token = generate_jwt_token(ti_id)
    sdk_client = Client(base_url=f"http://{TASK_SDK_HOST_PORT}/execution", token=jwt_token)

    # Extract auth token from headers if not provided
    if auth_token is None:
        auth_token = headers.get("Authorization", "").replace("Bearer ", "")

    # Build return dict
    result = {
        "auth_token": auth_token,
        "dag_info": {"dag_id": dag_id, "dag_run_id": dag_run_id, "logical_date": logical_date},
        "task_instance_id": ti_id,
        "sdk_client": sdk_client,
        "core_api_headers": headers,
    }

    if additional_metadata:
        result.update(additional_metadata)

    return result


@pytest.fixture(scope="session")
def airflow_ready(docker_compose_setup):
    """Shared fixture that waits for Airflow to be ready and provides auth token to communicate with Airflow."""
    import time

    import requests

    # Generous sleep for Airflow to be ready
    time.sleep(15)

    auth_url = "http://localhost:8080/auth/token"
    try:
        auth_response = requests.get(auth_url, timeout=10)
        auth_response.raise_for_status()
        auth_token = auth_response.json()["access_token"]
        console.print("[green]✅ Got auth token")
    except Exception as e:
        raise e

    headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}

    return {"auth_token": auth_token, "headers": headers}


@pytest.fixture(scope="session")
def airflow_test_setup(docker_compose_setup, airflow_ready):
    """Fixed session-scoped fixture that matches UI behavior."""
    headers = airflow_ready["headers"]
    auth_token = airflow_ready["auth_token"]

    return setup_dag_and_get_client(
        dag_id="test_dag",
        headers=headers,
        auth_token=auth_token,
        task_id_filter="long_running_task",
        wait_for_task_state="running",
        wait_timeout=30,
    )


@pytest.fixture(scope="session")
def task_sdk_api_version():
    """Get the API version from the installed Task SDK."""
    from airflow.sdk.api.datamodels._generated import API_VERSION

    return API_VERSION


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


@pytest.fixture(scope="session")
def sdk_client_for_assets(asset_test_setup):
    """Get SDK client for asset tests (doesn't require test_dag)."""
    return asset_test_setup["sdk_client"]


@pytest.fixture(scope="session")
def asset_test_setup(docker_compose_setup, airflow_ready):
    """Setup assets for testing by triggering asset_producer_dag."""
    headers = airflow_ready["headers"]
    auth_token = airflow_ready["auth_token"]

    return setup_dag_and_get_client(
        dag_id="asset_producer_dag",
        headers=headers,
        auth_token=auth_token,
        task_id_filter="produce_asset",
        wait_for_dag_state="success",
        wait_timeout=60,
        additional_metadata={
            "name": "test_asset",
            "uri": "test://asset1/",
        },
    )
