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
from collections.abc import Callable
from pathlib import Path

import pytest
import requests
from packaging import version
from tenacity import (
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential,
)

from task_sdk_tests import console
from task_sdk_tests.constants import (
    AIRFLOW_ROOT_PATH,
    DOCKER_IMAGE,
    MIN_DOCKER_COMPOSE_VERSION,
    MIN_DOCKER_VERSION,
    TASK_SDK_HOST_PORT,
    TASK_SDK_INTEGRATION_DOCKER_COMPOSE_FILE_PATH,
    TASK_SDK_INTEGRATION_ENV_FILE,
    TASK_SDK_INTEGRATION_LOCAL_DOCKER_COMPOSE_FILE_PATH,
)

from tests_common.test_utils.fernet import generate_fernet_key_string


def compare_gte(a: str, b: str) -> bool:
    """Compare two versions for greater than equal to. Validates a>=b"""
    try:
        return version.parse(a) >= version.parse(b)
    except Exception:
        return False


def check_docker_is_running() -> bool:
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=False,
            check=False,
            timeout=10,
        )
        return result.returncode == 0
    except Exception:
        return False


def check_docker_version() -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["docker", "version", "--format", "{{.Client.Version}}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )

        if result.returncode != 0:
            return False, "unknown"

        docker_version_str = result.stdout.strip()
        # extract version number (e.g., "25.0.0" from "25.0.0" or "25.0.0-ce")
        version_pattern = re.compile(r"(\d+)\.(\d+)\.(\d+)")
        version_match = version_pattern.search(docker_version_str)

        if version_match:
            docker_version_str = ".".join(version_match.groups())
            is_valid = compare_gte(docker_version_str, MIN_DOCKER_VERSION)
            return is_valid, docker_version_str

        return False, "unknown"
    except Exception:
        return False, "unknown"


def check_docker_compose_version() -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )

        if result.returncode != 0:
            return False, "unknown"

        docker_compose_output = result.stdout.strip()
        # extract version number from output like "Docker Compose version v2.20.2"
        version_pattern = re.compile(r"(\d+)\.(\d+)\.(\d+)")
        version_match = version_pattern.search(docker_compose_output)

        if version_match:
            compose_version_str = ".".join(version_match.groups())
            is_valid = compare_gte(compose_version_str, MIN_DOCKER_COMPOSE_VERSION)
            return is_valid, compose_version_str

        return False, "unknown"
    except Exception:
        return False, "unknown"


def check_docker_requirements():
    """
    Verify docker and docker Compose meet minimum version crieteria.

    1. Check if Docker is running
    2. Check Docker version
    3. Check Docker Compose version
    """
    console.print("[yellow]Checking Docker requirements...[/]")

    if not check_docker_is_running():
        console.print(
            "[red]Docker is not running.[/]\n"
            "[yellow]Please make sure Docker is installed and running.[/]\n"
            "Installation instructions: https://docs.docker.com/engine/install/"
        )
        sys.exit(1)

    valid, docker_version_str = check_docker_version()
    if not valid:
        if docker_version_str == "unknown":
            console.print(
                f"[red]Could not determine Docker version.[/]\n"
                f"[yellow]Please make sure Docker is at least version {MIN_DOCKER_VERSION}.[/]\n"
                "Installation instructions: https://docs.docker.com/engine/install/"
            )
        else:
            console.print(
                f"[red]Docker version {docker_version_str} is too old.[/]\n"
                f"[yellow]Minimum required version: {MIN_DOCKER_VERSION}[/]\n"
                "Installation instructions: https://docs.docker.com/engine/install/"
            )
        sys.exit(1)

    console.print(f"[green]Docker version {docker_version_str} meets requirements[/]")

    valid, compose_version_str = check_docker_compose_version()
    if not valid:
        if compose_version_str == "unknown":
            console.print(
                "[red]Could not determine Docker Compose version.[/]\n"
                "[yellow]You either do not have docker-compose or have docker-compose v1 installed.[/]\n"
                "[yellow]Breeze does not support docker-compose v1 any more as it has been replaced by v2.[/]\n"
                f"[yellow]Minimum required version: {MIN_DOCKER_COMPOSE_VERSION}[/]\n"
                "Follow https://docs.docker.com/compose/migrate/ to migrate to v2\n"
                "Installation instructions: https://docs.docker.com/compose/install/"
            )
        else:
            console.print(
                f"[red]Docker Compose version {compose_version_str} is too old.[/]\n"
                f"[yellow]Minimum required version: {MIN_DOCKER_COMPOSE_VERSION}[/]\n"
                "Installation instructions: https://docs.docker.com/compose/install/"
            )
        sys.exit(1)

    console.print(f"[green]Docker Compose version {compose_version_str} meets requirements[/]")


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
    from python_on_whales import DockerClient, docker

    falsey_values = ["false", "0"]

    verbose = os.environ.get("VERBOSE")
    debugging_on = verbose and verbose.lower() not in falsey_values
    skip_mounting_local_volumes = os.environ.get("SKIP_MOUNTING_LOCAL_VOLUMES")
    mount_volumes = not (skip_mounting_local_volumes and skip_mounting_local_volumes not in falsey_values)
    skip_docker_compose_deletion = os.environ.get("SKIP_DOCKER_COMPOSE_DELETION")
    delete_compose = not (
        skip_docker_compose_deletion and skip_docker_compose_deletion.lower() not in falsey_values
    )
    if mount_volumes:
        delete_compose = False

    with open(TASK_SDK_INTEGRATION_ENV_FILE, "w") as f:
        print(f"AIRFLOW_IMAGE_NAME={DOCKER_IMAGE}", file=f)
        print(f"AIRFLOW_UID={os.getuid()}", file=f)
        print(f"HOST_OS={platform.system().lower()}", file=f)
        #
        # Please Do not use this Fernet key in any deployments! Please generate your own key.
        # This is specifically generated for integration tests and not as default.
        #
        print(f"FERNET_KEY={generate_fernet_key_string()}", file=f)

    docker_compose_files = [TASK_SDK_INTEGRATION_DOCKER_COMPOSE_FILE_PATH.as_posix()]
    log_level = "debug" if debugging_on else "info"
    if mount_volumes:
        docker_compose_files.append(TASK_SDK_INTEGRATION_LOCAL_DOCKER_COMPOSE_FILE_PATH.as_posix())
    if debugging_on:
        console.print(f"[yellow]Using docker-compose files:\n{docker_compose_files}")
    compose = DockerClient(
        compose_files=docker_compose_files,
        debug=os.environ.get("VERBOSE"),
        log_level=log_level,
    )
    start_new_compose = True
    processes = compose.compose.ps(["airflow-apiserver"])
    if len(processes) > 0 and processes[0].state.status == "running":
        if not skip_mounting_local_volumes:
            console.print(
                "\n\n[yellow]Docker compose already running. Using it instead of starting a new one!\n"
            )
            console.print("\nIn order to stop the running docker compose and start from scratch, run:\n")
            console.print("    [magenta]docker compose down\n")
            start_new_compose = False
        else:
            console.print(
                "[yellow]Cleaning up docker-compose as we found "
                "one running and ant to start from scratch (--skip_mounting_local_files)..."
            )
            compose.compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose cleaned up")
    try:
        if start_new_compose:
            console.print("[yellow]Starting docker-compose for session...\n")
            if mount_volumes:
                console.print("\n\n[yellow]Local sources are mounted:")
                console.print("[yellow]     * UI is put in dev mode")
                console.print("[yellow]     * The components will hot-reload on local changes.")
                console.print("[yellow]     * Docker compose will NOT be stopped at completion.")
                console.print("\nIn order to stop docker compose later run:\n")
                console.print("    [magenta]docker compose down\n")
            console.print("\n[info]Command to start it manually:")
            files = " ".join(f'-f "{file}"' for file in docker_compose_files)
            console.print(f"\n[info]docker compose up --log-level {log_level} {files} --detach\n")
            compose.compose.up(detach=True, wait=True)
            console.print("[green]Docker compose started successfully!\n")
        yield compose
    except Exception as e:
        console.print(f"[red]❌ Docker compose failed to start: {e}")
        debug_environment()
        print_diagnostics(compose, compose.version(), docker.version())
        raise
    finally:
        if delete_compose:
            console.print("[yellow]Cleaning up docker-compose...")
            compose.compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose cleaned up")


def pytest_sessionstart(session):
    """Install Task SDK at the very start of the pytest session."""

    # Check Docker requirements first (similar to what Breeze does)
    check_docker_requirements()

    console.print("[yellow]Installing apache-airflow-task-sdk via pytest_sessionstart...")

    task_sdk_version = os.environ.get("TASK_SDK_VERSION")
    if task_sdk_version:
        installation_command = ["apache-airflow-task-sdk==" + task_sdk_version]
    else:
        task_sdk_path = AIRFLOW_ROOT_PATH / "task-sdk"
        console.print(f"[blue]Installing from: {task_sdk_path}")
        installation_command = ["--editable", str(task_sdk_path)]

    # Install directly to current UV environment
    console.print(f"[blue]Installing to current UV environment via {installation_command}")
    console.print(f"[blue]Current Python: {sys.executable}")

    try:
        cmd = ["uv", "pip", "install", *installation_command]
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
                "import airflow.sdk.api.client; print('✅ Task SDK import successful "
                "via pytest_sessionstart: ' + airflow.sdk.__version__)",
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


def _before_sleep_print(retry_state):
    """tenacity before_sleep handler that prints retry attempts to the test console."""
    attempt = retry_state.attempt_number
    url = retry_state.args[0] if retry_state.args else ""
    # Try to extract dag_id from URL for friendlier logging
    dag_part = url.split("/api/v2/dags/")[-1] if "/api/v2/dags/" in url else url
    last = retry_state.outcome
    if last.failed:
        exc = last.exception()
        console.print(f"[yellow]Error fetching DAG status (attempt {attempt}): {exc}")
    else:
        resp = last.result()
        status = getattr(resp, "status_code", None)
        if status == 404:
            console.print(f"[yellow]DAG {dag_part} not found (404) on attempt {attempt}; retrying...")
        else:
            console.print(
                f"[yellow]Retrying fetch for {dag_part} (attempt {attempt}) due to status {status}..."
            )


@retry(
    retry=(
        retry_if_exception_type(requests.RequestException)
        | retry_if_result(lambda resp: resp is not None and getattr(resp, "status_code", None) == 404)
    ),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    before_sleep=_before_sleep_print,
    reraise=True,
)
def _get_dag_response(url: str, headers: dict):
    """Get DAG response with tenacity retry handling.

    Retries on requests.RequestException and on HTTP 404 responses.
    Returns the requests.Response on success or raises the last exception on failure.
    """
    return requests.get(url, headers=headers, timeout=10)


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
    # Use tenacity-decorated helper to fetch the DAG response with retries (handles transient 404s).
    dag_url = f"http://localhost:8080/api/v2/dags/{dag_id}"
    dag_response = _get_dag_response(dag_url, headers)
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
