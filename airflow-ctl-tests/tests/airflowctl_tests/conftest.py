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

import pytest
from python_on_whales import DockerClient, docker

from airflowctl_tests import console
from airflowctl_tests.constants import (
    AIRFLOW_ROOT_PATH,
    DOCKER_COMPOSE_FILE_PATH,
    DOCKER_IMAGE,
)

from tests_common.test_utils.fernet import generate_fernet_key_string


class _CtlTestState:
    docker_client: DockerClient | None = None


# Pytest hook to run at the start of the session
def pytest_sessionstart(session):
    """Install airflowctl at the very start of the pytest session."""
    airflow_ctl_version = os.environ.get("AIRFLOW_CTL_VERSION", "0.1.0")
    console.print(f"[yellow]Installing apache-airflow-ctl=={airflow_ctl_version} via pytest_sessionstart...")

    airflow_ctl_path = AIRFLOW_ROOT_PATH / "airflow-ctl"
    console.print(f"[blue]Installing from: {airflow_ctl_path}")

    # Install directly to current UV environment
    console.print("[blue]Installing to current UV environment...")
    console.print(f"[blue]Current Python: {sys.executable}")

    try:
        cmd = ["uv", "pip", "install", str(airflow_ctl_path)]
        console.print(f"[cyan]Running command: {' '.join(cmd)}")
        subprocess.check_call(cmd)
        console.print("[green]airflowctl installed successfully to UV environment via pytest_sessionstart!")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        console.print(f"[yellow]UV installation failed: {e}")
        raise

    console.print("[yellow]Verifying airflowctl installation via pytest_sessionstart...")
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import airflowctl.api.client; print('✅ airflowctl import successful via pytest_sessionstart')",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"[green]{result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        console.print("[red]❌ airflowctl import verification failed via pytest_sessionstart:")
        console.print(f"[red]Return code: {e.returncode}")
        console.print(f"[red]Stdout: {e.stdout}")
        console.print(f"[red]Stderr: {e.stderr}")
        raise

    docker_compose_up(session.config._tmp_path_factory)


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
    from pathlib import Path

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
        import airflowctl

        console.print(f"[green]✅ airflow already available: {airflowctl.__file__}")
    except ImportError:
        console.print("[red]❌ airflowctl not available in current environment")

    console.print("[yellow]================================")


def docker_compose_up(tmp_path_factory):
    """Fixture to spin up Docker Compose environment for the test session."""
    from shutil import copyfile

    tmp_dir = tmp_path_factory.mktemp("airflow-ctl-test")
    console.print(f"[yellow]Tests are run in {tmp_dir}")

    # Copy docker-compose.yaml to temp directory
    tmp_docker_compose_file = tmp_dir / "docker-compose.yaml"
    copyfile(DOCKER_COMPOSE_FILE_PATH, tmp_docker_compose_file)

    dot_env_file = tmp_dir / ".env"
    dot_env_file.write_text(
        f"AIRFLOW_UID={os.getuid()}\n"
        # To enable debug mode for airflowctl CLI
        "AIRFLOW_CTL_CLI_DEBUG_MODE=true\n"
        # To enable config operations to work
        "AIRFLOW__API__EXPOSE_CONFIG=true\n"
    )

    # Set environment variables for the test
    os.environ["AIRFLOW_IMAGE_NAME"] = DOCKER_IMAGE
    os.environ["AIRFLOW_CTL_VERSION"] = os.environ.get("AIRFLOW_CTL_VERSION", "1.0.0")
    os.environ["ENV_FILE_PATH"] = str(tmp_dir / ".env")
    #
    # Please Do not use this Fernet key in any deployments! Please generate your own key.
    # This is specifically generated for integration tests and not as default.
    #
    os.environ["FERNET_KEY"] = generate_fernet_key_string()

    # Initialize Docker client
    _CtlTestState.docker_client = DockerClient(compose_files=[str(tmp_docker_compose_file)])

    try:
        console.print(f"[blue]Spinning up airflow environment using {DOCKER_IMAGE}")
        _CtlTestState.docker_client.compose.up(detach=True, wait=True)
        console.print("[green]Docker compose started for airflowctl test\n")
    except Exception:
        print_diagnostics(
            _CtlTestState.docker_client.compose,
            _CtlTestState.docker_client.compose.version(),
            docker.version(),
        )
        debug_environment()
        docker_compose_down()
        raise


def docker_compose_down():
    """Tear down Docker Compose environment."""
    if _CtlTestState.docker_client:
        _CtlTestState.docker_client.compose.down(remove_orphans=True, volumes=True, quiet=True)


def pytest_sessionfinish(session, exitstatus):
    """Tear down test environment at the end of the pytest session."""
    if not os.environ.get("SKIP_DOCKER_COMPOSE_DELETION"):
        docker_compose_down()


# Fixtures for tests
@pytest.fixture
def login_command():
    # Passing password via command line is insecure but acceptable for testing purposes
    # Please do not do this in production, it enables possibility of exposing your credentials
    return "auth login --username airflow --password airflow"


@pytest.fixture
def login_output():
    return "Login successful! Welcome to airflowctl!"


@pytest.fixture
def date_param():
    import random
    from datetime import datetime, timedelta

    from dateutil.relativedelta import relativedelta

    # original datetime string
    dt_str = "2025-10-25T00:02:00+00:00"

    # parse to datetime object
    dt = datetime.fromisoformat(dt_str)

    # boundaries
    start = dt - relativedelta(months=1)
    end = dt + relativedelta(months=1)

    # pick random time between start and end
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    random_dt = start + timedelta(seconds=random_seconds)
    return random_dt.isoformat()


@pytest.fixture
def test_commands(login_command, date_param):
    # Define test commands to run with actual running API server
    return [
        login_command,
        # Assets commands
        "assets list",
        "assets get --asset-id=1",
        "assets create-event --asset-id=1",
        # Backfill commands
        "backfill list",
        # Config commands
        "config get --section core --option executor",
        "config list",
        "config lint",
        # Connections commands
        "connections create --connection-id=test_con --conn-type=mysql --password=TEST_PASS -o json",
        "connections list",
        "connections list -o yaml",
        "connections list -o table",
        "connections get --conn-id=test_con",
        "connections get --conn-id=test_con -o json",
        "connections update --connection-id=test_con --conn-type=postgres",
        "connections import tests/airflowctl_tests/fixtures/test_connections.json",
        "connections delete --conn-id=test_con",
        "connections delete --conn-id=test_import_conn",
        # DAGs commands
        "dags list",
        "dags get --dag-id=example_bash_operator",
        "dags get-details --dag-id=example_bash_operator",
        "dags get-stats --dag-ids=example_bash_operator",
        "dags get-version --dag-id=example_bash_operator --version-number=1",
        "dags list-import-errors",
        "dags list-version --dag-id=example_bash_operator",
        "dags list-warning",
        # Order of trigger and pause/unpause is important for test stability because state checked
        f"dags trigger --dag-id=example_bash_operator --logical-date={date_param} --run-after={date_param}",
        "dags pause --dag-id=example_bash_operator",
        "dags unpause --dag-id=example_bash_operator",
        # DAG Run commands
        f'dagrun get --dag-id=example_bash_operator --dag-run-id="manual__{date_param}"',
        "dags update --dag-id=example_bash_operator --no-is-paused",
        # DAG Run commands
        "dagrun list --dag-id example_bash_operator --state success --limit=1",
        # Jobs commands
        "jobs list",
        # Pools commands
        "pools create --name=test_pool --slots=5",
        "pools list",
        "pools get --pool-name=test_pool",
        "pools get --pool-name=test_pool -o yaml",
        "pools update --pool=test_pool --slots=10",
        "pools import tests/airflowctl_tests/fixtures/test_pools.json",
        "pools delete --pool=test_pool",
        "pools delete --pool=test_import_pool",
        # Providers commands
        "providers list",
        # Variables commands
        "variables create --key=test_key --value=test_value",
        "variables list",
        "variables get --variable-key=test_key",
        "variables get --variable-key=test_key -o table",
        "variables update --key=test_key --value=updated_value",
        "variables import tests/airflowctl_tests/fixtures/test_variables.json",
        "variables delete --variable-key=test_key",
        "variables delete --variable-key=test_import_var",
        "variables delete --variable-key=test_import_var_with_desc",
        # Version command
        "version --remote",
    ]
