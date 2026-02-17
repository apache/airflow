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
    LOGIN_COMMAND,
    LOGIN_OUTPUT,
)

from tests_common.test_utils.fernet import generate_fernet_key_string


@pytest.fixture
def run_command():
    """Fixture that provides a helper to run airflowctl commands."""

    def _run_command(command: str, skip_login: bool = False) -> str:
        import os
        from subprocess import PIPE, STDOUT, Popen

        host_envs = os.environ.copy()
        host_envs["AIRFLOW_CLI_DEBUG_MODE"] = "true"

        command_from_config = f"airflowctl {command}"

        # We need to run auth login first for all commands except login itself (unless skipped)
        if not skip_login and command != LOGIN_COMMAND:
            run_cmd = f"airflowctl {LOGIN_COMMAND} && {command_from_config}"
        else:
            run_cmd = command_from_config

        console.print(f"[yellow]Running command: {command}")

        # Give some time for the command to execute and output to be ready
        proc = Popen(run_cmd.encode(), stdout=PIPE, stderr=STDOUT, stdin=PIPE, shell=True, env=host_envs)
        stdout_bytes, stderr_result = proc.communicate(timeout=60)

        # CLI command gave errors
        assert not stderr_result, (
            f"Errors while executing command '{command_from_config}':\n{stderr_result.decode()}"
        )

        # Decode the output
        stdout_result = stdout_bytes.decode()

        # We need to trim auth login output if the command is not login itself and clean backspaces
        if not skip_login and command != LOGIN_COMMAND:
            assert LOGIN_OUTPUT in stdout_result, (
                f"❌ Login output not found before command output for '{command_from_config}'\nFull output:\n{stdout_result}"
            )
            stdout_result = stdout_result.split(f"{LOGIN_OUTPUT}\n")[1].strip()
        else:
            stdout_result = stdout_result.strip()

        # Check for non-zero exit code
        assert proc.returncode == 0, (
            f"❌ Command '{command_from_config}' exited with code {proc.returncode}\nOutput:\n{stdout_result}"
        )

        # Error patterns to detect failures that might otherwise slip through
        # Please ensure it is aligning with airflowctl.api.client.get_json_error
        error_patterns = [
            "Server error",
            "command error",
            "unrecognized arguments",
            "invalid choice",
            "Traceback (most recent call last):",
        ]
        matched_error = next((error for error in error_patterns if error in stdout_result), None)
        assert not matched_error, (
            f"❌ Output contained unexpected text for command '{command_from_config}'\n"
            f"Matched error pattern: {matched_error}\n"
            f"Output:\n{stdout_result}"
        )

        console.print(f"[green]✅ Output did not contain unexpected text for command '{command_from_config}'")
        console.print(f"[cyan]Result:\n{stdout_result}\n")
        proc.kill()

        return stdout_result

    return _run_command


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
