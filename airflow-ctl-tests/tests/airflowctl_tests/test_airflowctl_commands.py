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
from subprocess import PIPE, STDOUT, Popen

from python_on_whales import DockerClient, docker
from rich.console import Console

from airflowctl_tests.constants import (
    DOCKER_COMPOSE_FILE_PATH,
    DOCKER_IMAGE,
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
        import airflowctl

        console.print(f"[green]✅ airflow already available: {airflowctl.__file__}")
    except ImportError:
        console.print("[red]❌ airflowctl not available in current environment")

    console.print("[yellow]================================")


def test_airflowctl_commands(tmp_path_factory, monkeypatch, login_command, login_output, test_commands):
    """Test airflowctl commands using docker-compose environment."""
    tmp_dir = tmp_path_factory.mktemp("airflow-ctl-test")
    console.print(f"[yellow]Tests are run in {tmp_dir}")

    # Copy docker-compose.yaml to temp directory
    tmp_docker_compose_file = tmp_dir / "docker-compose.yaml"
    copyfile(DOCKER_COMPOSE_FILE_PATH, tmp_docker_compose_file)

    # Set environment variables for the test
    monkeypatch.setenv("AIRFLOW_IMAGE_NAME", DOCKER_IMAGE)
    monkeypatch.setenv("AIRFLOW_CTL_VERSION", os.environ.get("AIRFLOW_CTL_VERSION", "1.0.0"))

    # Initialize Docker client
    compose = DockerClient(compose_files=[str(tmp_docker_compose_file)])

    host_envs = os.environ.copy()
    host_envs["AIRFLOW_CLI_DEBUG_MODE"] = "true"
    # Testing commands of airflowctl
    try:
        compose.compose.up(detach=True, wait=True)
        console.print("[green]Docker compose started for airflowctl test\n")
        for command in test_commands:
            command_from_config = f"airflowctl {command}"
            # We need to run auth login first for all commands except login itself
            if command != login_command:
                run_command = f"airflowctl {login_command} && {command_from_config}"
            else:
                run_command = command_from_config
            console.print(f"[yellow]Running command: {command}")

            # Give some time for the command to execute and output to be ready
            proc = Popen(
                run_command.encode(), stdout=PIPE, stderr=STDOUT, stdin=PIPE, shell=True, env=host_envs
            )
            stdout_result, stderr_result = proc.communicate(timeout=60)

            # CLI command gave errors
            if stderr_result:
                console.print(
                    f"[red]Errors while executing command '{command_from_config}':\n{stderr_result.decode()}"
                )

            # Decode the output
            stdout_result = stdout_result.decode()
            # We need to trim auth login output if the command is not login itself and clean backspaces
            if command != login_command:
                if login_output not in stdout_result:
                    console.print(
                        f"[red]❌ Login output not found before command output for '{command_from_config}'"
                    )
                    console.print(f"[red]Full output:\n{stdout_result}\n")
                    raise AssertionError("Login output not found before command output")
                stdout_result = stdout_result.split(f"{login_output}\n")[1].strip()
            else:
                stdout_result = stdout_result.strip()

            # This is a common error message that is thrown by client when something is wrong
            # Please ensure it is aligning with airflowctl.api.client.get_json_error
            airflowctl_client_server_response_error = "Server error"
            if airflowctl_client_server_response_error in stdout_result:
                console.print(f"[red]❌ Output contained unexpected text for command '{command_from_config}'")
                console.print(f"[red]Did not expect to find:\n{airflowctl_client_server_response_error}\n")
                console.print(f"[red]But got:\n{stdout_result}\n")
                raise AssertionError(f"Output contained unexpected text\nOutput:\n{stdout_result}")
            console.print(
                f"[green]✅ Output did not contain unexpected text for command '{command_from_config}'"
            )
            console.print(f"[cyan]Result:\n{stdout_result}\n")
            proc.kill()
    except Exception:
        print_diagnostics(compose, compose.version(), docker.version())
        raise
    finally:
        if not os.environ.get("SKIP_DOCKER_COMPOSE_DELETION"):
            compose.compose.down(remove_orphans=True, volumes=True, quiet=True)
            console.print("[green]Docker compose instance deleted")
