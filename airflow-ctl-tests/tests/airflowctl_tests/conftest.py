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
import sys

import pytest

from airflowctl_tests import console
from airflowctl_tests.constants import (
    AIRFLOW_ROOT_PATH,
    LOGIN_COMMAND,
    LOGIN_OUTPUT,
    TEST_AIRFLOW_VERSION,
)


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
    airflow_processes: list = []


# Pytest hook to run at the start of the session
def pytest_sessionstart(session):
    import subprocess

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

    start_airflow_services(session.config._tmp_path_factory)


def print_diagnostics(subprocess_results: list):
    """Print diagnostic information when test fails."""
    console.print("[red]=== DIAGNOSTIC INFORMATION ===[/]")
    try:
        for result in subprocess_results:
            console.print(f"[yellow]Command: {result['command']}")
            console.print(f"[blue]Return code: {result['returncode']}")
            console.print(f"[blue]Stdout:\n{result['stdout']}")
            console.print(f"[blue]Stderr:\n{result['stderr']}")
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


def start_airflow_services(tmp_path_factory):
    """Start Airflow services directly without docker-compose."""
    import subprocess
    import time

    # If things started getting flaky for waiting jobs to run, increase this
    process_wait_time = 10

    tmp_dir = tmp_path_factory.mktemp("airflow-ctl-test")
    airflow_home = tmp_dir / "airflow"
    airflow_home.mkdir(exist_ok=True)

    # Set Airflow environment
    os.environ["AIRFLOW_UID"] = str(os.getuid())

    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    os.environ["AIRFLOW_CTL_CLI_DEBUG_MODE"] = "true"
    os.environ["AIRFLOW__API__EXPOSE_CONFIG"] = "true"
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "LocalExecutor"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "true"

    # cat and read version from airflow.cfg
    console.print(f"[cyan]Airflow version used in airflowctl tests: {TEST_AIRFLOW_VERSION}")

    _CtlTestState.airflow_home = str(airflow_home)

    console.print("[yellow]Initializing Airflow database")
    subprocess.run(
        ["airflow", "db", "migrate"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={
            **os.environ,
        },
        check=False,
    )
    time.sleep(process_wait_time * 2)  # Give extra time for DB to initialize

    console.print("[blue]Starting Airflow API Server")
    api_server = subprocess.Popen(
        ["airflow", "api-server"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={
            **os.environ,
        },
    )
    _CtlTestState.airflow_processes.append(api_server)
    time.sleep(process_wait_time * 2)

    # Health check loop to ensure API server is up before starting other services
    import requests

    api_url = "http://localhost:8080/api/v2/monitor/health"
    for _ in range(10):
        try:
            response = requests.get(api_url)
            if (
                response.status_code == 200
                and response.json().get("metadatabase", {}).get("status") == "healthy"
            ):
                console.print("[green]Airflow API Server is healthy!")
                break
        except requests.RequestException:
            pass
        console.print("[yellow]Waiting for Airflow API Server to be healthy...")
        time.sleep(5)

    console.print("[blue]Starting Airflow Scheduler")
    scheduler = subprocess.Popen(
        ["airflow", "scheduler"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={
            **os.environ,
        },
    )
    _CtlTestState.airflow_processes.append(scheduler)
    time.sleep(process_wait_time)

    console.print("[blue]Starting Airflow Dag Processor")
    dag_processor = subprocess.Popen(
        ["airflow", "dag-processor"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={
            **os.environ,
        },
    )
    _CtlTestState.airflow_processes.append(dag_processor)
    time.sleep(process_wait_time)

    console.print("[green]Airflow services started!\n")


def stop_airflow_services():
    """Stop all Airflow processes."""
    console.print("[yellow]Stopping Airflow services...")
    for proc in _CtlTestState.airflow_processes:
        proc.terminate()
        proc.wait(timeout=10)
    console.print("[green]Airflow services stopped")


def pytest_sessionfinish(session, exitstatus):
    """Tear down test environment at the end of the pytest session."""
    stop_airflow_services()
