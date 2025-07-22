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

from task_sdk_tests.constants import AIRFLOW_ROOT_PATH, DOCKER_IMAGE


@pytest.fixture
def default_docker_image():
    return DOCKER_IMAGE


@pytest.fixture(scope="session")
def install_task_sdk():
    """Install Task SDK before any tests run to avoid import caching issues."""
    from rich.console import Console

    console = Console(width=400, color_system="standard")

    task_sdk_version = os.environ.get("TASK_SDK_VERSION", "1.1.0")
    console.print(f"[yellow]Installing apache-airflow-task-sdk=={task_sdk_version} via fixture...")

    task_sdk_path = AIRFLOW_ROOT_PATH / "task-sdk"
    console.print(f"[blue]Installing from: {task_sdk_path}")

    # Install directly to current UV environment (no --python flag needed)
    # When running under `uv run pytest`, we're already in UV's isolated environment
    console.print("[blue]Installing to current UV environment...")
    console.print(f"[blue]Current Python: {sys.executable}")

    try:
        # Install to current environment without --python flag
        cmd = ["uv", "pip", "install", str(task_sdk_path)]
        console.print(f"[cyan]Running command: {' '.join(cmd)}")
        subprocess.check_call(cmd)
        console.print("[green]Task SDK installed successfully to UV environment via fixture!")
    except (subprocess.CalledProcessError, FileNotFoundError) as uv_error:
        console.print(f"[yellow]UV installation failed: {uv_error}")
        console.print("[yellow]Trying fallback with pip in current environment...")

        # Fallback to pip in current environment
        cmd = [sys.executable, "-m", "pip", "install", str(task_sdk_path)]
        console.print(f"[cyan]Running fallback command: {' '.join(cmd)}")
        subprocess.check_call(cmd)
        console.print("[green]Task SDK installed successfully with pip via fixture!")

    # Verify installation
    console.print("[yellow]Verifying installation via fixture...")
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import airflow.sdk.api.client; print('✅ Import successful via fixture')",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        console.print(f"[green]{result.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        console.print("[red]❌ Import verification failed via fixture:")
        console.print(f"[red]Return code: {e.returncode}")
        console.print(f"[red]Stdout: {e.stdout}")
        console.print(f"[red]Stderr: {e.stderr}")
        raise

    return True
