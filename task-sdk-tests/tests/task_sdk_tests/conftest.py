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

from task_sdk_tests.constants import AIRFLOW_ROOT_PATH


def pytest_sessionstart(session):
    """Install Task SDK at the very start of the pytest session."""
    from rich.console import Console

    console = Console(width=400, color_system="standard")

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
