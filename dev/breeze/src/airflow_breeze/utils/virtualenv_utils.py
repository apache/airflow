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

import sys
from pathlib import Path

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_utils import run_command


def create_venv(
    venv_path: str | Path,
    pip_version: str,
    uv_version: str,
    requirements_file: str | Path | None = None,
) -> Path:
    venv_path = Path(venv_path).resolve().absolute()
    venv_command_result = run_command(
        ["uv", "venv", "--seed", venv_path.as_posix()],
        check=False,
        capture_output=True,
    )
    if venv_command_result.returncode != 0:
        get_console().print(
            f"[error]Error when initializing virtualenv in {venv_path.as_posix()}:[/]\n"
            f"{venv_command_result.stdout}\n{venv_command_result.stderr}"
        )
        sys.exit(venv_command_result.returncode)
    python_path = venv_path / "bin" / "python"
    if not python_path.exists():
        get_console().print(f"\n[errors]Python interpreter is not exist in path {python_path}. Exiting!\n")
        sys.exit(1)
    result = run_command(
        [python_path.as_posix(), "-m", "pip", "install", f"pip=={pip_version}", f"uv=={uv_version}", "-q"],
        check=False,
        capture_output=False,
        text=True,
    )
    if result.returncode != 0:
        get_console().print(
            f"[error]Error when installing pip and uv in {venv_path.as_posix()}[/]\n"
            f"{result.stdout}\n{result.stderr}"
        )
        sys.exit(result.returncode)
    if requirements_file:
        requirements_file = Path(requirements_file).absolute().as_posix()
        result = run_command(
            [python_path.as_posix(), "-m", "uv", "pip", "install", "-r", requirements_file, "-q"],
            check=True,
            capture_output=False,
            text=True,
        )
        if result.returncode != 0:
            get_console().print(
                f"[error]Error when installing packages from {requirements_file}[/]\n"
                f"{result.stdout}\n{result.stderr}"
            )
            sys.exit(result.returncode)
    return python_path
