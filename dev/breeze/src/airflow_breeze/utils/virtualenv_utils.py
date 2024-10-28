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

import contextlib
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path

from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_utils import run_command


def create_pip_command(python: str | Path) -> list[str]:
    return [
        python.as_posix() if hasattr(python, "as_posix") else str(python),
        "-m",
        "pip",
    ]


def create_venv(
    venv_path: str | Path,
    python: str | None = None,
    pip_version: str | None = None,
    requirements_file: str | Path | None = None,
) -> str:
    venv_path = Path(venv_path).resolve().absolute()
    venv_command_result = run_command(
        [python or sys.executable, "-m", "venv", venv_path.as_posix()],
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
        get_console().print(
            f"\n[errors]Python interpreter is not exist in path {python_path}. Exiting!\n"
        )
        sys.exit(1)
    pip_command = create_pip_command(python_path)
    if pip_version:
        result = run_command(
            [*pip_command, "install", f"pip=={pip_version}", "-q"],
            check=False,
            capture_output=False,
            text=True,
        )
        if result.returncode != 0:
            get_console().print(
                f"[error]Error when installing pip in {venv_path.as_posix()}[/]\n"
                f"{result.stdout}\n{result.stderr}"
            )
            sys.exit(result.returncode)
    if requirements_file:
        requirements_file = Path(requirements_file).absolute().as_posix()
        result = run_command(
            [*pip_command, "install", "-r", requirements_file, "-q"],
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
    return python_path.as_posix()


@contextlib.contextmanager
def create_temp_venv(
    python: str | None = None,
    pip_version: str | None = None,
    requirements_file: str | Path | None = None,
    prefix: str | None = None,
) -> Generator[str, None, None]:
    with tempfile.TemporaryDirectory(prefix=prefix) as tmp_dir_name:
        yield create_venv(
            Path(tmp_dir_name) / ".venv",
            python=python,
            pip_version=pip_version,
            requirements_file=requirements_file,
        )
