#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""Run mypy on entire folders using local virtualenv (uv) instead of breeze.

Used for non-provider projects: airflow-core, task-sdk, airflow-ctl, dev, scripts,
devel-common, airflow-ctl-tests, helm-tests, airflow-e2e-tests,
task-sdk-integration-tests, docker-tests, kubernetes-tests, shared.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

from common_prek_utils import (
    AIRFLOW_ROOT_PATH,
)

CI = os.environ.get("CI")

try:
    from rich.console import Console

    console = Console(width=400, color_system="standard")
except ImportError:
    console = None  # type: ignore[assignment]

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

_TOP_LEVEL_ALLOWED_FOLDERS = [
    "airflow-core",
    "dev",
    "scripts",
    "devel-common",
    "task-sdk",
    "airflow-ctl",
    "airflow-ctl-tests",
    "helm-tests",
    "airflow-e2e-tests",
    "task-sdk-integration-tests",
    "docker-tests",
    "kubernetes-tests",
]

# Each shared/<dist> workspace member is an allowed folder in its own right, giving
# every shared library its own dedicated mypy hook / virtualenv / cache.
_SHARED_DISTS = sorted(
    f"shared/{p.name}" for p in (AIRFLOW_ROOT_PATH / "shared").iterdir() if (p / "pyproject.toml").exists()
)

ALLOWED_FOLDERS = _TOP_LEVEL_ALLOWED_FOLDERS + _SHARED_DISTS

# Map folder(s) to the uv project to use for running mypy.
# When multiple folders are checked together, the first folder's project is used.
FOLDER_TO_PROJECT = {f: f for f in ALLOWED_FOLDERS}

# Projects that ship their own [tool.mypy] configuration in their pyproject.toml;
# mypy must be invoked with --config-file pointing at that file so those sections
# take precedence over the root pyproject.toml.
FOLDER_TO_MYPY_CONFIG = {
    "dev": "dev/pyproject.toml",
    "scripts": "scripts/pyproject.toml",
}

if len(sys.argv) < 2:
    if console:
        console.print(f"[yellow]You need to specify the folder to test as parameter: {ALLOWED_FOLDERS}\n")
    else:
        print(f"You need to specify the folder to test as parameter: {ALLOWED_FOLDERS}")
    sys.exit(1)

mypy_folders = sys.argv[1:]

show_unused_warnings = os.environ.get("SHOW_UNUSED_MYPY_WARNINGS", "false")
show_unreachable_warnings = os.environ.get("SHOW_UNREACHABLE_MYPY_WARNINGS", "false")

for mypy_folder in mypy_folders:
    if mypy_folder not in ALLOWED_FOLDERS:
        if console:
            console.print(
                f"\n[red]ERROR: Folder `{mypy_folder}` is wrong.[/]\n\n"
                f"All folders passed should be one of those: {ALLOWED_FOLDERS}\n"
            )
        else:
            print(
                f"\nERROR: Folder `{mypy_folder}` is wrong.\n\n"
                f"All folders passed should be one of those: {ALLOWED_FOLDERS}\n"
            )
        sys.exit(1)

exclude_regexps = [
    re.compile(x)
    for x in [
        r"^.*/node_modules/.*",
        r"^.*\\..*",
        r"^.*/src/airflow/__init__.py$",
    ]
]


def get_all_files(folder: str) -> list[str]:
    files_to_check = []
    python_file_paths = (AIRFLOW_ROOT_PATH / folder).resolve().rglob("*.py")
    for file in python_file_paths:
        if (
            file.name not in ("conftest.py",)
            and not any(x.match(file.as_posix()) for x in exclude_regexps)
            and not any(part.startswith(".") for part in file.parts)
        ):
            files_to_check.append(file.relative_to(AIRFLOW_ROOT_PATH).as_posix())
    return files_to_check


mypy_extra_args: list[str] = []

if show_unused_warnings == "true":
    if console:
        console.print("[info]Running mypy with --warn-unused-ignores")
    else:
        print("Running mypy with --warn-unused-ignores")
    mypy_extra_args.append("--warn-unused-ignores")

if show_unreachable_warnings == "true":
    if console:
        console.print("[info]Running mypy with --warn-unreachable")
    else:
        print("Running mypy with --warn-unreachable")
    mypy_extra_args.append("--warn-unreachable")


def write_file_list(files: list[str], suffix: str = "") -> Path:
    name = "mypy_files.txt" if not suffix else f"mypy_files_{suffix}.txt"
    mypy_file_list = AIRFLOW_ROOT_PATH / "files" / name
    mypy_file_list.parent.mkdir(parents=True, exist_ok=True)
    mypy_file_list.write_text("\n".join(files))
    if console:
        console.print(f"[info]You can check the list of files in:[/] {mypy_file_list}")
    else:
        print(f"You can check the list of files in: {mypy_file_list}")
    return mypy_file_list


def run_local_mypy(project: str, hook_name: str, files: list[str], config_file: str | None = None) -> int:
    """Sync a dedicated mypy venv under .build/ and run mypy on the given files.

    Each hook gets its own virtualenv and mypy cache so running mypy never mutates the
    contributor's regular project .venv and each hook keeps a stable, CI-aligned
    dependency set. UV_PROJECT_ENVIRONMENT redirects uv away from the default
    <project>/.venv to our cached location.
    """
    mypy_venv_dir = AIRFLOW_ROOT_PATH / ".build" / "mypy-venvs" / hook_name
    mypy_cache_dir = AIRFLOW_ROOT_PATH / ".build" / "mypy-caches" / hook_name
    mypy_venv_dir.parent.mkdir(parents=True, exist_ok=True)
    mypy_cache_dir.parent.mkdir(parents=True, exist_ok=True)

    run_env = {
        **os.environ,
        "TERM": "ansi",
        "UV_PROJECT_ENVIRONMENT": str(mypy_venv_dir),
    }

    sync_cmd = ["uv", "sync", "--frozen", "--project", project, "--group", "mypy"]
    if console:
        console.print(
            f"[magenta]Syncing dedicated mypy virtualenv ({mypy_venv_dir}) "
            f"for project {project}: {' '.join(sync_cmd)}[/]"
        )
    else:
        print(
            f"Syncing dedicated mypy virtualenv ({mypy_venv_dir}) for project {project}: {' '.join(sync_cmd)}"
        )
    sync_result = subprocess.run(
        sync_cmd,
        cwd=str(AIRFLOW_ROOT_PATH),
        check=False,
        env=run_env,
    )
    if sync_result.returncode != 0:
        msg = (
            f"`uv sync --frozen --project {project}` failed for the mypy virtualenv at "
            f"{mypy_venv_dir}. Fix the sync error before running mypy — otherwise the "
            "dedicated mypy virtualenv will not match uv.lock and results will diverge "
            "from CI. You can remove the cached virtualenv with:\n"
            "  breeze down --cleanup-mypy-cache\n"
        )
        if console:
            console.print(f"[red]{msg}")
        else:
            print(msg)
        return sync_result.returncode

    mypy_file_list = write_file_list(files, suffix=hook_name.replace("/", "_"))

    # --follow-imports=silent: each hook only reports errors for files it owns. Transitive
    # errors from imports into other workspace projects are not reported here — those files
    # are covered by their own hook. Without this, mypy can produce different results for the
    # same file across hooks because each hook's virtualenv installs a different dependency
    # set that influences type inference.
    cmd = [
        "uv",
        "run",
        "--frozen",
        "--project",
        project,
        "--group",
        "mypy",
        "mypy",
        "--cache-dir",
        str(mypy_cache_dir),
        "--follow-imports=silent",
    ]
    if config_file is not None:
        cmd += ["--config-file", config_file]
    cmd += [f"@{mypy_file_list}", *mypy_extra_args]

    result = subprocess.run(
        cmd,
        cwd=str(AIRFLOW_ROOT_PATH),
        check=False,
        env=run_env,
    )
    return result.returncode


if console:
    console.print(f"[magenta]Running mypy for folders: {mypy_folders}[/]")
else:
    print(f"Running mypy for folders: {mypy_folders}")

if CI:
    # In CI, run inside the breeze Docker image to avoid needing a local environment
    # and to not change uv.lock or synchronize dependencies.
    from common_prek_utils import (
        initialize_breeze_prek,
        run_command_via_breeze_shell,
    )

    initialize_breeze_prek(__name__, __file__)

    all_files_to_check = []
    for mypy_folder in mypy_folders:
        all_files_to_check.extend(get_all_files(mypy_folder))

    if not all_files_to_check:
        print("No files to test. Quitting")
        sys.exit(0)

    write_file_list(all_files_to_check)
    file_argument_ci = "@/files/mypy_files.txt"

    mypy_cmd = (
        f"TERM=ansi mypy --follow-imports=silent {shlex.quote(file_argument_ci)} {' '.join(mypy_extra_args)}"
    )
    result = run_command_via_breeze_shell(
        cmd=["bash", "-c", mypy_cmd],
        warn_image_upgrade_needed=True,
        extra_env={
            "INCLUDE_MYPY_VOLUME": "false",
            "MOUNT_SOURCES": "selected",
        },
    )
    returncode = result.returncode
else:
    all_files_to_check = []
    for mypy_folder in mypy_folders:
        all_files_to_check.extend(get_all_files(mypy_folder))

    if not all_files_to_check:
        print("No files to test. Quitting")
        sys.exit(0)

    project = FOLDER_TO_PROJECT.get(mypy_folders[0], "devel-common")
    # hook_name derives venv/cache paths under .build/; replace "/" so "shared/dist"
    # becomes ".build/mypy-venvs/shared-dist" rather than a nested path.
    hook_name = mypy_folders[0].replace("/", "-")
    returncode = run_local_mypy(
        project=project,
        hook_name=hook_name,
        files=all_files_to_check,
        config_file=FOLDER_TO_MYPY_CONFIG.get(mypy_folders[0]),
    )

if returncode != 0:
    hook_label = mypy_folders[0].replace("/", "-")
    msg = (
        "Mypy check failed. You can run mypy locally with:\n"
        f"  prek run mypy-{hook_label} --all-files\n"
        "The hook uses dedicated virtualenv(s) and mypy cache(s) under .build/ so it does\n"
        "not touch your regular project .venv. You can clear both with:\n"
        "  breeze down --cleanup-mypy-cache\n"
    )
    if console:
        console.print(f"[yellow]{msg}")
    else:
        print(msg)
sys.exit(returncode)
