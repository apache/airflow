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

Used for non-provider projects: airflow-core, task-sdk, airflow-ctl, dev, scripts, devel-common.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys

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

ALLOWED_FOLDERS = [
    "airflow-core",
    "dev",
    "scripts",
    "devel-common",
    "task-sdk",
    "airflow-ctl",
]

# Map folder(s) to the uv project to use for running mypy.
# When multiple folders are checked together (e.g. dev + scripts), the first folder's project is used.
FOLDER_TO_PROJECT = {
    "airflow-core": "airflow-core",
    "task-sdk": "task-sdk",
    "airflow-ctl": "airflow-ctl",
    "devel-common": "devel-common",
    "dev": "dev",
    "scripts": "scripts",
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


all_files_to_check: list[str] = []
for mypy_folder in mypy_folders:
    all_files_to_check.extend(get_all_files(mypy_folder))

if not all_files_to_check:
    print("No files to test. Quitting")
    sys.exit(0)

# Write file list
mypy_file_list = AIRFLOW_ROOT_PATH / "files" / "mypy_files.txt"
mypy_file_list.parent.mkdir(parents=True, exist_ok=True)
mypy_file_list.write_text("\n".join(all_files_to_check))

if console:
    console.print(f"[info]You can check the list of files in:[/] {mypy_file_list}")
else:
    print(f"You can check the list of files in: {mypy_file_list}")

file_argument_local = f"@{mypy_file_list}"
file_argument_ci = "@/files/mypy_files.txt"

project = FOLDER_TO_PROJECT.get(mypy_folders[0], "devel-common")

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

    mypy_cmd = f"TERM=ansi mypy {shlex.quote(file_argument_ci)} {' '.join(mypy_extra_args)}"
    result = run_command_via_breeze_shell(
        cmd=["bash", "-c", mypy_cmd],
        warn_image_upgrade_needed=True,
        extra_env={
            "INCLUDE_MYPY_VOLUME": "false",
            "MOUNT_SOURCES": "selected",
        },
    )
else:
    # Locally, first synchronize the project's virtualenv with uv.lock so that mypy runs
    # against the same dependency set CI uses. Without this, the local .venv can drift from
    # uv.lock (e.g. after switching branches or installing extras) and mypy results would
    # diverge from CI. --frozen ensures uv.lock itself is not updated.
    sync_cmd = ["uv", "sync", "--frozen", "--project", project]
    if console:
        console.print(f"[magenta]Syncing virtualenv for project {project}: {' '.join(sync_cmd)}[/]")
    else:
        print(f"Syncing virtualenv for project {project}: {' '.join(sync_cmd)}")
    sync_result = subprocess.run(
        sync_cmd,
        cwd=str(AIRFLOW_ROOT_PATH),
        check=False,
        env={**os.environ, "TERM": "ansi"},
    )
    if sync_result.returncode != 0:
        msg = (
            f"`uv sync --frozen --project {project}` failed. Fix the sync error before running mypy — "
            "otherwise the local virtualenv may not match uv.lock and mypy results will diverge from CI.\n"
        )
        if console:
            console.print(f"[red]{msg}")
        else:
            print(msg)
        sys.exit(sync_result.returncode)

    # Then run mypy via uv with --frozen to not update the lock file.
    cmd = [
        "uv",
        "run",
        "--frozen",
        "--project",
        project,
        "--with",
        "apache-airflow-devel-common[mypy]",
        "mypy",
        file_argument_local,
        *mypy_extra_args,
    ]

    result = subprocess.run(
        cmd,
        cwd=str(AIRFLOW_ROOT_PATH),
        check=False,
        env={**os.environ, "TERM": "ansi"},
    )

if result.returncode != 0:
    msg = (
        "Mypy check failed. You can run mypy locally with:\n"
        f"  prek run mypy-{mypy_folders[0]} --all-files\n"
        "Or directly (first sync the virtualenv to match CI's dependency set):\n"
        f"  uv sync --frozen --project {project}\n"
        f'  uv run --frozen --project {project} --with "apache-airflow-devel-common[mypy]" mypy <files>\n'
        "You can also clear the mypy cache with:\n"
        "  breeze down --cleanup-mypy-cache\n"
    )
    if console:
        console.print(f"[yellow]{msg}")
    else:
        print(msg)
sys.exit(result.returncode)
