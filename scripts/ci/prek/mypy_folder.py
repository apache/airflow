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
from __future__ import annotations

import os
import re
import shlex
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_prek_utils import (
    AIRFLOW_ROOT_PATH,
    KNOWN_SECOND_LEVEL_PATHS,
    console,
    get_all_provider_ids,
    initialize_breeze_prek,
    run_command_via_breeze_shell,
)

initialize_breeze_prek(__name__, __file__)


ALLOWED_FOLDERS = [
    "airflow-core",
    *[f"providers/{provider_id.replace('.', '/')}" for provider_id in get_all_provider_ids()],
    "dev",
    "devel-common",
    "task-sdk",
    "airflow-ctl",
    "providers",
]

if len(sys.argv) < 2:
    console.print(f"[yellow]You need to specify the folder to test as parameter: {ALLOWED_FOLDERS}\n")
    sys.exit(1)

mypy_folders = sys.argv[1:]

show_unused_warnings = os.environ.get("SHOW_UNUSED_MYPY_WARNINGS", "false")
show_unreachable_warnings = os.environ.get("SHOW_UNREACHABLE_MYPY_WARNINGS", "false")

for mypy_folder in mypy_folders:
    if mypy_folder not in ALLOWED_FOLDERS:
        console.print(
            f"\n[red]ERROR: Folder `{mypy_folder}` is wrong.[/]\n\n"
            f"All folders passed should be one of those: {ALLOWED_FOLDERS}\n"
        )
        sys.exit(1)

arguments = mypy_folders.copy()

MYPY_FILE_LIST = AIRFLOW_ROOT_PATH / "files" / "mypy_files.txt"
MYPY_FILE_LIST.parent.mkdir(parents=True, exist_ok=True)

FILE_ARGUMENT = "@/files/mypy_files.txt"

all_provider_duplicated_path_to_ignore = []

for second_level_path in KNOWN_SECOND_LEVEL_PATHS:
    all_provider_duplicated_path_to_ignore.extend(
        [
            rf"^.*/providers/{second_level_path}/.*/src/airflow/providers/{second_level_path}/__init__.py$",
            rf"^.*/providers/{second_level_path}/.*/tests/unit/{second_level_path}/__init__.py$",
            rf"^.*/providers/{second_level_path}/.*/tests/integration/{second_level_path}/__init__.py$",
            rf"^.*/providers/{second_level_path}/.*/tests/system/{second_level_path}/__init__.py$",
        ]
    )

exclude_regexps = [
    re.compile(x)
    for x in [
        r"^.*/node_modules/.*",
        r"^.*\\..*",
        r"^.*/src/airflow/__init__.py$",
        # Remove duplicated (legacy namespace packages) __init__.py files from providers
        r"^.*/providers/src/airflow/__init__.py$",
        r"^.*/providers/src/airflow/providers/__init__.py$",
        r"^.*/providers/.*/tests/unit/__init__.py$",
        r"^.*/providers/.*/tests/integration/__init__.py$",
        r"^.*/providers/.*/tests/system/__init__.py$",
        *all_provider_duplicated_path_to_ignore,
    ]
]


def get_all_files(folder: str) -> list[str]:
    files_to_check = []
    python_file_paths = (AIRFLOW_ROOT_PATH / folder).resolve().rglob("*.py")
    for file in python_file_paths:
        if (
            (file.name not in ("conftest.py",) and not any(x.match(file.as_posix()) for x in exclude_regexps))
            and not any(part.startswith(".") for part in file.parts)
        ) and not file.as_posix().endswith("src/airflow/providers/__init__.py"):
            files_to_check.append(file.relative_to(AIRFLOW_ROOT_PATH).as_posix())
    file_spec = "@/files/mypy_files.txt"
    if console:
        console.print(f"[info]Running mypy with {file_spec}")
    else:
        print(f"info: Running mypy with {file_spec}")
    return files_to_check


all_files_to_check = []

for mypy_folder in mypy_folders:
    all_files_to_check.extend(get_all_files(mypy_folder))
MYPY_FILE_LIST.write_text("\n".join(all_files_to_check))

if console:
    if os.environ.get("CI"):
        console.print("[info]The content of the file is:")
        console.print(all_files_to_check)
    else:
        console.print(f"[info]You cand check the list of files in:[/] {MYPY_FILE_LIST}")
else:
    if os.environ.get("CI"):
        print("info: The content of the file is:")
        print(all_files_to_check)
    else:
        print(f"info: You cand check the list of files in: {MYPY_FILE_LIST}")

print(f"Running mypy with {FILE_ARGUMENT}")

mypy_cmd_parts = [f"TERM=ansi mypy {shlex.quote(FILE_ARGUMENT)}"]

if show_unused_warnings == "true":
    if console:
        console.print(
            "[info]Running mypy with --warn-unused-ignores to display unused ignores, unset environment variable: SHOW_UNUSED_MYPY_WARNINGS to runoff this behaviour"
        )
    else:
        print(
            "info: Running mypy with --warn-unused-ignores to display unused ignores, unset environment variable: SHOW_UNUSED_MYPY_WARNINGS to runoff this behaviour"
        )
    mypy_cmd_parts.append("--warn-unused-ignores")
if show_unreachable_warnings == "true":
    if console:
        console.print(
            "[info]Running mypy with --warn-unreachable to display unreachable code, unset environment variable: SHOW_UNREACHABLE_MYPY_WARNINGS to runoff this behaviour"
        )
    else:
        print(
            "info: Running mypy with --warn-unreachable to display unreachable code, unset environment variable: SHOW_UNREACHABLE_MYPY_WARNINGS to runoff this behaviour"
        )
    mypy_cmd_parts.append("--warn-unreachable")

mypy_cmd = " ".join(mypy_cmd_parts)

cmd = ["bash", "-c", mypy_cmd]

res = run_command_via_breeze_shell(
    cmd=cmd,
    warn_image_upgrade_needed=True,
    extra_env={
        "INCLUDE_MYPY_VOLUME": os.environ.get("INCLUDE_MYPY_VOLUME", "true"),
        # Need to mount local sources when running it - to not have to rebuild the image
        # and to let CI work on it when running on PRs from forks - because mypy-dev uses files
        # that are not available at the time when image is built in CI
        "MOUNT_SOURCES": "selected",
    },
)
ci_environment = os.environ.get("CI")
if res.returncode != 0:
    if console:
        if ci_environment:
            console.print(
                "[yellow]You are running mypy with the folders selected. If you want to "
                "reproduce it locally, you need to run the following command:\n"
            )
            console.print("prek --hook-stage manual mypy-<folder> --all-files\n")
        upgrading = os.environ.get("UPGRADE_TO_NEWER_DEPENDENCIES", "false") != "false"
        if upgrading:
            console.print(
                "[yellow]You are running mypy with the image that has dependencies upgraded automatically.\n"
            )
        flag = " --upgrade-to-newer-dependencies" if upgrading else ""
        console.print(
            "[yellow]If you see strange stacktraces above, and can't reproduce it, please run"
            " this command and try again:\n"
        )
        console.print(f"breeze ci-image build --python 3.10{flag}\n")
        console.print(
            "[yellow]You can also run `breeze down --cleanup-mypy-cache` to clean up the cache used.\n"
        )
    else:
        if ci_environment:
            print(
                "You are running mypy with the folders selected. If you want to "
                "reproduce it locally, you need to run the following command:\n"
            )
            print("prek --hook-stage manual mypy-<folder> --all-files\n")
        upgrading = os.environ.get("UPGRADE_TO_NEWER_DEPENDENCIES", "false") != "false"
        if upgrading:
            print("You are running mypy with the image that has dependencies upgraded automatically.\n")
        flag = " --upgrade-to-newer-dependencies" if upgrading else ""
        print(
            "If you see strange stacktraces above, and can't reproduce it, please run"
            " this command and try again:\n"
        )
        print(f"breeze ci-image build --python 3.10{flag}\n")
        print("You can also run `breeze down --cleanup-mypy-cache` to clean up the cache used.\n")
sys.exit(res.returncode)
