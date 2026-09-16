#!/usr/bin/env python3
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

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

# NOTE!. This script is executed from a node environment created by a prek hook, and this environment
# Cannot have additional Python dependencies installed. We should not import any of the libraries
# here that are not available in stdlib! You should not import common_prek_utils.py here because
# They are importing the rich library which is not available in the node environment.
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH, AIRFLOW_ROOT_PATH

MAIN_UI_DIRECTORY = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "ui"
MAIN_UI_HASH_FILE = AIRFLOW_ROOT_PATH / ".build" / "ui" / "hash.txt"

SIMPLE_AUTH_MANAGER_UI_DIRECTORY = (
    AIRFLOW_CORE_SOURCES_PATH / "airflow" / "api_fastapi" / "auth" / "managers" / "simple" / "ui"
)
SIMPLE_AUTH_MANAGER_UI_HASH_FILE = AIRFLOW_ROOT_PATH / ".build" / "ui" / "simple-auth-manager-hash.txt"

INTERNAL_SERVER_ERROR = "500 Internal Server Error"
SKIPPED_DIRECTORY_NAMES = frozenset({"node_modules", ".pnpm-store"})


def get_directory_hash(directory: Path) -> str:
    sha = hashlib.sha256()
    for root, directory_names, file_names in os.walk(directory):
        directory_names[:] = sorted(name for name in directory_names if name not in SKIPPED_DIRECTORY_NAMES)
        for file_name in sorted(file_names):
            if not file_name.startswith("."):
                sha.update((Path(root) / file_name).read_bytes())
    return sha.hexdigest()


def compile_assets(ui_directory: Path, hash_file: Path):
    node_modules_directory = ui_directory / "node_modules"
    dist_directory = ui_directory / "dist"
    hash_file.parent.mkdir(exist_ok=True, parents=True)
    if node_modules_directory.exists() and dist_directory.exists():
        old_hash = hash_file.read_text() if hash_file.exists() else ""
        new_hash = get_directory_hash(ui_directory)
        if new_hash == old_hash:
            print(f"The UI directory '{ui_directory}' has not changed! Skip regeneration.")
            return
    else:
        shutil.rmtree(node_modules_directory, ignore_errors=True)
        shutil.rmtree(dist_directory, ignore_errors=True)
    env = os.environ.copy()
    env["FORCE_COLOR"] = "true"
    # Allow users to override via NODE_OPTIONS, default to 4GB if not set
    if "NODE_OPTIONS" not in env:
        env["NODE_OPTIONS"] = "--max-old-space-size=4096"
    for try_num in range(3):
        print(f"### Trying to install pnpm dependencies: attempt: {try_num + 1} ###")
        result = subprocess.run(
            ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"],
            cwd=os.fspath(ui_directory),
            text=True,
            check=False,
            capture_output=True,
        )
        if result.returncode == 0:
            break
        if try_num == 2 or INTERNAL_SERVER_ERROR not in result.stderr + result.stdout:
            print(result.stdout + "\n" + result.stderr)
            sys.exit(result.returncode)
    subprocess.check_call(["pnpm", "run", "build"], cwd=os.fspath(ui_directory), env=env)
    new_hash = get_directory_hash(ui_directory)
    hash_file.write_text(new_hash)


if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

if __name__ == "__main__":
    compile_assets(MAIN_UI_DIRECTORY, MAIN_UI_HASH_FILE)
    compile_assets(SIMPLE_AUTH_MANAGER_UI_DIRECTORY, SIMPLE_AUTH_MANAGER_UI_HASH_FILE)
