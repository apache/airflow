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
import re
import shutil
import subprocess
import sys
from pathlib import Path

# NOTE!. This script is executed from node environment created by pre-commit and this environment
# Cannot have additional Python dependencies installed. We should not import any of the libraries
# here that are not available in stdlib! You should not import common_precommit_utils.py here because
# They are importing rich library which is not available in the node environment.

AIRFLOW_SOURCES_PATH = Path(__file__).parents[3].resolve()
UI_HASH_FILE = AIRFLOW_SOURCES_PATH / ".build" / "ui" / "hash.txt"


def get_directory_hash(directory: Path, skip_path_regexp: str | None = None) -> str:
    files = sorted(directory.rglob("*"))
    if skip_path_regexp:
        matcher = re.compile(skip_path_regexp)
        files = [file for file in files if not matcher.match(os.fspath(file.resolve()))]
    sha = hashlib.sha256()
    for file in files:
        if file.is_file() and not file.name.startswith("."):
            sha.update(file.read_bytes())
    return sha.hexdigest()


if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

if __name__ == "__main__":
    ui_directory = AIRFLOW_SOURCES_PATH / "airflow" / "ui"
    node_modules_directory = ui_directory / "node_modules"
    dist_directory = ui_directory / "dist"
    UI_HASH_FILE.parent.mkdir(exist_ok=True, parents=True)
    if node_modules_directory.exists() and dist_directory.exists():
        old_hash = UI_HASH_FILE.read_text() if UI_HASH_FILE.exists() else ""
        new_hash = get_directory_hash(ui_directory, skip_path_regexp=r".*node_modules.*")
        if new_hash == old_hash:
            print("The UI directory has not changed! Skip regeneration.")
            sys.exit(0)
    else:
        shutil.rmtree(node_modules_directory, ignore_errors=True)
        shutil.rmtree(dist_directory, ignore_errors=True)
    env = os.environ.copy()
    env["FORCE_COLOR"] = "true"
    subprocess.check_call(
        ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"],
        cwd=os.fspath(ui_directory),
    )
    subprocess.check_call(["pnpm", "run", "build"], cwd=os.fspath(ui_directory), env=env)
    new_hash = get_directory_hash(ui_directory, skip_path_regexp=r".*node_modules.*")
    UI_HASH_FILE.write_text(new_hash)
