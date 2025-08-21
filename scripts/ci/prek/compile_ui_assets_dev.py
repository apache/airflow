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

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH, AIRFLOW_ROOT_PATH

# NOTE!. This script is executed from a node environment created by a prek hook, and this environment
# Cannot have additional Python dependencies installed. We should not import any of the libraries
# here that are not available in stdlib! You should not import common_prek_utils.py here because
# They are importing the rich library which is not available in the node environment.

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

UI_CACHE_DIR = AIRFLOW_ROOT_PATH / ".build" / "ui"


UI_DIRECTORY = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "ui"
UI_HASH_FILE = UI_CACHE_DIR / "hash.txt"
UI_ASSET_OUT_FILE = UI_CACHE_DIR / "asset_compile.out"
UI_ASSET_OUT_DEV_MODE_FILE = UI_CACHE_DIR / "asset_compile_dev_mode.out"


SIMPLE_AUTH_MANAGER_UI_DIRECTORY = (
    AIRFLOW_CORE_SOURCES_PATH / "airflow" / "api_fastapi" / "auth" / "managers" / "simple" / "ui"
)
SIMPLE_AUTH_MANAGER_UI_HASH_FILE = UI_CACHE_DIR / "simple-auth-manager-hash.txt"
SIMPLE_AUTH_MANAGER_UI_ASSET_OUT_FILE = UI_CACHE_DIR / "simple_auth_manager_asset_compile.out"
SIMPLE_AUTH_MANAGER_UI_ASSET_OUT_DEV_MODE_FILE = (
    UI_CACHE_DIR / "simple_auth_manager_asset_compile_dev_mode.out"
)

if __name__ == "__main__":
    UI_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["FORCE_COLOR"] = "true"

    if UI_HASH_FILE.exists():
        # cleanup hash of ui so that next compile-assets recompiles them
        UI_HASH_FILE.unlink()
    UI_ASSET_OUT_FILE.unlink(missing_ok=True)

    if SIMPLE_AUTH_MANAGER_UI_HASH_FILE.exists():
        # cleanup hash of ui so that next compile-assets recompiles them
        SIMPLE_AUTH_MANAGER_UI_HASH_FILE.unlink()
    SIMPLE_AUTH_MANAGER_UI_ASSET_OUT_FILE.unlink(missing_ok=True)

    with open(UI_ASSET_OUT_DEV_MODE_FILE, "w") as f:
        subprocess.run(
            ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"],
            cwd=os.fspath(UI_DIRECTORY),
            check=True,
            stdout=f,
            stderr=subprocess.STDOUT,
        )
        subprocess.Popen(
            ["pnpm", "dev"],
            cwd=os.fspath(UI_DIRECTORY),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
        )

    with open(SIMPLE_AUTH_MANAGER_UI_ASSET_OUT_DEV_MODE_FILE, "w") as f:
        subprocess.run(
            ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"],
            cwd=os.fspath(SIMPLE_AUTH_MANAGER_UI_DIRECTORY),
            check=True,
            stdout=f,
            stderr=subprocess.STDOUT,
        )
        subprocess.run(
            ["pnpm", "dev"],
            check=True,
            cwd=os.fspath(SIMPLE_AUTH_MANAGER_UI_DIRECTORY),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
        )
