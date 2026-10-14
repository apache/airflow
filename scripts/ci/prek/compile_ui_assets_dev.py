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

import contextlib
import os
import subprocess
import sys
import time

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

DEV_SERVER_POLL_INTERVAL_SECONDS = 1.0

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

    ui_dev_server = subprocess.Popen(
        ["pnpm", "dev"],
        cwd=os.fspath(UI_DIRECTORY),
        env=env,
        stdout=open(UI_ASSET_OUT_DEV_MODE_FILE, "a"),
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

    simple_auth_manager_ui_dev_server = subprocess.Popen(
        ["pnpm", "dev"],
        cwd=os.fspath(SIMPLE_AUTH_MANAGER_UI_DIRECTORY),
        env=env,
        stdout=open(SIMPLE_AUTH_MANAGER_UI_ASSET_OUT_DEV_MODE_FILE, "a"),
        stderr=subprocess.STDOUT,
    )

    dev_servers = {
        "airflow-ui": (ui_dev_server, UI_ASSET_OUT_DEV_MODE_FILE),
        "simple-auth-manager-ui": (
            simple_auth_manager_ui_dev_server,
            SIMPLE_AUTH_MANAGER_UI_ASSET_OUT_DEV_MODE_FILE,
        ),
    }

    # Keep script alive so child processes stay in the same process group.
    # When breeze exits, kill_process_group() will terminate all processes together.
    try:
        exited_server_name = None
        while exited_server_name is None:
            for server_name, (dev_server, _) in dev_servers.items():
                if dev_server.poll() is not None:
                    exited_server_name = server_name
                    break
            else:
                time.sleep(DEV_SERVER_POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        for dev_server, _ in dev_servers.values():
            with contextlib.suppress(OSError):
                dev_server.terminate()
        raise SystemExit(130)

    exited_server, exited_server_out_file = dev_servers[exited_server_name]
    print(
        f"\nThe {exited_server_name} dev server exited unexpectedly "
        f"with code {exited_server.returncode}. "
        f"Check {exited_server_out_file} for details. "
        "Terminating the remaining UI dev servers.",
        file=sys.stderr,
        flush=True,
    )
    for server_name, (dev_server, _) in dev_servers.items():
        if server_name != exited_server_name and dev_server.poll() is None:
            dev_server.terminate()
            with contextlib.suppress(subprocess.TimeoutExpired):
                dev_server.wait(timeout=5)
    raise SystemExit(1)
