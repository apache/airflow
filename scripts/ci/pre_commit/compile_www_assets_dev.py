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
from pathlib import Path

# NOTE!. This script is executed from node environment created by pre-commit and this environment
# Cannot have additional Python dependencies installed. We should not import any of the libraries
# here that are not available in stdlib! You should not import common_precommit_utils.py here because
# They are importing rich library which is not available in the node environment.

if __name__ not in ("__main__", "__mp_main__"):
    msg = (
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )
    raise SystemExit(msg)

AIRFLOW_SOURCES_PATH = Path(__file__).parents[3].resolve()
WWW_CACHE_DIR = AIRFLOW_SOURCES_PATH / ".build" / "www"
WWW_HASH_FILE = WWW_CACHE_DIR / "hash.txt"
WWW_ASSET_OUT_FILE = WWW_CACHE_DIR / "asset_compile.out"
WWW_ASSET_OUT_DEV_MODE_FILE = WWW_CACHE_DIR / "asset_compile_dev_mode.out"

if __name__ == "__main__":
    www_directory = AIRFLOW_SOURCES_PATH / "airflow" / "www"
    WWW_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if WWW_HASH_FILE.exists():
        # cleanup hash of www so that next compile-assets recompiles them
        WWW_HASH_FILE.unlink()
    env = os.environ.copy()
    env["FORCE_COLOR"] = "true"
    WWW_ASSET_OUT_FILE.unlink(missing_ok=True)
    with open(WWW_ASSET_OUT_DEV_MODE_FILE, "w") as f:
        subprocess.run(
            ["yarn", "install", "--frozen-lockfile"],
            cwd=os.fspath(www_directory),
            check=True,
            stdout=f,
            stderr=subprocess.STDOUT,
        )
        subprocess.run(
            ["yarn", "dev"],
            check=True,
            cwd=os.fspath(www_directory),
            env=env,
            stdout=f,
            stderr=subprocess.STDOUT,
        )
