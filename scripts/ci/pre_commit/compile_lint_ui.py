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

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
from common_precommit_utils import AIRFLOW_CORE_SOURCES_PATH

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

if __name__ == "__main__":
    dir = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "ui"
    subprocess.check_call(["pnpm", "config", "set", "store-dir", ".pnpm-store"], cwd=dir)
    subprocess.check_call(
        ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"], cwd=dir
    )
    subprocess.check_call(["pnpm", "codegen"], cwd=dir)
    subprocess.check_call(["pnpm", "format"], cwd=dir)
    subprocess.check_call(["pnpm", "lint:fix"], cwd=dir)

    dir = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "api_fastapi" / "auth" / "managers" / "simple" / "ui"
    subprocess.check_call(["pnpm", "config", "set", "store-dir", ".pnpm-store"], cwd=dir)

    subprocess.check_call(
        ["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"], cwd=dir
    )
    subprocess.check_call(["pnpm", "codegen"], cwd=dir)
    subprocess.check_call(["pnpm", "format"], cwd=dir)
    subprocess.check_call(["pnpm", "lint:fix"], cwd=dir)
