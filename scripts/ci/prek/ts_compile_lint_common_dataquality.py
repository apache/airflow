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
"""
Type-check the common dataquality provider's React UI.

Unlike edge3 / common.ai, the common dataquality plugin's ``package.json`` has no eslint or
prettier dependencies, so this hook only runs ``tsc --noEmit`` against the
changed TypeScript files. Keep it that way unless the UI grows enough to
justify the extra tooling.
"""

from __future__ import annotations

import sys
from pathlib import Path

from common_prek_utils import (
    AIRFLOW_PROVIDERS_ROOT_PATH,
    AIRFLOW_ROOT_PATH,
    run_command,
    temporary_tsc_project,
)

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

if __name__ == "__main__":
    original_files = sys.argv[1:]
    print("Original files:", original_files)
    dir = (
        AIRFLOW_PROVIDERS_ROOT_PATH
        / "common"
        / "dataquality"
        / "src"
        / "airflow"
        / "providers"
        / "common"
        / "dataquality"
        / "plugins"
        / "www"
    )
    relative_dir = Path(dir).relative_to(AIRFLOW_ROOT_PATH)
    files = [
        file[len(relative_dir.as_posix()) + 1 :]
        for file in original_files
        if Path(file).is_relative_to(relative_dir)
    ]
    all_ts_files = [file for file in files if file.endswith(".ts") or file.endswith(".tsx")]
    print("All TypeScript files:", all_ts_files)

    if not all_ts_files:
        sys.exit(0)

    run_command(["pnpm", "config", "set", "store-dir", ".pnpm-store"], cwd=dir)
    run_command(["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"], cwd=dir)
    with temporary_tsc_project(dir / "tsconfig.app.json", all_ts_files) as tsc_project:
        run_command(["pnpm", "tsc", "--p", tsc_project.name], cwd=dir)
