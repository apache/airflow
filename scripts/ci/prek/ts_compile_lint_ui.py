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
import sys
from pathlib import Path

from common_prek_utils import (
    AIRFLOW_CORE_ROOT_PATH,
    AIRFLOW_CORE_SOURCES_PATH,
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
    dir = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "ui"
    relative_dir = Path(dir).relative_to(AIRFLOW_CORE_ROOT_PATH)
    files = [
        file[len(relative_dir.as_posix()) + 1 :]
        for file in original_files
        if Path(file).is_relative_to(relative_dir)
    ]
    all_non_yaml_files = [file for file in files if not file.endswith(".yaml")]
    print("All non-YAML files:", all_non_yaml_files)
    all_ts_files = [file for file in files if file.endswith(".ts") or file.endswith(".tsx")]
    if all_ts_files:
        all_ts_files.append("src/vite-env.d.ts")
    print("All TypeScript files:", all_ts_files)

    hash_file = AIRFLOW_ROOT_PATH / ".build" / "ui" / "pnpm-install-hash.txt"
    node_modules = dir / "node_modules"
    hasher = hashlib.sha256()
    for hashed_file_name in ("package.json", "pnpm-lock.yaml"):
        file_bytes = (dir / hashed_file_name).read_bytes()
        hasher.update(f"{hashed_file_name}:{len(file_bytes)}:".encode())
        hasher.update(file_bytes)
    current_hash = hasher.hexdigest()
    stored_hash = hash_file.read_text().strip() if hash_file.exists() else ""

    # A stored hash only proves node_modules matched the lockfile when it was written; guard
    # against node_modules having been modified afterwards (e.g. a manual `pnpm install`).
    cache_valid = (
        node_modules.is_dir()
        and current_hash == stored_hash
        and hash_file.stat().st_mtime >= node_modules.stat().st_mtime
    )
    if cache_valid:
        print("pnpm deps unchanged — skipping install.")
    else:
        run_command(["pnpm", "config", "set", "store-dir", ".pnpm-store"], cwd=dir)
        run_command(["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"], cwd=dir)
        hash_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_hash_file = hash_file.with_suffix(".tmp")
        tmp_hash_file.write_text(current_hash)
        tmp_hash_file.replace(hash_file)
    if any("/openapi/" in file for file in original_files):
        run_command(["pnpm", "codegen"], cwd=dir)
    if all_non_yaml_files:
        run_command(["pnpm", "eslint", "--fix", *all_non_yaml_files], cwd=dir)
        run_command(["pnpm", "prettier", "--write", *all_non_yaml_files], cwd=dir)
    if all_ts_files:
        with temporary_tsc_project(dir / "tsconfig.app.json", all_ts_files) as tsc_project:
            run_command(["pnpm", "tsc", "--p", tsc_project.name], cwd=dir)
