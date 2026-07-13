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

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts" / "ci" / "prek"))

from common_prek_utils import AIRFLOW_ROOT_PATH, console, run_command

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

# Path of the generated file, relative to the repo root (for git diff / messages).
GENERATED = "ts-sdk/src/generated/supervisor.ts"

if __name__ == "__main__":
    directory = AIRFLOW_ROOT_PATH / "ts-sdk"
    run_command(["pnpm", "config", "set", "store-dir", ".pnpm-store"], cwd=directory)
    run_command(["pnpm", "install", "--frozen-lockfile", "--config.confirmModulesPurge=false"], cwd=directory)
    # Regenerate, then format exactly as `pnpm run format` would, so the diff
    # reflects a stale schema and not raw-vs-prettier formatting noise.
    run_command(["pnpm", "run", "generate:supervisor"], cwd=directory)
    run_command(["pnpm", "exec", "prettier", "--write", "src/generated/supervisor.ts"], cwd=directory)

    diff = subprocess.run(
        ["git", "diff", "--", GENERATED],
        cwd=AIRFLOW_ROOT_PATH,
        capture_output=True,
        text=True,
        check=False,
    )
    if diff.stdout.strip():
        message = (
            f"{GENERATED} is out of date with the supervisor wire schema "
            "(task-sdk/src/airflow/sdk/execution_time/schema/schema.json).\n"
            "Regenerate it with `pnpm run generate:supervisor` in ts-sdk/ and commit the result."
        )
        if console:
            console.print(f"[red]{message}[/]")
            console.print(diff.stdout)
        else:
            print(message, file=sys.stderr)
            print(diff.stdout, file=sys.stderr)
        raise SystemExit(1)
