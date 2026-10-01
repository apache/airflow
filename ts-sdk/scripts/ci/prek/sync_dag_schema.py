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
Refresh the TypeScript SDK's vendored copy of the Dag serialization schema.

The SDK vendors the schema so a published npm package can be built without the
monorepo, the same way the Java SDK vendors it into ``java-sdk/sdk/schema/``.
This keeps the copy honest; ``check-ts-sdk-dag-schema`` then keeps the
generated TypeScript honest with respect to the copy.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "scripts" / "ci" / "prek"))

from common_prek_utils import AIRFLOW_ROOT_PATH, console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

SOURCE = Path("airflow-core/src/airflow/serialization/schema.json")
VENDORED = Path("ts-sdk/schema/dag-schema.json")

if __name__ == "__main__":
    source_path = AIRFLOW_ROOT_PATH / SOURCE
    vendored_path = AIRFLOW_ROOT_PATH / VENDORED
    if not source_path.is_file():
        raise SystemExit(f"{SOURCE} is missing; cannot refresh {VENDORED}")

    if vendored_path.is_file() and vendored_path.read_bytes() == source_path.read_bytes():
        sys.exit(0)

    vendored_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, vendored_path)
    message = (
        f"Refreshed {VENDORED} from {SOURCE}.\n"
        "Review the diff, re-run `pnpm run generate:dag-schema` in ts-sdk/, and commit both."
    )
    if console:
        console.print(f"[yellow]{message}[/]")
    else:
        print(message, file=sys.stderr)
    raise SystemExit(1)
