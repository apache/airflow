#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.11,<3.12"
# ///
"""Check that API tests do not patch ``BaseAuthManager`` methods that ``SimpleAuthManager`` overrides.

The API test client runs with ``SimpleAuthManager``. Patching a ``BaseAuthManager`` method that
``SimpleAuthManager`` overrides leaves the override in place, so the mock never takes effect and a
permission test passes or fails for the wrong reason. Patch ``SimpleAuthManager`` instead.

The set of overridden methods is read from the two classes, so it follows the code.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

AIRFLOW_ROOT = Path(__file__).parents[3]
BASE_AUTH_MANAGER = AIRFLOW_ROOT / "airflow-core/src/airflow/api_fastapi/auth/managers/base_auth_manager.py"
SIMPLE_AUTH_MANAGER = (
    AIRFLOW_ROOT / "airflow-core/src/airflow/api_fastapi/auth/managers/simple/simple_auth_manager.py"
)

PATCH_TARGET = re.compile(r"BaseAuthManager\.(\w+)")


def get_methods(path: Path, class_name: str) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return {
                item.name
                for item in node.body
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and not item.name.startswith("_")
            }
    raise ValueError(f"class {class_name} not found in {path}")


def get_overridden_methods() -> set[str]:
    return get_methods(BASE_AUTH_MANAGER, "BaseAuthManager") & get_methods(
        SIMPLE_AUTH_MANAGER, "SimpleAuthManager"
    )


def find_bad_patch_targets(text: str, overridden: set[str]) -> list[tuple[int, str]]:
    return [
        (line_number, match.group(1))
        for line_number, line in enumerate(text.splitlines(), start=1)
        for match in PATCH_TARGET.finditer(line)
        if match.group(1) in overridden
    ]


def main(paths: list[str]) -> int:
    overridden = get_overridden_methods()
    errors = []
    for path in paths:
        for line_number, method in find_bad_patch_targets(Path(path).read_text(), overridden):
            errors.append(
                f"{path}:{line_number}: BaseAuthManager.{method} is overridden by SimpleAuthManager, "
                f"so patching it has no effect in API tests; patch SimpleAuthManager.{method} instead."
            )
    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
