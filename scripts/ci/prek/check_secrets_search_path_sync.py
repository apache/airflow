#!/usr/bin/env python
#
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

import ast
import sys
from pathlib import Path

AIRFLOW_ROOT = Path(__file__).parents[3].resolve()
CORE_SECRETS_FILE = AIRFLOW_ROOT / "airflow-core" / "src" / "airflow" / "secrets" / "base_secrets.py"
SDK_SECRETS_FILE = (
    AIRFLOW_ROOT / "task-sdk" / "src" / "airflow" / "sdk" / "execution_time" / "secrets" / "__init__.py"
)


def extract_from_file(file_path: Path, constant_name: str) -> list[str] | None:
    """Extract a list constant value from a Python file using AST parsing."""
    try:
        with open(file_path) as f:
            tree = ast.parse(f.read(), filename=str(file_path))

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == constant_name:
                        if isinstance(node.value, ast.List):
                            values = []
                            for elt in node.value.elts:
                                if isinstance(elt, ast.Constant):
                                    values.append(elt.value)
                            return values
        return None
    except Exception as e:
        print(f"Error parsing {file_path}: {e}", file=sys.stderr)
        return None


def main() -> None:
    # Extract DEFAULT_SECRETS_SEARCH_PATH from airflow-core
    core_path = extract_from_file(CORE_SECRETS_FILE, "DEFAULT_SECRETS_SEARCH_PATH")
    if core_path is None:
        print(
            f"ERROR: Could not extract DEFAULT_SECRETS_SEARCH_PATH from {CORE_SECRETS_FILE}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Extract _SERVER_DEFAULT_SECRETS_SEARCH_PATH from task-sdk
    sdk_path = extract_from_file(SDK_SECRETS_FILE, "_SERVER_DEFAULT_SECRETS_SEARCH_PATH")
    if sdk_path is None:
        print(
            f"ERROR: Could not extract _SERVER_DEFAULT_SECRETS_SEARCH_PATH from {SDK_SECRETS_FILE}",
            file=sys.stderr,
        )
        sys.exit(1)

    if core_path == sdk_path:
        sys.exit(0)
    else:
        print("\nERROR: Secrets search paths are not synchronized!", file=sys.stderr)
        print(
            "\nThe DEFAULT_SECRETS_SEARCH_PATH in airflow-core and "
            "_SERVER_DEFAULT_SECRETS_SEARCH_PATH in task-sdk must match.",
            file=sys.stderr,
        )
        print("\nPlease update either:", file=sys.stderr)
        print(f"  - {CORE_SECRETS_FILE}", file=sys.stderr)
        print(f"  - {SDK_SECRETS_FILE}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
