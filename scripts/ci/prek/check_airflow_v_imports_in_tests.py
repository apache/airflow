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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Check that AIRFLOW_V_X_Y_PLUS constants are only imported from test_utils in provider tests.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import console


def check_airflow_v_imports_and_fix(test_file: Path) -> list[str]:
    errors = []
    with test_file.open("r", encoding="utf-8") as f:
        source = f.read()
        tree = ast.parse(source, filename=str(test_file))
    new_lines = source.splitlines(keepends=True)
    modified = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module and "AIRFLOW_V_" in " ".join([a.name for a in node.names]):
                console.print("Found AIRFLOW_V_*_PLUS import in test file:", test_file)
                if node.module != "tests_common.test_utils.version_compat":
                    errors.append(
                        f"{test_file}: AIRFLOW_V_*_PLUS should only be imported from tests.test_utils.version_compat, "
                        f"but found import from '{node.module}'"
                    )
                    # Replace the import line
                    lineno = node.lineno - 1
                    import_names = ", ".join([a.name for a in node.names])
                    new_lines[lineno] = f"from tests_common.test_utils.version_compat import {import_names}\n"
                    console.print("[yellow]Replacing import in", test_file, "with correct one.")
                    modified = True
    if modified:
        with test_file.open("w", encoding="utf-8") as f:
            f.writelines(new_lines)
        console.print(f"[yellow] {test_file} - replaced import with correct one.")
    return errors


def main():
    if len(sys.argv) > 1:
        test_files = [Path(f) for f in sys.argv[1:]]
    else:
        base = AIRFLOW_ROOT_PATH / "providers"
        test_files = list(base.glob("**/tests/**/*.py"))
        console.print(test_files)
    all_errors = []
    for test_file in test_files:
        all_errors.extend(check_airflow_v_imports_and_fix(test_file))
    if all_errors:
        for err in all_errors:
            console.print(f"[red]{err}")
        console.print("\n[red]Some AIRFLOW_V_*_PLUS imports were incorrect![/]")
        sys.exit(1)
    console.print("[green]All AIRFLOW_V_*_PLUS imports in tests are from tests.test_utils.version_compat.")


if __name__ == "__main__":
    main()
