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
Verify the plugin ``applies_to`` destination tables stay in sync.

Which record an unqualified ``applies_to`` path is rooted at is decided twice: by
``_APPLIES_TO_ENTITY_ROOT`` when plugins load, to decide what to validate and warn about, and
again by ``ENTITY_ROOT_BY_DESTINATION`` in the browser, to decide what to match. The two have to
agree, and nothing at runtime notices when they do not -- a destination added to one side only
makes every path on it unevaluable, which shows the view everywhere instead of narrowing it.

The tables are not textually identical: Python keys records by their wire name (``dag_run``)
while the UI keys them by context field (``dagRun``), so this compares them through
``RECORD_FIELD_BY_WIRE_NAME`` below.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))

from common_prek_utils import AIRFLOW_ROOT_PATH

PYTHON_SOURCE = Path("airflow-core/src/airflow/plugins_manager.py")
PYTHON_TABLE = "_APPLIES_TO_ENTITY_ROOT"

UI_SOURCE = Path("airflow-core/src/airflow/ui/src/utils/pluginAppliesTo.ts")
UI_TABLE = "ENTITY_ROOT_BY_DESTINATION"

# The UI holds each record on a camelCase field of `AppliesToContext`; the wire names the Python
# side uses are the `applies_to` path prefixes a plugin author writes.
RECORD_FIELD_BY_WIRE_NAME = {
    "dag": "dag",
    "dag_run": "dagRun",
    "task": "task",
    "task_instance": "taskInstance",
}


def extract_python_table(source: str) -> dict[str, str]:
    """Read the destination -> record mapping out of the module without importing airflow."""
    for node in ast.parse(source).body:
        if isinstance(node, ast.AnnAssign):
            targets: list[ast.expr] = [node.target]
        elif isinstance(node, ast.Assign):
            targets = list(node.targets)
        else:
            continue
        if node.value is None:
            continue
        if any(isinstance(target, ast.Name) and target.id == PYTHON_TABLE for target in targets):
            return ast.literal_eval(node.value)
    raise SystemExit(f"Could not find `{PYTHON_TABLE}` in {PYTHON_SOURCE}")


def extract_ui_table(source: str) -> dict[str, str]:
    """Read the destination -> record mapping out of the TypeScript object literal."""
    match = re.search(
        rf"const {UI_TABLE}: Record<string, RootName> = \{{(.*?)\n\}};",
        source,
        re.DOTALL,
    )
    if match is None:
        raise SystemExit(f"Could not find `{UI_TABLE}` in {UI_SOURCE}")
    return dict(re.findall(r'^\s*([A-Za-z_]+):\s*"([A-Za-z]+)",', match.group(1), re.MULTILINE))


def main() -> int:
    python_table = extract_python_table((AIRFLOW_ROOT_PATH / PYTHON_SOURCE).read_text())
    ui_table = extract_ui_table((AIRFLOW_ROOT_PATH / UI_SOURCE).read_text())

    expected = {
        destination: RECORD_FIELD_BY_WIRE_NAME.get(record, record)
        for destination, record in python_table.items()
    }
    if expected == ui_table:
        return 0

    print(f"`{PYTHON_TABLE}` in {PYTHON_SOURCE} and `{UI_TABLE}` in {UI_SOURCE} disagree.\n")
    for destination in sorted(set(expected) | set(ui_table)):
        in_python, in_ui = expected.get(destination), ui_table.get(destination)
        if in_python == in_ui:
            continue
        print(
            f"  {destination}: {PYTHON_TABLE} says {in_python or '(missing)'}, "
            f"{UI_TABLE} says {in_ui or '(missing)'}"
        )
    print(
        "\nA destination only one side knows about makes every unqualified path on it "
        "unevaluable, which shows the view everywhere rather than scoping it. Update both "
        "tables, and the destination table in "
        "docs/administration-and-deployment/plugins.rst."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
