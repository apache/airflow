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
"""Check that every provider target of a core ``add_deprecated_classes`` shim still exists.

Core re-exports moved classes, such as ``airflow.sensors.time_delta.TimeDeltaSensorAsync``, from
providers. Released cores keep those shims, so a provider that removes a target breaks user
imports on every supported core that ships the shim, even if main drops the shim entry too.

The targets are recorded in ``core_shim_provider_targets.txt``. Every provider target of a shim on
main must be recorded there, and every recorded target must exist. Removing a target together
with its shim entry therefore also needs a deliberate edit to that file.
"""

from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

AIRFLOW_ROOT = Path(__file__).parents[3]
CORE_SRC = AIRFLOW_ROOT / "airflow-core" / "src"
PROVIDERS_ROOT = AIRFLOW_ROOT / "providers"
SNAPSHOT = Path(__file__).with_name("core_shim_provider_targets.txt")
PROVIDER_PREFIX = "airflow.providers."
WILDCARD_SUFFIX = ".*"


def find_shim_files(core_src: Path = CORE_SRC) -> list[Path]:
    return sorted(
        path
        for path in core_src.rglob("*.py")
        if path.name != "deprecation_tools.py" and "add_deprecated_classes(" in path.read_text()
    )


def find_provider_targets(path: Path) -> list[tuple[int, str, str]]:
    """Return ``(line, key, target)`` for each shim mapping that points into a provider."""
    targets = []
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if (
                isinstance(key, ast.Constant)
                and isinstance(key.value, str)
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
                and value.value.startswith(PROVIDER_PREFIX)
            ):
                targets.append((value.lineno, key.value, value.value))
    return targets


def find_module_file(module: str, providers_root: Path = PROVIDERS_ROOT) -> Path | None:
    parts = module.split(".")
    # Provider folders nest like the package (providers/apache/hive), so try each prefix.
    for end in range(3, len(parts) + 1):
        src = providers_root.joinpath(*parts[2:end], "src")
        if not src.is_dir():
            continue
        package = src.joinpath(*parts)
        for candidate in (package.with_suffix(".py"), package / "__init__.py"):
            if candidate.is_file():
                return candidate
    return None


def is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def get_module_names(path: Path) -> set[str]:
    """Return names bound at runtime at module level; ``TYPE_CHECKING``-only names are left out."""
    names: set[str] = set()

    def collect(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                names.add(node.name)
            elif isinstance(node, ast.Assign):
                names.update(t.id for t in node.targets if isinstance(t, ast.Name))
            elif isinstance(node, ast.AnnAssign) and node.value and isinstance(node.target, ast.Name):
                names.add(node.target.id)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                names.update(alias.asname or alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.If):
                # Version-gated branches cannot be resolved statically, so either branch counts.
                if not is_type_checking(node.test):
                    collect(node.body)
                collect(node.orelse)
            elif isinstance(node, ast.Try):
                collect(node.body)
                for handler in node.handlers:
                    collect(handler.body)
                collect(node.orelse)
                collect(node.finalbody)

    collect(ast.parse(path.read_text(), filename=str(path)).body)
    return names


def is_target_present(key: str, target: str, providers_root: Path = PROVIDERS_ROOT) -> bool:
    if key == "*" or find_module_file(target, providers_root):
        return find_module_file(target, providers_root) is not None
    module, name = target.rsplit(".", 1)
    module_file = find_module_file(module, providers_root)
    return module_file is not None and name in get_module_names(module_file)


def build_snapshot_entry(key: str, target: str) -> str:
    return f"{target}{WILDCARD_SUFFIX}" if key == "*" else target


def read_snapshot(path: Path) -> dict[str, int]:
    """Return each recorded target with its line number."""
    return {
        line.strip(): number
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if line.strip() and not line.startswith("#")
    }


def is_entry_present(entry: str, providers_root: Path = PROVIDERS_ROOT) -> bool:
    if entry.endswith(WILDCARD_SUFFIX):
        return is_target_present("*", entry.removesuffix(WILDCARD_SUFFIX), providers_root)
    return is_target_present("", entry, providers_root)


def main(core_src: Path = CORE_SRC, providers_root: Path = PROVIDERS_ROOT, snapshot: Path = SNAPSHOT) -> int:
    recorded = read_snapshot(snapshot)
    errors = []
    for path in find_shim_files(core_src):
        for line, key, target in find_provider_targets(path):
            entry = build_snapshot_entry(key, target)
            if entry not in recorded:
                errors.append(
                    f"{os.path.relpath(path)}:{line}: `{entry}` is missing from {os.path.relpath(snapshot)}. "
                    "Add it, so the target stays checked after main drops this shim."
                )
                if not is_entry_present(entry, providers_root):
                    errors.append(f"{os.path.relpath(path)}:{line}: `{entry}` does not exist.")
    for entry, line in recorded.items():
        if not is_entry_present(entry, providers_root):
            errors.append(
                f"{os.path.relpath(snapshot)}:{line}: `{entry}` does not exist. Released Airflow cores "
                "ship a shim for it, so removing it breaks user imports on them, even if main drops the "
                "shim entry. Keep it until the provider requires an Airflow version without the shim."
            )
    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
