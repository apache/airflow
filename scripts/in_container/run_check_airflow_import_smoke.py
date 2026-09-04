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
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from importlib import import_module
from pkgutil import walk_packages

from in_container_utils import console

import airflow

# Alembic env modules require a live DB context at import time.
MIGRATION_ENV_MODULES = {
    "airflow.migrations.env",
    "airflow.providers.edge3.migrations.env",
    "airflow.providers.fab.migrations.env",
}

# Heavy optional deps (e.g. apache-beam) absent from default dev env; validated by providers-tests matrix.
OPTIONAL_HEAVY_DEP_MODULES = {
    "airflow.providers.google.cloud.hooks.dataflow",
    "airflow.providers.google.cloud.operators.dataflow",
    "airflow.providers.google.cloud.sensors.dataflow",
    "airflow.providers.google.cloud.triggers.dataflow",
    # Worker-side Beam pipeline template, not a regular Airflow module.
    "airflow.providers.google.cloud.utils.mlengine_prediction_summary",
}

EXCLUDED_MODULES = MIGRATION_ENV_MODULES | OPTIONAL_HEAVY_DEP_MODULES


@dataclass
class ImportCheckResult:
    checked_count: int = 0
    failures: list[tuple[str, str, str]] = field(default_factory=list)
    skipped_modules: list[str] = field(default_factory=list)


def check_imports() -> ImportCheckResult:
    result = ImportCheckResult()

    def _on_walk_error(module_name: str) -> None:
        # Keeps walk_packages from aborting on non-ImportError; main loop records the failure.
        ex = sys.exc_info()[1]
        if ex is not None:
            console.print(f"[yellow][walk warning][/] {module_name}: {type(ex).__name__}: {ex}")

    modules = [
        module.name
        for module in walk_packages(
            airflow.__path__,
            prefix=f"{airflow.__name__}.",
            onerror=_on_walk_error,
        )
    ]
    console.print(f"Discovered {len(modules)} modules")

    for module_name in modules:
        if module_name in EXCLUDED_MODULES:
            result.skipped_modules.append(module_name)
            continue
        result.checked_count += 1
        try:
            import_module(module_name)
        except (Exception, SystemExit) as ex:
            result.failures.append((module_name, type(ex).__name__, str(ex)))

    return result


def main() -> int:
    result = check_imports()

    console.print(
        f"\nSummary: checked={result.checked_count}, "
        f"failed={len(result.failures)}, "
        f"skipped={len(result.skipped_modules)}"
    )

    if result.skipped_modules:
        console.print("\n[yellow]Skipped:[/]")
        for name in sorted(result.skipped_modules):
            console.print(f"  - {name}")

    if result.failures:
        console.print("\n[red]Failures:[/]")
        for name, exc_type, msg in sorted(result.failures):
            console.print(f"  - {name} ({exc_type}): {msg}")
        return 1

    console.print("\n[green]All modules imported successfully[/]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
