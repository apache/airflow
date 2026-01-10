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
Check that there are no imports of ORM classes in any of the alembic migration scripts.
This is to prevent the addition of migration code directly referencing any ORM definition,
which could potentially break downgrades. For more details, refer to the relevant discussion
thread at this link: https://github.com/apache/airflow/issues/59871
"""

from __future__ import annotations

import importlib
import inspect
import os
import sys
from pathlib import Path
from pprint import pformat
from typing import Final

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH, console, get_imports_from_file

sys.path.insert(0, AIRFLOW_CORE_SOURCES_PATH.resolve().as_posix())  # make sure airflow-core is importable
from airflow.models.base import Base

_MIGRATIONS_DIRPATH: Final[Path] = Path(
    os.path.join(AIRFLOW_CORE_SOURCES_PATH, "airflow/migrations/versions")
)


def main() -> None:
    if len(sys.argv) > 1:
        migration_scripts = [Path(f) for f in sys.argv[1:]]
    else:
        migration_scripts = list(_MIGRATIONS_DIRPATH.glob("**/*.py"))
    console.print("Checking migration scripts ...")
    violations = []
    bad_script_paths = []
    for script_path in migration_scripts:
        if violating_imports := _find_models_import_violations(script_path=script_path):
            violations.extend(violating_imports)
            bad_script_paths.append(str(script_path))
    if violations:
        for err in violations:
            console.print(f"[red]{err}")
        console.print("\n[red]ORM references detected in one or more migration scripts[/]")
        console.print(f"[red]Violating script paths: {pformat([p for p in bad_script_paths])}[/]")
        sys.exit(1)
    console.print("[green]No ORM references detected in migration scripts.")


def _find_models_import_violations(script_path: Path) -> list[str]:
    """
    Return a list of invalid imports of ORM definitions for the given migration script, if any.
    For simplicity and forward compatibility when individual tables are added / removed / renamed,
    this function uses the heuristic of checking for any non-allowlisted imports from within the
    `airflow.models` module.
    """
    bad_imports = []
    for import_ref in get_imports_from_file(file_path=script_path, only_top_level=False):
        if _is_violating_orm_import(import_ref=import_ref):
            bad_imports.append(f"Found bad import in migration script {str(script_path)}: '{import_ref}'")
    return bad_imports


def _is_violating_orm_import(import_ref: str) -> bool:
    """Return `True` if the imported object is an ORM class from within `airflow.models`, otherwise return `False`."""
    if not import_ref.startswith("airflow.models"):
        return False
    # import the fully qualified reference to check if the reference is a subclass of a declarative base
    mod_to_import, _, attr_name = import_ref.rpartition(".")
    referenced_module = importlib.import_module(mod_to_import)
    referenced_obj = getattr(referenced_module, attr_name)
    if inspect.isclass(referenced_obj) and referenced_obj in Base.__subclasses__():
        return True
    return False


if __name__ == "__main__":
    main()
