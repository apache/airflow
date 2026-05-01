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
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.0.0",
# ]
# ///
"""Check that no new top-level modules are added under ``airflow-core/src/airflow/utils/``.

The set of allowed top-level modules (files and sub-packages) is frozen in
``known_airflow_core_utils_modules.txt`` next to this script. New code should
generally land in one of the following locations instead:

* ``shared/`` - for utilities reused across distributions (core, task-sdk,
  providers, ...).
* ``airflow-core/src/airflow/<module-dir>/`` - a dedicated sub-package
  alongside the feature it belongs to (e.g. ``models/``, ``serialization/``,
  ``api_fastapi/``), rather than a grab-bag entry in ``utils/``.
* ``task-sdk/`` - for code used by Dag authoring or the task execution
  runtime.

If a new module under ``airflow-core/src/airflow/utils/`` is genuinely the
right home, discuss it with the maintainers first, then regenerate the
allowlist with ``--generate`` and commit the updated text file together with
the new module.

Modes
-----
Default:
    Scan ``airflow-core/src/airflow/utils/`` and fail if any top-level module
    is missing from the allowlist. Stale entries (modules that were removed)
    are pruned automatically and the hook exits non-zero so prek reports the
    modified allowlist - just stage the updated file and re-run.

``--generate``:
    Rewrite the allowlist to match the current state of the directory.
    Use this only when intentionally accepting a new module after review.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

console = Console(width=400, color_system="standard")

REPO_ROOT = Path(__file__).parents[3]
UTILS_DIR = REPO_ROOT / "airflow-core" / "src" / "airflow" / "utils"
ALLOWLIST_FILE = Path(__file__).parent / "known_airflow_core_utils_modules.txt"


def _current_modules() -> set[str]:
    """Return top-level module names directly under ``airflow-core/src/airflow/utils/``.

    A module is either a ``.py`` file (other than ``__init__.py``) or a
    sub-package directory containing an ``__init__.py``.
    """
    modules: set[str] = set()
    for entry in UTILS_DIR.iterdir():
        if entry.name.startswith((".", "__")):
            continue
        if entry.is_file() and entry.suffix == ".py":
            modules.add(entry.stem)
        elif entry.is_dir() and (entry / "__init__.py").exists():
            modules.add(entry.name)
    return modules


def _load_allowlist() -> set[str]:
    if not ALLOWLIST_FILE.exists():
        return set()
    return {line.strip() for line in ALLOWLIST_FILE.read_text().splitlines() if line.strip()}


def _save_allowlist(modules: set[str]) -> None:
    ALLOWLIST_FILE.write_text("\n".join(sorted(modules)) + "\n")


def _generate() -> int:
    modules = _current_modules()
    _save_allowlist(modules)
    rel = ALLOWLIST_FILE.relative_to(REPO_ROOT)
    console.print(f"[green]Wrote {len(modules)} entries to[/green] [cyan]{rel}[/cyan]")
    return 0


def _check() -> int:
    current = _current_modules()
    allowed = _load_allowlist()

    new_modules = sorted(current - allowed)
    removed_modules = sorted(allowed - current)

    exit_code = 0

    if removed_modules:
        _save_allowlist(current & allowed)
        rel = ALLOWLIST_FILE.relative_to(REPO_ROOT)
        console.print(
            f"[yellow]Pruned {len(removed_modules)} stale entr"
            f"{'y' if len(removed_modules) == 1 else 'ies'} from[/yellow] "
            f"[cyan]{rel}[/cyan] (stage the updated file):"
        )
        for name in removed_modules:
            console.print(f"  [dim]-[/dim] {name}")
        exit_code = 1

    if new_modules:
        rel = ALLOWLIST_FILE.relative_to(REPO_ROOT)
        console.print(
            Panel.fit(
                "New top-level module(s) detected under [cyan]airflow-core/src/airflow/utils/[/cyan]:\n\n"
                + "\n".join(f"  + {name}" for name in new_modules)
                + "\n\nAdding new modules under this directory is discouraged. "
                "Prefer placing the code in one of:\n"
                "  - [cyan]shared/[/cyan] (when shared across distributions)\n"
                "  - [cyan]airflow-core/src/airflow/<module-dir>/[/cyan] "
                "(a dedicated sub-package alongside the feature it belongs to)\n"
                "  - [cyan]task-sdk/[/cyan] (when used by Dag authoring or task execution)\n\n"
                "If the addition is intentional and has been agreed with the maintainers, "
                "regenerate the allowlist with:\n\n"
                "  [cyan]uv run ./scripts/ci/prek/check_no_new_airflow_core_utils_modules.py --generate[/cyan]\n\n"
                f"and commit the updated [cyan]{rel}[/cyan].",
                title="[red]New airflow.utils module rejected[/red]",
                border_style="red",
            )
        )
        exit_code = 1

    if exit_code == 0:
        console.print("[green]airflow.utils module set matches the allowlist.[/green]")
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="Ignored - the hook always scans the full utils/ directory.",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Rewrite the allowlist from the current directory state.",
    )
    args = parser.parse_args(argv)

    if not UTILS_DIR.is_dir():
        console.print(f"[red]Expected directory does not exist: {UTILS_DIR}[/red]")
        return 1

    if args.generate:
        return _generate()
    return _check()


if __name__ == "__main__":
    sys.exit(main())
