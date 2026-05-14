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
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Fail if any supervisor-schema body's class ``__name__`` drifts from its
``type`` ``Literal`` discriminator value. Walks six unions: ``ToTask``,
``ToSupervisor`` (``comms.py``), ``ToManager``, ``ToDagProcessor``
(``processor.py``) and ``ToTriggerRunner``, ``ToTriggerSupervisor``
(``triggerer_job_runner.py``).

The actual introspection runs under the airflow-core venv via the
sibling ``dump_supervisor_schemas_name_type_mismatches.py`` worker.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import TypedDict, cast

from common_prek_utils import console


class CheckedEntry(TypedDict):
    """A union member whose class ``__name__`` matches its ``type`` literal."""

    class_name: str
    literal: str
    union: str
    location: str


class MismatchEntry(CheckedEntry):
    """A union member that failed the name/type check, with a reason."""

    reason: str


class Payload(TypedDict):
    """The JSON payload emitted by the worker. The shape is a contract --
    the worker builds plain dicts that must match these keys."""

    mismatches: list[MismatchEntry]
    checked: list[CheckedEntry]


WORKER_SCRIPT = Path(__file__).parent / "dump_supervisor_schemas_name_type_mismatches.py"


def dump_mismatches(cwd: Path) -> Payload:
    """Run the worker in *cwd* and parse its JSON stdout."""
    result = subprocess.run(
        [
            "uv",
            "run",
            "-p",
            "3.12",
            "--no-progress",
            # --frozen keeps uv.lock untouched; without it, uv may re-resolve
            # against the relative ``exclude-newer-span`` in the workspace
            # pyproject and rewrite uv.lock as a side effect of running the
            # worker, which prek then flags as "files were modified".
            "--frozen",
            "--project",
            "airflow-core",
            "-s",
            str(WORKER_SCRIPT),
        ],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Mismatch dump failed.\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")
    return cast("Payload", json.loads(result.stdout))


def main() -> int:
    repo_root = Path(__file__).parents[3].resolve()
    try:
        payload = dump_mismatches(repo_root)
    except Exception as e:
        console.print(f"[bold red]ERROR:[/] {e}")
        return 1

    if not payload["mismatches"]:
        console.print(
            f"[green]OK:[/] all {len(payload['checked'])} supervisor-schema bodies have "
            "matching class __name__ and `type` literal."
        )
        return 0

    console.print(
        "[bold red]ERROR:[/] supervisor-schema bodies whose class __name__ "
        "does not match their `type` Literal discriminator:"
    )
    console.print("")
    for m in payload["mismatches"]:
        console.print(
            f"  [magenta]{m['location']}[/] in union "
            f"[cyan]{m['union']}[/]: class [yellow]{m['class_name']}[/] "
            f"vs. literal [yellow]{m['literal']!r}[/] -- {m['reason']}"
        )
    console.print("")
    console.print(
        "The class ``__name__`` and the ``type`` ``Literal`` value must be "
        "identical: the discriminated-union decoder routes on the literal, "
        "and consumers identify the model by its class name."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
