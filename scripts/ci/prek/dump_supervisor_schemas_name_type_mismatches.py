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
"""
Worker: for every member of the supervisor discriminated unions, check
that the class ``__name__`` equals its ``type`` ``Literal`` value. Emits
a ``{"mismatches": [...], "checked": [...]}`` JSON object to stdout.

The dict shapes the entrypoint script consumes are defined as TypedDicts
in ``check_supervisor_schemas_name_type_sync.py``; this worker builds
plain dicts that must match those keys.

Run with cwd at the repo root.
"""

from __future__ import annotations

import inspect
import json
import os
import sys
from collections.abc import Mapping
from typing import Annotated, Any, get_args, get_origin

from pydantic import BaseModel

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"


def _members_of_discriminated_union(union_type: object) -> tuple[type, ...]:
    """Return the BaseModel classes in an ``Annotated[A | B | ..., Field(...)]``."""
    if get_origin(union_type) is Annotated:
        union_type = get_args(union_type)[0]
    return tuple(m for m in get_args(union_type) if isinstance(m, type))


def _type_literal_value(model: type[BaseModel]) -> Any:
    """Return the single ``Literal[...]`` value of ``model.type``, or ``None``."""
    field = model.model_fields.get("type")
    if field is None:
        return None
    args = get_args(field.annotation)
    # The discriminator must resolve to exactly one wire string; a missing
    # or multi-value Literal is rejected by returning None.
    if len(args) != 1:
        return None
    return args[0]


def _source_location(model: type) -> str:
    """Return ``relpath:lineno`` for *model*'s class definition, best-effort."""
    try:
        source_file = inspect.getsourcefile(model) or "<unknown>"
        _, lineno = inspect.getsourcelines(model)
    except (OSError, TypeError):
        return "<unknown>:0"
    try:
        rel = os.path.relpath(source_file, os.getcwd())
    except ValueError:
        rel = source_file
    return f"{rel}:{lineno}"


def classify_unions(unions: Mapping[str, object]) -> dict[str, list[dict[str, str]]]:
    """Walk *unions* and split members into checked / mismatched entries.

    The returned dict's shape matches the ``Payload`` TypedDict in the
    sibling entrypoint script.
    """
    mismatches: list[dict[str, str]] = []
    checked: list[dict[str, str]] = []
    seen: set[type] = set()
    for union_name, union in unions.items():
        for member in _members_of_discriminated_union(union):
            if member in seen:
                continue
            seen.add(member)
            if not issubclass(member, BaseModel):
                mismatches.append(
                    {
                        "class_name": member.__qualname__,
                        "literal": "<not-a-pydantic-model>",
                        "union": union_name,
                        "location": _source_location(member),
                        "reason": "union member is not a Pydantic model class",
                    }
                )
                continue
            literal = _type_literal_value(member)
            base = {
                "class_name": member.__name__,
                "literal": "" if literal is None else str(literal),
                "union": union_name,
                "location": _source_location(member),
            }
            if literal is None:
                mismatches.append({**base, "reason": "model has no `type: Literal[...]` field"})
                continue
            if literal != member.__name__:
                mismatches.append({**base, "reason": "class __name__ != type literal value"})
                continue
            checked.append(base)
    return {"mismatches": mismatches, "checked": checked}


def main() -> int:
    from airflow.dag_processing.processor import ToDagProcessor, ToManager
    from airflow.jobs.triggerer_job_runner import ToTriggerRunner, ToTriggerSupervisor
    from airflow.sdk.execution_time.comms import ToSupervisor, ToTask

    payload = classify_unions(
        {
            "ToTask": ToTask,
            "ToSupervisor": ToSupervisor,
            "ToManager": ToManager,
            "ToDagProcessor": ToDagProcessor,
            "ToTriggerRunner": ToTriggerRunner,
            "ToTriggerSupervisor": ToTriggerSupervisor,
        }
    )
    json.dump(payload, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
