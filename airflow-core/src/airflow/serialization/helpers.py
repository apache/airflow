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
"""Serialized DAG and BaseOperator."""

from __future__ import annotations

import contextlib
import math
from typing import TYPE_CHECKING, Any

from airflow._shared.module_loading import qualname
from airflow._shared.secrets_masker import redact
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow.timetables.base import Timetable as CoreTimetable


def _is_jsonable(obj: Any) -> bool:
    """Check if object is JSON serializable without creating the full JSON string."""
    if obj is None or isinstance(obj, (bool, str)):
        return True
    if isinstance(obj, int):
        return True
    if isinstance(obj, float):
        # NaN and Infinity are not valid JSON
        return not (math.isnan(obj) or math.isinf(obj))
    if isinstance(obj, (list, tuple)):
        return all(_is_jsonable(item) for item in obj)
    if isinstance(obj, dict):
        return all(isinstance(k, str) and _is_jsonable(v) for k, v in obj.items())
    return False


def _normalize_for_serialization(obj: Any) -> Any:
    """Convert tuples to lists AND sort dicts."""
    if isinstance(obj, dict):
        return {k: _normalize_for_serialization(v) for k, v in sorted(obj.items())}
    if isinstance(obj, (list, tuple)):
        return [_normalize_for_serialization(item) for item in obj]
    return obj


def _truncate_field(serialized: str, name: str, max_length: int) -> str:
    """Truncate a serialized field and add truncation message."""
    rendered = redact(serialized, name)
    return (
        "Truncated. You can change this behaviour in [core]max_templated_field_length. "
        f"{rendered[: max_length - 79]!r}... "
    )


def serialize_template_field(template_field: Any, name: str) -> str | dict | list | int | float | None:
    """
    Return a serializable representation of the templated field.

    If ``templated_field`` is provided via a callable then
    return the following serialized value: ``<callable full_qualified_name>``

    If ``templated_field`` contains a class or instance that requires recursive
    templating, store them as strings. Otherwise simply return the field as-is.
    """
    max_length = conf.getint("core", "max_templated_field_length")

    if template_field is None:
        return template_field
    if isinstance(template_field, bool):
        return template_field
    if isinstance(template_field, (int, float)):
        return template_field
    if isinstance(template_field, str):
        if len(template_field) > max_length:
            return _truncate_field(template_field, name, max_length)
        return template_field

    if callable(template_field):
        full_qualified_name = qualname(template_field, True)
        serialized = f"<callable {full_qualified_name}>"
        if len(serialized) > max_length:
            return _truncate_field(serialized, name, max_length)
        return serialized

    # Handle objects with serialize() method
    if hasattr(template_field, "serialize"):
        serialized = template_field.serialize()
        if _is_jsonable(serialized):
            if not serialized and not isinstance(serialized, tuple):
                return serialized
            normalized = _normalize_for_serialization(serialized)
            serialized_str = str(normalized)
            if len(serialized_str) > max_length:
                return _truncate_field(serialized_str, name, max_length)
            return normalized
        serialized_str = str(serialized)
        if len(serialized_str) > max_length:
            return _truncate_field(serialized_str, name, max_length)
        return serialized_str

    # Check if the field is JSON serializable
    if not _is_jsonable(template_field):
        # Not JSON serializable, convert to string
        serialized = str(template_field)
        if len(serialized) > max_length:
            return _truncate_field(serialized, name, max_length)
        return serialized

    # Handle empty fields (but not empty tuples which need conversion)
    if not template_field and not isinstance(template_field, tuple):
        return template_field

    # For dicts, lists, tuples - normalize in a single pass
    # (converts tuples to lists AND sorts dicts)
    normalized = _normalize_for_serialization(template_field)

    # Check length on stringified version
    serialized = str(normalized)
    if len(serialized) > max_length:
        return _truncate_field(serialized, name, max_length)

    return normalized


class TimetableNotRegistered(ValueError):
    """When an unregistered timetable is being accessed."""

    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"Timetable class {self.type_string!r} is not registered or "
            "you have a top level database access that disrupted the session. "
            "Please check the airflow best practices documentation."
        )


def find_registered_custom_timetable(importable_string: str) -> type[CoreTimetable]:
    """Find a user-defined custom timetable class registered via a plugin."""
    from airflow import plugins_manager

    timetable_classes = plugins_manager.get_timetables_plugins()
    with contextlib.suppress(KeyError):
        return timetable_classes[importable_string]
    raise TimetableNotRegistered(importable_string)


def is_core_timetable_import_path(importable_string: str) -> bool:
    """Whether an importable string points to a core timetable class."""
    return importable_string.startswith("airflow.timetables.")
