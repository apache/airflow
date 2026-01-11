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
from typing import TYPE_CHECKING, Any

from airflow._shared.module_loading import qualname
from airflow._shared.secrets_masker import redact
from airflow.configuration import conf
from airflow.settings import json

if TYPE_CHECKING:
    from airflow.timetables.base import Timetable as CoreTimetable


def serialize_template_field(template_field: Any, name: str) -> str | dict | list | int | float:
    """
    Return a serializable representation of the templated field.

    If ``templated_field`` is provided via a callable then
    return the following serialized value: ``<callable full_qualified_name>``

    If ``templated_field`` contains a class or instance that requires recursive
    templating, store them as strings. Otherwise simply return the field as-is.
    """

    def is_jsonable(x):
        try:
            json.dumps(x)
        except (TypeError, OverflowError):
            return False
        else:
            return True

    def translate_tuples_to_lists(obj: Any):
        """Recursively convert tuples to lists."""
        if isinstance(obj, tuple):
            return [translate_tuples_to_lists(item) for item in obj]
        if isinstance(obj, list):
            return [translate_tuples_to_lists(item) for item in obj]
        if isinstance(obj, dict):
            return {key: translate_tuples_to_lists(value) for key, value in obj.items()}
        return obj

    def sort_dict_recursively(obj: Any) -> Any:
        """Recursively sort dictionaries to ensure consistent ordering."""
        if isinstance(obj, dict):
            return {k: sort_dict_recursively(v) for k, v in sorted(obj.items())}
        if isinstance(obj, list):
            return [sort_dict_recursively(item) for item in obj]
        if isinstance(obj, tuple):
            return tuple(sort_dict_recursively(item) for item in obj)
        return obj

    max_length = conf.getint("core", "max_templated_field_length")

    if not is_jsonable(template_field):
        try:
            serialized = template_field.serialize()
        except AttributeError:
            if callable(template_field):
                full_qualified_name = qualname(template_field, True)
                serialized = f"<callable {full_qualified_name}>"
            else:
                serialized = str(template_field)
        if len(serialized) > max_length:
            rendered = redact(serialized, name)
            return (
                "Truncated. You can change this behaviour in [core]max_templated_field_length. "
                f"{rendered[: max_length - 79]!r}... "
            )
        return serialized
    if not template_field and not isinstance(template_field, tuple):
        # Avoid unnecessary serialization steps for empty fields unless they are tuples
        # and need to be converted to lists
        return template_field
    template_field = translate_tuples_to_lists(template_field)
    # Sort dictionaries recursively to ensure consistent string representation
    # This prevents hash inconsistencies when dict ordering varies
    if isinstance(template_field, dict):
        template_field = sort_dict_recursively(template_field)
    serialized = str(template_field)
    if len(serialized) > max_length:
        rendered = redact(serialized, name)
        return (
            "Truncated. You can change this behaviour in [core]max_templated_field_length. "
            f"{rendered[: max_length - 79]!r}... "
        )
    return template_field


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
