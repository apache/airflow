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
"""Serialized Dag and BaseOperator."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

from airflow._shared.secrets_masker import redact
from airflow._shared.serialization.template_fields import normalize_template_field
from airflow._shared.template_rendering import truncate_rendered_value
from airflow.configuration import conf

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.models.deadline import DeadlineReferenceType
    from airflow.partition_mappers.base import PartitionMapper
    from airflow.partition_mappers.window import Window
    from airflow.timetables.base import Timetable as CoreTimetable


def serialize_template_field(template_field: object, name: str) -> JsonValue:
    """Normalize a template field and redact it before truncating oversized output."""
    max_length = conf.getint("core", "max_templated_field_length")

    serialized = normalize_template_field(template_field)

    if len(str(serialized)) > max_length:
        # Redact while still structured to preserve nested-key context (so values under
        # documented sensitive keys such as `password`, `token`, `secret`, `api_key`
        # are masked recursively); only stringify the redacted result for truncation.
        rendered = redact(serialized, name)
        return truncate_rendered_value(str(rendered), max_length)

    return serialized


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


def find_registered_custom_partition_mapper(importable_string: str) -> type[PartitionMapper]:
    """Find a user-defined custom partition mapper class registered via a plugin."""
    from airflow import plugins_manager

    partition_mapper_classes = plugins_manager.get_partition_mapper_plugins()
    with contextlib.suppress(KeyError):
        return partition_mapper_classes[importable_string]
    raise PartitionMapperNotFound(importable_string)


class DeadlineReferenceNotRegistered(ValueError):
    """When an unregistered custom deadline reference is being accessed."""

    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"Custom deadline reference class {self.type_string!r} is not "
            "registered. Custom deadline references must be registered via the "
            "`deadline_references` attribute on an AirflowPlugin."
        )


def find_registered_custom_deadline_reference(
    importable_string: str,
) -> type[DeadlineReferenceType]:
    """Find a user-defined custom deadline reference class registered via a plugin."""
    from airflow import plugins_manager

    deadline_ref_classes = plugins_manager.get_deadline_references_plugins()
    with contextlib.suppress(KeyError):
        return deadline_ref_classes[importable_string]
    raise DeadlineReferenceNotRegistered(importable_string)


def is_core_timetable_import_path(importable_string: str) -> bool:
    """Whether an importable string points to a core timetable class."""
    return importable_string.startswith("airflow.timetables.")


class PartitionMapperNotFound(ValueError):
    """Raise when a PartitionMapper cannot be found."""

    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"PartitionMapper class {self.type_string!r} could not be imported or "
            "you have a top level database access that disrupted the session. "
            "Please check the airflow best practices documentation."
        )


def is_core_partition_mapper_import_path(importable_string: str) -> bool:
    """Whether an importable string points to a core partition mapper class."""
    return importable_string.startswith("airflow.partition_mappers.")


class WindowNotSupported(ValueError):
    """Raise when serialization encounters an unregistered ``Window`` subclass."""

    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"Window class {self.type_string!r} is not registered. Custom Window "
            "subclasses must be registered via the ``windows`` attribute on an AirflowPlugin."
        )


def is_core_window_import_path(importable_string: str) -> bool:
    """Whether an importable string points to a core ``Window`` class."""
    return importable_string.startswith("airflow.partition_mappers.window.")


def find_registered_custom_window(importable_string: str) -> type[Window]:
    """Find a user-defined custom window class registered via a plugin."""
    from airflow import plugins_manager

    window_classes = plugins_manager.get_windows_plugins()
    with contextlib.suppress(KeyError):
        return window_classes[importable_string]
    raise WindowNotSupported(importable_string)


class WaitPolicyNotSupported(ValueError):
    """Raise when serialization encounters a non-built-in ``WaitPolicy`` subclass."""

    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"WaitPolicy class {self.type_string!r} is not a built-in. Custom WaitPolicy "
            "subclasses are not supported; use one of the built-in "
            "policies under ``airflow.partition_mappers.wait_policy``."
        )


def is_core_wait_policy_import_path(importable_string: str) -> bool:
    """Whether an importable string points to a core ``WaitPolicy`` class."""
    return importable_string.startswith("airflow.partition_mappers.wait_policy.")
