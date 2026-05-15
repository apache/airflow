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
import inspect
from typing import TYPE_CHECKING, Any

from airflow._shared.module_loading import qualname
from airflow._shared.secrets_masker import redact
from airflow._shared.template_rendering import truncate_rendered_value
from airflow.configuration import conf

if TYPE_CHECKING:
    from airflow.partition_mappers.base import PartitionMapper
    from airflow.timetables.base import Timetable as CoreTimetable


def serialize_template_field(template_field: Any, name: str) -> str | dict | list | int | float | bool | None:
    """
    Return a serializable representation of the templated field.

    The walk has two responsibilities:

    1. **Make the template_field JSON-encodable** — every container is rebuilt
       with primitive leaves (str/int/float/bool/None), tuples and sets are
       flattened to lists, and unsupported objects fall through to ``str()``
       so ``json.dumps`` never raises on the result.
    2. **Keep the output deterministic across parses** — callables are replaced
       with their qualified name (never the default ``<function ... at 0x...>``
       repr), dicts are key-sorted, and (frozen)sets are sorted by element so
       the same input always produces the same string.
    """

    def normalize_dict_key(key) -> str:
        """Normalize a dict key to a serialized string type."""
        # Serialized template_field keys must all be strings, not a mix of types, so that
        # downstream json.dumps(..., sort_keys=True) does not raise on mixed-type keys.
        return str(serialize_object(key))

    def serialize_object(obj):
        """Recursively rewrite ``obj`` into a JSON-encodable, hash-stable structure."""
        if obj is None or isinstance(obj, (str, int, float, bool)):
            return obj

        if isinstance(obj, dict):
            # Serialize keys/values first so each key is a string and the output is hash-stable,
            # then sort by the serialized key to prevent hash inconsistencies when dict ordering varies.
            serialized_pairs = [(normalize_dict_key(k), serialize_object(v)) for k, v in obj.items()]
            return dict(sorted(serialized_pairs, key=lambda kv: kv[0]))

        if isinstance(obj, (list, tuple)):
            return [serialize_object(item) for item in obj]

        if isinstance(obj, (set, frozenset)):
            # JSON has no set type → flatten to a list with deterministic ordering
            # so hash randomization on element types cannot shift cross-process iteration order.
            serialized_set = [serialize_object(e) for e in obj]
            return sorted(serialized_set, key=lambda x: (type(x).__name__, str(x)))

        # Use inspect.getattr_static to bypass any custom __getattr__ / metaclass magic
        if callable(inspect.getattr_static(obj, "serialize", None)):
            return serialize_object(obj.serialize())

        # Kubernetes client objects (V1Pod, V1Container, ...) expose their content via to_dict().
        # Scope the branch to the kubernetes namespace so unrelated user classes that happen to
        # define a to_dict() method fall through to str() instead of being treated as K8s payloads.
        if getattr(type(obj), "__module__", "").startswith(
            ("kubernetes.", "kubernetes_asyncio.")
        ) and callable(inspect.getattr_static(obj, "to_dict", None)):
            return serialize_object(obj.to_dict())

        if callable(obj):
            # Use qualified name; default repr embeds memory addresses, which would change the DAG hash on every parse
            return f"<callable {qualname(obj, True)}>"

        # A custom __str__ or __repr__ is treated as an intentional textual representation
        # supplied by the author and used as-is.
        if type(obj).__str__ is not object.__str__ or type(obj).__repr__ is not object.__repr__:
            return str(obj)

        # Otherwise fall back to a qualname marker. The default object repr is
        # `<ClassName object at 0x...>`, which embeds a memory address that flips per process
        # and would break DAG hash stability — use the class qualname instead.
        return f"<{qualname(type(obj), True)} object>"

    max_length = conf.getint("core", "max_templated_field_length")

    serialized = serialize_object(template_field)

    if len(str(serialized)) > max_length:
        rendered = redact(str(serialized), name)
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
