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

"""Observed-state diffs for stored Dag versions."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.models.dag_version import DagVersion
from airflow.serialization.serialized_objects import DagSerialization
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


DIFF_SCHEMA_VERSION = 1
DEFAULT_MAX_CHANGES = 500
MAX_ALLOWED_CHANGES = 5000
SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS = frozenset((1, 2, 3))

DiffMode = Literal["observed_state", "unavailable"]
SourceStatus = Literal["current_stored_code", "redacted", "unavailable"]
ValuesStatus = Literal["available", "unavailable"]

_ORDER_INSENSITIVE_LIST_PATHS = {
    ("dag", "tags"),
    ("dag", "allowed_run_types"),
}


class DagVersionNotFoundError(ValueError):
    """Raised when one of the requested Dag versions does not exist."""


def build_serialized_dag_diff(
    *,
    base_data: dict[str, Any] | None,
    target_data: dict[str, Any] | None,
    base_provenance: Mapping[str, Any] | None = None,
    target_provenance: Mapping[str, Any] | None = None,
    include_values: bool = False,
    max_changes: int = DEFAULT_MAX_CHANGES,
) -> dict[str, Any]:
    """Build a bounded, deterministic diff from two stored serialized Dag payloads."""
    _validate_max_changes(max_changes)

    base_schema_version = _get_schema_version(base_data)
    target_schema_version = _get_schema_version(target_data)
    result: dict[str, Any] = {
        "diff_schema_version": DIFF_SCHEMA_VERSION,
        "serialized_dag_schema_versions": {
            "base": base_schema_version,
            "target": target_schema_version,
        },
        "mode": "observed_state",
        "changes": [],
        "truncated": False,
    }

    if base_data is None or target_data is None:
        return _unavailable(result, "serialized_dag_missing")

    if base_schema_version is None or target_schema_version is None:
        return _unavailable(result, "serialized_dag_schema_version_missing")

    unsupported_versions = [
        version
        for version in (base_schema_version, target_schema_version)
        if version not in SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS
    ]
    if unsupported_versions:
        return _unavailable(result, f"unsupported_serialized_dag_schema_version:{unsupported_versions[0]}")

    try:
        base_document = _canonicalize_payload(base_data)
        target_document = _canonicalize_payload(target_data)
    except (AttributeError, KeyError, OverflowError, TypeError, ValueError):
        return _unavailable(result, "serialized_dag_canonicalization_failed")

    base_document["provenance"] = _canonicalize_value(dict(base_provenance or {}), path=("provenance",))
    target_document["provenance"] = _canonicalize_value(dict(target_provenance or {}), path=("provenance",))

    collector = _ChangeCollector(max_changes=max_changes, include_values=include_values)
    _collect_changes(base_document, target_document, path=(), collector=collector)
    result["changes"] = collector.changes
    result["truncated"] = collector.count > max_changes
    return result


@provide_session
def get_dag_version_diff(
    dag_id: str,
    base_version_number: int,
    target_version_number: int,
    *,
    include_values: bool = False,
    include_source: bool = False,
    max_changes: int = DEFAULT_MAX_CHANGES,
    source_status: SourceStatus | None = None,
    values_status: ValuesStatus | None = None,
    session: Session = NEW_SESSION,
) -> dict[str, Any]:
    """
    Compare two versions of a Dag using their currently stored state.

    ``source_status`` is supplied by callers that have an authorization context.  The CLI has
    operator-level authority and leaves it unset, while API callers calculate it before this
    function returns any source content.
    """
    if base_version_number < 1 or target_version_number < 1:
        raise ValueError("Dag version numbers must be positive integers")
    _validate_max_changes(max_changes)

    query = (
        select(DagVersion)
        .where(
            DagVersion.dag_id == dag_id,
            DagVersion.version_number.in_((base_version_number, target_version_number)),
        )
        .options(joinedload(DagVersion.serialized_dag))
    )
    if include_source and source_status not in {"redacted", "unavailable"}:
        query = query.options(joinedload(DagVersion.dag_code))

    versions = {version.version_number: version for version in session.scalars(query).all()}
    missing_version = next(
        (
            version_number
            for version_number in (base_version_number, target_version_number)
            if version_number not in versions
        ),
        None,
    )
    if missing_version is not None:
        raise DagVersionNotFoundError(
            f"The DagVersion with dag_id: `{dag_id}` and version_number: `{missing_version}` was not found"
        )

    base_version = versions[base_version_number]
    target_version = versions[target_version_number]
    effective_include_values = include_values and values_status != "unavailable"
    result = build_serialized_dag_diff(
        base_data=base_version.serialized_dag.data if base_version.serialized_dag else None,
        target_data=target_version.serialized_dag.data if target_version.serialized_dag else None,
        base_provenance=_get_provenance(base_version),
        target_provenance=_get_provenance(target_version),
        include_values=effective_include_values,
        max_changes=max_changes,
    )
    if include_values:
        values_available = effective_include_values and result["mode"] == "observed_state"
        result["values"] = {"status": "available" if values_available else "unavailable"}
    result["source"] = _get_source_diff(
        base_version,
        target_version,
        include_source=include_source,
        source_status=source_status or ("current_stored_code" if include_source else "unavailable"),
    )
    return result


class _ChangeCollector:
    def __init__(self, *, max_changes: int, include_values: bool) -> None:
        self.changes: list[dict[str, Any]] = []
        self.count = 0
        self.max_changes = max_changes
        self.include_values = include_values

    def add(
        self,
        *,
        path: tuple[str, ...],
        operation: Literal["added", "removed", "changed"],
        before: Any,
        after: Any,
    ) -> None:
        self.count += 1
        if len(self.changes) >= self.max_changes:
            return

        category = _get_category(path)
        change = {
            "path": _format_path(path),
            "operation": operation,
            "category": category,
            "impact": _get_impact(category),
            "before_digest": None if before is _MISSING else _get_digest(before),
            "after_digest": None if after is _MISSING else _get_digest(after),
        }
        if self.include_values:
            if before is not _MISSING:
                change["before_value"] = before
            if after is not _MISSING:
                change["after_value"] = after
        self.changes.append(change)


_MISSING = object()


def _validate_max_changes(max_changes: int) -> None:
    if max_changes < 1:
        raise ValueError("max_changes must be a positive integer")
    if max_changes > MAX_ALLOWED_CHANGES:
        raise ValueError(f"max_changes must not exceed {MAX_ALLOWED_CHANGES}")


def _get_schema_version(data: Mapping[str, Any] | None) -> int | None:
    if not isinstance(data, Mapping):
        return None
    version = data.get("__version")
    return version if isinstance(version, int) and not isinstance(version, bool) else None


def _unavailable(result: dict[str, Any], reason: str) -> dict[str, Any]:
    result["mode"] = "unavailable"
    result["unavailable_reason"] = reason
    return result


def _canonicalize_payload(data: dict[str, Any]) -> dict[str, Any]:
    payload = copy.deepcopy(data)
    version = _get_schema_version(payload)
    if version is None:
        raise ValueError("missing or invalid __version")
    if version == 1:
        DagSerialization.conversion_v1_to_v2(payload)
        DagSerialization.conversion_v2_to_v3(payload)
    elif version == 2:
        DagSerialization.conversion_v2_to_v3(payload)
    if not isinstance(payload.get("dag"), Mapping):
        raise ValueError("missing dag object")
    _apply_client_defaults(payload)
    payload.pop("__version", None)
    return _canonicalize_value(payload, path=())


def _apply_client_defaults(payload: dict[str, Any]) -> None:
    client_defaults = payload.pop("client_defaults", None)
    if client_defaults is None:
        return
    if not isinstance(client_defaults, Mapping):
        raise ValueError("client_defaults is not an object")

    task_defaults = client_defaults.get("tasks", {})
    if not isinstance(task_defaults, Mapping):
        raise ValueError("client_defaults.tasks is not an object")

    tasks = payload["dag"].get("tasks", [])
    if not isinstance(tasks, list):
        raise ValueError("dag.tasks is not a list")
    for task in tasks:
        if not isinstance(task, dict) or not isinstance(task.get("__var"), Mapping):
            raise ValueError("task entry is not an object")
        task_data = dict(task_defaults)
        task_data.update(task["__var"])
        if isinstance(partial_kwargs := task_data.get("partial_kwargs"), Mapping):
            effective_partial_kwargs = dict(task_defaults)
            effective_partial_kwargs.update(partial_kwargs)
            task_data["partial_kwargs"] = effective_partial_kwargs
        task["__var"] = task_data


def _canonicalize_value(value: Any, *, path: tuple[str, ...]) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize_value(item, path=path + (str(key),))
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        canonical_values = [_canonicalize_value(item, path=path) for item in value]
        if path == ("dag", "tasks"):
            return _canonicalize_keyed_list(canonical_values, _get_task_id, path)
        if path == ("dag", "dag_dependencies"):
            return _canonicalize_keyed_list(canonical_values, _get_dependency_key, path)
        if path in _ORDER_INSENSITIVE_LIST_PATHS:
            return _canonicalize_keyed_list(canonical_values, _get_string_key, path)
        return canonical_values
    return value


def _canonicalize_keyed_list(
    values: list[Any], key_getter: Callable[[Any], str], path: tuple[str, ...]
) -> dict[str, Any]:
    keyed_values: dict[str, Any] = {}
    for value in values:
        key = key_getter(value)
        if key in keyed_values:
            if path == ("dag", "dag_dependencies") and keyed_values[key] == value:
                continue
            raise ValueError(f"duplicate key {key!r} in /{'/'.join(path)}")
        canonical_value = value
        if path == ("dag", "tasks") and isinstance(value, Mapping) and "__var" in value:
            task_value = dict(value["__var"])
            if "__type" in value:
                task_value["__type"] = value["__type"]
            canonical_value = task_value
        keyed_values[key] = canonical_value
    return {key: keyed_values[key] for key in sorted(keyed_values)}


def _get_task_id(task: Any) -> str:
    if not isinstance(task, Mapping):
        raise ValueError("task entry is not an object")
    task_data = task.get("__var", task)
    task_id = task_data.get("task_id") if isinstance(task_data, Mapping) else None
    if not isinstance(task_id, str):
        raise ValueError("task entry has no task_id")
    return task_id


def _get_string_key(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("collection entry is not a string")
    return value


def _get_dependency_key(dependency: Any) -> str:
    if not isinstance(dependency, Mapping):
        raise ValueError("dependency entry is not an object")
    components = (
        dependency.get("dependency_type"),
        dependency.get("dependency_id"),
        dependency.get("source"),
        dependency.get("target"),
        dependency.get("label"),
    )
    return json.dumps(components, ensure_ascii=False, separators=(",", ":"))


def _collect_changes(
    before: Any,
    after: Any,
    *,
    path: tuple[str, ...],
    collector: _ChangeCollector,
) -> None:
    if isinstance(before, Mapping) and isinstance(after, Mapping):
        keys = sorted({str(key) for key in before} | {str(key) for key in after})
        for key in keys:
            before_value = before.get(key, _MISSING)
            after_value = after.get(key, _MISSING)
            _collect_changes(before_value, after_value, path=path + (key,), collector=collector)
        return

    if isinstance(before, list) and isinstance(after, list):
        if before != after:
            collector.add(path=path, operation="changed", before=before, after=after)
        return

    if before is _MISSING:
        collector.add(path=path, operation="added", before=_MISSING, after=after)
    elif after is _MISSING:
        collector.add(path=path, operation="removed", before=before, after=_MISSING)
    elif before != after:
        collector.add(path=path, operation="changed", before=before, after=after)


def _format_path(path: tuple[str, ...]) -> str:
    return "/" + "/".join(component.replace("~", "~0").replace("/", "~1") for component in path)


def _get_digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _get_category(path: tuple[str, ...]) -> str:
    lowered_path = tuple(component.lower() for component in path)
    if lowered_path and lowered_path[0] == "provenance":
        return "provenance"
    if "deadline" in lowered_path:
        return "deadline"
    if any("callback" in component for component in lowered_path):
        return "callback"
    if any(component in {"asset", "assets", "inlets", "outlets"} for component in lowered_path):
        return "asset"
    if any(component in {"param", "params", "default_args"} for component in lowered_path):
        return "param"
    if any(
        component
        in {"dag_dependencies", "dependencies", "edge_info", "upstream_task_ids", "downstream_task_ids"}
        for component in lowered_path
    ):
        return "dependency"
    if any(
        component
        in {
            "schedule",
            "schedule_interval",
            "timetable",
            "catchup",
            "max_active_runs",
            "max_active_tasks",
            "dagrun_timeout",
            "fail_fast",
            "fail_stop",
            "allowed_run_types",
        }
        for component in lowered_path
    ):
        return "schedule"
    if len(lowered_path) >= 2 and lowered_path[:2] == ("dag", "tasks"):
        return "task"
    if any(
        component in {"tags", "description", "doc_md", "owner", "owners", "email", "fileloc"}
        for component in lowered_path
    ):
        return "metadata"
    return "unknown"


def _get_impact(category: str) -> str:
    if category == "provenance":
        return "provenance"
    if category in {"task", "dependency", "schedule", "param", "asset", "deadline"}:
        return "execution"
    if category == "metadata":
        return "metadata"
    return "unknown"


def _get_provenance(version: DagVersion) -> dict[str, Any]:
    return {
        "bundle_name": version.bundle_name,
        "bundle_version": version.bundle_version,
        "version_data": version.version_data,
    }


def _get_source_diff(
    base_version: DagVersion,
    target_version: DagVersion,
    *,
    include_source: bool,
    source_status: SourceStatus,
) -> dict[str, Any]:
    if not include_source:
        return {"status": "unavailable", "fidelity": "unavailable"}
    if source_status != "current_stored_code":
        return {"status": source_status, "fidelity": source_status}

    base_code = base_version.dag_code
    target_code = target_version.dag_code
    if base_code is None or target_code is None:
        return {"status": "unavailable", "fidelity": "unavailable"}

    base_source = base_code.source_code
    target_source = target_code.source_code
    return {
        "status": "current_stored_code",
        "fidelity": "current_stored_code",
        "changed": base_source != target_source,
        "base": {"digest": _get_source_digest(base_source), "content": base_source},
        "target": {"digest": _get_source_digest(target_source), "content": target_source},
    }


def _get_source_digest(source: str) -> str:
    return f"sha256:{hashlib.sha256(source.encode()).hexdigest()}"
