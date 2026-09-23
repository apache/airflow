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
Observed-state diffs for serialized Dag payloads.

Diff schema v1 -- the wire format every entry point here returns.
:func:`build_serialized_dag_diff` and :func:`build_unavailable_dag_diff` both return one
dictionary in the shape described below, and ``DIFF_SCHEMA_VERSION`` is the version of that
shape: bump it whenever a client-observable part of this contract changes.

Top-level keys, all always present:

* ``diff_schema_version`` -- ``int``, the value of ``DIFF_SCHEMA_VERSION``.
* ``serialized_dag_schema_versions`` -- ``{"base": int | None, "target": int | None}``, the
  ``__version`` each stored payload carried. ``None`` means the key was absent or not an
  integer; ``bool`` is rejected, so ``__version: true`` reads as ``None`` and never as
  version 1. A version in ``SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS`` is upgraded to the
  newest supported version before anything is compared.
* ``mode`` -- ``"observed_state"`` when the two payloads were compared, ``"unavailable"``
  when no comparison happened.
* ``changes`` -- the change records described below, ordered by the deterministic walk.
  Always empty when ``mode`` is ``"unavailable"``.
* ``truncated`` -- ``True`` when more underlying changes exist than ``max_changes`` allowed.
  Always ``False`` when ``mode`` is ``"unavailable"``.
* ``values`` -- ``{"status": "available" | "unavailable"}``. ``"available"`` only for an
  ``"observed_state"`` result whose caller authorized value disclosure, so
  ``result["values"]["status"]`` is safe to read on every result of every entry point.

``unavailable_reason`` is the one conditional key: present exactly when ``mode`` is
``"unavailable"``. :func:`build_serialized_dag_diff` produces:

* ``serialized_dag_missing`` -- one side has no stored payload.
* ``serialized_dag_schema_version_missing`` -- one side carries no usable ``__version``.
* ``unsupported_serialized_dag_schema_version:<n>`` -- ``<n>`` is the first unsupported
  version found, base before target.
* ``serialized_dag_canonicalization_failed`` -- a payload could not be normalized (malformed
  structure, an unsupported ``client_defaults`` section, an unencodable value).
* ``serialized_dag_recursion_limit_exceeded`` -- the comparison walk ran too deep. Defensive
  guard; deeply nested payloads generally fail canonicalization first.
* ``serialized_dag_json_encoding_failed`` -- a value could not be encoded as canonical JSON.

:meth:`~airflow.models.dag_version.DagVersion.get_diff` reuses
``serialized_dag_decode_failed``, for a stored payload that will not decompress or parse, and
``deadline_alert_missing``, for a payload that references a deadline alert row that is gone.
:func:`build_unavailable_dag_diff` passes its caller's reason through unchanged, so a new
caller extends this list rather than inventing an undocumented value.

Each change record carries these keys in both disclosure modes:

* ``path`` -- a JSON-Pointer-style path into a synthetic document with two roots: ``/dag``
  (the canonicalized serialized Dag, with ``__version`` dropped and client defaults folded
  into the tasks) and ``/provenance`` (the provenance mapping the caller supplied --
  ``bundle_name``, ``bundle_version`` and ``version_data`` from ``get_diff``). ``~`` and
  ``/`` inside a component are escaped as ``~0`` and ``~1``.
* ``operation`` -- ``"added"``, ``"removed"`` or ``"changed"``.
* ``category`` -- a :data:`DiffCategory`: ``task`` for task definitions and the Dag fields
  that shape task execution, ``dependency`` for ``dag_dependencies`` and downstream task
  ids, ``schedule`` for timetable, dates and concurrency, ``param`` for params,
  ``asset`` for task inlets and outlets, ``deadline`` for deadline alerts,
  ``callback`` for callback presence, ``metadata`` for descriptive and display fields,
  ``authorization`` for ``access_control``, ``provenance`` for everything under
  ``/provenance`` plus the file and bundle locators, and ``unknown`` for anything
  unclassified. ``_DIFF_V1_DAG_FIELD_CATEGORIES`` is the authoritative Dag-field mapping;
  an unclassified task field falls back to ``task``, which over-reports impact.
  ``default_args`` is the one Dag field classified per key rather than as a whole, because its
  keys are the operator fields they will be applied to: an allowlisted key takes that field's
  category (``owner`` is ``metadata``, ``retries`` is ``task``), and the aggregate record for
  the rest keeps the conservative ``task``. Only a stored value that is not a readable dict
  envelope classifies as ``task`` as a whole.
* ``impact`` -- a :data:`DiffImpact` derived from ``category``: ``provenance``, ``metadata``
  and ``authorization`` carry through unchanged, every operational category becomes
  ``execution``, and ``unknown`` stays ``unknown``.
* ``occurrence_count`` -- how many underlying changes the record stands for.

A caller that authorized values (``include_values=True``, ``values.status`` ``"available"``)
additionally gets:

* ``before_digest`` / ``after_digest`` -- ``"sha256:<hex>"`` over the canonical JSON of that
  side, or ``None`` when that side is missing. Both keys are always present.
* ``before_value`` / ``after_value`` -- the canonicalized value, which is the stored one after
  schema and client defaults are folded in and params are normalized. ``before_value`` is absent from
  an ``added`` record and ``after_value`` from a ``removed`` one, which is how a missing side
  is told apart from a stored ``null``.

``path`` and ``occurrence_count`` also differ between the modes. A redacted record reports the
public path: members of keyed collections (``/dag/tasks``, ``/dag/dag_dependencies``,
``/dag/tags``, ``/dag/allowed_run_types`` and task group children) collapse to ``*`` instead
of naming a task, tag or group, and records sharing a public path and operation are merged
into one whose ``occurrence_count`` counts the merged changes. An authorized record keeps the
identifying component and always has an ``occurrence_count`` of 1. Fields outside the v1 task,
task group and ``default_args`` allowlists are aggregated into a single ``custom_fields`` record
in both modes, so neither a private serializer field nor a user-chosen ``default_args`` key is
ever named by either -- ``/dag/default_args`` names only allowlisted operator field names, which
is why its keys can be classified at all. A top-level key outside the v1 root allowlist is
likewise reported as ``/custom_fields`` when redacted, while an authorized record keeps its real
root path.

``max_changes`` bounds the result rather than the traversal: both modes walk the same shape and
reach the same changes, so authorization never decides which changes exist, and the public paths
either mode reports -- and the order of their first occurrence -- are the same. What the bound
admits does differ. An authorized record stands for one change, so the bound caps those directly.
A redacted record stands for every change sharing its public path and operation, and a change
reaching a record already in the result costs no new one, so redacted counting continues past the
bound until a change needs a record the bound will not allow. The same payload at the same bound
can therefore report more occurrences, and a different ``truncated``, when values are withheld.

``truncated`` reports that such a change was dropped. When it is ``False`` every changed path
is present with an exact ``occurrence_count``. When it is ``True`` the walk stopped early: some
paths are absent rather than unchanged, and every ``occurrence_count`` is a lower bound, since
occurrences after the stop were never reached.

Typed dictionaries for the result and the change record are deliberately deferred -- the
record shape varies with disclosure mode, so modelling it belongs with the REST layer that
exposes it. :data:`DiffCategory` and :data:`DiffImpact` are stable enough to generate enums
from today.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Callable, Mapping, MutableMapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any, Literal, NamedTuple

import structlog

from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.serialized_objects import (
    _DAG_CALLBACK_FIELDS,
    _OPERATOR_TIMEDELTA_FIELDS,
    DagSerialization,
    OperatorSerialization,
)

log = structlog.get_logger(__name__)

DIFF_SCHEMA_VERSION = 1
DEFAULT_MAX_CHANGES = 500
MAX_ALLOWED_CHANGES = 5000
SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS = frozenset((1, 2, 3))

DiffCategory = Literal[
    "asset",
    "authorization",
    "callback",
    "deadline",
    "dependency",
    "metadata",
    "param",
    "provenance",
    "schedule",
    "task",
    "unknown",
]
DiffImpact = Literal["authorization", "execution", "metadata", "provenance", "unknown"]

_ORDER_INSENSITIVE_LIST_PATHS = {
    ("dag", "tags"),
    ("dag", "allowed_run_types"),
}
_KEYED_COLLECTION_PATHS = {
    ("dag", "tasks"),
    ("dag", "dag_dependencies"),
    *_ORDER_INSENSITIVE_LIST_PATHS,
}
# Canonicalization folds away __version and client_defaults and adds provenance, so these are the
# only top-level sections the walk is allowed to name.
_DIFF_V1_PUBLIC_ROOT_FIELDS = frozenset({"dag", "provenance"})
_CUSTOM_TASK_FIELDS_PATH_COMPONENT = "custom_fields"
# This allowlist is part of diff schema v1. Serializer schema changes must not
# silently change the paths visible to callers of the diff API.
_DIFF_V1_PUBLIC_TASK_FIELDS = frozenset(
    {
        "__type",
        "_can_skip_downstream",
        "_disallow_kwargs_override",
        "_expand_input_attr",
        "_is_empty",
        "_is_mapped",
        "_is_sensor",
        "_logger_name",
        "_needs_expansion",
        "_operator_extra_links",
        "_operator_name",
        "_task_display_name",
        "_task_module",
        "allow_nested_operators",
        "depends_on_past",
        "do_xcom_push",
        "doc",
        "doc_json",
        "doc_md",
        "doc_rst",
        "doc_yaml",
        "downstream_task_ids",
        "email",
        "email_on_failure",
        "email_on_retry",
        "end_date",
        "execution_timeout",
        "executor",
        "executor_config",
        "expand_input",
        "has_on_execute_callback",
        "has_on_failure_callback",
        "has_on_retry_callback",
        "has_on_skipped_callback",
        "has_on_success_callback",
        "has_retry_policy",
        "ignore_first_depends_on_past",
        "inlets",
        "is_setup",
        "is_stub",
        "is_teardown",
        "map_index_template",
        "max_active_tis_per_dag",
        "max_active_tis_per_dagrun",
        "max_retry_delay",
        "multiple_outputs",
        "on_failure_fail_dagrun",
        "op_kwargs_expand_input",
        "outlets",
        "owner",
        "params",
        "partial_kwargs",
        "pool",
        "pool_slots",
        "priority_weight",
        "python_callable_name",
        "queue",
        "render_template_as_native_obj",
        "reschedule",
        "resources",
        "retries",
        "retry_delay",
        "retry_exponential_backoff",
        "run_as_user",
        "start_date",
        "start_from_trigger",
        "start_trigger_args",
        "task_id",
        "task_type",
        "template_ext",
        "template_fields",
        "template_fields_renderers",
        "trigger_rule",
        "ui_color",
        "ui_fgcolor",
        "wait_for_downstream",
        "wait_for_past_depends_before_skipping",
        "weight_rule",
    }
)
_DIFF_V1_REDACTED_SCHEMA_TASK_FIELDS = frozenset({"_arg_bindings"})
# Task-field categories used by _get_category.
_DIFF_V1_TASK_ASSET_FIELDS = frozenset({"inlets", "outlets"})
_DIFF_V1_TASK_PARAM_FIELDS = frozenset({"params"})
_DIFF_V1_TASK_DEPENDENCY_FIELDS = frozenset({"downstream_task_ids"})
_DIFF_V1_TASK_METADATA_FIELDS = frozenset(
    {
        "_operator_name",
        "doc",
        "doc_json",
        "doc_md",
        "doc_rst",
        "doc_yaml",
        "owner",
        "ui_color",
        "ui_fgcolor",
        "_task_display_name",
        "task_display_name",
    }
)
_DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS = _DIFF_V1_PUBLIC_TASK_FIELDS | {"task_display_name"}
_DEFAULT_ARGS_PATH = ("dag", "default_args")
_RETRY_BACKOFF_FIELD = "retry_exponential_backoff"
# Dag fields the schema gives no default for, added after the first serializer versions.
_ABSENT_AS_NULL_DAG_FIELDS = ("allowed_run_types", "deadline")
# These hydrate to sets (_deserialize_operator_field for a task, TaskGroup.*_ids.update for a group),
# so a reordered stored list means the same edges and must not read as a dependency change.
_SET_VALUED_ID_FIELDS = frozenset(
    {"downstream_task_ids", "upstream_task_ids", "downstream_group_ids", "upstream_group_ids"}
)
# A Dag's default_args holds the same operator __init__ kwargs partial_kwargs holds -- the
# serializer rewrites _HAS_FLAG_FIELDS in both -- so it reuses that allowlist. "__type" names the
# stored value's own dict envelope rather than a key inside it.
_DIFF_V1_PUBLIC_DEFAULT_ARGS_FIELDS = _DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS - {"__type"}
# Classify every Dag schema field explicitly so new fields require a policy decision.
_DIFF_V1_DAG_FIELD_CATEGORIES: dict[str, DiffCategory] = {
    "_concurrency": "schedule",
    "_processor_dags_folder": "provenance",
    "access_control": "authorization",
    "allowed_run_types": "schedule",
    "bundle_name": "provenance",
    "catchup": "schedule",
    "dag_dependencies": "dependency",
    "dag_display_name": "metadata",
    "dag_id": "metadata",
    "dagrun_timeout": "schedule",
    "deadline": "deadline",
    # Only reached when the stored value is not the dict envelope _collect_default_args_changes
    # walks per key; an unreadable envelope keeps the conservative impact of its widest key.
    "default_args": "task",
    "description": "metadata",
    # Decides whether a run pins a bundle version, so it changes which code later runs execute
    # and whether triggering a historical version is allowed at all.
    "disable_bundle_versioning": "task",
    "doc_md": "metadata",
    "edge_info": "metadata",
    "end_date": "schedule",
    "fail_fast": "schedule",
    "fileloc": "provenance",
    "has_on_failure_callback": "callback",
    "has_on_success_callback": "callback",
    "is_paused_upon_creation": "schedule",
    "max_active_runs": "schedule",
    "max_active_tasks": "schedule",
    "max_consecutive_failed_dag_runs": "schedule",
    "owner_links": "metadata",
    "params": "param",
    "relative_fileloc": "provenance",
    "render_template_as_native_obj": "task",
    "rerun_with_latest_version": "task",
    "start_date": "schedule",
    "tags": "metadata",
    "task_group": "task",
    "tasks": "task",
    "timetable": "schedule",
    "timezone": "schedule",
}
# Fields dropped from the current schema that a stored payload can still carry. Every entry must
# survive _canonicalize_payload_v1, or it classifies nothing.
_DIFF_V1_LEGACY_DAG_FIELD_CATEGORIES: dict[str, DiffCategory] = {
    "fail_stop": "schedule",
    "on_failure_callback": "callback",
    "on_success_callback": "callback",
    "schedule": "schedule",
}
_RECURSIVE_MAPPING_PATHS = {
    (),
    ("dag",),
    ("provenance",),
    *_KEYED_COLLECTION_PATHS,
}
_DIFF_V1_TASK_GROUP_METADATA_FIELDS = frozenset(
    {"group_display_name", "tooltip", "doc_md", "ui_color", "ui_fgcolor"}
)
_DIFF_V1_PUBLIC_TASK_GROUP_FIELDS = _DIFF_V1_TASK_GROUP_METADATA_FIELDS | {
    "_group_id",
    "prefix_group_id",
    "children",
    "upstream_group_ids",
    "downstream_group_ids",
    "upstream_task_ids",
    "downstream_task_ids",
    "expand_input",
    "is_mapped",
}


def build_unavailable_dag_diff(
    *,
    base_data: dict[str, Any] | None,
    target_data: dict[str, Any] | None,
    reason: str,
) -> dict[str, Any]:
    """Report a known unavailable reason without comparing the stored payloads."""
    return _mark_unavailable(
        _build_diff_result(_get_schema_version(base_data), _get_schema_version(target_data)), reason
    )


def build_serialized_dag_diff(
    *,
    base_data: dict[str, Any] | None,
    target_data: dict[str, Any] | None,
    base_provenance: Mapping[str, Any] | None = None,
    target_provenance: Mapping[str, Any] | None = None,
    include_values: bool = False,
    max_changes: int = DEFAULT_MAX_CHANGES,
) -> dict[str, Any]:
    """
    Compare two serialized Dag payloads and their provenance deterministically.

    Callers must authorize disclosure of the entire serialized payload, including
    access-control roles and permissions, before setting ``include_values=True``.
    This exposes canonicalized values, digests, and identifying path components.

    ``max_changes`` limits output records. Redacted changes with the same public path
    and operation share a record. A new record exceeding the limit stops the walk;
    ``truncated`` is then true and occurrence counts are lower bounds.

    An ``unavailable`` result includes the reason comparison failed.
    """
    validate_max_changes(max_changes)

    base_schema_version = _get_schema_version(base_data)
    target_schema_version = _get_schema_version(target_data)
    result = _build_diff_result(base_schema_version, target_schema_version)

    if base_data is None or target_data is None:
        return _mark_unavailable(result, "serialized_dag_missing")

    if base_schema_version is None or target_schema_version is None:
        return _mark_unavailable(result, "serialized_dag_schema_version_missing")

    unsupported_versions = [
        version
        for version in (base_schema_version, target_schema_version)
        if version not in SUPPORTED_SERIALIZED_DAG_SCHEMA_VERSIONS
    ]
    if unsupported_versions:
        return _mark_unavailable(
            result, f"unsupported_serialized_dag_schema_version:{unsupported_versions[0]}"
        )

    try:
        base_document = _canonicalize_payload_v1(base_data)
        target_document = _canonicalize_payload_v1(target_data)
        base_document["provenance"] = _canonicalize_value(dict(base_provenance or {}), path=("provenance",))
        target_document["provenance"] = _canonicalize_value(
            dict(target_provenance or {}), path=("provenance",)
        )
    except (AttributeError, KeyError, OverflowError, RecursionError, TypeError, ValueError) as error:
        log.warning(
            "Serialized Dag diff canonicalization failed",
            error_type=type(error).__name__,
            reason=str(error),
            base_schema_version=base_schema_version,
            target_schema_version=target_schema_version,
        )
        return _mark_unavailable(result, "serialized_dag_canonicalization_failed")

    collector = _ChangeCollector(max_changes=max_changes, include_values=include_values)
    try:
        _collect_changes(base_document, target_document, path=(), collector=collector)
    except RecursionError:
        log.warning(
            "Serialized Dag diff recursion limit exceeded",
            base_schema_version=base_schema_version,
            target_schema_version=target_schema_version,
        )
        return _mark_unavailable(result, "serialized_dag_recursion_limit_exceeded")
    except _JsonEncodingError:
        log.warning(
            "Serialized Dag diff JSON encoding failed",
            base_schema_version=base_schema_version,
            target_schema_version=target_schema_version,
        )
        return _mark_unavailable(result, "serialized_dag_json_encoding_failed")

    result["changes"] = collector.changes
    result["truncated"] = collector.is_truncated
    if include_values:
        result["values"] = {"status": "available"}
    return result


class _ChangeCollector:
    def __init__(self, *, max_changes: int, include_values: bool) -> None:
        self.changes: list[dict[str, Any]] = []
        self._record_count = 0
        self.max_changes = max_changes
        self.include_values = include_values
        self._truncated = False
        self._redacted_changes: dict[tuple[tuple[str, ...], str], dict[str, Any]] = {}

    @property
    def is_truncated(self) -> bool:
        return self._truncated

    def add(
        self,
        *,
        path: tuple[str, ...],
        operation: Literal["added", "removed", "changed"],
        before: Any,
        after: Any,
    ) -> None:
        public_path = _get_public_path(path)
        key = (public_path, operation)
        if not self.include_values and (existing := self._redacted_changes.get(key)) is not None:
            # Keep repeated changes from crowding out distinct paths.
            existing["occurrence_count"] += 1
            return
        self._record_count += 1
        if self._record_count > self.max_changes:
            self._truncated = True
            return

        category = _get_category(public_path)
        change: dict[str, Any] = {
            "path": _format_path(path if self.include_values else public_path),
            "operation": operation,
            "category": category,
            "impact": _get_impact(category),
            "occurrence_count": 1,
        }
        if self.include_values:
            change["before_digest"] = None if before is _MISSING else _get_digest(before)
            change["after_digest"] = None if after is _MISSING else _get_digest(after)
            if before is not _MISSING:
                change["before_value"] = before
            if after is not _MISSING:
                change["after_value"] = after
        else:
            self._redacted_changes[key] = change
        self.changes.append(change)


_MISSING = object()


def validate_max_changes(max_changes: int) -> None:
    if not isinstance(max_changes, int) or isinstance(max_changes, bool) or max_changes < 1:
        raise ValueError("max_changes must be a positive integer")
    if max_changes > MAX_ALLOWED_CHANGES:
        raise ValueError(f"max_changes must not exceed {MAX_ALLOWED_CHANGES}")


def _get_schema_version(data: Mapping[str, Any] | None) -> int | None:
    if not isinstance(data, Mapping):
        return None
    version = data.get("__version")
    return version if isinstance(version, int) and not isinstance(version, bool) else None


def _mark_unavailable(result: dict[str, Any], reason: str) -> dict[str, Any]:
    result["mode"] = "unavailable"
    result["unavailable_reason"] = reason
    return result


def _build_diff_result(base_schema_version: int | None, target_schema_version: int | None) -> dict[str, Any]:
    return {
        "diff_schema_version": DIFF_SCHEMA_VERSION,
        "serialized_dag_schema_versions": {
            "base": base_schema_version,
            "target": target_schema_version,
        },
        "mode": "observed_state",
        "changes": [],
        "truncated": False,
        # Always present so reading result["values"]["status"] is safe on every outcome.
        "values": {"status": "unavailable"},
    }


def _canonicalize_payload_v1(data: dict[str, Any]) -> dict[str, Any]:
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
    dag_defaults = {
        field: value
        for field, value in DagSerialization.get_schema_defaults("dag").items()
        # Dag callback flags are enabled by their presence, even when their value is false.
        if field not in _DAG_CALLBACK_FIELDS
    }
    payload["dag"] = {**dag_defaults, **payload["dag"]}
    for field in _DAG_CALLBACK_FIELDS & payload["dag"].keys():
        payload["dag"][field] = True
    if "params" in payload["dag"]:
        payload["dag"]["params"] = _normalize_params(payload["dag"]["params"])
    # The schema carries no default, so a Dag without default_args omits the key entirely. An
    # absent side would reach the walk as a missing value and report the whole mapping as one
    # opaque change, which is how adding the very first entry would escape per-key classification.
    payload["dag"].setdefault("default_args", {"__type": "dict", "__var": {}})
    # Neither field has a schema default to fill the gap in, so a producer that predates them omits
    # the key while the current one writes an explicit null. Both hydrate the same, and a diff that
    # spans an upgrade would otherwise report an added execution change on every Dag.
    for field in _ABSENT_AS_NULL_DAG_FIELDS:
        payload["dag"].setdefault(field, None)
    if payload["dag"].get("allowed_run_types") == []:
        # An empty restriction list hydrates to None, the same as no restriction at all.
        payload["dag"]["allowed_run_types"] = None
    _apply_task_defaults(payload)
    payload.pop("__version", None)
    return _canonicalize_value(payload, path=())


@dataclass(frozen=True)
class _TaskDefaultSets:
    """The default sets that every task in one payload is folded against."""

    schema_defaults: Mapping[str, Any]
    outer_schema_defaults: Mapping[str, Any]
    partial_schema_defaults: Mapping[str, Any]
    partial_fields: frozenset[str]
    client_task_defaults: Mapping[str, Any]
    inherited_dates: Mapping[str, Any]


def _build_task_default_sets(
    payload: dict[str, Any], client_task_defaults: Mapping[str, Any]
) -> _TaskDefaultSets:
    # get_schema_defaults is lru_cache'd and hands back the very mapping the live Dag-hydration path
    # reads, so every use of it in this module must stay read-only. Mutating it in place would
    # corrupt Dag hydration process-wide, not merely produce a wrong diff.
    schema_defaults = DagSerialization.get_schema_defaults("operator")
    partial_fields = (
        SerializedBaseOperator.get_serialized_fields() - SerializedMappedOperator.get_serialized_fields()
    )
    return _TaskDefaultSets(
        schema_defaults=schema_defaults,
        outer_schema_defaults={
            field: value for field, value in schema_defaults.items() if field not in partial_fields
        },
        # A mapped task resolves these through partial_kwargs, so their defaults have to be applied
        # there and encoded like any other partial value rather than left at the outer level.
        partial_schema_defaults={
            field: value for field, value in schema_defaults.items() if field in partial_fields
        },
        partial_fields=partial_fields,
        client_task_defaults=client_task_defaults,
        # set_task_dag_references falls back to the Dag's dates, and the serializer elides a task date
        # that already matches, so an absent task date means the Dag's date rather than a change.
        inherited_dates={field: payload["dag"].get(field) for field in ("start_date", "end_date")},
    )


def _apply_task_defaults(payload: dict[str, Any]) -> None:
    client_defaults = payload.pop("client_defaults", None)
    if client_defaults is None:
        client_defaults = {}
    if not isinstance(client_defaults, Mapping):
        raise ValueError("client_defaults is not an object")

    # Fail loudly on a section this version cannot fold in: dropping it would silently
    # compare two payloads as equal when the unhandled defaults actually differ.
    if unknown_sections := client_defaults.keys() - {"tasks"}:
        raise ValueError(f"unsupported client_defaults sections: {sorted(unknown_sections)}")

    client_task_defaults = client_defaults.get("tasks", {})
    if not isinstance(client_task_defaults, Mapping):
        raise ValueError("client_defaults.tasks is not an object")

    defaults = _build_task_default_sets(payload, client_task_defaults)
    tasks = payload["dag"].get("tasks", [])
    if not isinstance(tasks, list):
        raise ValueError("dag.tasks is not a list")
    for task in tasks:
        upgraded_task = _upgrade_task_entry(task, client_defaults)
        template_fields = upgraded_task.get("template_fields", [])
        if upgraded_task.get("_is_mapped"):
            task_data = _build_mapped_task_data(upgraded_task, template_fields, defaults)
        else:
            task_data = _build_unmapped_task_data(upgraded_task, template_fields, defaults)
        _apply_inherited_dates(task_data, template_fields, defaults)
        if "params" in task_data and "params" not in template_fields:
            task_data["params"] = _normalize_params(task_data["params"])
        for field in _SET_VALUED_ID_FIELDS & task_data.keys():
            if field not in template_fields:
                task_data[field] = _sort_id_list(task_data[field])
        task["__var"] = task_data


def _upgrade_task_entry(task: Any, client_defaults: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(task, dict) or not isinstance(task.get("__var"), Mapping):
        raise ValueError("task entry is not an object")
    encoded_task = OperatorSerialization._apply_defaults_to_encoded_op(
        dict(task["__var"]), dict(client_defaults)
    )
    upgraded_task = OperatorSerialization._upgrade_encoded_operator(encoded_task)
    # Operator identity is selected before client defaults are applied during hydration.
    upgraded_task["_is_mapped"] = bool(task["__var"].get("_is_mapped", False))
    return upgraded_task


def _build_mapped_task_data(
    upgraded_task: dict[str, Any], template_fields: Any, defaults: _TaskDefaultSets
) -> dict[str, Any]:
    task_data = {**defaults.outer_schema_defaults, **upgraded_task}
    effective_partial_kwargs = _build_effective_partial_kwargs(task_data, template_fields, defaults)
    task_data["partial_kwargs"] = effective_partial_kwargs
    _normalize_retry_backoff(effective_partial_kwargs)
    return task_data


def _build_effective_partial_kwargs(
    task_data: dict[str, Any], template_fields: Any, defaults: _TaskDefaultSets
) -> dict[str, Any]:
    """Resolve a mapped task's partial_kwargs, consuming the outer fields that move into them."""
    partial_kwargs = task_data.get("partial_kwargs", {})
    if not isinstance(partial_kwargs, Mapping):
        raise ValueError("partial_kwargs is not an object")
    # populate_operator only folds client defaults into partial_kwargs when the payload
    # carries the key, so an absent one leaves the top-level value as the effective value.
    effective_partial_kwargs = (
        {
            field: _encode_partial_field_value(field, value)
            for field, value in defaults.client_task_defaults.items()
        }
        if "partial_kwargs" in task_data
        else {}
    )
    for field, value in partial_kwargs.items():
        effective_partial_kwargs[field] = (
            value if _is_encoded_partial_value(value) else _encode_partial_field_value(field, value)
        )
    # Match populate_operator: partial values take precedence over outer task defaults.
    for field in defaults.partial_fields & task_data.keys():
        value = task_data.pop(field)
        # Outer fields are already encoded unless template handling bypasses deserialization.
        if field in template_fields:
            value = _encode_json_value(value)
        effective_partial_kwargs.setdefault(field, value)
    for field, value in defaults.partial_schema_defaults.items():
        effective_partial_kwargs.setdefault(field, _encode_partial_field_value(field, value))
    return effective_partial_kwargs


def _build_unmapped_task_data(
    upgraded_task: dict[str, Any], template_fields: Any, defaults: _TaskDefaultSets
) -> dict[str, Any]:
    task_data = {**defaults.schema_defaults, **upgraded_task}
    # populate_operator leaves a template field at its stored value, so converting one here would
    # compare a stored True and a stored 2.0 as equal when they hydrate to different backoff factors.
    if not (_RETRY_BACKOFF_FIELD in template_fields and _RETRY_BACKOFF_FIELD in upgraded_task):
        _normalize_retry_backoff(task_data)
    for field in _OPERATOR_TIMEDELTA_FIELDS & task_data.keys():
        value = task_data[field]
        task_data[field] = (
            _encode_json_value(value)
            if field in template_fields and field in upgraded_task
            else _encode_partial_field_value(field, value)
        )
    return task_data


def _apply_inherited_dates(
    task_data: dict[str, Any], template_fields: Any, defaults: _TaskDefaultSets
) -> None:
    for field, dag_date in defaults.inherited_dates.items():
        if task_data.get(field) is None:
            task_data[field] = _encode_partial_field_value(field, dag_date)
        elif field in template_fields:
            task_data[field] = _encode_json_value(task_data[field])
        else:
            task_data[field] = _encode_partial_field_value(field, task_data[field])


# These two predicates deliberately disagree on what counts as already encoded, because the
# serializer sites they mirror disagree: keep them apart rather than "aligning" them.
def _is_encoded_partial_value(value: Any) -> bool:
    # _deserialize_partial_kwargs only takes the full-deserialization path with BOTH keys present.
    return isinstance(value, Mapping) and "__type" in value and "__var" in value


def _is_encoded_param_attribute(value: Any) -> bool:
    # _deserialize_param detects a legacy encoding from "__type" alone, with no "__var" required.
    if isinstance(value, Mapping):
        return "__type" in value
    if isinstance(value, list):
        return all(isinstance(item, Mapping) and "__type" in item for item in value)
    return False


def _normalize_params(params: Any) -> Any:
    """Normalize legacy Params without losing the order used by the trigger form."""
    if isinstance(params, Mapping):
        # 2.9.2 and earlier stored params as a JSON object instead of ordered pairs.
        pairs: Any = params.items()
    elif isinstance(params, list):
        pairs = params
    else:
        return params

    normalized: dict[str, Any] = {}
    for pair in pairs:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            # Leave an unrecognised shape to the raw comparison rather than guessing at it.
            return params
        name, value = pair
        if isinstance(value, Mapping) and "__class" in value:
            value = {
                "default": _normalize_param_attribute(value.get("default")),
                "description": _normalize_param_attribute(value.get("description")),
                "schema": _normalize_param_attribute(value.get("schema") or {}),
                "source": _normalize_param_attribute(value.get("source")),
            }
        else:
            value = {
                "default": _encode_json_value(value),
                "description": None,
                "schema": _encode_json_value({}),
                "source": None,
            }
        normalized[str(name)] = value
    return [[name, value] for name, value in normalized.items()]


def _normalize_param_attribute(value: Any) -> Any:
    # Match _deserialize_param's legacy-encoding detection without hydrating user objects.
    if _is_encoded_param_attribute(value):
        return value
    return _encode_json_value(value)


def _encode_partial_field_value(field: str, value: Any) -> Any:
    if field in _OPERATOR_TIMEDELTA_FIELDS and value is not None:
        return {"__type": "timedelta", "__var": value}
    if field.endswith("_date") and value is not None and not isinstance(value, str):
        return {"__type": "datetime", "__var": value}
    if field == "resources":
        # Resources serialize as a raw mapping; a plain dictionary has a dict envelope instead.
        return value
    return _encode_json_value(value)


def _encode_json_value(value: Any) -> Any:
    """Encode plain JSON without invoking object serializers."""
    if isinstance(value, Mapping):
        return {"__type": "dict", "__var": {key: _encode_json_value(item) for key, item in value.items()}}
    if isinstance(value, list):
        return [_encode_json_value(item) for item in value]
    return value


def _is_group_dependency_path(path: tuple[str, ...]) -> bool:
    """
    Identify a dependency id list that belongs to a task group itself.

    Canonicalization paths carry no list indices, so a group's own fields sit under repeated
    ``children/<group_id>`` pairs. Requiring that exact shape keeps the normalization off a mapped
    group's ``expand_input``, where a user argument's order and multiplicity decide how many task
    instances run. A task group declares no template fields, so nothing else needs guarding.
    """
    if path[:2] != ("dag", "task_group") or path[-1] not in _SET_VALUED_ID_FIELDS:
        return False
    nesting = path[2:-1]
    return len(nesting) % 2 == 0 and all(name == "children" for name in nesting[::2])


def _sort_id_list(value: Any) -> Any:
    """Order a stored id list the way its hydrated set compares, leaving any other shape alone."""
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return sorted(set(value))
    return value


def _normalize_retry_backoff(task_fields: dict[str, Any]) -> None:
    if _RETRY_BACKOFF_FIELD in task_fields:
        value = task_fields[_RETRY_BACKOFF_FIELD]
        # A non-numeric stored value stays as it is so the comparison reports just this field
        # instead of collapsing the whole diff to a canonicalization failure.
        with suppress(TypeError, ValueError):
            task_fields[_RETRY_BACKOFF_FIELD] = 2.0 if value is True else float(value)


def _canonicalize_value(value: Any, *, path: tuple[str, ...]) -> Any:
    if isinstance(value, Mapping):
        if path == ("dag", "deadline"):
            if value.get("__type") == "deadline_alert":
                value = value["__var"]
            value = {"name": None, **value}
            interval = value.get("interval")
            if isinstance(interval, (int, float)) and not isinstance(interval, bool):
                # Diff schema v1 uses the SDK's version-2 timedelta encoding for legacy seconds.
                value["interval"] = {
                    "__classname__": "datetime.timedelta",
                    "__version__": 2,
                    "__data__": timedelta(seconds=interval).total_seconds(),
                }
        return {
            canonical_key: _canonicalize_value(item, path=path + (canonical_key,))
            for canonical_key, item in (
                (_canonicalize_mapping_key(key), item)
                for key, item in sorted(value.items(), key=lambda item: _canonicalize_mapping_key(item[0]))
            )
        }
    if isinstance(value, (list, tuple)):
        canonical_values = [_canonicalize_value(item, path=path) for item in value]
        if path == ("dag", "tasks"):
            return _canonicalize_keyed_list(canonical_values, _get_task_id, path)
        if path == ("dag", "dag_dependencies"):
            return _canonicalize_keyed_list(canonical_values, _get_dependency_key, path)
        if path in _ORDER_INSENSITIVE_LIST_PATHS:
            return _canonicalize_keyed_list(canonical_values, _get_string_key, path)
        if _is_group_dependency_path(path):
            return _sort_id_list(canonical_values)
        return canonical_values
    return value


def _canonicalize_mapping_key(key: Any) -> str:
    if isinstance(key, str) and isinstance(key, Enum):
        return key.value
    return str(key)


def _canonicalize_keyed_list(
    values: list[Any], key_getter: Callable[[Any], str], path: tuple[str, ...]
) -> dict[str, Any]:
    keyed_values: dict[str, Any] = {}
    for value in values:
        key = key_getter(value)
        if key in keyed_values:
            if path in _ORDER_INSENSITIVE_LIST_PATHS or (
                path == ("dag", "dag_dependencies") and keyed_values[key] == value
            ):
                continue
            raise ValueError(f"duplicate key {key!r} in /{'/'.join(path)}")
        keyed_values[key] = value
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
    if not isinstance(dependency, MutableMapping):
        raise ValueError("dependency entry is not an object")
    # DagDependency.node_id identifies an edge by the components below alone, and ``label`` only
    # mirrors state compared elsewhere (a task's _task_display_name, or an asset name already inside
    # dependency_id). Keying on it turns a rename into one edge removed and another added; leaving
    # it in the entry turns two entries on one edge into duplicates that fail canonicalization.
    dependency.pop("label", None)
    components = (
        dependency.get("dependency_type"),
        dependency.get("dependency_id"),
        dependency.get("source"),
        dependency.get("target"),
    )
    return json.dumps(components, ensure_ascii=False, separators=(",", ":"))


def _collect_changes(
    before: Any,
    after: Any,
    *,
    path: tuple[str, ...],
    collector: _ChangeCollector,
) -> None:
    if before is _MISSING and after is _MISSING:
        return
    if isinstance(before, Mapping) and isinstance(after, Mapping):
        # Never branch on ``include_values`` here; see build_serialized_dag_diff.
        if _is_task_mapping_path(path):
            _collect_task_changes(before, after, path=path, collector=collector)
            return
        if _is_task_group_mapping_path(path):
            _collect_public_field_changes(
                before,
                after,
                path=path,
                public_fields=_DIFF_V1_PUBLIC_TASK_GROUP_FIELDS,
                collector=collector,
            )
            return
        if path == _DEFAULT_ARGS_PATH:
            _collect_default_args_changes(before, after, path=path, collector=collector)
            return
        if not _should_recurse_mapping(path):
            if not _is_json_equal(before, after):
                collector.add(path=path, operation="changed", before=before, after=after)
            return
        keys = sorted({str(key) for key in before} | {str(key) for key in after})
        for key in keys:
            before_value = before.get(key, _MISSING)
            after_value = after.get(key, _MISSING)
            _collect_changes(before_value, after_value, path=path + (key,), collector=collector)
            if collector.is_truncated:
                return
        return

    if isinstance(before, list) and isinstance(after, list):
        if (
            _is_task_group_child_path(path)
            and len(before) == len(after) == 2
            and before[0] == after[0] == "taskgroup"
            and isinstance(before[1], Mapping)
            and isinstance(after[1], Mapping)
        ):
            _collect_changes(before[1], after[1], path=path + ("1",), collector=collector)
            return
        if not _is_json_equal(before, after):
            collector.add(path=path, operation="changed", before=before, after=after)
        return

    if before is _MISSING:
        collector.add(path=path, operation="added", before=_MISSING, after=after)
    elif after is _MISSING:
        collector.add(path=path, operation="removed", before=before, after=_MISSING)
    elif not _is_json_equal(before, after):
        collector.add(path=path, operation="changed", before=before, after=after)


def _collect_task_changes(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    path: tuple[str, ...],
    collector: _ChangeCollector,
) -> None:
    if len(path) == 3:
        _collect_changes(
            before.get("__type", _MISSING),
            after.get("__type", _MISSING),
            path=path + ("__type",),
            collector=collector,
        )
        if collector.is_truncated:
            return
        before, after = before["__var"], after["__var"]
        public_fields = _DIFF_V1_PUBLIC_TASK_FIELDS - {"__type"}
        if not (before.get("_is_mapped") and after.get("_is_mapped")):
            # Comparing partial_kwargs field by field only means anything when both sides really are
            # mapped. An unmapped operator may declare partial_kwargs a template field, making the
            # mapping its own execution input: reading those keys as operator fields classified user
            # data by whichever field a key collided with, and named a key the author chose. Keep it
            # opaque, like any other field off the allowlist.
            public_fields = public_fields - {"partial_kwargs"}
    else:
        public_fields = _DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS
    _collect_public_field_changes(before, after, path=path, public_fields=public_fields, collector=collector)


def _collect_default_args_changes(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    path: tuple[str, ...],
    collector: _ChangeCollector,
) -> None:
    """Compare a Dag's default_args key by key so each key carries its own impact."""
    if not (_is_encoded_default_args(before) and _is_encoded_default_args(after)):
        # An envelope this version cannot read stays one opaque leaf: the allowlist was written for
        # the keys of a dict envelope, so applying it to another shape could name something else.
        if not _is_json_equal(before, after):
            collector.add(path=path, operation="changed", before=before, after=after)
        return
    _collect_public_field_changes(
        before["__var"],
        after["__var"],
        path=path,
        public_fields=_DIFF_V1_PUBLIC_DEFAULT_ARGS_FIELDS,
        collector=collector,
    )


def _is_encoded_default_args(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and value.get("__type") == "dict"
        and isinstance(value.get("__var"), Mapping)
    )


def _collect_public_field_changes(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    path: tuple[str, ...],
    public_fields: frozenset[str] | set[str],
    collector: _ChangeCollector,
) -> None:
    """Walk allowlisted fields one by one and report everything else as a single change."""
    keys = {str(key) for key in before} | {str(key) for key in after}
    for key in sorted(keys & public_fields):
        _collect_changes(
            before.get(key, _MISSING),
            after.get(key, _MISSING),
            path=path + (key,),
            collector=collector,
        )
        if collector.is_truncated:
            return

    before_custom_fields = {key: before[key] for key in before if key not in public_fields}
    after_custom_fields = {key: after[key] for key in after if key not in public_fields}
    if not _is_json_equal(before_custom_fields, after_custom_fields):
        collector.add(
            path=path + (_CUSTOM_TASK_FIELDS_PATH_COMPONENT,),
            operation="changed",
            before=before_custom_fields,
            after=after_custom_fields,
        )


def _is_task_mapping_path(path: tuple[str, ...]) -> bool:
    return path[:2] == ("dag", "tasks") and (
        len(path) == 3 or (len(path) == 4 and path[3] == "partial_kwargs")
    )


class _TaskGroupPath(NamedTuple):
    """One reading of the task group path grammar, shared by everything that walks it."""

    field_index: int
    id_positions: tuple[int, ...]
    remainder: tuple[str, ...]


def _classify_task_group_path(path: tuple[str, ...]) -> _TaskGroupPath | None:
    """
    Split a task group path into its nesting prefix and what follows the innermost group.

    The grammar is ``dag/task_group ( /children/<group_id>/1 )* [ /children/<child_id> | /<field> ... ]``:
    a child entry is the two-element ``["taskgroup", {...}]`` list the serializer writes, so the
    literal ``1`` selects the nested group mapping inside it. ``field_index`` is where the innermost
    group's own keys start, ``id_positions`` the group-id components a redacted path must mask, and
    ``remainder`` the components from ``field_index`` on: ``()`` for the group mapping itself,
    ``("children",)`` for its children mapping, and a two-element ``("children", child_id)`` for one
    child entry. Returns ``None`` for a path that is not rooted at the Dag's task group.
    """
    if path[:2] != ("dag", "task_group"):
        return None
    index = 2
    id_positions: list[int] = []
    while len(path) >= index + 2 and path[index] == "children":
        id_positions.append(index + 1)
        if len(path) < index + 3 or path[index + 2] != "1":
            break
        index += 3
    return _TaskGroupPath(index, tuple(id_positions), path[index:])


def _is_task_group_mapping_path(path: tuple[str, ...]) -> bool:
    task_group_path = _classify_task_group_path(path)
    return task_group_path is not None and task_group_path.remainder == ()


def _should_recurse_mapping(path: tuple[str, ...]) -> bool:
    if path in _RECURSIVE_MAPPING_PATHS:
        return True
    task_group_path = _classify_task_group_path(path)
    return task_group_path is not None and task_group_path.remainder == ("children",)


def _is_task_group_child_path(path: tuple[str, ...]) -> bool:
    task_group_path = _classify_task_group_path(path)
    return (
        task_group_path is not None
        and len(task_group_path.remainder) == 2
        and task_group_path.remainder[0] == "children"
    )


def _get_public_path(path: tuple[str, ...]) -> tuple[str, ...]:
    if path and path[0] not in _DIFF_V1_PUBLIC_ROOT_FIELDS:
        # Root is the one level no allowlisted walk covers, so an unrecognised section name would
        # otherwise be the single path component a redacted caller receives verbatim.
        return (_CUSTOM_TASK_FIELDS_PATH_COMPONENT,)
    if (task_group_path := _classify_task_group_path(path)) is not None:
        public_path = list(path)
        for position in task_group_path.id_positions:
            public_path[position] = "*"
        remainder = task_group_path.remainder
        if remainder and remainder[0] not in _DIFF_V1_PUBLIC_TASK_GROUP_FIELDS:
            # Unreachable by construction (_collect_public_field_changes aggregates non-public group
            # keys first), but truncating like the task branch stops a future caller leaking a key.
            return (*public_path[: task_group_path.field_index], _CUSTOM_TASK_FIELDS_PATH_COMPONENT)
        return tuple(public_path)
    if len(path) >= 3 and path[:2] in _KEYED_COLLECTION_PATHS:
        path = (*path[:2], "*", *path[3:])
    if len(path) >= 4 and path[:2] == ("dag", "tasks"):
        field_index = 4 if path[3] == "partial_kwargs" and len(path) >= 5 else 3
        public_fields = (
            _DIFF_V1_PUBLIC_PARTIAL_TASK_FIELDS if field_index == 4 else _DIFF_V1_PUBLIC_TASK_FIELDS
        )
        if path[field_index] not in public_fields:
            return (*path[:field_index], _CUSTOM_TASK_FIELDS_PATH_COMPONENT)
    return path


def _format_path(path: tuple[str, ...]) -> str:
    return "/" + "/".join(component.replace("~", "~0").replace("/", "~1") for component in path)


class _JsonEncodingError(TypeError):
    """Distinguish unsupported JSON values from comparison implementation errors."""


def _serialize_canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except TypeError as error:
        raise _JsonEncodingError from error


def _is_json_equal(before: Any, after: Any) -> bool:
    # Compare canonical JSON, not Python ==, so a JSON type change (1 vs 1.0, True vs 1,
    # False vs 0) registers as a change instead of collapsing under Python equality.
    return _serialize_canonical_json(before) == _serialize_canonical_json(after)


def _get_digest(value: Any) -> str:
    # SHA-256 provides a content fingerprint, not password protection.
    # Stored payloads can carry escaped lone surrogates, which strict UTF-8 refuses to encode;
    # surrogatepass keeps them digestible without altering the bytes of any other payload.
    canonical_json = _serialize_canonical_json(value).encode("utf-8", errors="surrogatepass")
    return f"sha256:{hashlib.sha256(canonical_json).hexdigest()}"


def _get_category(path: tuple[str, ...]) -> DiffCategory:
    # Matched case-sensitively: every classified name is a schema field name, so a differently
    # cased key is a different key and belongs in "unknown" rather than borrowing a category.
    if path and path[0] == "provenance":
        return "provenance"
    if path[:2] == ("dag", "tasks"):
        field = path[3] if len(path) >= 4 else None
        if field == "partial_kwargs" and len(path) >= 5:
            field = path[4]
    elif path[:2] == _DEFAULT_ARGS_PATH and len(path) >= 3:
        # A default_args key is the operator field it will be applied to, so it classifies like one.
        field = path[2]
    elif path and path[0] == "dag":
        field = path[1] if len(path) >= 2 else ""
        if field == "task_group":
            task_group_path = _classify_task_group_path(path)
            if task_group_path is not None and task_group_path.remainder:
                if task_group_path.remainder[0] in _DIFF_V1_TASK_GROUP_METADATA_FIELDS:
                    return "metadata"
        return _DIFF_V1_DAG_FIELD_CATEGORIES.get(
            field, _DIFF_V1_LEGACY_DAG_FIELD_CATEGORIES.get(field, "unknown")
        )
    else:
        return "unknown"

    # Unclassified task fields fall back to "task", over-reporting impact rather than under it.
    if field is not None and "callback" in field:
        return "callback"
    if field in _DIFF_V1_TASK_ASSET_FIELDS:
        return "asset"
    if field in _DIFF_V1_TASK_PARAM_FIELDS:
        return "param"
    if field in _DIFF_V1_TASK_DEPENDENCY_FIELDS:
        return "dependency"
    if field in _DIFF_V1_TASK_METADATA_FIELDS:
        return "metadata"
    return "task"


def _get_impact(category: DiffCategory) -> DiffImpact:
    if category in {"provenance", "metadata", "authorization"}:
        return category
    if category in {"task", "dependency", "schedule", "param", "asset", "deadline", "callback"}:
        return "execution"
    return "unknown"
