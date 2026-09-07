#
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

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, TypeVar

import dateutil.relativedelta

from airflow._shared.module_loading import import_string
from airflow.serialization.definitions.assets import (
    SerializedAsset,
    SerializedAssetAlias,
    SerializedAssetAll,
    SerializedAssetAny,
    SerializedAssetBase,
    SerializedAssetNameRef,
    SerializedAssetUriRef,
    SerializedAssetWatcher,
)
from airflow.serialization.definitions.deadline import (
    DeadlineAlertFields,
    SerializedDeadlineAlert,
    SerializedReferenceModels,
    SerializedVariableInterval,
)
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import (
    WaitPolicyNotSupported,
    find_registered_custom_partition_mapper,
    find_registered_custom_timetable,
    find_registered_custom_window,
    is_core_partition_mapper_import_path,
    is_core_timetable_import_path,
    is_core_wait_policy_import_path,
    is_core_window_import_path,
)

if TYPE_CHECKING:
    from airflow.partition_mappers.base import PartitionMapper
    from airflow.partition_mappers.wait_policy import WaitPolicy
    from airflow.partition_mappers.window import Window
    from airflow.timetables.base import Timetable as CoreTimetable

R = TypeVar("R")


def decode_relativedelta(var: dict[str, Any]) -> dateutil.relativedelta.relativedelta:
    """Dencode a relativedelta object."""
    copy_var = var.copy()
    if "weekday" in copy_var:
        copy_var["weekday"] = dateutil.relativedelta.weekday(*copy_var["weekday"])
    return dateutil.relativedelta.relativedelta(**copy_var)


def decode_interval(value: int | dict) -> datetime.timedelta | dateutil.relativedelta.relativedelta:
    if isinstance(value, dict):
        return decode_relativedelta(value)
    return datetime.timedelta(seconds=value)


def decode_run_immediately(value: bool | float) -> bool | datetime.timedelta:
    if isinstance(value, float):
        return datetime.timedelta(seconds=value)
    return value


def smart_decode_trigger_kwargs(d):
    """
    Slightly clean up kwargs for display or execution.

    This detects one level of BaseSerialization and tries to deserialize the
    content, removing some __type __var ugliness when the value is displayed
    in UI to the user and/or while execution.
    """
    from airflow.serialization.serialized_objects import BaseSerialization

    d = _normalize_stringified_encoding_keys(d)
    if not isinstance(d, dict):
        return d
    if Encoding.TYPE in d and Encoding.VAR in d:
        return BaseSerialization.deserialize(d)
    return {k: smart_decode_trigger_kwargs(v) for k, v in d.items()}


def _normalize_stringified_encoding_keys(value):
    if isinstance(value, list):
        return [_normalize_stringified_encoding_keys(v) for v in value]
    if not isinstance(value, dict):
        return value

    if str(Encoding.TYPE) in value and str(Encoding.VAR) in value:
        return {
            Encoding.TYPE if k == str(Encoding.TYPE) else Encoding.VAR if k == str(Encoding.VAR) else k: (
                _normalize_stringified_encoding_keys(v)
            )
            for k, v in value.items()
        }
    return {k: _normalize_stringified_encoding_keys(v) for k, v in value.items()}


def _decode_asset(var: dict[str, Any]):
    watchers = var.get("watchers", [])
    return SerializedAsset(
        name=var["name"],
        uri=var["uri"],
        group=var["group"],
        extra=var["extra"],
        watchers=[
            SerializedAssetWatcher(
                name=watcher["name"],
                trigger={
                    "classpath": watcher["trigger"]["classpath"],
                    "kwargs": smart_decode_trigger_kwargs(watcher["trigger"]["kwargs"]),
                    **({"queue": watcher["trigger"]["queue"]} if "queue" in watcher["trigger"] else {}),
                },
            )
            for watcher in watchers
        ],
        access_control=var.get("access_control", {}),
    )


def decode_asset_like(var: dict[str, Any]) -> SerializedAssetBase:
    """
    Decode a previously serialized asset-like object.

    :meta private:
    """
    typ = var[Encoding.TYPE]
    if Encoding.VAR in var:
        var = var[Encoding.VAR]
    else:
        var = {k: v for k, v in var.items() if k != Encoding.TYPE}
    match typ:
        case DAT.ASSET:
            return _decode_asset(var)
        case DAT.ASSET_ALL:
            return SerializedAssetAll([decode_asset_like(x) for x in var["objects"]])
        case DAT.ASSET_ANY:
            return SerializedAssetAny([decode_asset_like(x) for x in var["objects"]])
        case DAT.ASSET_ALIAS:
            return SerializedAssetAlias(name=var["name"], group=var["group"])
        case DAT.ASSET_REF:
            if "name" in var:
                return SerializedAssetNameRef(**var)
            return SerializedAssetUriRef(**var)
        case data_type:
            raise ValueError(f"deserialization not implemented for DAT {data_type!r}")


def decode_deadline_reference(reference_data: dict):
    """Decode a previously serialized deadline reference."""
    ref_name = reference_data.get(SerializedReferenceModels.REFERENCE_TYPE_FIELD)

    # A custom reference may share a name with a builtin, so ``__class_path`` wins.
    if "__class_path" in reference_data:
        reference_class: type[SerializedReferenceModels.SerializedBaseDeadlineReference] = (
            SerializedReferenceModels.SerializedCustomReference
        )
    elif ref_name and SerializedReferenceModels.is_builtin_reference(ref_name):
        reference_class = SerializedReferenceModels.get_reference_class(ref_name)
    else:
        reference_class = SerializedReferenceModels.SerializedCustomReference

    return reference_class.deserialize_reference(reference_data)


_TIMEDELTA_CLASSNAME = "datetime.timedelta"
_VARIABLE_INTERVAL_CLASSNAMES = frozenset(
    {
        "airflow.sdk.definitions.deadline.VariableInterval",
        "airflow.serialization.definitions.deadline.SerializedVariableInterval",
    }
)


def _normalised_payload(encoded: Any, field: str) -> tuple[str, Any]:
    """
    Return ``(classname, data)`` for a serde payload, in either encoding.

    ``serde`` accepts a legacy ``{"__type": ..., "__var": ...}`` shape and rewrites it
    into the current one *inside* ``deserialize``. Anything inspecting the payload before
    that call therefore has to normalise it first, or the legacy spelling carries no
    ``__classname__`` at the moment it is looked at and slips past unexamined.
    """
    from airflow.sdk.serde import _convert, CLASSNAME, DATA

    if not isinstance(encoded, dict):
        raise ValueError(f"Deadline {field} is not a serialized object.")
    converted = _convert(encoded)
    if not isinstance(converted, dict) or CLASSNAME not in converted:
        raise ValueError(f"Deadline {field} names no class.")
    return converted[CLASSNAME], converted.get(DATA)


def _decode_deadline_interval(raw_interval: Any) -> datetime.timedelta | SerializedVariableInterval:
    """
    Build the interval from its encoded form without importing what the payload names.

    Only three shapes are legitimate, and each is reconstructed from primitives directly.
    Nothing here reaches ``serde.deserialize``, so no class named by a Dag author is
    imported or instantiated in the scheduler or the API server.
    """
    # Backward compatibility: previously stored as total_seconds().
    if isinstance(raw_interval, (int, float)) and not isinstance(raw_interval, bool):
        return datetime.timedelta(seconds=raw_interval)

    classname, data = _normalised_payload(raw_interval, "interval")

    if classname == _TIMEDELTA_CLASSNAME:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return datetime.timedelta(seconds=data)
        raise ValueError("Deadline interval timedelta payload is not a number.")

    if classname in _VARIABLE_INTERVAL_CLASSNAMES:
        key = data.get("key") if isinstance(data, dict) else None
        if not isinstance(key, str):
            raise ValueError("Deadline interval variable payload has no string key.")
        return SerializedVariableInterval(key=key)

    raise ValueError(
        f"Refusing to deserialize {classname!r} as a deadline interval. "
        f"Permitted: {_TIMEDELTA_CLASSNAME}, {', '.join(sorted(_VARIABLE_INTERVAL_CLASSNAMES))}."
    )


def _decode_deadline_callback(raw_callback: Any):
    """
    Build the callback from its encoded form without importing what the payload names.

    ``kwargs`` is still passed through generic deserialization, and that is a deliberate,
    documented limit rather than an oversight. Leaving it encoded would close a real
    residual -- a legitimate callback can carry an arbitrary allow-listed class under its
    kwargs, which serde constructs while the outer payload looks entirely valid -- but the
    kwargs are consumed through two different paths that use two different encodings
    (``BaseSerialization`` in the triggerer, serde here), and deferring the decode without
    getting both exactly right silently hands user code an encoded dict in place of its
    argument. That residual is not specific to deadlines: it is the general property of
    deserializing Dag-author data, shared with every other serde call site. Closing it
    belongs with that broader work, not smuggled in here.
    """
    from airflow.sdk.definitions.callback import (
        AsyncCallback,
        SyncCallback,
        _SerializedCallbackPath,
    )

    permitted = {f"{cls.__module__}.{cls.__qualname__}": cls for cls in (AsyncCallback, SyncCallback)}
    classname, data = _normalised_payload(raw_callback, "callback")
    callback_cls = permitted.get(classname)
    if callback_cls is None:
        raise ValueError(
            f"Refusing to deserialize {classname!r} as a deadline callback. "
            f"Permitted: {', '.join(sorted(permitted))}."
        )
    if not isinstance(data, dict):
        raise ValueError("Deadline callback payload is not a mapping.")

    path = data.get("path")
    if not isinstance(path, str):
        raise ValueError("Deadline callback payload has no string path.")

    from airflow.sdk.serde import deserialize

    raw_kwargs = data.get("kwargs") or {}
    fields: dict[str, Any] = {"kwargs": deserialize(raw_kwargs) if raw_kwargs else {}}
    for optional in ("queue", "executor"):
        if optional in data:
            value = data[optional]
            if value is not None and not isinstance(value, str):
                raise ValueError(f"Deadline callback {optional} is not a string.")
            fields[optional] = value

    unexpected = set(data) - {"path", "kwargs", "queue", "executor"}
    if unexpected:
        raise ValueError(f"Unexpected deadline callback fields: {', '.join(sorted(unexpected))}.")

    return callback_cls(callback_callable=_SerializedCallbackPath(path), **fields)


def decode_deadline_alert(encoded_data: dict):
    """
    Decode a previously serialized deadline alert.

    :meta private:
    """
    data = encoded_data.get(Encoding.VAR, encoded_data)

    reference_data = data[DeadlineAlertFields.REFERENCE]
    reference = decode_deadline_reference(reference_data)

    raw_interval = data[DeadlineAlertFields.INTERVAL]

    if raw_interval is None:
        raise ValueError(
            "DeadlineAlert interval is missing. This can happen after downgrading "
            "from a version that supports VariableInterval. Downgrade is not fully reversible."
        )

    interval = _decode_deadline_interval(raw_interval)

    return SerializedDeadlineAlert(
        reference=reference,
        interval=interval,
        callback=_decode_deadline_callback(data[DeadlineAlertFields.CALLBACK]),
        name=data.get(DeadlineAlertFields.NAME),
    )


def decode_timetable(var: dict[str, Any]) -> CoreTimetable:
    """
    Decode a previously serialized timetable.

    Most of the deserialization logic is delegated to the actual type, which
    we import from string.

    :meta private:
    """
    if is_core_timetable_import_path(importable_string := var[Encoding.TYPE]):
        timetable_type: type[CoreTimetable] = import_string(importable_string)
    else:
        timetable_type = find_registered_custom_timetable(importable_string)
    return timetable_type.deserialize(var[Encoding.VAR])


def decode_partition_mapper(var: dict[str, Any]) -> PartitionMapper:
    """
    Decode a previously serialized PartitionMapper.

    Most of the deserialization logic is delegated to the actual type, which
    we import from string.

    :meta private:
    """
    importable_string = var[Encoding.TYPE]
    if is_core_partition_mapper_import_path(importable_string):
        partition_mapper_cls = import_string(importable_string)
    else:
        partition_mapper_cls = find_registered_custom_partition_mapper(importable_string)
    return partition_mapper_cls.deserialize(var[Encoding.VAR])


def decode_window(var: dict[str, Any]) -> Window:
    """
    Decode a previously serialized :class:`Window`.

    Custom windows must be registered via the ``windows`` plugin attribute;
    unregistered import paths are rejected up-front instead of being handed to
    ``import_string``. See :func:`encode_window` for the matching encode-side
    restriction.

    :meta private:
    """
    importable_string = var[Encoding.TYPE]
    if is_core_window_import_path(importable_string):
        window_cls: type[Window] = import_string(importable_string)
    else:
        window_cls = find_registered_custom_window(importable_string)
    return window_cls.deserialize(var[Encoding.VAR])


def decode_wait_policy(var: dict[str, Any]) -> WaitPolicy:
    """
    Decode a previously serialized :class:`WaitPolicy`.

    Only built-in trigger policies are accepted — a tampered serialized Dag
    naming a non-core import path is rejected up-front instead of being handed
    to ``import_string``. See :func:`encode_wait_policy` for the matching
    encode-side restriction.

    :meta private:
    """
    importable_string = var[Encoding.TYPE]
    if not is_core_wait_policy_import_path(importable_string):
        raise WaitPolicyNotSupported(importable_string)
    policy_cls: type[WaitPolicy] = import_string(importable_string)
    return policy_cls.deserialize(var[Encoding.VAR])
