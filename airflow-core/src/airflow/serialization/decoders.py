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
from typing import TYPE_CHECKING, Any, TypeVar, cast

import dateutil.relativedelta

from airflow._shared.module_loading import import_string
from airflow.models.deadline import ReferenceModels
from airflow.sdk import (  # TODO: Implement serialized assets.
    Asset,
    AssetAlias,
    AssetAll,
    AssetAny,
)
from airflow.sdk.definitions.deadline import DeadlineAlert  # TODO: Implement serialized types.
from airflow.serialization.definitions.assets import SerializedAssetWatcher
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import find_registered_custom_timetable, is_core_timetable_import_path

if TYPE_CHECKING:
    from airflow.sdk.definitions.asset import BaseAsset
    from airflow.sdk.definitions.callback import Callback
    from airflow.timetables.base import Timetable as CoreTimetable

R = TypeVar("R")


def decode_deadline_alert(var: dict[str, Any]) -> DeadlineAlert:
    from airflow.serialization.serde import deserialize  # TODO: Do not use this!!

    data = var.get(Encoding.VAR, var)  # Compat.

    reference_data = data["reference"]
    reference_type = reference_data[ReferenceModels.REFERENCE_TYPE_FIELD]

    reference_class = ReferenceModels.get_reference_class(reference_type)
    reference = reference_class.deserialize_reference(reference_data)

    return DeadlineAlert(
        reference=reference,
        interval=decode_interval(data["interval"]),
        callback=cast("Callback", deserialize(data["callback"])),
    )


def decode_relativedelta(var: dict[str, Any]) -> dateutil.relativedelta.relativedelta:
    """Dencode a relativedelta object."""
    if "weekday" in var:
        var["weekday"] = dateutil.relativedelta.weekday(*var["weekday"])
    return dateutil.relativedelta.relativedelta(**var)


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

    if not isinstance(d, dict) or Encoding.TYPE not in d:
        return d
    return BaseSerialization.deserialize(d)


def decode_asset(var: dict[str, Any]):
    watchers = var.get("watchers", [])
    return Asset(
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
                },
            )
            for watcher in watchers
        ],
    )


def decode_asset_condition(var: dict[str, Any]) -> BaseAsset:
    """
    Decode a previously serialized asset condition.

    :meta private:
    """
    match var["__type"]:
        case DAT.ASSET:
            return decode_asset(var)
        case DAT.ASSET_ALL:
            return AssetAll(*(decode_asset_condition(x) for x in var["objects"]))
        case DAT.ASSET_ANY:
            return AssetAny(*(decode_asset_condition(x) for x in var["objects"]))
        case DAT.ASSET_ALIAS:
            return AssetAlias(name=var["name"], group=var["group"])
        case DAT.ASSET_REF:
            return Asset.ref(**{k: v for k, v in var.items() if k != "__type"})
        case data_type:
            raise ValueError(f"deserialization not implemented for DAT {data_type!r}")


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
