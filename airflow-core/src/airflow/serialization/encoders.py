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

import contextlib
import datetime
import functools
from typing import TYPE_CHECKING, Any, TypeVar, overload

import attrs
import pendulum

from airflow._shared.module_loading import qualname
from airflow.sdk import (
    Asset,
    AssetAlias,
    AssetAll,
    AssetAny,
    AssetOrTimeSchedule,
    CronDataIntervalTimetable,
    CronTriggerTimetable,
    DeltaDataIntervalTimetable,
    DeltaTriggerTimetable,
    EventsTimetable,
    MultipleCronTriggerTimetable,
)
from airflow.sdk.bases.timetable import BaseTimetable
from airflow.sdk.definitions.asset import AssetRef
from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable
from airflow.sdk.definitions.timetables.simple import ContinuousTimetable, NullTimetable, OnceTimetable
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import find_registered_custom_timetable, is_core_timetable_import_path
from airflow.timetables.base import Timetable as CoreTimetable
from airflow.utils.docs import get_docs_url

if TYPE_CHECKING:
    from dateutil.relativedelta import relativedelta

    from airflow.sdk.definitions.asset import BaseAsset
    from airflow.triggers.base import BaseEventTrigger

    T = TypeVar("T")


def encode_relativedelta(var: relativedelta) -> dict[str, Any]:
    """Encode a relativedelta object."""
    encoded = {k: v for k, v in var.__dict__.items() if not k.startswith("_") and v}
    if var.weekday and var.weekday.n:
        # Every n'th Friday for example
        encoded["weekday"] = [var.weekday.weekday, var.weekday.n]
    elif var.weekday:
        encoded["weekday"] = [var.weekday.weekday]
    return encoded


def encode_timezone(var: str | pendulum.Timezone | pendulum.FixedTimezone) -> str | int:
    """
    Encode a Pendulum Timezone for serialization.

    Airflow only supports timezone objects that implements Pendulum's Timezone
    interface. We try to keep as much information as possible to make conversion
    round-tripping possible (see ``decode_timezone``). We need to special-case
    UTC; Pendulum implements it as a FixedTimezone (i.e. it gets encoded as
    0 without the special case), but passing 0 into ``pendulum.timezone`` does
    not give us UTC (but ``+00:00``).
    """
    if isinstance(var, str):
        return var
    if isinstance(var, pendulum.FixedTimezone):
        if var.offset == 0:
            return "UTC"
        return var.offset
    if isinstance(var, pendulum.Timezone):
        return var.name
    raise ValueError(
        f"DAG timezone should be a pendulum.tz.Timezone, not {var!r}. "
        f"See {get_docs_url('timezone.html#time-zone-aware-dags')}"
    )


def encode_interval(interval: datetime.timedelta | relativedelta) -> float | dict:
    if isinstance(interval, datetime.timedelta):
        return interval.total_seconds()
    return encode_relativedelta(interval)


def encode_run_immediately(value: bool | datetime.timedelta) -> bool | float:
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    return value


def encode_trigger(trigger: BaseEventTrigger | dict):
    from airflow.serialization.serialized_objects import BaseSerialization

    def _ensure_serialized(d):
        """
        Make sure the kwargs dict is JSON-serializable.

        This is done with BaseSerialization logic. A simple check is added to
        ensure we don't double-serialize, which is possible when a trigger goes
        through multiple serialization layers.
        """
        if isinstance(d, dict) and Encoding.TYPE in d:
            return d
        return BaseSerialization.serialize(d)

    if isinstance(trigger, dict):
        classpath = trigger["classpath"]
        kwargs = trigger["kwargs"]
    else:
        classpath, kwargs = trigger.serialize()
    return {
        "classpath": classpath,
        "kwargs": {k: _ensure_serialized(v) for k, v in kwargs.items()},
    }


def encode_asset_condition(a: BaseAsset) -> dict[str, Any]:
    """
    Encode an asset condition.

    :meta private:
    """
    d: dict[str, Any]
    match a:
        case Asset():
            d = {"__type": DAT.ASSET, "name": a.name, "uri": a.uri, "group": a.group, "extra": a.extra}
            if a.watchers:
                d["watchers"] = [{"name": w.name, "trigger": encode_trigger(w.trigger)} for w in a.watchers]
            return d
        case AssetAlias():
            return {"__type": DAT.ASSET_ALIAS, "name": a.name, "group": a.group}
        case AssetAll():
            return {"__type": DAT.ASSET_ALL, "objects": [encode_asset_condition(x) for x in a.objects]}
        case AssetAny():
            return {"__type": DAT.ASSET_ANY, "objects": [encode_asset_condition(x) for x in a.objects]}
        case AssetRef():
            return {"__type": DAT.ASSET_REF, **attrs.asdict(a)}
    raise ValueError(f"serialization not implemented for {type(a).__name__!r}")


def _get_serialized_timetable_import_path(var: BaseTimetable | CoreTimetable) -> str:
    # Find SDK classes.
    with contextlib.suppress(KeyError):
        return _serializer.BUILTIN_TIMETABLES[var_type := type(var)]

    # Check Core classes.
    if is_core_timetable_import_path(importable_string := qualname(var_type)):
        return importable_string

    # Find user-registered classes.
    find_registered_custom_timetable(importable_string)  # This raises if not found.
    return importable_string


def encode_timetable(var: BaseTimetable | CoreTimetable) -> dict[str, Any]:
    """
    Encode a timetable instance.

    See ``_TimetableSerializer.serialize()`` for more implementation detail.

    :meta private:
    """
    importable_string = _get_serialized_timetable_import_path(var)
    return {Encoding.TYPE: importable_string, Encoding.VAR: _serializer.serialize(var)}


class _TimetableSerializer:
    """Timetable serialization logic."""

    BUILTIN_TIMETABLES: dict[type, str] = {
        AssetOrTimeSchedule: "airflow.timetables.assets.AssetOrTimeSchedule",
        AssetTriggeredTimetable: "airflow.timetables.simple.AssetTriggeredTimetable",
        ContinuousTimetable: "airflow.timetables.simple.ContinuousTimetable",
        CronDataIntervalTimetable: "airflow.timetables.interval.CronDataIntervalTimetable",
        CronTriggerTimetable: "airflow.timetables.trigger.CronTriggerTimetable",
        DeltaDataIntervalTimetable: "airflow.timetables.interval.DeltaDataIntervalTimetable",
        DeltaTriggerTimetable: "airflow.timetables.trigger.DeltaTriggerTimetable",
        EventsTimetable: "airflow.timetables.events.EventsTimetable",
        MultipleCronTriggerTimetable: "airflow.timetables.trigger.MultipleCronTriggerTimetable",
        NullTimetable: "airflow.timetables.simple.NullTimetable",
        OnceTimetable: "airflow.timetables.simple.OnceTimetable",
    }

    @functools.singledispatchmethod
    def serialize(self, timetable: BaseTimetable | CoreTimetable) -> dict[str, Any]:
        """
        Serialize a timetable into a JSON-compatible dict for storage.

        All timetables defined in the SDK should be handled by registered
        single-dispatch variants below.

        This function's body should only be
        called on timetables defined in Core (under ``airflow.timetables``),
        and user-defined custom timetables registered via plugins, which also
        inherit from the Core timetable base class.

        For timetables in Core, serialization work is delegated to the type.
        """
        if not isinstance(timetable, CoreTimetable):
            raise NotImplementedError(f"can not serialize timetable {type(timetable).__name__}")
        return timetable.serialize()

    @serialize.register(ContinuousTimetable)
    @serialize.register(NullTimetable)
    @serialize.register(OnceTimetable)
    def _(self, timetable: ContinuousTimetable | NullTimetable | OnceTimetable) -> dict[str, Any]:
        return {}

    @serialize.register
    def _(self, timetable: AssetTriggeredTimetable) -> dict[str, Any]:
        return {"asset_condition": encode_asset_condition(timetable.asset_condition)}

    @serialize.register
    def _(self, timetable: EventsTimetable) -> dict[str, Any]:
        return {
            "event_dates": [x.isoformat(sep="T") for x in timetable.event_dates],
            "restrict_to_events": timetable.restrict_to_events,
            "description": timetable.description,
        }

    @serialize.register
    def _(self, timetable: CronDataIntervalTimetable) -> dict[str, Any]:
        return {"expression": timetable.expression, "timezone": encode_timezone(timetable.timezone)}

    @serialize.register
    def _(self, timetable: DeltaDataIntervalTimetable) -> dict[str, Any]:
        return {"delta": encode_interval(timetable.delta)}

    @serialize.register
    def _(self, timetable: CronTriggerTimetable) -> dict[str, Any]:
        return {
            "expression": timetable.expression,
            "timezone": encode_timezone(timetable.timezone),
            "interval": encode_interval(timetable.interval),
            "run_immediately": encode_run_immediately(timetable.run_immediately),
        }

    @serialize.register
    def _(self, timetable: DeltaTriggerTimetable) -> dict[str, Any]:
        return {
            "delta": encode_interval(timetable.delta),
            "interval": encode_interval(timetable.interval),
        }

    @serialize.register
    def _(self, timetable: MultipleCronTriggerTimetable) -> dict[str, Any]:
        # All timetables share the same timezone, interval, and run_immediately
        # values, so we can just use the first to represent them.
        representitive = timetable.timetables[0]
        return {
            "expressions": [t.expression for t in timetable.timetables],
            "timezone": encode_timezone(representitive.timezone),
            "interval": encode_interval(representitive.interval),
            "run_immediately": encode_run_immediately(representitive.run_immediately),
        }

    @serialize.register
    def _(self, timetable: AssetOrTimeSchedule) -> dict[str, Any]:
        return {
            "asset_condition": encode_asset_condition(timetable.asset_condition),
            "timetable": encode_timetable(timetable.timetable),
        }

    @serialize.register
    def _(self, timetable: CoreTimetable) -> dict[str, Any]:
        return timetable.serialize()


_serializer = _TimetableSerializer()


@overload
def coerce_to_core_timetable(obj: BaseTimetable | CoreTimetable) -> CoreTimetable: ...


@overload
def coerce_to_core_timetable(obj: T) -> T: ...


def coerce_to_core_timetable(obj: object) -> object:
    """
    Convert *obj* from an SDK timetable to a Core tiemtable instance if possible.

    :meta private:
    """
    if isinstance(obj, CoreTimetable) or not isinstance(obj, BaseTimetable):
        return obj

    from airflow.serialization.decoders import decode_timetable

    return decode_timetable(encode_timetable(obj))
