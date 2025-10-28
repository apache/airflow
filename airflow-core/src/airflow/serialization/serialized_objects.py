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

# TODO: update test_recursive_serialize_calls_must_forward_kwargs and re-enable RET505
# ruff: noqa: RET505
from __future__ import annotations

import collections.abc
import copy
import datetime
import enum
import itertools
import logging
import math
import re
import sys
import weakref
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from functools import cached_property, lru_cache
from inspect import signature
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
    NamedTuple,
    TypeAlias,
    TypeGuard,
    TypeVar,
    cast,
    overload,
)

import attrs
import lazy_object_proxy
import methodtools
import pydantic
from dateutil import relativedelta
from pendulum.tz.timezone import FixedTimezone, Timezone
from sqlalchemy import func, or_, select, tuple_

from airflow import macros
from airflow._shared.timezones.timezone import coerce_datetime, from_timestamp, parse_timezone, utcnow
from airflow.callbacks.callback_requests import DagCallbackRequest, TaskCallbackRequest
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import (
    AirflowException,
    DeserializationError,
    SerializationError,
    TaskDeferred,
    TaskNotFound,
)
from airflow.models.connection import Connection
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import RUN_ID_REGEX, DagRun
from airflow.models.deadline import Deadline
from airflow.models.expandinput import create_expand_input
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models.tasklog import LogTemplate
from airflow.models.xcom import XComModel
from airflow.models.xcom_arg import SchedulerXComArg, deserialize_xcom_arg
from airflow.sdk import DAG, Asset, AssetAlias, AssetAll, AssetAny, AssetWatcher, BaseOperator, XComArg
from airflow.sdk.bases.operator import OPERATOR_DEFAULTS  # TODO: Copy this into the scheduler?
from airflow.sdk.definitions._internal.node import DAGNode
from airflow.sdk.definitions.asset import (
    AssetAliasEvent,
    AssetAliasUniqueKey,
    AssetRef,
    AssetUniqueKey,
    BaseAsset,
)
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.operator_resources import Resources
from airflow.sdk.definitions.param import Param, ParamsDict
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.sdk.definitions.xcom_arg import serialize_xcom_arg
from airflow.sdk.execution_time.context import OutletEventAccessor, OutletEventAccessors
from airflow.serialization.dag_dependency import DagDependency
from airflow.serialization.definitions.taskgroup import SerializedMappedTaskGroup, SerializedTaskGroup
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import serialize_template_field
from airflow.serialization.json_schema import load_dag_schema
from airflow.settings import DAGS_FOLDER, json
from airflow.task.priority_strategy import (
    PriorityWeightStrategy,
    airflow_priority_weight_strategies,
    airflow_priority_weight_strategies_classes,
    validate_and_load_priority_weight_strategy,
)
from airflow.ti_deps.deps.mapped_task_upstream_dep import MappedTaskUpstreamDep
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.triggers.base import BaseTrigger, StartTriggerArgs
from airflow.utils.code_utils import get_python_source
from airflow.utils.context import ConnectionAccessor, Context, VariableAccessor
from airflow.utils.db import LazySelectSequence
from airflow.utils.docs import get_docs_url
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string, qualname
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import NOTSET, ArgNotSet, DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from inspect import Parameter

    from pydantic import NonNegativeInt
    from sqlalchemy.orm import Session

    from airflow.models.expandinput import SchedulerExpandInput
    from airflow.models.mappedoperator import MappedOperator as SerializedMappedOperator
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import BaseOperatorLink
    from airflow.sdk.definitions.edges import EdgeInfoType
    from airflow.serialization.json_schema import Validator
    from airflow.task.trigger_rule import TriggerRule
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
    from airflow.triggers.base import BaseEventTrigger

    HAS_KUBERNETES: bool
    try:
        from kubernetes.client import models as k8s  # noqa: TC004

        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator  # noqa: TC004
    except ImportError:
        pass

    SerializedOperator: TypeAlias = "SerializedMappedOperator | SerializedBaseOperator"
    SdkOperator: TypeAlias = BaseOperator | MappedOperator

DEFAULT_OPERATOR_DEPS: frozenset[BaseTIDep] = frozenset(
    (
        NotInRetryPeriodDep(),
        PrevDagrunDep(),
        TriggerRuleDep(),
        NotPreviouslySkippedDep(),
        MappedTaskUpstreamDep(),
    )
)

log = logging.getLogger(__name__)


def encode_relativedelta(var: relativedelta.relativedelta) -> dict[str, Any]:
    """Encode a relativedelta object."""
    encoded = {k: v for k, v in var.__dict__.items() if not k.startswith("_") and v}
    if var.weekday and var.weekday.n:
        # Every n'th Friday for example
        encoded["weekday"] = [var.weekday.weekday, var.weekday.n]
    elif var.weekday:
        encoded["weekday"] = [var.weekday.weekday]
    return encoded


def decode_relativedelta(var: dict[str, Any]) -> relativedelta.relativedelta:
    """Dencode a relativedelta object."""
    if "weekday" in var:
        var["weekday"] = relativedelta.weekday(*var["weekday"])
    return relativedelta.relativedelta(**var)


def encode_timezone(var: Timezone | FixedTimezone) -> str | int:
    """
    Encode a Pendulum Timezone for serialization.

    Airflow only supports timezone objects that implements Pendulum's Timezone
    interface. We try to keep as much information as possible to make conversion
    round-tripping possible (see ``decode_timezone``). We need to special-case
    UTC; Pendulum implements it as a FixedTimezone (i.e. it gets encoded as
    0 without the special case), but passing 0 into ``pendulum.timezone`` does
    not give us UTC (but ``+00:00``).
    """
    if isinstance(var, FixedTimezone):
        if var.offset == 0:
            return "UTC"
        return var.offset
    if isinstance(var, Timezone):
        return var.name
    raise ValueError(
        f"DAG timezone should be a pendulum.tz.Timezone, not {var!r}. "
        f"See {get_docs_url('timezone.html#time-zone-aware-dags')}"
    )


def decode_timezone(var: str | int) -> Timezone | FixedTimezone:
    """Decode a previously serialized Pendulum Timezone."""
    return parse_timezone(var)


def _get_registered_timetable(importable_string: str) -> type[Timetable] | None:
    from airflow import plugins_manager

    if importable_string.startswith("airflow.timetables."):
        return import_string(importable_string)
    plugins_manager.initialize_timetables_plugins()
    if plugins_manager.timetable_classes:
        return plugins_manager.timetable_classes.get(importable_string)
    else:
        return None


def _get_registered_priority_weight_strategy(
    importable_string: str,
) -> type[PriorityWeightStrategy] | None:
    from airflow import plugins_manager

    if importable_string in airflow_priority_weight_strategies:
        return airflow_priority_weight_strategies[importable_string]
    plugins_manager.initialize_priority_weight_strategy_plugins()
    if plugins_manager.priority_weight_strategy_classes:
        return plugins_manager.priority_weight_strategy_classes.get(importable_string)
    else:
        return None


class _TimetableNotRegistered(ValueError):
    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"Timetable class {self.type_string!r} is not registered or "
            "you have a top level database access that disrupted the session. "
            "Please check the airflow best practices documentation."
        )


class _PriorityWeightStrategyNotRegistered(AirflowException):
    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"Priority weight strategy class {self.type_string!r} is not registered or "
            "you have a top level database access that disrupted the session. "
            "Please check the airflow best practices documentation."
        )


def _encode_trigger(trigger: BaseEventTrigger | dict):
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


def encode_asset_condition(var: BaseAsset) -> dict[str, Any]:
    """
    Encode an asset condition.

    :meta private:
    """
    if isinstance(var, Asset):

        def _encode_watcher(watcher: AssetWatcher):
            return {
                "name": watcher.name,
                "trigger": _encode_trigger(watcher.trigger),
            }

        asset = {
            "__type": DAT.ASSET,
            "name": var.name,
            "uri": var.uri,
            "group": var.group,
            "extra": var.extra,
        }

        if len(var.watchers) > 0:
            asset["watchers"] = [_encode_watcher(watcher) for watcher in var.watchers]

        return asset
    if isinstance(var, AssetAlias):
        return {"__type": DAT.ASSET_ALIAS, "name": var.name, "group": var.group}
    if isinstance(var, AssetAll):
        return {
            "__type": DAT.ASSET_ALL,
            "objects": [encode_asset_condition(x) for x in var.objects],
        }
    if isinstance(var, AssetAny):
        return {
            "__type": DAT.ASSET_ANY,
            "objects": [encode_asset_condition(x) for x in var.objects],
        }
    if isinstance(var, AssetRef):
        return {"__type": DAT.ASSET_REF, **attrs.asdict(var)}
    raise ValueError(f"serialization not implemented for {type(var).__name__!r}")


def decode_asset_condition(var: dict[str, Any]) -> BaseAsset:
    """
    Decode a previously serialized asset condition.

    :meta private:
    """
    dat = var["__type"]
    if dat == DAT.ASSET:
        return decode_asset(var)
    if dat == DAT.ASSET_ALL:
        return AssetAll(*(decode_asset_condition(x) for x in var["objects"]))
    if dat == DAT.ASSET_ANY:
        return AssetAny(*(decode_asset_condition(x) for x in var["objects"]))
    if dat == DAT.ASSET_ALIAS:
        return AssetAlias(name=var["name"], group=var["group"])
    if dat == DAT.ASSET_REF:
        return Asset.ref(**{k: v for k, v in var.items() if k != "__type"})
    raise ValueError(f"deserialization not implemented for DAT {dat!r}")


def smart_decode_trigger_kwargs(d):
    """
    Slightly clean up kwargs for display or execution.

    This detects one level of BaseSerialization and tries to deserialize the
    content, removing some __type __var ugliness when the value is displayed
    in UI to the user and/or while execution.
    """
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


def encode_outlet_event_accessor(var: OutletEventAccessor) -> dict[str, Any]:
    key = var.key
    return {
        "key": BaseSerialization.serialize(key),
        "extra": var.extra,
        "asset_alias_events": [attrs.asdict(cast("attrs.AttrsInstance", e)) for e in var.asset_alias_events],
    }


def decode_outlet_event_accessor(var: dict[str, Any]) -> OutletEventAccessor:
    asset_alias_events = var.get("asset_alias_events", [])
    outlet_event_accessor = OutletEventAccessor(
        key=BaseSerialization.deserialize(var["key"]),
        extra=var["extra"],
        asset_alias_events=[
            AssetAliasEvent(
                source_alias_name=e["source_alias_name"],
                dest_asset_key=AssetUniqueKey(
                    name=e["dest_asset_key"]["name"], uri=e["dest_asset_key"]["uri"]
                ),
                extra=e["extra"],
            )
            for e in asset_alias_events
        ],
    )
    return outlet_event_accessor


def encode_outlet_event_accessors(var: OutletEventAccessors) -> dict[str, Any]:
    return {
        "__type": DAT.ASSET_EVENT_ACCESSORS,
        "_dict": [
            {"key": BaseSerialization.serialize(k), "value": encode_outlet_event_accessor(v)}
            for k, v in var._dict.items()
        ],
    }


def decode_outlet_event_accessors(var: dict[str, Any]) -> OutletEventAccessors:
    d = OutletEventAccessors()
    d._dict = {
        BaseSerialization.deserialize(row["key"]): decode_outlet_event_accessor(row["value"])
        for row in var["_dict"]
    }
    return d


def encode_timetable(var: Timetable) -> dict[str, Any]:
    """
    Encode a timetable instance.

    This delegates most of the serialization work to the type, so the behavior
    can be completely controlled by a custom subclass.

    :meta private:
    """
    timetable_class = type(var)
    importable_string = qualname(timetable_class)
    if _get_registered_timetable(importable_string) is None:
        raise _TimetableNotRegistered(importable_string)
    return {Encoding.TYPE: importable_string, Encoding.VAR: var.serialize()}


def decode_timetable(var: dict[str, Any]) -> Timetable:
    """
    Decode a previously serialized timetable.

    Most of the deserialization logic is delegated to the actual type, which
    we import from string.

    :meta private:
    """
    importable_string = var[Encoding.TYPE]
    timetable_class = _get_registered_timetable(importable_string)
    if timetable_class is None:
        raise _TimetableNotRegistered(importable_string)
    return timetable_class.deserialize(var[Encoding.VAR])


def encode_priority_weight_strategy(var: PriorityWeightStrategy) -> str:
    """
    Encode a priority weight strategy instance.

    In this version, we only store the importable string, so the class should not wait
    for any parameters to be passed to it. If you need to store the parameters, you
    should store them in the class itself.
    """
    priority_weight_strategy_class = type(var)
    if priority_weight_strategy_class in airflow_priority_weight_strategies_classes:
        return airflow_priority_weight_strategies_classes[priority_weight_strategy_class]
    importable_string = qualname(priority_weight_strategy_class)
    if _get_registered_priority_weight_strategy(importable_string) is None:
        raise _PriorityWeightStrategyNotRegistered(importable_string)
    return importable_string


def decode_priority_weight_strategy(var: str) -> PriorityWeightStrategy:
    """
    Decode a previously serialized priority weight strategy.

    In this version, we only store the importable string, so we just need to get the class
    from the dictionary of registered classes and instantiate it with no parameters.
    """
    priority_weight_strategy_class = _get_registered_priority_weight_strategy(var)
    if priority_weight_strategy_class is None:
        raise _PriorityWeightStrategyNotRegistered(var)
    return priority_weight_strategy_class()


def encode_start_trigger_args(var: StartTriggerArgs) -> dict[str, Any]:
    """
    Encode a StartTriggerArgs.

    :meta private:
    """

    def serialize_kwargs(key: str) -> Any:
        if (val := getattr(var, key)) is None:
            return None
        return BaseSerialization.serialize(val)

    return {
        "__type": "START_TRIGGER_ARGS",
        "trigger_cls": var.trigger_cls,
        "trigger_kwargs": serialize_kwargs("trigger_kwargs"),
        "next_method": var.next_method,
        "next_kwargs": serialize_kwargs("next_kwargs"),
        "timeout": var.timeout.total_seconds() if var.timeout else None,
    }


def decode_start_trigger_args(var: dict[str, Any]) -> StartTriggerArgs:
    """
    Decode a StartTriggerArgs.

    :meta private:
    """

    def deserialize_kwargs(key: str) -> Any:
        if (val := var[key]) is None:
            return None
        return BaseSerialization.deserialize(val)

    return StartTriggerArgs(
        trigger_cls=var["trigger_cls"],
        trigger_kwargs=deserialize_kwargs("trigger_kwargs"),
        next_method=var["next_method"],
        next_kwargs=deserialize_kwargs("next_kwargs"),
        timeout=datetime.timedelta(seconds=var["timeout"]) if var["timeout"] else None,
    )


class _XComRef(NamedTuple):
    """
    Store info needed to create XComArg.

    We can't turn it in to a XComArg until we've loaded _all_ the tasks, so when
    deserializing an operator, we need to create something in its place, and
    post-process it in ``deserialize_dag``.
    """

    data: dict

    def deref(self, dag: SerializedDAG) -> SchedulerXComArg:
        return deserialize_xcom_arg(self.data, dag)


# These two should be kept in sync. Note that these are intentionally not using
# the type declarations in expandinput.py so we always remember to update
# serialization logic when adding new ExpandInput variants. If you add things to
# the unions, be sure to update _ExpandInputRef to match.
# Mapping[str, Any], For .expand(**kwargs).
# XComArg # For expand_kwargs(arg).
_ExpandInputOriginalValue = Mapping[str, Any] | XComArg | Collection[XComArg | Mapping[str, Any]]

# Mapping[str, Any], For .expand(**kwargs).
# _XComRef For expand_kwargs(arg).
_ExpandInputSerializedValue = Mapping[str, Any] | _XComRef | Collection[_XComRef | Mapping[str, Any]]


class _ExpandInputRef(NamedTuple):
    """
    Store info needed to create a mapped operator's expand input.

    This references a ``ExpandInput`` type, but replaces ``XComArg`` objects
    with ``_XComRef`` (see documentation on the latter type for reasoning).
    """

    key: str
    value: _ExpandInputSerializedValue

    @classmethod
    def validate_expand_input_value(cls, value: _ExpandInputOriginalValue) -> None:
        """
        Validate we've covered all ``ExpandInput.value`` types.

        This function does not actually do anything, but is called during
        serialization so Mypy will *statically* check we have handled all
        possible ExpandInput cases.
        """

    def deref(self, dag: SerializedDAG) -> SchedulerExpandInput:
        """
        De-reference into a concrete ExpandInput object.

        If you add more cases here, be sure to update _ExpandInputOriginalValue
        and _ExpandInputSerializedValue to match the logic.
        """
        if isinstance(self.value, _XComRef):
            value: Any = self.value.deref(dag)
        elif isinstance(self.value, collections.abc.Mapping):
            value = {k: v.deref(dag) if isinstance(v, _XComRef) else v for k, v in self.value.items()}
        else:
            value = [v.deref(dag) if isinstance(v, _XComRef) else v for v in self.value]
        return create_expand_input(self.key, value)


class BaseSerialization:
    """BaseSerialization provides utils for serialization."""

    # JSON primitive types.
    _primitive_types = (int, bool, float, str)

    # Time types.
    # datetime.date and datetime.time are converted to strings.
    _datetime_types = (datetime.datetime,)

    # Object types that are always excluded in serialization.
    _excluded_types = (logging.Logger, Connection, type, property)

    _json_schema: ClassVar[Validator | None] = None

    # Should the extra operator link be loaded via plugins when
    # de-serializing the DAG? This flag is set to False in Scheduler so that Extra Operator links
    # are not loaded to not run User code in Scheduler.
    _load_operator_extra_links = True

    _CONSTRUCTOR_PARAMS: dict[str, Parameter] = {}

    SERIALIZER_VERSION = 3

    @classmethod
    def to_json(cls, var: Any) -> str:
        """Stringify DAGs and operators contained by var and returns a JSON string of var."""
        return json.dumps(cls.to_dict(var), ensure_ascii=True)

    @classmethod
    def to_dict(cls, var: Any) -> dict:
        """Stringify DAGs and operators contained by var and returns a dict of var."""
        # Don't call on this class directly - only SerializedDAG or
        # SerializedBaseOperator should be used as the "entrypoint"
        raise NotImplementedError()

    @classmethod
    def from_json(cls, serialized_obj: str) -> BaseSerialization | dict | list | set | tuple:
        """Deserialize json_str and reconstructs all DAGs and operators it contains."""
        return cls.from_dict(json.loads(serialized_obj))

    @classmethod
    def from_dict(cls, serialized_obj: dict[Encoding, Any]) -> BaseSerialization | dict | list | set | tuple:
        """Deserialize a dict of type decorators and reconstructs all DAGs and operators it contains."""
        return cls.deserialize(serialized_obj)

    @classmethod
    def validate_schema(cls, serialized_obj: str | dict) -> None:
        """Validate serialized_obj satisfies JSON schema."""
        if cls._json_schema is None:
            raise AirflowException(f"JSON schema of {cls.__name__:s} is not set.")

        if isinstance(serialized_obj, dict):
            cls._json_schema.validate(serialized_obj)
        elif isinstance(serialized_obj, str):
            cls._json_schema.validate(json.loads(serialized_obj))
        else:
            raise TypeError("Invalid type: Only dict and str are supported.")

    @staticmethod
    def _encode(x: Any, type_: Any) -> dict[Encoding, Any]:
        """Encode data by a JSON dict."""
        return {Encoding.VAR: x, Encoding.TYPE: type_}

    @classmethod
    def _is_primitive(cls, var: Any) -> bool:
        """Primitive types."""
        return var is None or isinstance(var, cls._primitive_types)

    @classmethod
    def _is_excluded(cls, var: Any, attrname: str, instance: Any) -> bool:
        """Check if type is excluded from serialization."""
        if var is None:
            if not cls._is_constructor_param(attrname, instance):
                # Any instance attribute, that is not a constructor argument, we exclude None as the default
                return True

            return cls._value_is_hardcoded_default(attrname, var, instance)
        return isinstance(var, cls._excluded_types) or cls._value_is_hardcoded_default(
            attrname, var, instance
        )

    @classmethod
    def serialize_to_json(
        cls,
        # TODO (GH-52141): When can we remove scheduler constructs here?
        object_to_serialize: SdkOperator | SerializedOperator | DAG | SerializedDAG,
        decorated_fields: set,
    ) -> dict[str, Any]:
        """Serialize an object to JSON."""
        serialized_object: dict[str, Any] = {}
        keys_to_serialize = object_to_serialize.get_serialized_fields()
        for key in keys_to_serialize:
            # None is ignored in serialized form and is added back in deserialization.
            value = getattr(object_to_serialize, key, None)
            if cls._is_excluded(value, key, object_to_serialize):
                continue

            if key == "_operator_name":
                # when operator_name matches task_type, we can remove
                # it to reduce the JSON payload
                task_type = getattr(object_to_serialize, "task_type", None)
                if value != task_type:
                    serialized_object[key] = cls.serialize(value)
            elif key in decorated_fields:
                serialized_object[key] = cls.serialize(value)
            elif key == "timetable" and value is not None:
                serialized_object[key] = encode_timetable(value)
            elif key == "weight_rule" and value is not None:
                encoded_priority_weight_strategy = encode_priority_weight_strategy(value)

                # Exclude if it is just default
                default_pri_weight_stra = cls.get_schema_defaults("operator").get(key, None)
                if default_pri_weight_stra != encoded_priority_weight_strategy:
                    serialized_object[key] = encoded_priority_weight_strategy

            else:
                value = cls.serialize(value)
                if isinstance(value, dict) and Encoding.TYPE in value:
                    value = value[Encoding.VAR]
                serialized_object[key] = value
        return serialized_object

    @classmethod
    def serialize(
        cls, var: Any, *, strict: bool = False
    ) -> Any:  # Unfortunately there is no support for recursive types in mypy
        """
        Serialize an object; helper function of depth first search for serialization.

        The serialization protocol is:

        (1) keeping JSON supported types: primitives, dict, list;
        (2) encoding other types as ``{TYPE: 'foo', VAR: 'bar'}``, the deserialization
            step decode VAR according to TYPE;
        (3) Operator has a special field CLASS to record the original class
            name for displaying in UI.

        :meta private:
        """
        if cls._is_primitive(var):
            # enum.IntEnum is an int instance, it causes json dumps error so we use its value.
            if isinstance(var, enum.Enum):
                return var.value
            # These are not allowed in JSON. https://datatracker.ietf.org/doc/html/rfc8259#section-6
            if isinstance(var, float) and (math.isnan(var) or math.isinf(var)):
                return str(var)
            return var
        elif isinstance(var, dict):
            return cls._encode(
                {str(k): cls.serialize(v, strict=strict) for k, v in var.items()},
                type_=DAT.DICT,
            )
        elif isinstance(var, list):
            return [cls.serialize(v, strict=strict) for v in var]
        elif var.__class__.__name__ == "V1Pod" and _has_kubernetes() and isinstance(var, k8s.V1Pod):
            json_pod = PodGenerator.serialize_pod(var)
            return cls._encode(json_pod, type_=DAT.POD)
        elif isinstance(var, OutletEventAccessors):
            return cls._encode(
                encode_outlet_event_accessors(var),
                type_=DAT.ASSET_EVENT_ACCESSORS,
            )
        elif isinstance(var, AssetUniqueKey):
            return cls._encode(
                attrs.asdict(var),
                type_=DAT.ASSET_UNIQUE_KEY,
            )
        elif isinstance(var, AssetAliasUniqueKey):
            return cls._encode(
                attrs.asdict(var),
                type_=DAT.ASSET_ALIAS_UNIQUE_KEY,
            )
        elif isinstance(var, DAG):
            return cls._encode(SerializedDAG.serialize_dag(var), type_=DAT.DAG)
        elif isinstance(var, DeadlineAlert):
            return cls._encode(DeadlineAlert.serialize_deadline_alert(var), type_=DAT.DEADLINE_ALERT)
        elif isinstance(var, Resources):
            return var.to_dict()
        elif isinstance(var, MappedOperator):
            return cls._encode(SerializedBaseOperator.serialize_mapped_operator(var), type_=DAT.OP)
        elif isinstance(var, BaseOperator):
            var._needs_expansion = var.get_needs_expansion()
            return cls._encode(SerializedBaseOperator.serialize_operator(var), type_=DAT.OP)
        elif isinstance(var, cls._datetime_types):
            return cls._encode(var.timestamp(), type_=DAT.DATETIME)
        elif isinstance(var, datetime.timedelta):
            return cls._encode(var.total_seconds(), type_=DAT.TIMEDELTA)
        elif isinstance(var, (Timezone, FixedTimezone)):
            return cls._encode(encode_timezone(var), type_=DAT.TIMEZONE)
        elif isinstance(var, relativedelta.relativedelta):
            return cls._encode(encode_relativedelta(var), type_=DAT.RELATIVEDELTA)
        elif isinstance(var, TaskInstanceKey):
            return cls._encode(
                var._asdict(),
                type_=DAT.TASK_INSTANCE_KEY,
            )
        elif isinstance(var, (AirflowException, TaskDeferred)) and hasattr(var, "serialize"):
            exc_cls_name, args, kwargs = var.serialize()
            return cls._encode(
                cls.serialize(
                    {"exc_cls_name": exc_cls_name, "args": args, "kwargs": kwargs},
                    strict=strict,
                ),
                type_=DAT.AIRFLOW_EXC_SER,
            )
        elif isinstance(var, (KeyError, AttributeError)):
            return cls._encode(
                cls.serialize(
                    {
                        "exc_cls_name": var.__class__.__name__,
                        "args": [var.args],
                        "kwargs": {},
                    },
                    strict=strict,
                ),
                type_=DAT.BASE_EXC_SER,
            )
        elif isinstance(var, BaseTrigger):
            return cls._encode(
                cls.serialize(
                    var.serialize(),
                    strict=strict,
                ),
                type_=DAT.BASE_TRIGGER,
            )
        elif callable(var):
            return str(get_python_source(var))
        elif isinstance(var, set):
            # FIXME: casts set to list in customized serialization in future.
            try:
                return cls._encode(
                    sorted(cls.serialize(v, strict=strict) for v in var),
                    type_=DAT.SET,
                )
            except TypeError:
                return cls._encode(
                    [cls.serialize(v, strict=strict) for v in var],
                    type_=DAT.SET,
                )
        elif isinstance(var, tuple):
            # FIXME: casts tuple to list in customized serialization in future.
            return cls._encode(
                [cls.serialize(v, strict=strict) for v in var],
                type_=DAT.TUPLE,
            )
        elif isinstance(var, TaskGroup):
            return TaskGroupSerialization.serialize_task_group(var)
        elif isinstance(var, Param):
            return cls._encode(cls._serialize_param(var), type_=DAT.PARAM)
        elif isinstance(var, XComArg):
            return cls._encode(serialize_xcom_arg(var), type_=DAT.XCOM_REF)
        elif isinstance(var, LazySelectSequence):
            return cls.serialize(list(var))
        elif isinstance(var, BaseAsset):
            serialized_asset = encode_asset_condition(var)
            return cls._encode(serialized_asset, type_=serialized_asset.pop("__type"))
        elif isinstance(var, AssetRef):
            return cls._encode(attrs.asdict(var), type_=DAT.ASSET_REF)
        elif isinstance(var, Connection):
            return cls._encode(var.to_dict(validate=True), type_=DAT.CONNECTION)
        elif isinstance(var, TaskCallbackRequest):
            return cls._encode(var.to_json(), type_=DAT.TASK_CALLBACK_REQUEST)
        elif isinstance(var, DagCallbackRequest):
            return cls._encode(var.to_json(), type_=DAT.DAG_CALLBACK_REQUEST)
        elif var.__class__ == Context:
            d = {}
            for k, v in var.items():
                obj = cls.serialize(v, strict=strict)
                d[str(k)] = obj
            return cls._encode(d, type_=DAT.TASK_CONTEXT)
        elif isinstance(var, ArgNotSet):
            return cls._encode(None, type_=DAT.ARG_NOT_SET)
        else:
            return cls.default_serialization(strict, var)

    @classmethod
    def default_serialization(cls, strict, var) -> str:
        log.debug("Cast type %s to str in serialization.", type(var))
        if strict:
            raise SerializationError("Encountered unexpected type")
        return str(var)

    @classmethod
    def deserialize(cls, encoded_var: Any) -> Any:
        """
        Deserialize an object; helper function of depth first search for deserialization.

        :meta private:
        """
        if cls._is_primitive(encoded_var):
            return encoded_var
        elif isinstance(encoded_var, list):
            return [cls.deserialize(v) for v in encoded_var]

        if not isinstance(encoded_var, dict):
            raise ValueError(f"The encoded_var should be dict and is {type(encoded_var)}")
        var = encoded_var[Encoding.VAR]
        type_ = encoded_var[Encoding.TYPE]
        if type_ == DAT.TASK_CONTEXT:
            d = {}
            for k, v in var.items():
                if k == "task":  # todo: add `_encode` of Operator so we don't need this
                    continue
                d[k] = cls.deserialize(v)
            d["task"] = d["task_instance"].task  # todo: add `_encode` of Operator so we don't need this
            d["macros"] = macros
            d["var"] = {
                "json": VariableAccessor(deserialize_json=True),
                "value": VariableAccessor(deserialize_json=False),
            }
            d["conn"] = ConnectionAccessor()
            return Context(**d)
        elif type_ == DAT.DICT:
            return {k: cls.deserialize(v) for k, v in var.items()}
        elif type_ == DAT.ASSET_EVENT_ACCESSORS:
            return decode_outlet_event_accessors(var)
        elif type_ == DAT.ASSET_UNIQUE_KEY:
            return AssetUniqueKey(name=var["name"], uri=var["uri"])
        elif type_ == DAT.ASSET_ALIAS_UNIQUE_KEY:
            return AssetAliasUniqueKey(name=var["name"])
        elif type_ == DAT.DAG:
            return SerializedDAG.deserialize_dag(var)
        elif type_ == DAT.OP:
            return SerializedBaseOperator.deserialize_operator(var)
        elif type_ == DAT.DATETIME:
            return from_timestamp(var)
        elif type_ == DAT.POD:
            # Attempt to import kubernetes for deserialization. Using attempt_import=True allows
            # lazy loading of kubernetes libraries only when actually needed for POD deserialization.
            if not _has_kubernetes(attempt_import=True):
                raise RuntimeError(
                    "Cannot deserialize POD objects without kubernetes libraries. "
                    "Please install the cncf.kubernetes provider."
                )
            pod = PodGenerator.deserialize_model_dict(var)
            return pod
        elif type_ == DAT.TIMEDELTA:
            return datetime.timedelta(seconds=var)
        elif type_ == DAT.TIMEZONE:
            return decode_timezone(var)
        elif type_ == DAT.RELATIVEDELTA:
            return decode_relativedelta(var)
        elif type_ == DAT.AIRFLOW_EXC_SER or type_ == DAT.BASE_EXC_SER:
            deser = cls.deserialize(var)
            exc_cls_name = deser["exc_cls_name"]
            args = deser["args"]
            kwargs = deser["kwargs"]
            del deser
            if type_ == DAT.AIRFLOW_EXC_SER:
                exc_cls = import_string(exc_cls_name)
            else:
                exc_cls = import_string(f"builtins.{exc_cls_name}")
            return exc_cls(*args, **kwargs)
        elif type_ == DAT.BASE_TRIGGER:
            tr_cls_name, kwargs = cls.deserialize(var)
            tr_cls = import_string(tr_cls_name)
            return tr_cls(**kwargs)
        elif type_ == DAT.SET:
            return {cls.deserialize(v) for v in var}
        elif type_ == DAT.TUPLE:
            return tuple(cls.deserialize(v) for v in var)
        elif type_ == DAT.PARAM:
            return cls._deserialize_param(var)
        elif type_ == DAT.XCOM_REF:
            return _XComRef(var)  # Delay deserializing XComArg objects until we have the entire DAG.
        elif type_ == DAT.ASSET:
            return decode_asset(var)
        elif type_ == DAT.ASSET_ALIAS:
            return AssetAlias(**var)
        elif type_ == DAT.ASSET_ANY:
            return AssetAny(*(decode_asset_condition(x) for x in var["objects"]))
        elif type_ == DAT.ASSET_ALL:
            return AssetAll(*(decode_asset_condition(x) for x in var["objects"]))
        elif type_ == DAT.ASSET_REF:
            return Asset.ref(**var)
        elif type_ == DAT.CONNECTION:
            return Connection(**var)
        elif type_ == DAT.TASK_CALLBACK_REQUEST:
            return TaskCallbackRequest.from_json(var)
        elif type_ == DAT.DAG_CALLBACK_REQUEST:
            return DagCallbackRequest.from_json(var)
        elif type_ == DAT.TASK_INSTANCE_KEY:
            return TaskInstanceKey(**var)
        elif type_ == DAT.ARG_NOT_SET:
            return NOTSET
        elif type_ == DAT.DEADLINE_ALERT:
            return DeadlineAlert.deserialize_deadline_alert(var)
        else:
            raise TypeError(f"Invalid type {type_!s} in deserialization.")

    _deserialize_datetime = from_timestamp
    _deserialize_timezone = parse_timezone

    @classmethod
    def _deserialize_timedelta(cls, seconds: int) -> datetime.timedelta:
        return datetime.timedelta(seconds=seconds)

    @classmethod
    def _is_constructor_param(cls, attrname: str, instance: Any) -> bool:
        return attrname in cls._CONSTRUCTOR_PARAMS

    @classmethod
    def _value_is_hardcoded_default(cls, attrname: str, value: Any, instance: Any) -> bool:
        """
        Return true if ``value`` is the hard-coded default for the given attribute.

        This takes in to account cases where the ``max_active_tasks`` parameter is
        stored in the ``_max_active_tasks`` attribute.

        And by using `is` here only and not `==` this copes with the case a
        user explicitly specifies an attribute with the same "value" as the
        default. (This is because ``"default" is "default"`` will be False as
        they are different strings with the same characters.)

        Also returns True if the value is an empty list or empty dict. This is done
        to account for the case where the default value of the field is None but has the
        ``field = field or {}`` set.
        """
        if attrname in cls._CONSTRUCTOR_PARAMS:
            if cls._CONSTRUCTOR_PARAMS[attrname] is value or (value in [{}, []]):
                return True
            if cls._CONSTRUCTOR_PARAMS[attrname] is attrs.NOTHING and value is None:
                return True
        if attrs.has(type(instance)):
            return any(fld.default is value for fld in attrs.fields(type(instance)) if fld.name == attrname)
        return False

    @classmethod
    def _serialize_param(cls, param: Param):
        return {
            "__class": f"{param.__module__}.{param.__class__.__name__}",
            "default": cls.serialize(param.value),
            "description": cls.serialize(param.description),
            "schema": cls.serialize(param.schema),
        }

    @classmethod
    def _deserialize_param(cls, param_dict: dict):
        """
        Workaround to serialize Param on older versions.

        In 2.2.0, Param attrs were assumed to be json-serializable and were not run through
        this class's ``serialize`` method.  So before running through ``deserialize``,
        we first verify that it's necessary to do.
        """
        class_name = param_dict["__class"]
        class_: type[Param] = import_string(class_name)
        attrs = ("default", "description", "schema")
        kwargs = {}

        def is_serialized(val):
            if isinstance(val, dict):
                return Encoding.TYPE in val
            if isinstance(val, list):
                return all(isinstance(item, dict) and Encoding.TYPE in item for item in val)
            return False

        for attr in attrs:
            if attr in param_dict:
                val = param_dict[attr]
                if is_serialized(val):
                    val = cls.deserialize(val)
                kwargs[attr] = val
        return class_(**kwargs)

    @classmethod
    def _serialize_params_dict(cls, params: ParamsDict | dict) -> list[tuple[str, dict]]:
        """Serialize Params dict for a DAG or task as a list of tuples to ensure ordering."""
        serialized_params = []
        for k, v in params.items():
            if isinstance(params, ParamsDict):
                # Use native param object, not resolved value if possible
                v = params.get_param(k)
            try:
                class_identity = f"{v.__module__}.{v.__class__.__name__}"
            except AttributeError:
                class_identity = ""
            if class_identity == "airflow.sdk.definitions.param.Param":
                serialized_params.append((k, cls._serialize_param(v)))
            else:
                # Auto-box other values into Params object like it is done by DAG parsing as well
                serialized_params.append((k, cls._serialize_param(Param(v))))
        return serialized_params

    @classmethod
    def _deserialize_params_dict(cls, encoded_params: list[tuple[str, dict]]) -> ParamsDict:
        """Deserialize a DAG's Params dict."""
        if isinstance(encoded_params, collections.abc.Mapping):
            # in 2.9.2 or earlier params were serialized as JSON objects
            encoded_param_pairs: Iterable[tuple[str, dict]] = encoded_params.items()
        else:
            encoded_param_pairs = encoded_params

        op_params = {}
        for k, v in encoded_param_pairs:
            if isinstance(v, dict) and "__class" in v:
                op_params[k] = cls._deserialize_param(v)
            else:
                # Old style params, convert it
                op_params[k] = Param(v)

        return ParamsDict(op_params)

    @classmethod
    @lru_cache(maxsize=4)  # Cache for "operator", "dag", and a few others
    def get_schema_defaults(cls, object_type: str) -> dict[str, Any]:
        """
        Extract default values from JSON schema for any object type.

        :param object_type: The object type to get defaults for (e.g., "operator", "dag")
        :return: Dictionary of field name -> default value
        """
        # Load schema if needed (handles lazy loading)
        schema_loader = cls._json_schema

        if schema_loader is None:
            return {}

        # Access the schema definitions (trigger lazy loading)
        schema_data = schema_loader.schema
        object_def = schema_data.get("definitions", {}).get(object_type, {})
        properties = object_def.get("properties", {})

        defaults = {}
        for field_name, field_def in properties.items():
            if isinstance(field_def, dict) and "default" in field_def:
                defaults[field_name] = field_def["default"]

        return defaults


class DependencyDetector:
    """
    Detects dependencies between DAGs.

    :meta private:
    """

    @staticmethod
    def detect_task_dependencies(task: SdkOperator) -> list[DagDependency]:
        """Detect dependencies caused by tasks."""
        from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
        from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

        deps = []
        if isinstance(task, TriggerDagRunOperator):
            deps.append(
                DagDependency(
                    source=task.dag_id,
                    target=getattr(task, "trigger_dag_id"),
                    label=task.task_display_name,
                    dependency_type="trigger",
                    dependency_id=task.task_id,
                )
            )
        elif (
            isinstance(task, MappedOperator)
            and issubclass(task.operator_class, TriggerDagRunOperator)
            and "trigger_dag_id" in task.partial_kwargs
        ):
            deps.append(
                DagDependency(
                    source=task.dag_id,
                    target=task.partial_kwargs["trigger_dag_id"],
                    label=task.task_display_name,
                    dependency_type="trigger",
                    dependency_id=task.task_id,
                )
            )
        elif isinstance(task, ExternalTaskSensor):
            deps.append(
                DagDependency(
                    source=getattr(task, "external_dag_id"),
                    target=task.dag_id,
                    label=task.task_display_name,
                    dependency_type="sensor",
                    dependency_id=task.task_id,
                )
            )
        elif (
            isinstance(task, MappedOperator)
            and issubclass(task.operator_class, ExternalTaskSensor)
            and "external_dag_id" in task.partial_kwargs
        ):
            deps.append(
                DagDependency(
                    source=task.partial_kwargs["external_dag_id"],
                    target=task.dag_id,
                    label=task.task_display_name,
                    dependency_type="sensor",
                    dependency_id=task.task_id,
                )
            )

        for obj in task.outlets or []:
            if isinstance(obj, Asset):
                deps.append(
                    DagDependency(
                        source=task.dag_id,
                        target="asset",
                        label=obj.name,
                        dependency_type="asset",
                        dependency_id=AssetUniqueKey.from_asset(obj).to_str(),
                    )
                )
            elif isinstance(obj, AssetAlias):
                deps.extend(obj.iter_dag_dependencies(source=task.dag_id, target=""))

        return deps

    @staticmethod
    def detect_dag_dependencies(dag: DAG | None) -> Iterable[DagDependency]:
        """Detect dependencies set directly on the DAG object."""
        if not dag:
            return
        yield from dag.timetable.asset_condition.iter_dag_dependencies(source="", target=dag.dag_id)


# TODO (GH-52141): Duplicate DAGNode in the scheduler.
class SerializedBaseOperator(DAGNode, BaseSerialization):
    """
    A JSON serializable representation of operator.

    All operators are casted to SerializedBaseOperator after deserialization.
    Class specific attributes used by UI are move to object attributes.

    Creating a SerializedBaseOperator is a three-step process:

    1. Instantiate a :class:`SerializedBaseOperator` object.
    2. Populate attributes with :func:`SerializedBaseOperator.populated_operator`.
    3. When the task's containing DAG is available, fix references to the DAG
       with :func:`SerializedBaseOperator.set_task_dag_references`.
    """

    _decorated_fields = {"executor_config"}

    _CONSTRUCTOR_PARAMS = {}

    _json_schema: ClassVar[Validator] = lazy_object_proxy.Proxy(load_dag_schema)

    _can_skip_downstream: bool
    _is_empty: bool
    _needs_expansion: bool
    _task_display_name: str | None
    _weight_rule: str | PriorityWeightStrategy = "downstream"

    # TODO (GH-52141): These should contain serialized containers, but currently
    # this class inherits from an SDK one.
    dag: SerializedDAG | None = None  # type: ignore[assignment]
    task_group: SerializedTaskGroup | None = None  # type: ignore[assignment]

    allow_nested_operators: bool = True
    depends_on_past: bool = False
    do_xcom_push: bool = True
    doc: str | None = None
    doc_md: str | None = None
    doc_json: str | None = None
    doc_yaml: str | None = None
    doc_rst: str | None = None
    downstream_task_ids: set[str] = set()
    email: str | Sequence[str] | None

    # Following 2 should be deprecated
    email_on_retry: bool = True
    email_on_failure: bool = True

    execution_timeout: datetime.timedelta | None
    executor: str | None
    executor_config: dict = {}
    ignore_first_depends_on_past: bool = False

    inlets: Sequence = []
    is_setup: bool = False
    is_teardown: bool = False

    map_index_template: str | None = None
    max_active_tis_per_dag: int | None = None
    max_active_tis_per_dagrun: int | None = None
    max_retry_delay: datetime.timedelta | float | None = None
    multiple_outputs: bool = False

    # Boolean flags for callback existence
    has_on_execute_callback: bool = False
    has_on_failure_callback: bool = False
    has_on_retry_callback: bool = False
    has_on_success_callback: bool = False
    has_on_skipped_callback: bool = False

    operator_extra_links: Collection[BaseOperatorLink] = []
    on_failure_fail_dagrun: bool = False

    outlets: Sequence = []
    owner: str = "airflow"
    pool: str = "default_pool"
    pool_slots: int = 1
    priority_weight: int = 1
    queue: str = "default"

    resources: dict[str, Any] | None = None
    retries: int = 0
    retry_delay: datetime.timedelta = datetime.timedelta(seconds=300)
    retry_exponential_backoff: bool = False
    run_as_user: str | None = None

    start_date: datetime.datetime | None = None
    end_date: datetime.datetime | None = None

    start_from_trigger: bool = False
    start_trigger_args: StartTriggerArgs | None = None

    task_type: str = "BaseOperator"
    template_ext: Sequence[str] = []
    template_fields: Collection[str] = []
    template_fields_renderers: ClassVar[dict[str, str]] = {}

    trigger_rule: str | TriggerRule = "all_success"

    # TODO: Remove the following, they aren't used anymore
    ui_color: str = "#fff"
    ui_fgcolor: str = "#000"

    wait_for_downstream: bool = False
    wait_for_past_depends_before_skipping: bool = False

    is_mapped = False

    def __init__(
        self,
        *,
        task_id: str,
        params: Mapping[str, Any] | None = None,
        _airflow_from_mapped: bool = False,
    ) -> None:
        super().__init__()

        self._BaseOperator__from_mapped = _airflow_from_mapped
        self.task_id = task_id
        self.params = ParamsDict(params)
        # Move class attributes into object attributes.
        self.deps = DEFAULT_OPERATOR_DEPS
        self._operator_name: str | None = None

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (SerializedBaseOperator, BaseOperator)):
            return NotImplemented
        return self.task_type == other.task_type and all(
            getattr(self, c, None) == getattr(other, c, None) for c in BaseOperator._comps
        )

    def __repr__(self) -> str:
        return f"<SerializedTask({self.task_type}): {self.task_id}>"

    @property
    def node_id(self) -> str:
        return self.task_id

    # TODO (GH-52141): Replace DAGNode with a scheduler type.
    def get_dag(self) -> SerializedDAG | None:  # type: ignore[override]
        return self.dag

    @property
    def roots(self) -> Sequence[DAGNode]:
        """Required by DAGNode."""
        return [self]

    @property
    def leaves(self) -> Sequence[DAGNode]:
        """Required by DAGNode."""
        return [self]

    @cached_property
    def operator_extra_link_dict(self) -> dict[str, BaseOperatorLink]:
        """Returns dictionary of all extra links for the operator."""
        return {link.name: link for link in self.operator_extra_links}

    @cached_property
    def global_operator_extra_link_dict(self) -> dict[str, Any]:
        """Returns dictionary of all global extra links."""
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.global_operator_extra_links is None:
            raise AirflowException("Can't load operators")
        return {link.name: link for link in plugins_manager.global_operator_extra_links}

    @cached_property
    def extra_links(self) -> list[str]:
        return sorted(set(self.operator_extra_link_dict).union(self.global_operator_extra_link_dict))

    def get_extra_links(self, ti: TaskInstance, name: str) -> str | None:
        """
        For an operator, gets the URLs that the ``extra_links`` entry points to.

        :meta private:

        :raise ValueError: The error message of a ValueError will be passed on through to
            the fronted to show up as a tooltip on the disabled link.
        :param ti: The TaskInstance for the URL being searched for.
        :param name: The name of the link we're looking for the URL for. Should be
            one of the options specified in ``extra_links``.
        """
        link = self.operator_extra_link_dict.get(name) or self.global_operator_extra_link_dict.get(name)
        if not link:
            return None
        # TODO: GH-52141 - BaseOperatorLink.get_link expects BaseOperator but receives SerializedBaseOperator.
        return link.get_link(self, ti_key=ti.key)  # type: ignore[arg-type]

    @property
    def operator_name(self) -> str:
        # Overwrites operator_name of BaseOperator to use _operator_name instead of
        # __class__.operator_name.
        return self._operator_name or self.task_type

    @operator_name.setter
    def operator_name(self, operator_name: str):
        self._operator_name = operator_name

    @property
    def task_display_name(self) -> str:
        return self._task_display_name or self.task_id

    def expand_start_trigger_args(self, *, context: Context) -> StartTriggerArgs | None:
        return self.start_trigger_args

    @property
    def weight_rule(self) -> PriorityWeightStrategy:
        if isinstance(self._weight_rule, PriorityWeightStrategy):
            return self._weight_rule
        return validate_and_load_priority_weight_strategy(self._weight_rule)

    def __getattr__(self, name):
        # Handle missing attributes with task_type instead of SerializedBaseOperator
        # Don't intercept special methods that Python internals might check
        if name.startswith("__") and name.endswith("__"):
            # For special methods, raise the original error
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        # For regular attributes, use task_type in the error message
        raise AttributeError(f"'{self.task_type}' object has no attribute '{name}'")

    @classmethod
    def serialize_mapped_operator(cls, op: MappedOperator) -> dict[str, Any]:
        serialized_op = cls._serialize_node(op)
        # Handle expand_input and op_kwargs_expand_input.
        expansion_kwargs = op._get_specified_expand_input()
        if TYPE_CHECKING:  # Let Mypy check the input type for us!
            _ExpandInputRef.validate_expand_input_value(expansion_kwargs.value)
        serialized_op[op._expand_input_attr] = {
            "type": type(expansion_kwargs).EXPAND_INPUT_TYPE,
            "value": cls.serialize(expansion_kwargs.value),
        }

        if op.partial_kwargs:
            serialized_op["partial_kwargs"] = {}
            for k, v in op.partial_kwargs.items():
                if cls._is_excluded(v, k, op):
                    continue

                if k in [f"on_{x}_callback" for x in ("execute", "failure", "success", "retry", "skipped")]:
                    if bool(v):
                        serialized_op["partial_kwargs"][f"has_{k}"] = True
                    continue
                serialized_op["partial_kwargs"].update({k: cls.serialize(v)})

        # we want to store python_callable_name, not python_callable
        python_callable = op.partial_kwargs.get("python_callable", None)
        if python_callable:
            callable_name = qualname(python_callable)
            serialized_op["partial_kwargs"]["python_callable_name"] = callable_name
            del serialized_op["partial_kwargs"]["python_callable"]

        serialized_op["_is_mapped"] = True
        return serialized_op

    @classmethod
    def serialize_operator(cls, op: SdkOperator) -> dict[str, Any]:
        return cls._serialize_node(op)

    @classmethod
    def _serialize_node(cls, op: SdkOperator) -> dict[str, Any]:
        """Serialize operator into a JSON object."""
        serialize_op = cls.serialize_to_json(op, cls._decorated_fields)

        if not op.email:
            # If "email" is empty, we do not need to include other email attrs
            for attr in ["email_on_failure", "email_on_retry"]:
                if attr in serialize_op:
                    del serialize_op[attr]

        # Detect if there's a change in python callable name
        python_callable = getattr(op, "python_callable", None)
        if python_callable:
            callable_name = qualname(python_callable)
            serialize_op["python_callable_name"] = callable_name

        serialize_op["task_type"] = getattr(op, "task_type", type(op).__name__)
        serialize_op["_task_module"] = getattr(op, "_task_module", type(op).__module__)
        if op.operator_name != serialize_op["task_type"]:
            serialize_op["_operator_name"] = op.operator_name

        # Used to determine if an Operator is inherited from EmptyOperator
        if op.inherits_from_empty_operator:
            serialize_op["_is_empty"] = True

        # Used to determine if an Operator is inherited from SkipMixin or BranchMixin
        if op.inherits_from_skipmixin:
            serialize_op["_can_skip_downstream"] = True

        if op.start_trigger_args:
            serialize_op["start_trigger_args"] = encode_start_trigger_args(op.start_trigger_args)

        if op.operator_extra_links:
            serialize_op["_operator_extra_links"] = cls._serialize_operator_extra_links(
                op.operator_extra_links.__get__(op)
                if isinstance(op.operator_extra_links, property)
                else op.operator_extra_links
            )

        # Store all template_fields as they are if there are JSON Serializable
        # If not, store them as strings
        # And raise an exception if the field is not templateable
        forbidden_fields = set(signature(BaseOperator.__init__).parameters.keys())
        # Though allow some of the BaseOperator fields to be templated anyway
        forbidden_fields.difference_update({"email"})
        if op.template_fields:
            for template_field in op.template_fields:
                if template_field in forbidden_fields:
                    raise AirflowException(
                        dedent(
                            f"""Cannot template BaseOperator field:
                        {template_field!r} {op.__class__.__name__=} {op.template_fields=}"""
                        )
                    )
                value = getattr(op, template_field, None)
                if not cls._is_excluded(value, template_field, op):
                    serialize_op[template_field] = serialize_template_field(value, template_field)

        if op.params:
            serialize_op["params"] = cls._serialize_params_dict(op.params)

        return serialize_op

    @classmethod
    def populate_operator(
        cls,
        op: SerializedOperator,
        encoded_op: dict[str, Any],
        client_defaults: dict[str, Any] | None = None,
    ) -> None:
        """
        Populate operator attributes with serialized values.

        This covers simple attributes that don't reference other things in the
        DAG. Setting references (such as ``op.dag`` and task dependencies) is
        done in ``set_task_dag_references`` instead, which is called after the
        DAG is hydrated.
        """
        # Apply defaults by merging them into encoded_op BEFORE main deserialization
        encoded_op = cls._apply_defaults_to_encoded_op(encoded_op, client_defaults)

        # Preprocess and upgrade all field names for backward compatibility and consistency
        encoded_op = cls._preprocess_encoded_operator(encoded_op)
        # Extra Operator Links defined in Plugins
        op_extra_links_from_plugin = {}

        # We don't want to load Extra Operator links in Scheduler
        if cls._load_operator_extra_links:
            from airflow import plugins_manager

            plugins_manager.initialize_extra_operators_links_plugins()

            if plugins_manager.operator_extra_links is None:
                raise AirflowException("Can not load plugins")

            for ope in plugins_manager.operator_extra_links:
                for operator in ope.operators:
                    if (
                        operator.__name__ == encoded_op["task_type"]
                        and operator.__module__ == encoded_op["_task_module"]
                    ):
                        op_extra_links_from_plugin.update({ope.name: ope})

            # If OperatorLinks are defined in Plugins but not in the Operator that is being Serialized
            # set the Operator links attribute
            # The case for "If OperatorLinks are defined in the operator that is being Serialized"
            # is handled in the deserialization loop where it matches k == "_operator_extra_links"
            if op_extra_links_from_plugin and "_operator_extra_links" not in encoded_op:
                setattr(
                    op,
                    "operator_extra_links",
                    list(op_extra_links_from_plugin.values()),
                )

        deserialized_partial_kwarg_defaults = {}

        for k, v in encoded_op.items():
            # Use centralized field deserialization logic
            if k in encoded_op.get("template_fields", []):
                pass  # Template fields are handled separately
            elif k == "_operator_extra_links":
                if cls._load_operator_extra_links:
                    op_predefined_extra_links = cls._deserialize_operator_extra_links(v)

                    # If OperatorLinks with the same name exists, Links via Plugin have higher precedence
                    op_predefined_extra_links.update(op_extra_links_from_plugin)
                else:
                    op_predefined_extra_links = {}

                v = list(op_predefined_extra_links.values())
                k = "operator_extra_links"

            elif k == "params":
                v = cls._deserialize_params_dict(v)
                if op.params:  # Merge existing params if needed.
                    v, new = op.params, v
                    v.update(new)
            elif k == "partial_kwargs":
                # Use unified deserializer that supports both encoded and non-encoded values
                v = cls._deserialize_partial_kwargs(v, client_defaults)
            elif k in {"expand_input", "op_kwargs_expand_input"}:
                v = _ExpandInputRef(v["type"], cls.deserialize(v["value"]))
            elif k == "operator_class":
                v = {k_: cls.deserialize(v_) for k_, v_ in v.items()}
            elif k == "_is_sensor":
                from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep

                if v is False:
                    raise RuntimeError("_is_sensor=False should never have been serialized!")
                object.__setattr__(op, "deps", op.deps | {ReadyToRescheduleDep()})
                continue
            elif (
                k in cls._decorated_fields
                or k not in op.get_serialized_fields()
                or k in ("outlets", "inlets")
            ):
                v = cls.deserialize(v)
            elif k == "_on_failure_fail_dagrun":
                k = "on_failure_fail_dagrun"
            elif k == "weight_rule":
                k = "_weight_rule"
                v = decode_priority_weight_strategy(v)
            else:
                # Apply centralized deserialization for all other fields
                v = cls._deserialize_field_value(k, v)

            # Handle field differences between SerializedBaseOperator and MappedOperator
            # Fields that exist in SerializedBaseOperator but not in MappedOperator need to go to partial_kwargs
            if (
                op.is_mapped
                and k in SerializedBaseOperator.get_serialized_fields()
                and k not in op.get_serialized_fields()
            ):
                # This field belongs to SerializedBaseOperator but not MappedOperator
                # Store it in partial_kwargs where it belongs
                deserialized_partial_kwarg_defaults[k] = v
                continue

            # else use v as it is
            setattr(op, k, v)

        # Apply the fields that belong in partial_kwargs for MappedOperator
        if op.is_mapped:
            for k, v in deserialized_partial_kwarg_defaults.items():
                if k not in op.partial_kwargs:
                    op.partial_kwargs[k] = v

        for k in op.get_serialized_fields() - encoded_op.keys():
            # TODO: refactor deserialization of BaseOperator and MappedOperator (split it out), then check
            # could go away.
            if not hasattr(op, k):
                setattr(op, k, None)

        # Set all the template_field to None that were not present in Serialized JSON
        for field in op.template_fields:
            if not hasattr(op, field):
                setattr(op, field, None)

        # Used to determine if an Operator is inherited from EmptyOperator
        setattr(op, "_is_empty", bool(encoded_op.get("_is_empty", False)))

        # Used to determine if an Operator is inherited from SkipMixin
        setattr(op, "_can_skip_downstream", bool(encoded_op.get("_can_skip_downstream", False)))

        start_trigger_args = None
        encoded_start_trigger_args = encoded_op.get("start_trigger_args", None)
        if encoded_start_trigger_args:
            encoded_start_trigger_args = cast("dict", encoded_start_trigger_args)
            start_trigger_args = decode_start_trigger_args(encoded_start_trigger_args)
        setattr(op, "start_trigger_args", start_trigger_args)
        setattr(op, "start_from_trigger", bool(encoded_op.get("start_from_trigger", False)))

    @staticmethod
    def set_task_dag_references(task: SerializedOperator | MappedOperator, dag: SerializedDAG) -> None:
        """
        Handle DAG references on an operator.

        The operator should have been mostly populated earlier by calling
        ``populate_operator``. This function further fixes object references
        that were not possible before the task's containing DAG is hydrated.
        """
        task.dag = dag

        for date_attr in ("start_date", "end_date"):
            if getattr(task, date_attr, None) is None:
                setattr(task, date_attr, getattr(dag, date_attr, None))

        # Dereference expand_input and op_kwargs_expand_input.
        for k in ("expand_input", "op_kwargs_expand_input"):
            if isinstance(kwargs_ref := getattr(task, k, None), _ExpandInputRef):
                setattr(task, k, kwargs_ref.deref(dag))

        for task_id in task.downstream_task_ids:
            # Bypass set_upstream etc here - it does more than we want
            dag.task_dict[task_id].upstream_task_ids.add(task.task_id)

    @classmethod
    @lru_cache(maxsize=1)  # Only one type: "operator"
    def get_operator_optional_fields_from_schema(cls) -> set[str]:
        schema_loader = cls._json_schema

        if schema_loader is None:
            return set()

        schema_data = schema_loader.schema
        operator_def = schema_data.get("definitions", {}).get("operator", {})
        operator_fields = set(operator_def.get("properties", {}).keys())
        required_fields = set(operator_def.get("required", []))

        optional_fields = operator_fields - required_fields
        return optional_fields

    @classmethod
    def deserialize_operator(
        cls,
        encoded_op: dict[str, Any],
        client_defaults: dict[str, Any] | None = None,
    ) -> SerializedOperator:
        """Deserializes an operator from a JSON object."""
        op: SerializedOperator
        if encoded_op.get("_is_mapped", False):
            from airflow.models.mappedoperator import MappedOperator as SerializedMappedOperator

            try:
                operator_name = encoded_op["_operator_name"]
            except KeyError:
                operator_name = encoded_op["task_type"]

            # Only store minimal class type information instead of full operator data
            # This significantly reduces memory usage for mapped operators
            operator_class_info = {
                "task_type": encoded_op["task_type"],
                "_operator_name": operator_name,
            }

            op = SerializedMappedOperator(
                operator_class=operator_class_info,
                task_id=encoded_op["task_id"],
                operator_extra_links=SerializedBaseOperator.operator_extra_links,
                template_ext=SerializedBaseOperator.template_ext,
                template_fields=SerializedBaseOperator.template_fields,
                template_fields_renderers=SerializedBaseOperator.template_fields_renderers,
                ui_color=SerializedBaseOperator.ui_color,
                ui_fgcolor=SerializedBaseOperator.ui_fgcolor,
                is_sensor=encoded_op.get("_is_sensor", False),
                can_skip_downstream=encoded_op.get("_can_skip_downstream", False),
                task_module=encoded_op["_task_module"],
                task_type=encoded_op["task_type"],
                operator_name=operator_name,
                disallow_kwargs_override=encoded_op["_disallow_kwargs_override"],
                expand_input_attr=encoded_op["_expand_input_attr"],
                start_trigger_args=encoded_op.get("start_trigger_args", None),
                start_from_trigger=encoded_op.get("start_from_trigger", False),
            )
        else:
            op = SerializedBaseOperator(task_id=encoded_op["task_id"])

        cls.populate_operator(op, encoded_op, client_defaults)

        return op

    @classmethod
    def _preprocess_encoded_operator(cls, encoded_op: dict[str, Any]) -> dict[str, Any]:
        """
        Preprocess and upgrade all field names for backward compatibility and consistency.

        This consolidates all field name transformations in one place:
        - Callback field renaming (on_*_callback -> has_on_*_callback)
        - Other field upgrades and renames
        - Field exclusions
        """
        preprocessed = encoded_op.copy()

        # Handle callback field renaming for backward compatibility
        for callback_type in ("execute", "failure", "success", "retry", "skipped"):
            old_key = f"on_{callback_type}_callback"
            new_key = f"has_{old_key}"
            if old_key in preprocessed:
                preprocessed[new_key] = bool(preprocessed[old_key])
                del preprocessed[old_key]

        # Handle other field renames and upgrades from old format/name
        field_renames = {
            "task_display_name": "_task_display_name",
            "_downstream_task_ids": "downstream_task_ids",
            "_task_type": "task_type",
            "_outlets": "outlets",
            "_inlets": "inlets",
        }

        for old_name, new_name in field_renames.items():
            if old_name in preprocessed:
                preprocessed[new_name] = preprocessed.pop(old_name)

        # Remove fields that shouldn't be processed
        fields_to_exclude = {
            "python_callable_name",  # Only serves to detect function name changes
            "label",  # Shouldn't be set anymore - computed from task_id now
        }

        for field in fields_to_exclude:
            preprocessed.pop(field, None)

        return preprocessed

    @classmethod
    def detect_dependencies(cls, op: SdkOperator) -> set[DagDependency]:
        """Detect between DAG dependencies for the operator."""
        dependency_detector = DependencyDetector()
        deps = set(dependency_detector.detect_task_dependencies(op))
        return deps

    @classmethod
    def _matches_client_defaults(cls, var: Any, attrname: str) -> bool:
        """
        Check if a field value matches client_defaults and should be excluded.

        This implements the hierarchical defaults optimization where values that match
        client_defaults are omitted from individual task serialization.

        :param var: The value to check
        :param attrname: The attribute name
        :return: True if value matches client_defaults and should be excluded
        """
        try:
            # Get cached client defaults for tasks
            task_defaults = cls.generate_client_defaults()

            # Check if this field is in client_defaults and values match
            if attrname in task_defaults and var == task_defaults[attrname]:
                return True

        except Exception:
            # If anything goes wrong with client_defaults, fall back to normal logic
            pass

        return False

    @classmethod
    def _is_excluded(cls, var: Any, attrname: str, op: DAGNode):
        """
        Determine if a variable is excluded from the serialized object.

        :param var: The value to check. [var == getattr(op, attrname)]
        :param attrname: The name of the attribute to check.
        :param op: The operator to check.
        :return: True if a variable is excluded, False otherwise.
        """
        # Check if value matches client_defaults (hierarchical defaults optimization)
        if cls._matches_client_defaults(var, attrname):
            return True
        schema_defaults = cls.get_schema_defaults("operator")

        if attrname in schema_defaults:
            if schema_defaults[attrname] == var:
                return True
        optional_fields = cls.get_operator_optional_fields_from_schema()
        if var is None:
            return True
        if attrname in optional_fields:
            if var in [[], (), set(), {}]:
                return True

        if var is not None and op.has_dag() and attrname.endswith("_date"):
            # If this date is the same as the matching field in the dag, then
            # don't store it again at the task level.
            dag_date = getattr(op.dag, attrname, None)
            if var is dag_date or var == dag_date:
                return True

        # If none of the exclusion conditions are met, don't exclude the field
        return False

    @classmethod
    def _deserialize_operator_extra_links(
        cls, encoded_op_links: dict[str, str]
    ) -> dict[str, XComOperatorLink]:
        """
        Deserialize Operator Links if the Classes are registered in Airflow Plugins.

        Error is raised if the OperatorLink is not found in Plugins too.

        :param encoded_op_links: Serialized Operator Link
        :return: De-Serialized Operator Link
        """
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()

        if plugins_manager.registered_operator_link_classes is None:
            raise AirflowException("Can't load plugins")
        op_predefined_extra_links = {}

        for name, xcom_key in encoded_op_links.items():
            # Get the name and xcom_key of the encoded operator and use it to create a XComOperatorLink object
            # during deserialization.
            #
            # Example:
            # enc_operator['_operator_extra_links'] =
            # {
            #     'airflow': 'airflow_link_key',
            #     'foo-bar': 'link-key',
            #     'no_response': 'key',
            #     'raise_error': 'key'
            # }

            op_predefined_extra_link = XComOperatorLink(name=name, xcom_key=xcom_key)
            op_predefined_extra_links.update({op_predefined_extra_link.name: op_predefined_extra_link})

        return op_predefined_extra_links

    @classmethod
    def _serialize_operator_extra_links(
        cls, operator_extra_links: Iterable[BaseOperatorLink]
    ) -> dict[str, str]:
        """
        Serialize Operator Links.

        Store the "name" of the link mapped with the xcom_key which can be later used to retrieve this
        operator extra link from XComs.
        For example:
        ``{'link-name-1': 'xcom-key-1'}``

        :param operator_extra_links: Operator Link
        :return: Serialized Operator Link
        """
        return {link.name: link.xcom_key for link in operator_extra_links}

    @classmethod
    def serialize(cls, var: Any, *, strict: bool = False) -> Any:
        # the wonders of multiple inheritance BaseOperator defines an instance method
        return BaseSerialization.serialize(var=var, strict=strict)

    @classmethod
    def deserialize(cls, encoded_var: Any) -> Any:
        return BaseSerialization.deserialize(encoded_var=encoded_var)

    def serialize_for_task_group(self) -> tuple[DAT, Any]:
        """Serialize; required by DAGNode."""
        return DAT.OP, self.task_id

    @property
    def inherits_from_empty_operator(self) -> bool:
        return self._is_empty

    @property
    def inherits_from_skipmixin(self) -> bool:
        return self._can_skip_downstream

    def expand_start_from_trigger(self, *, context: Context) -> bool:
        """
        Get the start_from_trigger value of the current abstract operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original value of start_from_trigger.

        :meta private:
        """
        return self.start_from_trigger

    @classmethod
    def get_serialized_fields(cls):
        """Fields to deserialize from the serialized JSON object."""
        return frozenset(
            {
                "_logger_name",
                "_needs_expansion",
                "_task_display_name",
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
                "ignore_first_depends_on_past",
                "inlets",
                "is_setup",
                "is_teardown",
                "map_index_template",
                "max_active_tis_per_dag",
                "max_active_tis_per_dagrun",
                "max_retry_delay",
                "multiple_outputs",
                "has_on_execute_callback",
                "has_on_failure_callback",
                "has_on_retry_callback",
                "has_on_skipped_callback",
                "has_on_success_callback",
                "on_failure_fail_dagrun",
                "outlets",
                "owner",
                "params",
                "pool",
                "pool_slots",
                "priority_weight",
                "queue",
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

    @classmethod
    @lru_cache(maxsize=1)
    def generate_client_defaults(cls) -> dict[str, Any]:
        """
        Generate `client_defaults` section that only includes values differing from schema defaults.

        This optimizes serialization size by avoiding redundant storage of schema defaults.
        Uses OPERATOR_DEFAULTS as the source of truth for task default values.

        :return: client_defaults dictionary with only non-schema values
        """
        # Get schema defaults for comparison
        schema_defaults = cls.get_schema_defaults("operator")

        client_defaults = {}

        # Only include OPERATOR_DEFAULTS values that differ from schema defaults
        for k, v in OPERATOR_DEFAULTS.items():
            if k not in cls.get_serialized_fields():
                continue

            # Exclude values that are None or empty collections
            if v is None or v in [[], (), set(), {}]:
                continue

            # Check schema defaults first with raw value comparison (fast path)
            if k in schema_defaults and schema_defaults[k] == v:
                continue

            # Use the existing serialize method to ensure consistent format
            serialized_value = cls.serialize(v)
            # Extract just the value part, consistent with serialize_to_json behavior
            if isinstance(serialized_value, dict) and Encoding.TYPE in serialized_value:
                serialized_value = serialized_value[Encoding.VAR]

            # For cases where raw comparison failed but serialized values might match
            # (e.g., timedelta vs float), check again with serialized value
            if k in schema_defaults and schema_defaults[k] == serialized_value:
                continue

            client_defaults[k] = serialized_value

        return client_defaults

    @classmethod
    def _deserialize_field_value(cls, field_name: str, value: Any) -> Any:
        """
        Deserialize a single field value using the same logic as populate_operator.

        This method centralizes field-specific deserialization logic to avoid duplication.

        :param field_name: The name of the field being deserialized
        :param value: The value to deserialize
        :return: The deserialized value
        """
        if field_name == "downstream_task_ids":
            return set(value) if value is not None else set()
        elif field_name in [
            f"has_on_{x}_callback" for x in ("execute", "failure", "success", "retry", "skipped")
        ]:
            return bool(value)
        elif field_name in {"retry_delay", "execution_timeout", "max_retry_delay"}:
            # Reuse existing timedelta deserialization logic
            if value is not None:
                return cls._deserialize_timedelta(value)
            return None
        elif field_name == "resources":
            return Resources.from_dict(value) if value is not None else None
        elif field_name.endswith("_date"):
            return cls._deserialize_datetime(value) if value is not None else None
        else:
            # For all other fields, return as-is (strings, ints, bools, etc.)
            return value

    @classmethod
    def _deserialize_partial_kwargs(
        cls, partial_kwargs_data: dict[str, Any], client_defaults: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        Deserialize partial_kwargs supporting both encoded and non-encoded values.

        This method can handle:
        1. Encoded values: {"__type": "timedelta", "__var": 300.0}
        2. Non-encoded values: 300.0 (for optimization)

        It also applies client_defaults for missing fields.

        :param partial_kwargs_data: The partial_kwargs data from serialized JSON
        :param client_defaults: Client defaults to apply for missing fields
        :return: Deserialized partial_kwargs dict
        """
        deserialized = {}

        for k, v in partial_kwargs_data.items():
            # Check if this is an encoded value (has __type and __var structure)
            if isinstance(v, dict) and Encoding.TYPE in v and Encoding.VAR in v:
                # This is encoded - use full deserialization
                deserialized[k] = cls.deserialize(v)
            else:
                # This is non-encoded (optimized format)
                # Reuse the same deserialization logic from populate_operator
                deserialized[k] = cls._deserialize_field_value(k, v)

        # Apply client_defaults for missing fields if provided
        if client_defaults and "tasks" in client_defaults:
            task_defaults = client_defaults["tasks"]
            for k, default_value in task_defaults.items():
                if k not in deserialized:
                    # Apply the same deserialization logic to client_defaults
                    deserialized[k] = cls._deserialize_field_value(k, default_value)

        return deserialized

    @classmethod
    def _apply_defaults_to_encoded_op(
        cls,
        encoded_op: dict[str, Any],
        client_defaults: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Apply client defaults to encoded operator before deserialization.

        Args:
            encoded_op: The serialized operator data (already includes applied default_args)
            client_defaults: SDK-specific defaults from client_defaults section

        Note: DAG default_args are already applied during task creation in the SDK,
        so encoded_op contains the final resolved values.

        Hierarchy (lowest to highest priority):
        1. client_defaults.tasks (SDK-wide defaults for size optimization)
        2. Explicit task values (already in encoded_op, includes applied default_args)

        Returns a new dict with defaults merged in.
        """
        # Build hierarchy from lowest to highest priority
        result = {}

        # Level 1: Apply client_defaults.tasks (lowest priority)
        # Values are already serialized in generate_client_defaults()
        if client_defaults:
            task_defaults = client_defaults.get("tasks", {})
            result.update(task_defaults)

        # Level 2: Apply explicit task values (highest priority - overrides everything)
        # Note: encoded_op already contains default_args applied during task creation
        result.update(encoded_op)

        return result

    def _iter_all_mapped_downstreams(self) -> Iterator[MappedOperator | SerializedMappedTaskGroup]:
        """
        Return mapped nodes that are direct dependencies of the current task.

        For now, this walks the entire DAG to find mapped nodes that has this
        current task as an upstream. We cannot use ``downstream_list`` since it
        only contains operators, not task groups. In the future, we should
        provide a way to record an DAG node's all downstream nodes instead.

        Note that this does not guarantee the returned tasks actually use the
        current task for task mapping, but only checks those task are mapped
        operators, and are downstreams of the current task.

        To get a list of tasks that uses the current task for task mapping, use
        :meth:`iter_mapped_dependants` instead.
        """

        def _walk_group(group: SerializedTaskGroup) -> Iterable[tuple[str, DAGNode]]:
            """
            Recursively walk children in a task group.

            This yields all direct children (including both tasks and task
            groups), and all children of any task groups.
            """
            for key, child in group.children.items():
                yield key, child
                if isinstance(child, SerializedTaskGroup):
                    yield from _walk_group(child)

        if not (dag := self.dag):
            raise RuntimeError("Cannot check for mapped dependants when not attached to a DAG")
        for key, child in _walk_group(dag.task_group):
            if key == self.node_id:
                continue
            if not isinstance(child, MappedOperator | SerializedMappedTaskGroup):
                continue
            if self.node_id in child.upstream_task_ids:
                yield child

    def iter_mapped_dependants(self) -> Iterator[MappedOperator | SerializedMappedTaskGroup]:
        """
        Return mapped nodes that depend on the current task the expansion.

        For now, this walks the entire DAG to find mapped nodes that has this
        current task as an upstream. We cannot use ``downstream_list`` since it
        only contains operators, not task groups. In the future, we should
        provide a way to record an DAG node's all downstream nodes instead.
        """
        return (
            downstream
            for downstream in self._iter_all_mapped_downstreams()
            if any(p.node_id == self.node_id for p in downstream.iter_mapped_dependencies())
        )

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def iter_mapped_task_groups(self) -> Iterator[SerializedMappedTaskGroup]:
        """
        Return mapped task groups this task belongs to.

        Groups are returned from the innermost to the outmost.

        :meta private:
        """
        if (group := self.task_group) is None:
            return
        yield from group.iter_mapped_task_groups()

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def get_closest_mapped_task_group(self) -> SerializedMappedTaskGroup | None:
        """
        Get the mapped task group "closest" to this task in the DAG.

        :meta private:
        """
        return next(self.iter_mapped_task_groups(), None)

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def get_needs_expansion(self) -> bool:
        """
        Return true if the task is MappedOperator or is in a mapped task group.

        :meta private:
        """
        return self._needs_expansion

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    @methodtools.lru_cache(maxsize=1)
    def get_parse_time_mapped_ti_count(self) -> int:
        """
        Return the number of mapped task instances that can be created on DAG run creation.

        This only considers literal mapped arguments, and would return *None*
        when any non-literal values are used for mapping.

        :raise NotFullyPopulated: If non-literal mapped arguments are encountered.
        :raise NotMapped: If the operator is neither mapped, nor has any parent
            mapped task groups.
        :return: Total number of mapped TIs this task should have.
        """
        from airflow.exceptions import NotMapped

        group = self.get_closest_mapped_task_group()
        if group is None:
            raise NotMapped()
        return group.get_parse_time_mapped_ti_count()


@provide_session
def _create_orm_dagrun(
    *,
    dag: SerializedDAG,
    run_id: str,
    logical_date: datetime.datetime | None,
    data_interval: DataInterval | None,
    run_after: datetime.datetime,
    start_date: datetime.datetime | None,
    conf: Any,
    state: DagRunState | None,
    run_type: DagRunType,
    creating_job_id: int | None,
    backfill_id: NonNegativeInt | None,
    triggered_by: DagRunTriggeredByType,
    triggering_user_name: str | None = None,
    session: Session = NEW_SESSION,
) -> DagRun:
    bundle_version = None
    if not dag.disable_bundle_versioning:
        bundle_version = session.scalar(
            select(DagModel.bundle_version).where(DagModel.dag_id == dag.dag_id),
        )
    dag_version = DagVersion.get_latest_version(dag.dag_id, session=session)
    if not dag_version:
        raise AirflowException(f"Cannot create DagRun for DAG {dag.dag_id} because the dag is not serialized")

    run = DagRun(
        dag_id=dag.dag_id,
        run_id=run_id,
        logical_date=logical_date,
        start_date=start_date,
        run_after=run_after,
        conf=conf,
        state=state,
        run_type=run_type,
        creating_job_id=creating_job_id,
        data_interval=data_interval,
        triggered_by=triggered_by,
        triggering_user_name=triggering_user_name,
        backfill_id=backfill_id,
        bundle_version=bundle_version,
    )
    # Load defaults into the following two fields to ensure result can be serialized detached
    run.log_template_id = int(session.scalar(select(func.max(LogTemplate.__table__.c.id))))
    run.created_dag_version = dag_version
    run.consumed_asset_events = []
    session.add(run)
    session.flush()
    run.dag = dag
    # create the associated task instances
    # state is None at the moment of creation
    run.verify_integrity(session=session, dag_version_id=dag_version.id)
    return run


class SerializedDAG(BaseSerialization):
    """
    A JSON serializable representation of DAG.

    A stringified DAG can only be used in the scope of scheduler and webserver, because fields
    that are not serializable, such as functions and customer defined classes, are casted to
    strings.
    """

    _decorated_fields: ClassVar[set[str]] = {"default_args", "access_control"}

    access_control: dict[str, dict[str, Collection[str]]] | None = None
    catchup: bool
    dag_id: str
    dag_display_name: str
    dagrun_timeout: datetime.timedelta | None
    deadline: list[DeadlineAlert] | DeadlineAlert | None
    default_args: dict[str, Any]
    description: str | None
    disable_bundle_versioning: bool
    doc_md: str | None
    edge_info: dict[str, dict[str, EdgeInfoType]]
    end_date: datetime.datetime | None
    fail_fast: bool
    has_on_failure_callback: bool
    has_on_success_callback: bool
    is_paused_upon_creation: bool | None
    max_active_runs: int
    max_active_tasks: int
    max_consecutive_failed_dag_runs: int
    owner_links: dict[str, str]
    params: ParamsDict  # TODO (GH-52141): Should use a scheduler-specific type.
    partial: bool
    render_template_as_native_obj: bool
    start_date: datetime.datetime | None
    tags: set[str]
    task_dict: dict[str, SerializedOperator]
    task_group: SerializedTaskGroup
    template_searchpath: tuple[str, ...] | None
    timetable: Timetable
    timezone: FixedTimezone | Timezone

    last_loaded: datetime.datetime
    # this will only be set at serialization time
    # it's only use is for determining the relative fileloc based only on the serialize dag
    _processor_dags_folder: str

    def __init__(self, *, dag_id: str) -> None:
        self.catchup = False  # Schema default
        self.dag_id = self.dag_display_name = dag_id
        self.dagrun_timeout = None
        self.deadline = None
        self.default_args = {}
        self.description = None
        self.disable_bundle_versioning = False
        self.doc_md = None
        self.edge_info = {}
        self.end_date = None
        self.fail_fast = False
        self.has_on_failure_callback = False
        self.has_on_success_callback = False
        self.is_paused_upon_creation = None
        self.max_active_runs = 16  # Schema default
        self.max_active_tasks = 16  # Schema default
        self.max_consecutive_failed_dag_runs = 0  # Schema default
        self.owner_links = {}
        self.params = ParamsDict()
        self.partial = False
        self.render_template_as_native_obj = False
        self.start_date = None
        self.tags = set()
        self.template_searchpath = None

    def __repr__(self) -> str:
        return f"<SerializedDAG: {self.dag_id}>"

    @staticmethod
    def __get_constructor_defaults():
        param_to_attr = {
            "description": "_description",
        }
        return {
            param_to_attr.get(k, k): v.default
            for k, v in signature(DAG.__init__).parameters.items()
            if v.default is not v.empty
        }

    _CONSTRUCTOR_PARAMS = __get_constructor_defaults.__func__()  # type: ignore
    del __get_constructor_defaults

    _json_schema: ClassVar[Validator] = lazy_object_proxy.Proxy(load_dag_schema)

    @classmethod
    def get_serialized_fields(cls) -> frozenset[str]:
        return frozenset(
            {
                "access_control",
                "catchup",
                "dag_display_name",
                "dag_id",
                "dagrun_timeout",
                "deadline",
                "default_args",
                "description",
                "disable_bundle_versioning",
                "doc_md",
                "edge_info",
                "end_date",
                "fail_fast",
                "fileloc",
                "is_paused_upon_creation",
                "max_active_runs",
                "max_active_tasks",
                "max_consecutive_failed_dag_runs",
                "owner_links",
                "relative_fileloc",
                "render_template_as_native_obj",
                "start_date",
                "tags",
                "task_group",
                "timetable",
                "timezone",
            }
        )

    @classmethod
    def serialize_dag(cls, dag: DAG) -> dict:
        """Serialize a DAG into a JSON object."""
        try:
            serialized_dag = cls.serialize_to_json(dag, cls._decorated_fields)
            serialized_dag["_processor_dags_folder"] = DAGS_FOLDER
            serialized_dag["tasks"] = [cls.serialize(task) for _, task in dag.task_dict.items()]

            dag_deps = [
                dep
                for task in dag.task_dict.values()
                for dep in SerializedBaseOperator.detect_dependencies(task)
            ]
            dag_deps.extend(DependencyDetector.detect_dag_dependencies(dag))
            serialized_dag["dag_dependencies"] = [x.__dict__ for x in sorted(dag_deps)]
            serialized_dag["task_group"] = TaskGroupSerialization.serialize_task_group(dag.task_group)

            serialized_dag["deadline"] = (
                [deadline.serialize_deadline_alert() for deadline in dag.deadline]
                if isinstance(dag.deadline, list)
                else None
            )

            # Edge info in the JSON exactly matches our internal structure
            serialized_dag["edge_info"] = dag.edge_info
            serialized_dag["params"] = cls._serialize_params_dict(dag.params)

            # has_on_*_callback are only stored if the value is True, as the default is False
            if dag.has_on_success_callback:
                serialized_dag["has_on_success_callback"] = True
            if dag.has_on_failure_callback:
                serialized_dag["has_on_failure_callback"] = True

            # TODO: Move this logic to a better place -- ideally before serializing contents of default_args.
            #   There is some duplication with this and SerializedBaseOperator.partial_kwargs serialization.
            #   Ideally default_args goes through same logic as fields of SerializedBaseOperator.
            if serialized_dag.get("default_args", {}):
                default_args_dict = serialized_dag["default_args"][Encoding.VAR]
                callbacks_to_remove = []
                for k, v in list(default_args_dict.items()):
                    if k in [
                        f"on_{x}_callback" for x in ("execute", "failure", "success", "retry", "skipped")
                    ]:
                        if bool(v):
                            default_args_dict[f"has_{k}"] = True
                        callbacks_to_remove.append(k)
                for k in callbacks_to_remove:
                    del default_args_dict[k]

            return serialized_dag
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"Failed to serialize DAG {dag.dag_id!r}: {e}")

    @classmethod
    def deserialize_dag(
        cls, encoded_dag: dict[str, Any], client_defaults: dict[str, Any] | None = None
    ) -> SerializedDAG:
        """Deserializes a DAG from a JSON object."""
        if "dag_id" not in encoded_dag:
            raise DeserializationError(
                message="Encoded dag object has no dag_id key. "
                "You may need to run `airflow dags reserialize`."
            )

        dag_id = encoded_dag["dag_id"]

        try:
            return cls._deserialize_dag_internal(encoded_dag, client_defaults)
        except (_TimetableNotRegistered, DeserializationError):
            # Let specific errors bubble up unchanged
            raise
        except Exception as err:
            # Wrap all other errors consistently
            raise DeserializationError(dag_id) from err

    @classmethod
    def _deserialize_dag_internal(
        cls, encoded_dag: dict[str, Any], client_defaults: dict[str, Any] | None = None
    ) -> SerializedDAG:
        """Handle the main Dag deserialization logic."""
        dag = SerializedDAG(dag_id=encoded_dag["dag_id"])
        dag.last_loaded = utcnow()

        # Note: Context is passed explicitly through method parameters, no class attributes needed

        for k, v in encoded_dag.items():
            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "tasks":
                SerializedBaseOperator._load_operator_extra_links = cls._load_operator_extra_links
                tasks = {}
                for obj in v:
                    if obj.get(Encoding.TYPE) == DAT.OP:
                        deser = SerializedBaseOperator.deserialize_operator(
                            obj[Encoding.VAR], client_defaults
                        )
                        tasks[deser.task_id] = deser
                k = "task_dict"
                v = tasks
            elif k == "timezone":
                v = cls._deserialize_timezone(v)
            elif k == "dagrun_timeout":
                v = cls._deserialize_timedelta(v)
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
            elif k == "edge_info":
                # Value structure matches exactly
                pass
            elif k == "timetable":
                v = decode_timetable(v)
            elif k == "weight_rule":
                v = decode_priority_weight_strategy(v)
            elif k in cls._decorated_fields:
                v = cls.deserialize(v)
            elif k == "params":
                v = cls._deserialize_params_dict(v)
            elif k == "tags":
                v = set(v)
            # else use v as it is

            object.__setattr__(dag, k, v)

        # Set _task_group
        if "task_group" in encoded_dag:
            tg = TaskGroupSerialization.deserialize_task_group(
                encoded_dag["task_group"],
                None,
                dag.task_dict,
                dag,
            )
            object.__setattr__(dag, "task_group", tg)
        else:
            # This must be old data that had no task_group. Create a root
            # task group and add all tasks to it.
            tg = SerializedTaskGroup(
                group_id=None,
                group_display_name=None,
                prefix_group_id=True,
                parent_group=None,
                dag=dag,
                tooltip="",
            )
            object.__setattr__(dag, "task_group", tg)
            for task in dag.tasks:
                tg.add(task)

        # Set has_on_*_callbacks to True if they exist in Serialized blob as False is the default
        if "has_on_success_callback" in encoded_dag:
            dag.has_on_success_callback = True
        if "has_on_failure_callback" in encoded_dag:
            dag.has_on_failure_callback = True

        if "deadline" in encoded_dag and encoded_dag["deadline"] is not None:
            dag.deadline = (
                [
                    DeadlineAlert.deserialize_deadline_alert(deadline_data)
                    for deadline_data in encoded_dag["deadline"]
                ]
                if encoded_dag["deadline"]
                else None
            )

        keys_to_set_none = dag.get_serialized_fields() - encoded_dag.keys() - cls._CONSTRUCTOR_PARAMS.keys()
        for k in keys_to_set_none:
            setattr(dag, k, None)

        for t in dag.task_dict.values():
            SerializedBaseOperator.set_task_dag_references(t, dag)

        return dag

    @classmethod
    def _is_excluded(cls, var: Any, attrname: str, op: DAGNode):
        # {} is explicitly different from None in the case of DAG-level access control
        # and as a result we need to preserve empty dicts through serialization for this field
        if attrname == "access_control" and var is not None:
            return False
        if attrname == "dag_display_name" and var == op.dag_id:
            return True

        # DAG schema defaults exclusion (same pattern as SerializedBaseOperator)
        dag_schema_defaults = cls.get_schema_defaults("dag")
        if attrname in dag_schema_defaults:
            if dag_schema_defaults[attrname] == var:
                return True

        optional_fields = cls.get_dag_optional_fields_from_schema()
        if var is None:
            return True
        if attrname in optional_fields:
            if var in [[], (), set(), {}]:
                return True

        return super()._is_excluded(var, attrname, op)

    @classmethod
    @lru_cache(maxsize=1)  # Only one type: "dag"
    def get_dag_optional_fields_from_schema(cls) -> set[str]:
        schema_loader = cls._json_schema

        if schema_loader is None:
            return set()

        schema_data = schema_loader.schema
        operator_def = schema_data.get("definitions", {}).get("dag", {})
        operator_fields = set(operator_def.get("properties", {}).keys())
        required_fields = set(operator_def.get("required", []))

        optional_fields = operator_fields - required_fields
        return optional_fields

    @classmethod
    def to_dict(cls, var: Any) -> dict:
        """Stringifies DAGs and operators contained by var and returns a dict of var."""
        # Clear any cached client_defaults to ensure fresh generation for this DAG
        # Clear lru_cache for client defaults
        SerializedBaseOperator.generate_client_defaults.cache_clear()

        json_dict = {"__version": cls.SERIALIZER_VERSION, "dag": cls.serialize_dag(var)}

        # Add client_defaults section with only values that differ from schema defaults
        # for tasks
        client_defaults = SerializedBaseOperator.generate_client_defaults()
        if client_defaults:
            json_dict["client_defaults"] = {"tasks": client_defaults}

        # Validate Serialized DAG with Json Schema. Raises Error if it mismatches
        cls.validate_schema(json_dict)
        return json_dict

    @staticmethod
    def conversion_v1_to_v2(ser_obj: dict):
        dag_dict = ser_obj["dag"]
        dag_renames = [
            ("_dag_id", "dag_id"),
            ("_task_group", "task_group"),
            ("_access_control", "access_control"),
        ]
        task_renames = [("_task_type", "task_type"), ("task_display_name", "_task_display_name")]
        #
        tasks_remove = [
            "_log_config_logger_name",
            "deps",
            "sla",
            # Operator extra links from Airflow 2 won't work anymore, only new ones, so remove these
            "_operator_extra_links",
        ]

        ser_obj["__version"] = 2

        def replace_dataset_in_str(s):
            return s.replace("Dataset", "Asset").replace("dataset", "asset")

        def _replace_dataset_with_asset_in_timetables(obj, parent_key=None):
            if isinstance(obj, dict):
                new_obj = {}
                for k, v in obj.items():
                    new_key = replace_dataset_in_str(k) if isinstance(k, str) else k
                    # Don't replace uri values
                    if new_key == "uri":
                        new_obj[new_key] = v
                    else:
                        new_value = (
                            replace_dataset_in_str(v)
                            if isinstance(v, str)
                            else _replace_dataset_with_asset_in_timetables(v, parent_key=new_key)
                        )
                        new_obj[new_key] = new_value
                # Insert "name" and "group" if this is inside the 'objects' list
                if parent_key == "objects":
                    new_obj["name"] = None
                    new_obj["group"] = None
                return new_obj

            elif isinstance(obj, list):
                return [_replace_dataset_with_asset_in_timetables(i, parent_key=parent_key) for i in obj]

            return obj

        def _create_compat_timetable(value):
            from airflow import settings
            from airflow.sdk.definitions.dag import _create_timetable

            if tzs := dag_dict.get("timezone"):
                timezone = decode_timezone(tzs)
            else:
                timezone = settings.TIMEZONE
            timetable = _create_timetable(value, timezone)
            return encode_timetable(timetable)

        for old, new in dag_renames:
            if old in dag_dict:
                dag_dict[new] = dag_dict.pop(old)

        if default_args := dag_dict.get("default_args"):
            for k in tasks_remove:
                default_args["__var"].pop(k, None)

        if timetable := dag_dict.get("timetable"):
            if timetable["__type"] in {
                "airflow.timetables.simple.DatasetTriggeredTimetable",
                "airflow.timetables.datasets.DatasetOrTimeSchedule",
            }:
                dag_dict["timetable"] = _replace_dataset_with_asset_in_timetables(dag_dict["timetable"])
        elif (sched := dag_dict.pop("schedule_interval", None)) is None:
            dag_dict["timetable"] = _create_compat_timetable(None)
        elif isinstance(sched, str):
            dag_dict["timetable"] = _create_compat_timetable(sched)
        elif sched.get("__type") == "timedelta":
            dag_dict["timetable"] = _create_compat_timetable(datetime.timedelta(seconds=sched["__var"]))
        elif sched.get("__type") == "relativedelta":
            dag_dict["timetable"] = _create_compat_timetable(decode_relativedelta(sched["__var"]))
        else:
            # We should maybe convert this to None and warn instead
            raise ValueError(f"Unknown schedule_interval field {sched!r}")

        if "dag_dependencies" in dag_dict:
            for dep in dag_dict["dag_dependencies"]:
                dep_type = dep.get("dependency_type")
                if dep_type in ("dataset", "dataset-alias"):
                    dep["dependency_type"] = dep_type.replace("dataset", "asset")

                if not dep.get("label"):
                    dep["label"] = dep["dependency_id"]

                for fld in ("target", "source"):
                    val = dep.get(fld)
                    if val == dep_type and val in ("dataset", "dataset-alias"):
                        dep[fld] = dep[fld].replace("dataset", "asset")
                    elif val.startswith("dataset:"):
                        dep[fld] = dep[fld].replace("dataset:", "asset:")
                    elif val.startswith("dataset-alias:"):
                        dep[fld] = dep[fld].replace("dataset-alias:", "asset-alias:")

        for task in dag_dict["tasks"]:
            task_var: dict = task["__var"]
            if "airflow.ti_deps.deps.ready_to_reschedule.ReadyToRescheduleDep" in task_var.get("deps", []):
                task_var["_is_sensor"] = True
            for k in tasks_remove:
                task_var.pop(k, None)
            for old, new in task_renames:
                if old in task_var:
                    task_var[new] = task_var.pop(old)
            for item in itertools.chain(*(task_var.get(key, []) for key in ("inlets", "outlets"))):
                original_item_type = item["__type"]
                if isinstance(item, dict) and "__type" in item:
                    item["__type"] = replace_dataset_in_str(original_item_type)

                var_ = item["__var"]
                if original_item_type == "dataset":
                    var_["name"] = var_["uri"]
                var_["group"] = "asset"

            for k, v in list(task_var.items()):
                op_defaults = SerializedDAG.get_schema_defaults("operator")
                if k in op_defaults and v == op_defaults[k]:
                    del task_var[k]

        # Set on the root TG
        dag_dict["task_group"]["group_display_name"] = ""

    @staticmethod
    def conversion_v2_to_v3(ser_obj: dict):
        # V2 to V3 changes are minimal - mainly client_defaults optimization and
        # field presence differences. Only version bump needed.
        ser_obj["__version"] = 3

    @classmethod
    def from_dict(cls, serialized_obj: dict) -> SerializedDAG:
        """Deserializes a python dict in to the DAG and operators it contains."""
        ver = serialized_obj.get("__version", "<not present>")
        if ver not in (1, 2, 3):
            raise ValueError(f"Unsure how to deserialize version {ver!r}")
        if ver == 1:
            cls.conversion_v1_to_v2(serialized_obj)
        if ver == 2:
            cls.conversion_v2_to_v3(serialized_obj)

        # Extract client_defaults for hierarchical defaults resolution
        client_defaults = serialized_obj.get("client_defaults", {})

        # Pass client_defaults directly to deserialize_dag
        return cls.deserialize_dag(serialized_obj["dag"], client_defaults)

    @classmethod
    @provide_session
    def bulk_write_to_db(
        cls,
        bundle_name: str,
        bundle_version: str | None,
        dags: Collection[DAG | LazyDeserializedDAG],
        parse_duration: float | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Ensure the DagModel rows for the given dags are up-to-date in the dag table in the DB.

        :param dags: the DAG objects to save to the DB
        :return: None
        """
        if not dags:
            return

        from airflow.dag_processing.collection import AssetModelOperation, DagModelOperation

        log.info("Sync %s DAGs", len(dags))
        dag_op = DagModelOperation(
            bundle_name=bundle_name,
            bundle_version=bundle_version,
            dags={d.dag_id: LazyDeserializedDAG.from_dag(d) for d in dags},
        )

        orm_dags = dag_op.add_dags(session=session)
        dag_op.update_dags(orm_dags, parse_duration, session=session)

        asset_op = AssetModelOperation.collect(dag_op.dags)

        orm_assets = asset_op.sync_assets(session=session)
        orm_asset_aliases = asset_op.sync_asset_aliases(session=session)
        session.flush()  # This populates id so we can create fks in later calls.

        orm_dags = dag_op.find_orm_dags(session=session)  # Refetch so relationship is up to date.
        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.add_dag_asset_alias_references(orm_dags, orm_asset_aliases, session=session)
        asset_op.add_dag_asset_name_uri_references(session=session)
        asset_op.add_task_asset_references(orm_dags, orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        session.flush()  # Activation is needed when we add trigger references.

        asset_op.add_asset_trigger_references(orm_assets, session=session)
        dag_op.update_dag_asset_expression(orm_dags=orm_dags, orm_assets=orm_assets)
        session.flush()

    @property
    def tasks(self) -> Sequence[SerializedOperator]:
        return list(self.task_dict.values())

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict)

    @property
    def roots(self) -> list[SerializedOperator]:
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def owner(self) -> str:
        return ", ".join({t.owner for t in self.tasks})

    @property
    def timetable_summary(self) -> str:
        return self.timetable.summary

    def has_task(self, task_id: str) -> bool:
        return task_id in self.task_dict

    def get_task(self, task_id: str) -> SerializedOperator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")

    @property
    def task_group_dict(self):
        return {k: v for k, v in self.task_group.get_task_group_dict().items() if k is not None}

    def partial_subset(
        self,
        task_ids: str | Iterable[str],
        include_downstream: bool = False,
        include_upstream: bool = True,
        include_direct_upstream: bool = False,
        exclude_original: bool = False,
    ):
        from airflow.models.mappedoperator import MappedOperator as SerializedMappedOperator

        def is_task(obj) -> TypeGuard[SerializedOperator]:
            return isinstance(obj, (SerializedMappedOperator, SerializedBaseOperator))

        # deep-copying self.task_dict and self.task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        memo = {id(self.task_dict): None, id(self.task_group): None}
        dag = copy.deepcopy(self, memo)

        if isinstance(task_ids, str):
            matched_tasks = [t for t in self.tasks if task_ids in t.task_id]
        else:
            matched_tasks = [t for t in self.tasks if t.task_id in task_ids]

        also_include_ids: set[str] = set()
        for t in matched_tasks:
            if include_downstream:
                for rel in t.get_flat_relatives(upstream=False):
                    also_include_ids.add(rel.task_id)
                    if rel not in matched_tasks:  # if it's in there, we're already processing it
                        # need to include setups and teardowns for tasks that are in multiple
                        # non-collinear setup/teardown paths
                        if not rel.is_setup and not rel.is_teardown:
                            also_include_ids.update(
                                x.task_id for x in rel.get_upstreams_only_setups_and_teardowns()
                            )
            if include_upstream:
                also_include_ids.update(x.task_id for x in t.get_upstreams_follow_setups())
            else:
                if not t.is_setup and not t.is_teardown:
                    also_include_ids.update(x.task_id for x in t.get_upstreams_only_setups_and_teardowns())
            if t.is_setup and not include_downstream:
                also_include_ids.update(x.task_id for x in t.downstream_list if x.is_teardown)

        also_include: list[SerializedOperator] = [self.task_dict[x] for x in also_include_ids]
        direct_upstreams: list[SerializedOperator] = []
        if include_direct_upstream:
            for t in itertools.chain(matched_tasks, also_include):
                upstream = (u for u in t.upstream_list if is_task(u))
                direct_upstreams.extend(upstream)

        # Make sure to not recursively deepcopy the dag or task_group while copying the task.
        # task_group is reset later
        def _deepcopy_task(t) -> SerializedOperator:
            memo.setdefault(id(t.task_group), None)
            return copy.deepcopy(t, memo)

        # Compiling the unique list of tasks that made the cut
        if exclude_original:
            matched_tasks = []
        dag.task_dict = {
            t.task_id: _deepcopy_task(t)
            for t in itertools.chain(matched_tasks, also_include, direct_upstreams)
        }

        def filter_task_group(group, parent_group):
            """Exclude tasks not included in the partial dag from the given TaskGroup."""
            # We want to deepcopy _most but not all_ attributes of the task group, so we create a shallow copy
            # and then manually deep copy the instances. (memo argument to deepcopy only works for instances
            # of classes, not "native" properties of an instance)
            copied = copy.copy(group)

            memo[id(group.children)] = {}
            if parent_group:
                memo[id(group.parent_group)] = parent_group
            for attr in type(group).__slots__:
                value = getattr(group, attr)
                value = copy.deepcopy(value, memo)
                object.__setattr__(copied, attr, value)

            proxy = weakref.proxy(copied)

            for child in group.children.values():
                if is_task(child):
                    if child.task_id in dag.task_dict:
                        task = copied.children[child.task_id] = dag.task_dict[child.task_id]
                        task.task_group = proxy
                else:
                    filtered_child = filter_task_group(child, proxy)

                    # Only include this child TaskGroup if it is non-empty.
                    if filtered_child.children:
                        copied.children[child.group_id] = filtered_child

            return copied

        object.__setattr__(dag, "task_group", filter_task_group(self.task_group, None))

        # Removing upstream/downstream references to tasks and TaskGroups that did not make
        # the cut.
        groups = dag.task_group.get_task_group_dict()
        for g in groups.values():
            g.upstream_group_ids.intersection_update(groups)
            g.downstream_group_ids.intersection_update(groups)
            g.upstream_task_ids.intersection_update(dag.task_dict)
            g.downstream_task_ids.intersection_update(dag.task_dict)

        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # make the cut
            t.upstream_task_ids.intersection_update(dag.task_dict)
            t.downstream_task_ids.intersection_update(dag.task_dict)

        dag.partial = len(dag.tasks) < len(self.tasks)

        return dag

    @cached_property
    def _time_restriction(self) -> TimeRestriction:
        start_dates = [t.start_date for t in self.tasks if t.start_date]
        if self.start_date is not None:
            start_dates.append(self.start_date)
        earliest = None
        if start_dates:
            earliest = coerce_datetime(min(start_dates))
        latest = coerce_datetime(self.end_date)
        end_dates = [t.end_date for t in self.tasks if t.end_date]
        if len(end_dates) == len(self.tasks):  # not exists null end_date
            if self.end_date is not None:
                end_dates.append(self.end_date)
            if end_dates:
                latest = coerce_datetime(max(end_dates))
        return TimeRestriction(earliest, latest, self.catchup)

    def next_dagrun_info(
        self,
        last_automated_dagrun: None | DataInterval,
        *,
        restricted: bool = True,
    ) -> DagRunInfo | None:
        """
        Get information about the next DagRun of this dag after ``date_last_automated_dagrun``.

        This calculates what time interval the next DagRun should operate on
        (its logical date) and when it can be scheduled, according to the
        dag's timetable, start_date, end_date, etc. This doesn't check max
        active run or any other "max_active_tasks" type limits, but only
        performs calculations based on the various date and interval fields of
        this dag and its tasks.

        :param last_automated_dagrun: The ``max(logical_date)`` of
            existing "automated" DagRuns for this dag (scheduled or backfill,
            but not manual).
        :param restricted: If set to *False* (default is *True*), ignore
            ``start_date``, ``end_date``, and ``catchup`` specified on the DAG
            or tasks.
        :return: DagRunInfo of the next dagrun, or None if a dagrun is not
            going to be scheduled.
        """
        if restricted:
            restriction = self._time_restriction
        else:
            restriction = TimeRestriction(earliest=None, latest=None, catchup=True)
        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=last_automated_dagrun,
                restriction=restriction,
            )
        except Exception:
            log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                last_automated_dagrun,
                self.dag_id,
            )
            info = None
        return info

    def iter_dagrun_infos_between(
        self,
        earliest: datetime.datetime | None,
        latest: datetime.datetime,
        *,
        align: bool = True,
    ) -> Iterable[DagRunInfo]:
        """
        Yield DagRunInfo using this DAG's timetable between given interval.

        DagRunInfo instances yielded if their ``logical_date`` is not earlier
        than ``earliest``, nor later than ``latest``. The instances are ordered
        by their ``logical_date`` from earliest to latest.

        If ``align`` is ``False``, the first run will happen immediately on
        ``earliest``, even if it does not fall on the logical timetable schedule.
        The default is ``True``.

        Example: A DAG is scheduled to run every midnight (``0 0 * * *``). If
        ``earliest`` is ``2021-06-03 23:00:00``, the first DagRunInfo would be
        ``2021-06-03 23:00:00`` if ``align=False``, and ``2021-06-04 00:00:00``
        if ``align=True``.
        """
        if earliest is None:
            earliest = self._time_restriction.earliest
        if earliest is None:
            raise ValueError("earliest was None and we had no value in time_restriction to fallback on")
        earliest = coerce_datetime(earliest)
        latest = coerce_datetime(latest)

        restriction = TimeRestriction(earliest, latest, catchup=True)

        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=restriction,
            )
        except Exception:
            log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                None,
                self.dag_id,
            )
            info = None

        if info is None:
            # No runs to be scheduled between the user-supplied timeframe. But
            # if align=False, "invent" a data interval for the timeframe itself.
            if not align:
                yield DagRunInfo.interval(earliest, latest)
            return

        # If align=False and earliest does not fall on the timetable's logical
        # schedule, "invent" a data interval for it.
        if not align and info.logical_date != earliest:
            yield DagRunInfo.interval(earliest, info.data_interval.start)

        # Generate naturally according to schedule.
        while info is not None:
            yield info
            try:
                info = self.timetable.next_dagrun_info(
                    last_automated_data_interval=info.data_interval,
                    restriction=restriction,
                )
            except Exception:
                log.exception(
                    "Failed to fetch run info after data interval %s for DAG %r",
                    info.data_interval if info else "<NONE>",
                    self.dag_id,
                )
                break

    @provide_session
    def get_concurrency_reached(self, session=NEW_SESSION) -> bool:
        """Return a boolean indicating whether the max_active_tasks limit for this DAG has been reached."""
        from airflow.models.taskinstance import TaskInstance

        total_tasks = session.scalar(
            select(func.count(TaskInstance.task_id)).where(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.state == TaskInstanceState.RUNNING,
            )
        )
        return total_tasks >= self.max_active_tasks

    @provide_session
    def create_dagrun(
        self,
        *,
        run_id: str,
        logical_date: datetime.datetime | None = None,
        data_interval: tuple[datetime.datetime, datetime.datetime] | None = None,
        run_after: datetime.datetime,
        conf: dict | None = None,
        run_type: DagRunType,
        triggered_by: DagRunTriggeredByType,
        triggering_user_name: str | None = None,
        state: DagRunState,
        start_date: datetime.datetime | None = None,
        creating_job_id: int | None = None,
        backfill_id: NonNegativeInt | None = None,
        session: Session = NEW_SESSION,
    ) -> DagRun:
        """
        Create a run for this DAG to run its tasks.

        :param run_id: ID of the dag_run
        :param logical_date: date of execution
        :param run_after: the datetime before which dag won't run
        :param conf: Dict containing configuration/parameters to pass to the DAG
        :param triggered_by: the entity which triggers the dag_run
        :param triggering_user_name: the user name who triggers the dag_run
        :param start_date: the date this dag run should be evaluated
        :param creating_job_id: ID of the job creating this DagRun
        :param backfill_id: ID of the backfill run if one exists
        :param session: Unused. Only added in compatibility with database isolation mode
        :return: The created DAG run.

        :meta private:
        """
        logical_date = coerce_datetime(logical_date)
        # For manual runs where logical_date is None, ensure no data_interval is set.
        if logical_date is None and data_interval is not None:
            raise ValueError("data_interval must be None when logical_date is None")

        if data_interval and not isinstance(data_interval, DataInterval):
            data_interval = DataInterval(*map(coerce_datetime, data_interval))

        if isinstance(run_type, DagRunType):
            pass
        elif isinstance(run_type, str):  # Ensure the input value is valid.
            run_type = DagRunType(run_type)
        else:
            raise ValueError(f"run_type should be a DagRunType, not {type(run_type)}")

        if not isinstance(run_id, str):
            raise ValueError(f"`run_id` should be a str, not {type(run_id)}")

        # This is also done on the DagRun model class, but SQLAlchemy column
        # validator does not work well for some reason.
        if not re.match(RUN_ID_REGEX, run_id):
            regex = airflow_conf.get("scheduler", "allowed_run_id_pattern").strip()
            if not regex or not re.match(regex, run_id):
                raise ValueError(
                    f"The run_id provided '{run_id}' does not match regex pattern "
                    f"'{regex}' or '{RUN_ID_REGEX}'"
                )

        # Prevent a manual run from using an ID that looks like a scheduled run.
        if run_type == DagRunType.MANUAL:
            if (inferred_run_type := DagRunType.from_run_id(run_id)) != DagRunType.MANUAL:
                raise ValueError(
                    f"A {run_type.value} DAG run cannot use ID {run_id!r} since it "
                    f"is reserved for {inferred_run_type.value} runs"
                )

        # todo: AIP-78 add verification that if run type is backfill then we have a backfill id

        # create a copy of params before validating
        copied_params = copy.deepcopy(self.params)
        if conf:
            copied_params.update(conf)
        copied_params.validate()
        orm_dagrun = _create_orm_dagrun(
            dag=self,
            run_id=run_id,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=coerce_datetime(run_after),
            start_date=coerce_datetime(start_date),
            conf=conf,
            state=state,
            run_type=run_type,
            creating_job_id=creating_job_id,
            backfill_id=backfill_id,
            triggered_by=triggered_by,
            triggering_user_name=triggering_user_name,
            session=session,
        )

        if self.deadline:
            for deadline in cast("list", self.deadline):
                if isinstance(deadline.reference, DeadlineReference.TYPES.DAGRUN):
                    session.add(
                        Deadline(
                            deadline_time=deadline.reference.evaluate_with(
                                session=session,
                                interval=deadline.interval,
                                dag_id=self.dag_id,
                                run_id=run_id,
                            ),
                            callback=deadline.callback,
                            dagrun_id=orm_dagrun.id,
                        )
                    )

        return orm_dagrun

    @provide_session
    def set_task_instance_state(
        self,
        *,
        task_id: str,
        map_indexes: Collection[int] | None = None,
        run_id: str | None = None,
        state: TaskInstanceState,
        upstream: bool = False,
        downstream: bool = False,
        future: bool = False,
        past: bool = False,
        commit: bool = True,
        session=NEW_SESSION,
    ) -> list[TaskInstance]:
        """
        Set the state of a TaskInstance and clear downstream tasks in failed or upstream_failed state.

        :param task_id: Task ID of the TaskInstance
        :param map_indexes: Only set TaskInstance if its map_index matches.
            If None (default), all mapped TaskInstances of the task are set.
        :param run_id: The run_id of the TaskInstance
        :param state: State to set the TaskInstance to
        :param upstream: Include all upstream tasks of the given task_id
        :param downstream: Include all downstream tasks of the given task_id
        :param future: Include all future TaskInstances of the given task_id
        :param commit: Commit changes
        :param past: Include all past TaskInstances of the given task_id
        """
        from airflow.api.common.mark_tasks import set_state

        task = self.get_task(task_id)
        task.dag = self

        tasks_to_set_state: list[SerializedOperator | tuple[SerializedOperator, int]]
        if map_indexes is None:
            tasks_to_set_state = [task]
        else:
            tasks_to_set_state = [(task, map_index) for map_index in map_indexes]

        altered = set_state(
            tasks=tasks_to_set_state,
            run_id=run_id,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
            state=state,
            commit=commit,
            session=session,
        )

        if not commit:
            return altered

        # Clear downstream tasks that are in failed/upstream_failed state to resume them.
        # Flush the session so that the tasks marked success are reflected in the db.
        session.flush()
        subset = self.partial_subset(
            task_ids={task_id},
            include_downstream=True,
            include_upstream=False,
        )

        # Raises an error if not found
        dr_id, logical_date = session.execute(
            select(DagRun.id, DagRun.logical_date).where(
                DagRun.run_id == run_id, DagRun.dag_id == self.dag_id
            )
        ).one()

        # Now we want to clear downstreams of tasks that had their state set...
        clear_kwargs = {
            "only_failed": True,
            "session": session,
            # Exclude the task itself from being cleared.
            "exclude_task_ids": frozenset((task_id,)),
        }
        if not future and not past:  # Simple case 1: we're only dealing with exactly one run.
            clear_kwargs["run_id"] = run_id
            subset.clear(**clear_kwargs)
        elif future and past:  # Simple case 2: we're clearing ALL runs.
            subset.clear(**clear_kwargs)
        else:  # Complex cases: we may have more than one run, based on a date range.
            # Make 'future' and 'past' make some sense when multiple runs exist
            # for the same logical date. We order runs by their id and only
            # clear runs have larger/smaller ids.
            exclude_run_id_stmt = select(DagRun.run_id).where(DagRun.logical_date == logical_date)
            if future:
                clear_kwargs["start_date"] = logical_date
                exclude_run_id_stmt = exclude_run_id_stmt.where(DagRun.id > dr_id)
            else:
                clear_kwargs["end_date"] = logical_date
                exclude_run_id_stmt = exclude_run_id_stmt.where(DagRun.id < dr_id)
            subset.clear(exclude_run_ids=frozenset(session.scalars(exclude_run_id_stmt)), **clear_kwargs)
        return altered

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        start_date: datetime.datetime | None,
        end_date: datetime.datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
    ) -> Iterable[TaskInstance]: ...  # pragma: no cover

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True],
        start_date: datetime.datetime | None,
        end_date: datetime.datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
    ) -> set[TaskInstanceKey]: ...  # pragma: no cover

    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True, None] = None,
        start_date: datetime.datetime | None,
        end_date: datetime.datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
    ) -> Iterable[TaskInstance] | set[TaskInstanceKey]:
        from airflow.models.taskinstance import TaskInstance

        # If we are looking at dependent dags we want to avoid UNION calls
        # in SQL (it doesn't play nice with fields that have no equality operator,
        # like JSON types), we instead build our result set separately.
        #
        # This will be empty if we are only looking at one dag, in which case
        # we can return the filtered TI query object directly.
        result: set[TaskInstanceKey] = set()

        # Do we want full objects, or just the primary columns?
        if as_pk_tuple:
            tis = select(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.run_id,
                TaskInstance.map_index,
            )
        else:
            tis = select(TaskInstance)
        tis = tis.join(TaskInstance.dag_run)

        if self.partial:
            tis = tis.where(TaskInstance.dag_id == self.dag_id, TaskInstance.task_id.in_(self.task_ids))
        else:
            tis = tis.where(TaskInstance.dag_id == self.dag_id)
        if run_id:
            tis = tis.where(TaskInstance.run_id == run_id)
        if start_date:
            tis = tis.where(DagRun.logical_date >= start_date)
        if task_ids is not None:
            tis = tis.where(TaskInstance.ti_selector_condition(task_ids))
        if end_date:
            tis = tis.where(DagRun.logical_date <= end_date)

        if state:
            if isinstance(state, (str, TaskInstanceState)):
                tis = tis.where(TaskInstance.state == state)
            elif len(state) == 1:
                tis = tis.where(TaskInstance.state == state[0])
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.where(TaskInstance.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.where(
                            or_(TaskInstance.state.in_(not_none_state), TaskInstance.state.is_(None))
                        )
                else:
                    tis = tis.where(TaskInstance.state.in_(state))

        if exclude_run_ids:
            tis = tis.where(TaskInstance.run_id.not_in(exclude_run_ids))

        if result or as_pk_tuple:
            # Only execute the `ti` query if we have also collected some other results
            if as_pk_tuple:
                tis_query = session.execute(tis).all()
                result.update(TaskInstanceKey(**cols._mapping) for cols in tis_query)
            else:
                result.update(ti.key for ti in session.scalars(tis))

            if exclude_task_ids is not None:
                result = {
                    task
                    for task in result
                    if task.task_id not in exclude_task_ids
                    and (task.task_id, task.map_index) not in exclude_task_ids
                }

        if as_pk_tuple:
            return result
        if result:
            # We've been asked for objects, lets combine it all back in to a result set
            ti_filters = TaskInstance.filter_for_tis(result)
            if ti_filters is not None:
                tis = select(TaskInstance).where(ti_filters)
        elif exclude_task_ids is None:
            pass  # Disable filter if not set.
        elif isinstance(next(iter(exclude_task_ids), None), str):
            tis = tis.where(TaskInstance.task_id.notin_(exclude_task_ids))
        else:
            tis = tis.where(tuple_(TaskInstance.task_id, TaskInstance.map_index).not_in(exclude_task_ids))

        return tis

    @overload
    def clear(
        self,
        *,
        dry_run: Literal[True],
        task_ids: Collection[str | tuple[str, int]] | None = None,
        run_id: str,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> list[TaskInstance]: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        run_id: str,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: Literal[False] = False,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> int: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        dry_run: Literal[True],
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> list[TaskInstance]: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: Literal[False] = False,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> int: ...  # pragma: no cover

    @provide_session
    def clear(
        self,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        *,
        run_id: str | None = None,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: bool = False,
        session: Session = NEW_SESSION,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
        run_on_latest_version: bool = False,
    ) -> int | Iterable[TaskInstance]:
        """
        Clear a set of task instances associated with the current dag for a specified date range.

        :param task_ids: List of task ids or (``task_id``, ``map_index``) tuples to clear
        :param run_id: The run_id for which the tasks should be cleared
        :param start_date: The minimum logical_date to clear
        :param end_date: The maximum logical_date to clear
        :param only_failed: Only clear failed tasks
        :param only_running: Only clear running tasks.
        :param dag_run_state: state to set DagRun to. If set to False, dagrun state will not
            be changed.
        :param dry_run: Find the tasks to clear but don't clear them.
        :param run_on_latest_version: whether to run on latest serialized DAG and Bundle version
        :param session: The sqlalchemy session to use
        :param exclude_task_ids: A set of ``task_id`` or (``task_id``, ``map_index``)
            tuples that should not be cleared
        :param exclude_run_ids: A set of ``run_id`` or (``run_id``)
        """
        from airflow.models.taskinstance import clear_task_instances

        state: list[TaskInstanceState] = []
        if only_failed:
            state += [TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
        if only_running:
            # Yes, having `+=` doesn't make sense, but this was the existing behaviour
            state += [TaskInstanceState.RUNNING]

        tis = self._get_task_instances(
            task_ids=task_ids,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id,
            state=state,
            session=session,
            exclude_task_ids=exclude_task_ids,
            exclude_run_ids=exclude_run_ids,
        )

        if dry_run:
            return session.scalars(tis).all()

        tis = session.scalars(tis).all()

        count = len(list(tis))
        if count == 0:
            return 0

        clear_task_instances(
            list(tis),
            session,
            dag_run_state=dag_run_state,
            run_on_latest_version=run_on_latest_version,
        )

        session.flush()
        return count

    @classmethod
    def clear_dags(
        cls,
        dags,
        start_date=None,
        end_date=None,
        only_failed=False,
        only_running=False,
        dag_run_state=DagRunState.QUEUED,
        dry_run=False,
    ):
        def _coerce_dag(dag):
            if isinstance(dag, SerializedDAG):
                return dag
            return SerializedDAG.deserialize_dag(SerializedDAG.serialize_dag(dag))

        if dry_run:
            tis = itertools.chain.from_iterable(
                _coerce_dag(dag).clear(
                    start_date=start_date,
                    end_date=end_date,
                    only_failed=only_failed,
                    only_running=only_running,
                    dag_run_state=dag_run_state,
                    dry_run=True,
                )
                for dag in dags
            )
            return list(tis)

        return sum(
            _coerce_dag(dag).clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                dag_run_state=dag_run_state,
                dry_run=False,
            )
            for dag in dags
        )

    def get_edge_info(self, upstream_task_id: str, downstream_task_id: str) -> EdgeInfoType:
        """Return edge information for the given pair of tasks or an empty edge if there is no information."""
        # Note - older serialized dags may not have edge_info being a dict at all
        empty = cast("EdgeInfoType", {})
        if self.edge_info:
            return self.edge_info.get(upstream_task_id, {}).get(downstream_task_id, empty)
        return empty


class TaskGroupSerialization(BaseSerialization):
    """JSON serializable representation of a task group."""

    @classmethod
    def serialize_task_group(cls, task_group: TaskGroup) -> dict[str, Any] | None:
        """Serialize TaskGroup into a JSON object."""
        if not task_group:
            return None

        # task_group.xxx_ids needs to be sorted here, because task_group.xxx_ids is a set,
        # when converting set to list, the order is uncertain.
        # When calling json.dumps(self.data, sort_keys=True) to generate dag_hash, misjudgment will occur
        encoded = {
            "_group_id": task_group._group_id,
            "group_display_name": task_group.group_display_name,
            "prefix_group_id": task_group.prefix_group_id,
            "tooltip": task_group.tooltip,
            "ui_color": task_group.ui_color,
            "ui_fgcolor": task_group.ui_fgcolor,
            "children": {
                label: child.serialize_for_task_group() for label, child in task_group.children.items()
            },
            "upstream_group_ids": cls.serialize(sorted(task_group.upstream_group_ids)),
            "downstream_group_ids": cls.serialize(sorted(task_group.downstream_group_ids)),
            "upstream_task_ids": cls.serialize(sorted(task_group.upstream_task_ids)),
            "downstream_task_ids": cls.serialize(sorted(task_group.downstream_task_ids)),
        }

        if isinstance(task_group, MappedTaskGroup):
            expand_input = task_group._expand_input
            encoded["expand_input"] = {
                "type": expand_input.EXPAND_INPUT_TYPE,
                "value": cls.serialize(expand_input.value),
            }
            encoded["is_mapped"] = True

        return encoded

    @classmethod
    def deserialize_task_group(
        cls,
        encoded_group: dict[str, Any],
        parent_group: SerializedTaskGroup | None,
        task_dict: dict[str, SerializedOperator],
        dag: SerializedDAG,
    ) -> SerializedTaskGroup:
        """Deserializes a TaskGroup from a JSON object."""
        group_id = cls.deserialize(encoded_group["_group_id"])
        kwargs = {
            key: cls.deserialize(encoded_group[key])
            for key in ["prefix_group_id", "tooltip", "ui_color", "ui_fgcolor"]
        }
        kwargs["group_display_name"] = cls.deserialize(encoded_group.get("group_display_name", ""))

        if not encoded_group.get("is_mapped"):
            group = SerializedTaskGroup(group_id=group_id, parent_group=parent_group, dag=dag, **kwargs)
        else:
            xi = encoded_group["expand_input"]
            group = SerializedMappedTaskGroup(
                group_id=group_id,
                parent_group=parent_group,
                dag=dag,
                expand_input=_ExpandInputRef(xi["type"], cls.deserialize(xi["value"])).deref(dag),
                **kwargs,
            )

        def set_ref(task: SerializedOperator) -> SerializedOperator:
            task.task_group = weakref.proxy(group)
            return task

        group.children = {
            label: (
                set_ref(task_dict[val])
                if _type == DAT.OP
                else cls.deserialize_task_group(val, group, task_dict, dag=dag)
            )
            for label, (_type, val) in sorted(encoded_group["children"].items())
        }
        group.upstream_group_ids.update(cls.deserialize(encoded_group["upstream_group_ids"]))
        group.downstream_group_ids.update(cls.deserialize(encoded_group["downstream_group_ids"]))
        group.upstream_task_ids.update(cls.deserialize(encoded_group["upstream_task_ids"]))
        group.downstream_task_ids.update(cls.deserialize(encoded_group["downstream_task_ids"]))
        return group


class SerializedAssetWatcher(AssetWatcher):
    """JSON serializable representation of an asset watcher."""

    trigger: dict


def _has_kubernetes(attempt_import: bool = False) -> bool:
    """
    Check if kubernetes libraries are available.

    :param attempt_import: If true, attempt to import kubernetes libraries if not already loaded. If
        False, only check if already in sys.modules (avoids expensive import).
    :return: True if kubernetes libraries are available, False otherwise.
    """
    global HAS_KUBERNETES
    if "HAS_KUBERNETES" in globals():
        return HAS_KUBERNETES

    # Check if kubernetes is already imported before triggering expensive import
    if "kubernetes.client" not in sys.modules and not attempt_import:
        HAS_KUBERNETES = False
        return False

    # Loading kube modules is expensive, so delay it until the last moment

    try:
        from kubernetes.client import models as k8s

        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

        globals()["k8s"] = k8s
        globals()["PodGenerator"] = PodGenerator
        HAS_KUBERNETES = True
    except ImportError:
        HAS_KUBERNETES = False
    return HAS_KUBERNETES


AssetT = TypeVar("AssetT", bound=BaseAsset, covariant=True)


class LazyDeserializedDAG(pydantic.BaseModel):
    """
    Lazily build information from the serialized DAG structure.

    An object that will present "enough" of the DAG like interface to update DAG db models etc, without having
    to deserialize the full DAG and Task hierarchy.
    """

    data: dict
    last_loaded: datetime.datetime | None = None

    NULLABLE_PROPERTIES: ClassVar[set[str]] = {
        # Non attr fields that should be nullable, or attrs with a different default
        "owner",
        "owner_links",
        "dag_display_name",
        "has_on_success_callback",
        "has_on_failure_callback",
        "tags",
        # Attr properties that are nullable, or have a default that loads from config
        "description",
        "start_date",
        "end_date",
        "template_searchpath",
        "user_defined_macros",
        "user_defined_filters",
        "max_active_tasks",
        "max_active_runs",
        "max_consecutive_failed_dag_runs",
        "dagrun_timeout",
        "deadline",
        "catchup",
        "doc_md",
        "access_control",
        "is_paused_upon_creation",
        "jinja_environment_kwargs",
        "relative_fileloc",
        "disable_bundle_versioning",
        "last_loaded",
    }

    @classmethod
    def from_dag(cls, dag: DAG | LazyDeserializedDAG) -> LazyDeserializedDAG:
        if isinstance(dag, LazyDeserializedDAG):
            return dag
        return cls(data=SerializedDAG.to_dict(dag))

    @property
    def hash(self) -> str:
        from airflow.models.serialized_dag import SerializedDagModel

        return SerializedDagModel.hash(self.data)

    def next_dagrun_info(self, *args, **kwargs) -> DagRunInfo | None:
        # This function is complex to implement, for now we delegate deserialize the dag and delegate to that.
        return self._real_dag.next_dagrun_info(*args, **kwargs)

    @property
    def access_control(self) -> Mapping[str, Mapping[str, Collection[str]] | Collection[str]] | None:
        return BaseSerialization.deserialize(self.data["dag"].get("access_control"))

    @cached_property
    def _real_dag(self):
        try:
            return SerializedDAG.from_dict(self.data)
        except Exception:
            log.exception("Failed to deserialize DAG")
            raise

    def __getattr__(self, name: str, /) -> Any:
        if name in self.NULLABLE_PROPERTIES:
            return self.data["dag"].get(name)
        try:
            return self.data["dag"][name]
        except KeyError:
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}") from None

    @property
    def timetable(self) -> Timetable:
        return decode_timetable(self.data["dag"]["timetable"])

    @property
    def has_task_concurrency_limits(self) -> bool:
        return any(
            task[Encoding.VAR].get("max_active_tis_per_dag") is not None
            or task[Encoding.VAR].get("max_active_tis_per_dagrun") is not None
            or task[Encoding.VAR].get("partial_kwargs", {}).get("max_active_tis_per_dag") is not None
            or task[Encoding.VAR].get("partial_kwargs", {}).get("max_active_tis_per_dagrun") is not None
            for task in self.data["dag"]["tasks"]
        )

    @property
    def owner(self) -> str:
        return ", ".join(
            set(filter(None, (task[Encoding.VAR].get("owner") for task in self.data["dag"]["tasks"])))
        )


@attrs.define()
class XComOperatorLink(LoggingMixin):
    """
    Generic operator link class that can retrieve link only using XCOMs.

    Used while deserializing operators.
    """

    name: str
    xcom_key: str

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        """
        Retrieve the link from the XComs.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system, but by pulling it from XComs
        """
        self.log.info(
            "Attempting to retrieve link from XComs with key: %s for task id: %s", self.xcom_key, ti_key
        )
        with create_session() as session:
            value = session.execute(
                XComModel.get_many(
                    key=self.xcom_key,
                    run_id=ti_key.run_id,
                    dag_ids=ti_key.dag_id,
                    task_ids=ti_key.task_id,
                    map_indexes=ti_key.map_index,
                ).with_only_columns(XComModel.value)
            ).first()
        if not value:
            self.log.debug(
                "No link with name: %s present in XCom as key: %s, returning empty link",
                self.name,
                self.xcom_key,
            )
            return ""
        return XComModel.deserialize_value(value)


@overload
def create_scheduler_operator(op: BaseOperator | SerializedBaseOperator) -> SerializedBaseOperator: ...


@overload
def create_scheduler_operator(op: MappedOperator | SerializedMappedOperator) -> SerializedMappedOperator: ...


def create_scheduler_operator(op: SdkOperator | SerializedOperator) -> SerializedOperator:
    from airflow.models.mappedoperator import MappedOperator as SerializedMappedOperator

    if isinstance(op, (SerializedBaseOperator, SerializedMappedOperator)):
        return op
    if isinstance(op, BaseOperator):
        d = SerializedBaseOperator.serialize_operator(op)
    elif isinstance(op, MappedOperator):
        d = SerializedBaseOperator.serialize_mapped_operator(op)
    else:
        raise TypeError(type(op).__name__)
    return SerializedBaseOperator.deserialize_operator(d)
