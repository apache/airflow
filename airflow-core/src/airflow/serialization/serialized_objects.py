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
import datetime
import enum
import itertools
import logging
import weakref
from collections.abc import Collection, Generator, Iterable, Mapping
from functools import cache, cached_property
from inspect import signature
from textwrap import dedent
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, TypeVar, Union, cast

import attrs
import lazy_object_proxy
import pydantic
from dateutil import relativedelta
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow import macros
from airflow.callbacks.callback_requests import DagCallbackRequest, TaskCallbackRequest
from airflow.exceptions import AirflowException, SerializationError, TaskDeferred
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.models.dag import DAG, _get_model_data_interval
from airflow.models.expandinput import (
    create_expand_input,
)
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models.xcom import XComModel
from airflow.models.xcom_arg import SchedulerXComArg, deserialize_xcom_arg
from airflow.providers_manager import ProvidersManager
from airflow.sdk.bases.operator import BaseOperator as TaskSDKBaseOperator
from airflow.sdk.definitions._internal.expandinput import EXPAND_INPUT_EMPTY
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAliasEvent,
    AssetAliasUniqueKey,
    AssetAll,
    AssetAny,
    AssetRef,
    AssetUniqueKey,
    AssetWatcher,
    BaseAsset,
)
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.param import Param, ParamsDict
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.sdk.definitions.xcom_arg import XComArg, serialize_xcom_arg
from airflow.sdk.execution_time.context import OutletEventAccessor, OutletEventAccessors
from airflow.serialization.dag_dependency import DagDependency
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import serialize_template_field
from airflow.serialization.json_schema import load_dag_schema
from airflow.settings import DAGS_FOLDER, json
from airflow.task.priority_strategy import (
    PriorityWeightStrategy,
    airflow_priority_weight_strategies,
    airflow_priority_weight_strategies_classes,
)
from airflow.triggers.base import BaseTrigger, StartTriggerArgs
from airflow.utils.code_utils import get_python_source
from airflow.utils.context import (
    ConnectionAccessor,
    Context,
    VariableAccessor,
)
from airflow.utils.db import LazySelectSequence
from airflow.utils.docs import get_docs_url
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string, qualname
from airflow.utils.operator_resources import Resources
from airflow.utils.timezone import from_timestamp, parse_timezone
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from inspect import Parameter

    from airflow.models import DagRun
    from airflow.models.expandinput import ExpandInput
    from airflow.sdk import BaseOperatorLink
    from airflow.sdk.definitions._internal.node import DAGNode
    from airflow.sdk.types import Operator
    from airflow.serialization.json_schema import Validator
    from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
    from airflow.triggers.base import BaseEventTrigger

    HAS_KUBERNETES: bool
    try:
        from kubernetes.client import models as k8s  # noqa: TC004

        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator  # noqa: TC004
    except ImportError:
        pass

log = logging.getLogger(__name__)

_OPERATOR_EXTRA_LINKS: set[str] = {
    "airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunLink",
    "airflow.providers.standard.sensors.external_task.ExternalDagLink",
    # Deprecated names, so that existing serialized dags load straight away.
    "airflow.providers.standard.sensors.external_task.ExternalTaskSensorLink",
    "airflow.operators.dagrun_operator.TriggerDagRunLink",
    "airflow.providers.standard.sensors.external_task_sensor.ExternalTaskSensorLink",
}


@cache
def get_operator_extra_links() -> set[str]:
    """
    Get the operator extra links.

    This includes both the built-in ones, and those come from the providers.
    """
    _OPERATOR_EXTRA_LINKS.update(ProvidersManager().extra_links_class_names)
    return _OPERATOR_EXTRA_LINKS


@cache
def _get_default_mapped_partial() -> dict[str, Any]:
    """
    Get default partial kwargs in a mapped operator.

    This is used to simplify a serialized mapped operator by excluding default
    values supplied in the implementation from the serialized dict. Since those
    are defaults, they are automatically supplied on de-serialization, so we
    don't need to store them.
    """
    # Use the private _expand() method to avoid the empty kwargs check.
    default = BaseOperator.partial(task_id="_")._expand(EXPAND_INPUT_EMPTY, strict=False).partial_kwargs
    return BaseSerialization.serialize(default)[Encoding.VAR]


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
        var["weekday"] = relativedelta.weekday(*var["weekday"])  # type: ignore
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
    if isinstance(trigger, dict):
        return trigger
    classpath, kwargs = trigger.serialize()
    return {
        "classpath": classpath,
        "kwargs": kwargs,
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


def decode_asset(var: dict[str, Any]):
    watchers = var.get("watchers", [])
    return Asset(
        name=var["name"],
        uri=var["uri"],
        group=var["group"],
        extra=var["extra"],
        watchers=[
            SerializedAssetWatcher(name=watcher["name"], trigger=watcher["trigger"]) for watcher in watchers
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
            for k, v in var._dict.items()  # type: ignore[attr-defined]
        ],
    }


def decode_outlet_event_accessors(var: dict[str, Any]) -> OutletEventAccessors:
    d = OutletEventAccessors()  # type: ignore[assignment]
    d._dict = {  # type: ignore[attr-defined]
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

    def deref(self, dag: DAG) -> SchedulerXComArg:
        return deserialize_xcom_arg(self.data, dag)


# These two should be kept in sync. Note that these are intentionally not using
# the type declarations in expandinput.py so we always remember to update
# serialization logic when adding new ExpandInput variants. If you add things to
# the unions, be sure to update _ExpandInputRef to match.
_ExpandInputOriginalValue = Union[
    # For .expand(**kwargs).
    Mapping[str, Any],
    # For expand_kwargs(arg).
    XComArg,
    Collection[Union[XComArg, Mapping[str, Any]]],
]
_ExpandInputSerializedValue = Union[
    # For .expand(**kwargs).
    Mapping[str, Any],
    # For expand_kwargs(arg).
    _XComRef,
    Collection[Union[_XComRef, Mapping[str, Any]]],
]


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

    def deref(self, dag: DAG) -> ExpandInput:
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

    _json_schema: Validator | None = None

    # Should the extra operator link be loaded via plugins when
    # de-serializing the DAG? This flag is set to False in Scheduler so that Extra Operator links
    # are not loaded to not run User code in Scheduler.
    _load_operator_extra_links = True

    _CONSTRUCTOR_PARAMS: dict[str, Parameter] = {}

    SERIALIZER_VERSION = 2

    @classmethod
    def to_json(cls, var: DAG | BaseOperator | dict | list | set | tuple) -> str:
        """Stringify DAGs and operators contained by var and returns a JSON string of var."""
        return json.dumps(cls.to_dict(var), ensure_ascii=True)

    @classmethod
    def to_dict(cls, var: DAG | BaseOperator | dict | list | set | tuple) -> dict:
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
        object_to_serialize: TaskSDKBaseOperator | MappedOperator | DAG,
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
                serialized_object[key] = encode_priority_weight_strategy(value)
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
        elif isinstance(var, Resources):
            return var.to_dict()
        elif isinstance(var, MappedOperator):
            return cls._encode(SerializedBaseOperator.serialize_mapped_operator(var), type_=DAT.OP)
        elif isinstance(var, TaskSDKBaseOperator):
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
        elif isinstance(var, SimpleTaskInstance):
            return cls._encode(
                cls.serialize(var.__dict__, strict=strict),
                type_=DAT.SIMPLE_TASK_INSTANCE,
            )
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
            if not _has_kubernetes():
                raise RuntimeError("Cannot deserialize POD objects without kubernetes libraries installed!")
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
        elif type_ == DAT.SIMPLE_TASK_INSTANCE:
            return SimpleTaskInstance(**cls.deserialize(var))
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


class DependencyDetector:
    """
    Detects dependencies between DAGs.

    :meta private:
    """

    @staticmethod
    def detect_task_dependencies(task: Operator) -> list[DagDependency]:
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
            and issubclass(cast("type[BaseOperator]", task.operator_class), TriggerDagRunOperator)
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
            and issubclass(cast("type[BaseOperator]", task.operator_class), ExternalTaskSensor)
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


class SerializedBaseOperator(BaseOperator, BaseSerialization):
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

    _CONSTRUCTOR_PARAMS = {
        k: v.default
        for k, v in itertools.chain(
            signature(BaseOperator.__init__).parameters.items(),
            signature(TaskSDKBaseOperator.__init__).parameters.items(),
        )
        if v.default is not v.empty
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # task_type is used by UI to display the correct class type, because UI only
        # receives BaseOperator from deserialized DAGs.
        self._task_type = "BaseOperator"
        # Move class attributes into object attributes.
        self.ui_color = BaseOperator.ui_color
        self.ui_fgcolor = BaseOperator.ui_fgcolor
        self.template_ext = BaseOperator.template_ext
        self.template_fields = BaseOperator.template_fields
        self.operator_extra_links = BaseOperator.operator_extra_links
        self._operator_name = None

    @cached_property
    def operator_extra_link_dict(self) -> dict[str, BaseOperatorLink]:
        """Returns dictionary of all extra links for the operator."""
        op_extra_links_from_plugin: dict[str, Any] = {}
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.operator_extra_links is None:
            raise AirflowException("Can't load operators")
        for ope in plugins_manager.operator_extra_links:
            if ope.operators and self.operator_class in ope.operators:
                op_extra_links_from_plugin.update({ope.name: ope})

        operator_extra_links_all = {link.name: link for link in self.operator_extra_links}
        # Extra links defined in Plugins overrides operator links defined in operator
        operator_extra_links_all.update(op_extra_links_from_plugin)

        return operator_extra_links_all

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
        return link.get_link(self.unmap(None), ti_key=ti.key)

    @property
    def task_type(self) -> str:
        # Overwrites task_type of BaseOperator to use _task_type instead of
        # __class__.__name__.

        return self._task_type

    @task_type.setter
    def task_type(self, task_type: str):
        self._task_type = task_type

    @property
    def operator_name(self) -> str:
        # Overwrites operator_name of BaseOperator to use _operator_name instead of
        # __class__.operator_name.
        return self._operator_name or self.task_type

    @operator_name.setter
    def operator_name(self, operator_name: str):
        self._operator_name = operator_name

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

        # Simplify partial_kwargs by comparing it to the most barebone object.
        # Remove all entries that are simply default values.
        serialized_partial = serialized_op["partial_kwargs"]
        for k, default in _get_default_mapped_partial().items():
            try:
                v = serialized_partial[k]
            except KeyError:
                continue
            if v == default:
                del serialized_partial[k]

        serialized_op["_is_mapped"] = True
        return serialized_op

    @classmethod
    def serialize_operator(cls, op: TaskSDKBaseOperator | MappedOperator) -> dict[str, Any]:
        return cls._serialize_node(op)

    @classmethod
    def _serialize_node(cls, op: TaskSDKBaseOperator | MappedOperator) -> dict[str, Any]:
        """Serialize operator into a JSON object."""
        serialize_op = cls.serialize_to_json(op, cls._decorated_fields)

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

        serialize_op["start_trigger_args"] = (
            encode_start_trigger_args(op.start_trigger_args) if op.start_trigger_args else None
        )
        serialize_op["start_from_trigger"] = op.start_from_trigger

        if op.operator_extra_links:
            serialize_op["_operator_extra_links"] = cls._serialize_operator_extra_links(
                op.operator_extra_links.__get__(op)
                if isinstance(op.operator_extra_links, property)
                else op.operator_extra_links
            )

        # Store all template_fields as they are if there are JSON Serializable
        # If not, store them as strings
        # And raise an exception if the field is not templateable
        forbidden_fields = set(SerializedBaseOperator._CONSTRUCTOR_PARAMS.keys())
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
    def populate_operator(cls, op: Operator, encoded_op: dict[str, Any]) -> None:
        """
        Populate operator attributes with serialized values.

        This covers simple attributes that don't reference other things in the
        DAG. Setting references (such as ``op.dag`` and task dependencies) is
        done in ``set_task_dag_references`` instead, which is called after the
        DAG is hydrated.
        """
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

        for k, v in encoded_op.items():
            # python_callable_name only serves to detect function name changes
            if k == "python_callable_name":
                continue
            if k in ("_outlets", "_inlets"):
                # `_outlets` -> `outlets`
                k = k[1:]
            elif k == "task_type":
                k = "_task_type"
            if k == "_downstream_task_ids":
                # Upgrade from old format/name
                k = "downstream_task_ids"

            if k == "label":
                # Label shouldn't be set anymore --  it's computed from task_id now
                continue
            if k == "downstream_task_ids":
                v = set(v)
            elif k in {"retry_delay", "execution_timeout", "max_retry_delay"}:
                # If operator's execution_timeout is None and core.default_task_execution_timeout is not None,
                # v will be None so do not deserialize into timedelta
                if v is not None:
                    v = cls._deserialize_timedelta(v)
            elif k in encoded_op["template_fields"]:
                pass
            elif k == "resources":
                v = Resources.from_dict(v)
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
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
                v = {arg: cls.deserialize(value) for arg, value in v.items()}
            elif k in {"expand_input", "op_kwargs_expand_input"}:
                v = _ExpandInputRef(v["type"], cls.deserialize(v["value"]))
            elif k == "operator_class":
                v = {k_: cls.deserialize(v_) for k_, v_ in v.items()}
            elif k == "_is_sensor":
                from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep

                if v is False:
                    raise RuntimeError("_is_sensor=False should never have been serialized!")
                object.__setattr__(op, "deps", op.deps | {ReadyToRescheduleDep()})  # type: ignore[union-attr]
                continue
            elif (
                k in cls._decorated_fields
                or k not in op.get_serialized_fields()
                or k in ("outlets", "inlets")
            ):
                v = cls.deserialize(v)
            elif k == "on_failure_fail_dagrun":
                k = "_on_failure_fail_dagrun"
            elif k == "weight_rule":
                v = decode_priority_weight_strategy(v)

            # else use v as it is

            setattr(op, k, v)

        for k in op.get_serialized_fields() - encoded_op.keys() - cls._CONSTRUCTOR_PARAMS.keys():
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
    def set_task_dag_references(task: Operator, dag: DAG) -> None:
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
    def deserialize_operator(cls, encoded_op: dict[str, Any]) -> Operator:
        """Deserializes an operator from a JSON object."""
        op: Operator
        if encoded_op.get("_is_mapped", False):
            # Most of these will be loaded later, these are just some stand-ins.
            op_data = {
                k: v for k, v in encoded_op.items() if k in TaskSDKBaseOperator.get_serialized_fields()
            }

            from airflow.models.mappedoperator import MappedOperator as MappedOperatorWithDB

            try:
                operator_name = encoded_op["_operator_name"]
            except KeyError:
                operator_name = encoded_op["task_type"]

            op = MappedOperatorWithDB(
                operator_class=op_data,
                expand_input=EXPAND_INPUT_EMPTY,
                partial_kwargs={},
                task_id=encoded_op["task_id"],
                params={},
                operator_extra_links=BaseOperator.operator_extra_links,
                template_ext=BaseOperator.template_ext,
                template_fields=BaseOperator.template_fields,
                template_fields_renderers=BaseOperator.template_fields_renderers,
                ui_color=BaseOperator.ui_color,
                ui_fgcolor=BaseOperator.ui_fgcolor,
                is_empty=False,
                is_sensor=encoded_op.get("_is_sensor", False),
                can_skip_downstream=encoded_op.get("_can_skip_downstream", False),
                task_module=encoded_op["_task_module"],
                task_type=encoded_op["task_type"],
                operator_name=operator_name,
                dag=None,
                task_group=None,
                start_date=None,
                end_date=None,
                disallow_kwargs_override=encoded_op["_disallow_kwargs_override"],
                expand_input_attr=encoded_op["_expand_input_attr"],
                start_trigger_args=encoded_op.get("start_trigger_args", None),
                start_from_trigger=encoded_op.get("start_from_trigger", False),
            )
        else:
            op = SerializedBaseOperator(task_id=encoded_op["task_id"])
        cls.populate_operator(op, encoded_op)

        return op

    @classmethod
    def detect_dependencies(cls, op: Operator) -> set[DagDependency]:
        """Detect between DAG dependencies for the operator."""
        dependency_detector = DependencyDetector()
        deps = set(dependency_detector.detect_task_dependencies(op))
        return deps

    @classmethod
    def _is_excluded(cls, var: Any, attrname: str, op: DAGNode):
        if var is not None and op.has_dag() and attrname.endswith("_date"):
            # If this date is the same as the matching field in the dag, then
            # don't store it again at the task level.
            dag_date = getattr(op.dag, attrname, None)
            if var is dag_date or var == dag_date:
                return True
        return super()._is_excluded(var, attrname, op)

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


class SerializedDAG(DAG, BaseSerialization):
    """
    A JSON serializable representation of DAG.

    A stringified DAG can only be used in the scope of scheduler and webserver, because fields
    that are not serializable, such as functions and customer defined classes, are casted to
    strings.
    """

    _decorated_fields = {"default_args", "access_control"}

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

    _json_schema = lazy_object_proxy.Proxy(load_dag_schema)

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

            # Edge info in the JSON exactly matches our internal structure
            serialized_dag["edge_info"] = dag.edge_info
            serialized_dag["params"] = cls._serialize_params_dict(dag.params)

            # has_on_*_callback are only stored if the value is True, as the default is False
            if dag.has_on_success_callback:
                serialized_dag["has_on_success_callback"] = True
            if dag.has_on_failure_callback:
                serialized_dag["has_on_failure_callback"] = True
            return serialized_dag
        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"Failed to serialize DAG {dag.dag_id!r}: {e}")

    @classmethod
    def deserialize_dag(cls, encoded_dag: dict[str, Any]) -> SerializedDAG:
        """Deserializes a DAG from a JSON object."""
        if "dag_id" not in encoded_dag:
            raise RuntimeError(
                "Encoded dag object has no dag_id key.  You may need to run `airflow dags reserialize`."
            )

        dag = SerializedDAG(dag_id=encoded_dag["dag_id"], schedule=None)

        for k, v in encoded_dag.items():
            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "tasks":
                SerializedBaseOperator._load_operator_extra_links = cls._load_operator_extra_links
                tasks = {}
                for obj in v:
                    if obj.get(Encoding.TYPE) == DAT.OP:
                        deser = SerializedBaseOperator.deserialize_operator(obj[Encoding.VAR])
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
            # This must be old data that had no task_group. Create a root TaskGroup and add
            # all tasks to it.
            object.__setattr__(dag, "task_group", TaskGroup.create_root(dag))
            for task in dag.tasks:
                dag.task_group.add(task)

        # Set has_on_*_callbacks to True if they exist in Serialized blob as False is the default
        if "has_on_success_callback" in encoded_dag:
            dag.has_on_success_callback = True
        if "has_on_failure_callback" in encoded_dag:
            dag.has_on_failure_callback = True

        keys_to_set_none = dag.get_serialized_fields() - encoded_dag.keys() - cls._CONSTRUCTOR_PARAMS.keys()
        for k in keys_to_set_none:
            setattr(dag, k, None)

        for task in dag.task_dict.values():
            SerializedBaseOperator.set_task_dag_references(task, dag)

        return dag

    @classmethod
    def _is_excluded(cls, var: Any, attrname: str, op: DAGNode):
        # {} is explicitly different from None in the case of DAG-level access control
        # and as a result we need to preserve empty dicts through serialization for this field
        if attrname == "access_control" and var is not None:
            return False
        if attrname == "dag_display_name" and var == op.dag_id:
            return True
        return super()._is_excluded(var, attrname, op)

    @classmethod
    def to_dict(cls, var: Any) -> dict:
        """Stringifies DAGs and operators contained by var and returns a dict of var."""
        json_dict = {"__version": cls.SERIALIZER_VERSION, "dag": cls.serialize_dag(var)}

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
        task_renames = [("_task_type", "task_type")]
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
                task_var[new] = task_var.pop(old)
            for item in itertools.chain(*(task_var.get(key, []) for key in ("inlets", "outlets"))):
                original_item_type = item["__type"]
                if isinstance(item, dict) and "__type" in item:
                    item["__type"] = replace_dataset_in_str(original_item_type)

                var_ = item["__var"]
                if original_item_type == "dataset":
                    var_["name"] = var_["uri"]
                var_["group"] = "asset"

        # Set on the root TG
        dag_dict["task_group"]["group_display_name"] = ""

    @classmethod
    def from_dict(cls, serialized_obj: dict) -> SerializedDAG:
        """Deserializes a python dict in to the DAG and operators it contains."""
        ver = serialized_obj.get("__version", "<not present>")
        if ver not in (1, 2):
            raise ValueError(f"Unsure how to deserialize version {ver!r}")
        if ver == 1:
            cls.conversion_v1_to_v2(serialized_obj)
        return cls.deserialize_dag(serialized_obj["dag"])


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
        parent_group: TaskGroup | None,
        task_dict: dict[str, Operator],
        dag: SerializedDAG,
    ) -> TaskGroup:
        """Deserializes a TaskGroup from a JSON object."""
        group_id = cls.deserialize(encoded_group["_group_id"])
        kwargs = {
            key: cls.deserialize(encoded_group[key])
            for key in ["prefix_group_id", "tooltip", "ui_color", "ui_fgcolor"]
        }
        kwargs["group_display_name"] = cls.deserialize(encoded_group.get("group_display_name", ""))

        if not encoded_group.get("is_mapped"):
            group = TaskGroup(group_id=group_id, parent_group=parent_group, dag=dag, **kwargs)
        else:
            xi = encoded_group["expand_input"]
            group = MappedTaskGroup(
                group_id=group_id,
                parent_group=parent_group,
                dag=dag,
                expand_input=_ExpandInputRef(xi["type"], cls.deserialize(xi["value"])).deref(dag),
                **kwargs,
            )

        def set_ref(task: Operator) -> Operator:
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


def _has_kubernetes() -> bool:
    global HAS_KUBERNETES
    if "HAS_KUBERNETES" in globals():
        return HAS_KUBERNETES

    # Loading kube modules is expensive, so delay it until the last moment

    try:
        from kubernetes.client import models as k8s

        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

        globals()["k8s"] = k8s
        globals()["PodGenerator"] = PodGenerator

        # isort: on
        HAS_KUBERNETES = True
    except ImportError:
        HAS_KUBERNETES = False
    return HAS_KUBERNETES


AssetT = TypeVar("AssetT", bound=BaseAsset)
MaybeSerializedDAG = Union[DAG, "LazyDeserializedDAG"]


class LazyDeserializedDAG(pydantic.BaseModel):
    """
    Lazily build information from the serialized DAG structure.

    An object that will present "enough" of the DAG like interface to update DAG db models etc, without having
    to deserialize the full DAG and Task hierarchy.
    """

    data: dict

    NULLABLE_PROPERTIES: ClassVar[set[str]] = {
        "is_paused_upon_creation",
        "owner",
        "dag_display_name",
        "description",
        "max_active_tasks",
        "max_active_runs",
        "max_consecutive_failed_dag_runs",
        "owner_links",
        "access_control",
    }

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
        return SerializedDAG.from_dict(self.data)

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
            task[Encoding.VAR].get("max_active_tis_per_dag") is not None for task in self.data["dag"]["tasks"]
        )

    @property
    def owner(self) -> str:
        return ", ".join(
            set(filter(None, (task[Encoding.VAR].get("owner") for task in self.data["dag"]["tasks"])))
        )

    @staticmethod
    def _get_mapped_operator_ports(task: dict, direction: str):
        return task["partial_kwargs"][direction]

    @staticmethod
    def _get_base_operator_ports(task: dict, direction: str):
        return task[direction]

    def get_task_assets(
        self,
        inlets: bool = True,
        outlets: bool = True,
        of_type: type[AssetT] = Asset,  # type: ignore[assignment]
    ) -> Generator[tuple[str, AssetT], None, None]:
        for task in self.data["dag"]["tasks"]:
            task = task[Encoding.VAR]
            if task.get("_is_mapped"):
                ports_getter = self._get_mapped_operator_ports
            else:
                ports_getter = self._get_base_operator_ports
            directions = ("inlets",) if inlets else ()
            if outlets:
                directions += ("outlets",)
            for direction in directions:
                try:
                    ports = ports_getter(task, direction)
                except KeyError:
                    continue
                for port in ports:
                    obj = BaseSerialization.deserialize(port)
                    if isinstance(obj, of_type):
                        yield task["task_id"], obj

    def get_run_data_interval(self, run: DagRun) -> DataInterval:
        """Get the data interval of this run."""
        if run.dag_id is not None and run.dag_id != self.dag_id:
            raise ValueError(f"Arguments refer to different DAGs: {self.dag_id} != {run.dag_id}")

        data_interval = _get_model_data_interval(run, "data_interval_start", "data_interval_end")
        # the older implementation has call to infer_automated_data_interval if data_interval is None, do we want to keep that or raise
        # an exception?
        if data_interval is None:
            raise ValueError(f"Cannot calculate data interval for run {run}")

        return data_interval


@attrs.define()
class XComOperatorLink(LoggingMixin):
    """A generic operator link class that can retrieve link only using XCOMs. Used while deserializing operators."""

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
        value = XComModel.get_many(
            key=self.xcom_key,
            run_id=ti_key.run_id,
            dag_ids=ti_key.dag_id,
            task_ids=ti_key.task_id,
            map_indexes=ti_key.map_index,
        ).first()
        if not value:
            self.log.debug(
                "No link with name: %s present in XCom as key: %s, returning empty link",
                self.name,
                self.xcom_key,
            )
            return ""
        return XComModel.deserialize_value(value)
