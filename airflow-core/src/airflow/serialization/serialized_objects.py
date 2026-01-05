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
import contextlib
import datetime
import enum
import itertools
import logging
import math
import sys
import weakref
from collections.abc import Collection, Iterable, Mapping
from functools import cache, cached_property, lru_cache
from inspect import signature
from textwrap import dedent
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, TypeVar, cast, overload

import attrs
import lazy_object_proxy
import pydantic
from dateutil import relativedelta
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow._shared.module_loading import import_string, qualname
from airflow._shared.timezones.timezone import from_timestamp, parse_timezone, utcnow
from airflow.callbacks.callback_requests import DagCallbackRequest, TaskCallbackRequest
from airflow.exceptions import AirflowException, DeserializationError, SerializationError
from airflow.models.connection import Connection
from airflow.models.expandinput import SchedulerMappedArgument, create_expand_input
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.sdk import DAG, Asset, AssetAlias, BaseOperator, XComArg
from airflow.sdk.bases.operator import OPERATOR_DEFAULTS  # TODO: Copy this into the scheduler?
from airflow.sdk.definitions._internal.expandinput import MappedArgument
from airflow.sdk.definitions.asset import (
    AssetAliasEvent,
    AssetAliasUniqueKey,
    AssetUniqueKey,
    BaseAsset,
)
from airflow.sdk.definitions.deadline import DeadlineAlert
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.operator_resources import Resources
from airflow.sdk.definitions.param import Param, ParamsDict
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.sdk.definitions.xcom_arg import serialize_xcom_arg
from airflow.sdk.execution_time.context import OutletEventAccessor, OutletEventAccessors
from airflow.serialization.dag_dependency import DagDependency
from airflow.serialization.decoders import decode_asset_like, decode_relativedelta, decode_timetable
from airflow.serialization.definitions.assets import (
    SerializedAsset,
    SerializedAssetAlias,
    SerializedAssetBase,
    SerializedAssetUniqueKey,
)
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.serialization.definitions.node import DAGNode
from airflow.serialization.definitions.operatorlink import XComOperatorLink
from airflow.serialization.definitions.param import SerializedParam, SerializedParamsDict
from airflow.serialization.definitions.taskgroup import SerializedMappedTaskGroup, SerializedTaskGroup
from airflow.serialization.definitions.xcom_arg import SchedulerXComArg, deserialize_xcom_arg
from airflow.serialization.encoders import (
    coerce_to_core_timetable,
    encode_asset_like,
    encode_expand_input,
    encode_relativedelta,
    encode_timetable,
    encode_timezone,
    ensure_serialized_asset,
)
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import TimetableNotRegistered, serialize_template_field
from airflow.serialization.json_schema import load_dag_schema
from airflow.settings import DAGS_FOLDER, json
from airflow.task.priority_strategy import (
    PriorityWeightStrategy,
    get_airflow_priority_weight_strategies,
    get_weight_rule_from_priority_weight_strategy,
    validate_and_load_priority_weight_strategy,
)
from airflow.timetables.base import DagRunInfo, Timetable
from airflow.triggers.base import BaseTrigger, StartTriggerArgs
from airflow.utils.code_utils import get_python_source
from airflow.utils.db import LazySelectSequence

if TYPE_CHECKING:
    from inspect import Parameter

    from kubernetes.client import models as k8s  # noqa: TC004

    from airflow.models.expandinput import SchedulerExpandInput
    from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator  # noqa: TC004
    from airflow.sdk import BaseOperatorLink
    from airflow.sdk.definitions._internal.node import DAGNode as SDKDAGNode
    from airflow.sdk.types import Operator as SdkOperator
    from airflow.serialization.definitions.mappedoperator import (
        Operator as SerializedOperator,
        SerializedMappedOperator,
    )
    from airflow.serialization.json_schema import Validator
    from airflow.timetables.base import DagRunInfo, Timetable
    from airflow.timetables.simple import PartitionMapper

log = logging.getLogger(__name__)


def _get_registered_priority_weight_strategy(
    importable_string: str,
) -> type[PriorityWeightStrategy] | None:
    from airflow import plugins_manager

    with contextlib.suppress(KeyError):
        return get_airflow_priority_weight_strategies()[importable_string]
    return plugins_manager.get_priority_weight_strategy_plugins().get(importable_string)


class _PartitionMapperNotFound(ValueError):
    def __init__(self, type_string: str) -> None:
        self.type_string = type_string

    def __str__(self) -> str:
        return (
            f"PartitionMapper class {self.type_string!r} could not be imported or "
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
                # fallback for backward compatibility
                dest_asset_extra=e.get("dest_asset_extra", {}),
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


def _load_partition_mapper(importable_string) -> PartitionMapper | None:
    if importable_string.startswith("airflow.timetables."):
        return import_string(importable_string)
    return None


def encode_partition_mapper(var: PartitionMapper) -> dict[str, Any]:
    """
    Encode a PartitionMapper instance.

    This delegates most of the serialization work to the type, so the behavior
    can be completely controlled by a custom subclass.

    :meta private:
    """
    partition_mapper_class = type(var)
    importable_string = qualname(partition_mapper_class)
    if _load_partition_mapper(importable_string) is None:
        raise _PartitionMapperNotFound(importable_string)
    return {Encoding.TYPE: importable_string, Encoding.VAR: var.serialize()}


def decode_partition_mapper(var: dict[str, Any]) -> PartitionMapper:
    """
    Decode a previously serialized PartitionMapper.

    Most of the deserialization logic is delegated to the actual type, which
    we import from string.

    :meta private:
    """
    importable_string = var[Encoding.TYPE]
    partition_mapper_class = _load_partition_mapper(importable_string)
    if partition_mapper_class is None:
        raise _PartitionMapperNotFound(importable_string)
    return partition_mapper_class.deserialize(var[Encoding.VAR])


def encode_priority_weight_strategy(var: PriorityWeightStrategy | str) -> str:
    """
    Encode a priority weight strategy instance.

    In this version, we only store the importable string, so the class should not wait
    for any parameters to be passed to it. If you need to store the parameters, you
    should store them in the class itself.
    """
    priority_weight_strategy_class = type(validate_and_load_priority_weight_strategy(var))
    with contextlib.suppress(KeyError):
        return get_weight_rule_from_priority_weight_strategy(priority_weight_strategy_class)
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
    def from_dict(cls, serialized_obj: dict[Encoding, Any]) -> Any:
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
        from airflow.sdk.definitions._internal.types import is_arg_set
        from airflow.sdk.exceptions import TaskDeferred

        if not is_arg_set(var):
            return cls._encode(None, type_=DAT.ARG_NOT_SET)
        elif cls._is_primitive(var):
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
            return cls._encode(DagSerialization.serialize_dag(var), type_=DAT.DAG)
        elif isinstance(var, DeadlineAlert):
            return cls._encode(DeadlineAlert.serialize_deadline_alert(var), type_=DAT.DEADLINE_ALERT)
        elif isinstance(var, Resources):
            return var.to_dict()
        elif isinstance(var, MappedOperator):
            return cls._encode(OperatorSerialization.serialize_mapped_operator(var), type_=DAT.OP)
        elif isinstance(var, BaseOperator):
            var._needs_expansion = var.get_needs_expansion()
            return cls._encode(OperatorSerialization.serialize_operator(var), type_=DAT.OP)
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
        elif isinstance(var, (BaseAsset, SerializedAssetBase)):
            serialized_asset = encode_asset_like(var)
            return cls._encode(serialized_asset, type_=serialized_asset.pop("__type"))
        elif isinstance(var, Connection):
            return cls._encode(var.to_dict(validate=True), type_=DAT.CONNECTION)
        elif isinstance(var, TaskCallbackRequest):
            return cls._encode(var.to_json(), type_=DAT.TASK_CALLBACK_REQUEST)
        elif isinstance(var, DagCallbackRequest):
            return cls._encode(var.to_json(), type_=DAT.DAG_CALLBACK_REQUEST)
        elif isinstance(var, MappedArgument):
            data = {"input": encode_expand_input(var._input), "key": var._key}
            return cls._encode(data, type_=DAT.MAPPED_ARGUMENT)
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
        if type_ == DAT.DICT:
            return {k: cls.deserialize(v) for k, v in var.items()}
        elif type_ == DAT.ASSET_EVENT_ACCESSORS:
            return decode_outlet_event_accessors(var)
        elif type_ == DAT.ASSET_UNIQUE_KEY:
            return AssetUniqueKey(name=var["name"], uri=var["uri"])
        elif type_ == DAT.ASSET_ALIAS_UNIQUE_KEY:
            return AssetAliasUniqueKey(name=var["name"])
        elif type_ == DAT.DAG:
            return DagSerialization.deserialize_dag(var)
        elif type_ == DAT.OP:
            return OperatorSerialization.deserialize_operator(var)
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
            return parse_timezone(var)
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
        elif type_ in (DAT.ASSET, DAT.ASSET_ALIAS, DAT.ASSET_ALL, DAT.ASSET_ANY, DAT.ASSET_REF):
            return decode_asset_like(encoded_var)
        elif type_ == DAT.CONNECTION:
            return Connection(**var)
        elif type_ == DAT.TASK_CALLBACK_REQUEST:
            return TaskCallbackRequest.from_json(var)
        elif type_ == DAT.DAG_CALLBACK_REQUEST:
            return DagCallbackRequest.from_json(var)
        elif type_ == DAT.TASK_INSTANCE_KEY:
            return TaskInstanceKey(**var)
        elif type_ == DAT.MAPPED_ARGUMENT:
            expand_input = create_expand_input(var["input"]["type"], var["input"]["value"])
            return SchedulerMappedArgument(input=expand_input, key=var["key"])
        elif type_ == DAT.ARG_NOT_SET:
            from airflow.serialization.definitions.notset import NOTSET

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
            "source": cls.serialize(getattr(param, "source", None)),
        }

    @classmethod
    def _deserialize_param(cls, param_dict: dict) -> SerializedParam:
        """
        Deserialize an encoded Param to a server-side SerializedParam.

        In 2.2.0, Param attrs were assumed to be json-serializable and were not run through
        this class's ``serialize`` method.  So before running through ``deserialize``,
        we first verify that it's necessary to do.
        """
        attrs = ("default", "description", "schema", "source")
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

        return SerializedParam(
            default=kwargs.get("default"),
            description=kwargs.get("description"),
            source=kwargs.get("source", None),
            **(kwargs.get("schema") or {}),
        )

    @classmethod
    def _serialize_params_dict(cls, params: ParamsDict | dict) -> list[tuple[str, dict]]:
        """Serialize Params dict for a DAG or task as a list of tuples to ensure ordering."""
        serialized_params = []
        for k, raw_v in params.items():
            # Use native param object, not resolved value if possible
            v = params.get_param(k) if isinstance(params, ParamsDict) else raw_v
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
    def _deserialize_params_dict(cls, encoded_params: list[tuple[str, dict]]) -> SerializedParamsDict:
        """Deserialize an encoded ParamsDict to a server-side SerializedParamsDict."""
        if isinstance(encoded_params, collections.abc.Mapping):
            # in 2.9.2 or earlier params were serialized as JSON objects
            encoded_param_pairs: Iterable[tuple[str, dict]] = encoded_params.items()
        else:
            encoded_param_pairs = encoded_params

        def deserialized_param(v):
            if not isinstance(v, dict) or "__class" not in v:
                return SerializedParam(v)  # Old style param serialization format.
            return cls._deserialize_param(v)

        op_params = {k: deserialized_param(v) for k, v in encoded_param_pairs}
        return SerializedParamsDict(op_params)

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
            if isinstance(obj, (Asset, SerializedAsset)):
                serialized_asset = ensure_serialized_asset(obj)
                deps.append(
                    DagDependency(
                        source=task.dag_id,
                        target="asset",
                        label=obj.name,
                        dependency_type="asset",
                        dependency_id=SerializedAssetUniqueKey.from_asset(serialized_asset).to_str(),
                    )
                )
            elif isinstance(obj, (AssetAlias, SerializedAssetAlias)):
                serialized_alias = ensure_serialized_asset(obj)
                deps.extend(serialized_alias.iter_dag_dependencies(source=task.dag_id, target=""))

        return deps

    @staticmethod
    def detect_dag_dependencies(dag: DAG | None) -> Iterable[DagDependency]:
        """Detect dependencies set directly on the DAG object."""
        if not dag:
            return
        tt = coerce_to_core_timetable(dag.timetable)
        yield from tt.asset_condition.iter_dag_dependencies(source="", target=dag.dag_id)


class OperatorSerialization(DAGNode, BaseSerialization):
    """
    Logic to encode an operator and decode the data.

    This covers serialization of both BaseOperator and MappedOperator. Creating
    a serializaed operator is a three-step process:

    1. Instantiate a :class:`SerializedBaseOperator` or :class:`MappedOperator` object.
    2. Populate attributes with :func:`OperatorSerialization.populated_operator`.
    3. When the task's containing DAG is available, fix references to the DAG
       with :func:`OperatorSerialization.set_task_dag_references`.
    """

    _decorated_fields = {"executor_config"}

    _CONSTRUCTOR_PARAMS = {}

    _json_schema: ClassVar[Validator] = lazy_object_proxy.Proxy(load_dag_schema)

    _const_fields: ClassVar[set[str] | None] = None

    @classmethod
    def serialize_mapped_operator(cls, op: MappedOperator) -> dict[str, Any]:
        serialized_op = cls._serialize_node(op)
        # Handle expand_input and op_kwargs_expand_input.
        expansion_kwargs = op._get_specified_expand_input()
        if TYPE_CHECKING:  # Let Mypy check the input type for us!
            _ExpandInputRef.validate_expand_input_value(expansion_kwargs.value)
        serialized_op[op._expand_input_attr] = encode_expand_input(expansion_kwargs)

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

            for ope in plugins_manager.get_operator_extra_links():
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

        for k_in, v_in in encoded_op.items():
            k = k_in  # surpass PLW2901
            v = v_in  # surpass PLW2901
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
            elif k == "retry_exponential_backoff":
                if isinstance(v, bool):
                    v = 2.0 if v else 0
                else:
                    v = float(v)
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
    def get_operator_const_fields(cls) -> set[str]:
        """Get the set of operator fields that are marked as const in the JSON schema."""
        if (schema_loader := cls._json_schema) is None:
            return set()

        schema_data = schema_loader.schema
        operator_def = schema_data.get("definitions", {}).get("operator", {})
        properties = operator_def.get("properties", {})

        return {
            field_name
            for field_name, field_def in properties.items()
            if isinstance(field_def, dict) and field_def.get("const")
        }

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
            from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator

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
    def _is_excluded(cls, var: Any, attrname: str, op: SDKDAGNode) -> bool:
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

        # for const fields, we should always be excluded when False, regardless of client_defaults
        # Use class-level cache for optimisation
        if cls._const_fields is None:
            cls._const_fields = cls.get_operator_const_fields()
        if attrname in cls._const_fields and var is False:
            return True

        schema_defaults = cls.get_schema_defaults("operator")
        if attrname in schema_defaults:
            if schema_defaults[attrname] == var:
                # If it also matches client_defaults, exclude (optimization)
                client_defaults = cls.generate_client_defaults()
                if attrname in client_defaults:
                    if client_defaults[attrname] == var:
                        return True
                    # If client_defaults differs, preserve explicit override from user
                    # Example: default_args={"retries": 0}, schema default=0, client_defaults={"retries": 3}
                    if client_defaults[attrname] != var:
                        if op.has_dag():
                            dag = op.dag
                            if dag and attrname in dag.default_args and dag.default_args[attrname] == var:
                                return False
                        if (
                            hasattr(op, "_BaseOperator__init_kwargs")
                            and attrname in op._BaseOperator__init_kwargs
                            and op._BaseOperator__init_kwargs[attrname] == var
                        ):
                            return False

                # If client_defaults doesn't have this field (matches schema default),
                # exclude for optimization even if in default_args
                # Example: default_args={"depends_on_past": False}, schema default=False
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

        plugins_manager.get_operator_extra_links()
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
            if k not in SerializedBaseOperator.get_serialized_fields():
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


class DagSerialization(BaseSerialization):
    """Logic to encode a ``DAG`` object and decode the data into ``SerializedDAG``."""

    _decorated_fields: ClassVar[set[str]] = {"default_args", "access_control"}

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
    def serialize_dag(cls, dag: DAG) -> dict:
        """Serialize a DAG into a JSON object."""
        try:
            serialized_dag = cls.serialize_to_json(dag, cls._decorated_fields)
            serialized_dag["_processor_dags_folder"] = DAGS_FOLDER
            serialized_dag["tasks"] = [cls.serialize(task) for _, task in dag.task_dict.items()]

            dag_deps = [
                dep
                for task in dag.task_dict.values()
                for dep in OperatorSerialization.detect_dependencies(task)
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
            raise SerializationError(f"Failed to serialize DAG {dag.dag_id!r}: {e}") from e

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
        except (TimetableNotRegistered, DeserializationError):
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

        for k_in, v_in in encoded_dag.items():
            k = k_in  # surpass PLW2901
            v = v_in  # surpass PLW2901
            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "tasks":
                OperatorSerialization._load_operator_extra_links = cls._load_operator_extra_links
                tasks = {}
                for obj in v:
                    if obj.get(Encoding.TYPE) == DAT.OP:
                        deser = OperatorSerialization.deserialize_operator(obj[Encoding.VAR], client_defaults)
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
            OperatorSerialization.set_task_dag_references(t, dag)

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
        OperatorSerialization.generate_client_defaults.cache_clear()

        json_dict = {"__version": cls.SERIALIZER_VERSION, "dag": cls.serialize_dag(var)}

        # Add client_defaults section with only values that differ from schema defaults
        # for tasks
        client_defaults = OperatorSerialization.generate_client_defaults()
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
                timezone = parse_timezone(tzs)
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
                op_defaults = DagSerialization.get_schema_defaults("operator")
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
            encoded["expand_input"] = encode_expand_input(task_group._expand_input)
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


@cache
def _has_kubernetes(attempt_import: bool = False) -> bool:
    """
    Check if kubernetes libraries are available.

    :param attempt_import: If true, attempt to import kubernetes libraries if not already loaded. If
        False, only check if already in sys.modules (avoids expensive import).
    :return: True if kubernetes libraries are available, False otherwise.
    """
    # Check if kubernetes is already imported before triggering expensive import
    if "kubernetes.client" not in sys.modules and not attempt_import:
        return False

    # Loading kube modules is expensive, so delay it until the last moment
    try:
        from kubernetes.client import models as k8s

        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

        globals()["k8s"] = k8s
        globals()["PodGenerator"] = PodGenerator
        return True
    except ImportError:
        return False


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
        "fail_fast",
        "last_loaded",
    }

    @classmethod
    def from_dag(cls, dag: DAG | LazyDeserializedDAG) -> LazyDeserializedDAG:
        if isinstance(dag, LazyDeserializedDAG):
            return dag
        return cls(data=DagSerialization.to_dict(dag))

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
            return DagSerialization.from_dict(self.data)
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


@overload
def create_scheduler_operator(op: BaseOperator | SerializedBaseOperator) -> SerializedBaseOperator: ...


@overload
def create_scheduler_operator(op: MappedOperator | SerializedMappedOperator) -> SerializedMappedOperator: ...


def create_scheduler_operator(op: SdkOperator | SerializedOperator) -> SerializedOperator:
    from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator

    if isinstance(op, (SerializedBaseOperator, SerializedMappedOperator)):
        return op
    if isinstance(op, BaseOperator):
        d = OperatorSerialization.serialize_operator(op)
    elif isinstance(op, MappedOperator):
        d = OperatorSerialization.serialize_mapped_operator(op)
    else:
        raise TypeError(type(op).__name__)
    return OperatorSerialization.deserialize_operator(d)
