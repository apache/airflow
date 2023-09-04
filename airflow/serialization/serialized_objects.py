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
from __future__ import annotations

import collections.abc
import datetime
import enum
import inspect
import logging
import warnings
import weakref
from dataclasses import dataclass
from inspect import Parameter, signature
from typing import TYPE_CHECKING, Any, Collection, Iterable, Mapping, NamedTuple, Union

import cattr
import lazy_object_proxy
import pendulum
from dateutil import relativedelta
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow.compat.functools import cache
from airflow.configuration import conf
from airflow.datasets import Dataset
from airflow.exceptions import AirflowException, RemovedInAirflow3Warning, SerializationError
from airflow.jobs.job import Job
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.connection import Connection
from airflow.models.dag import DAG, create_timetable
from airflow.models.dagrun import DagRun
from airflow.models.expandinput import EXPAND_INPUT_EMPTY, ExpandInput, create_expand_input, get_map_type_key
from airflow.models.mappedoperator import MappedOperator
from airflow.models.operator import Operator
from airflow.models.param import Param, ParamsDict
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance
from airflow.models.taskmixin import DAGNode
from airflow.models.xcom_arg import XComArg, deserialize_xcom_arg, serialize_xcom_arg
from airflow.providers_manager import ProvidersManager
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import serialize_template_field
from airflow.serialization.json_schema import Validator, load_dag_schema
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.serialization.pydantic.dataset import DatasetPydantic
from airflow.serialization.pydantic.job import JobPydantic
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.settings import _ENABLE_AIP_44, DAGS_FOLDER, json
from airflow.timetables.base import Timetable
from airflow.utils.code_utils import get_python_source
from airflow.utils.docs import get_docs_url
from airflow.utils.module_loading import import_string, qualname
from airflow.utils.operator_resources import Resources
from airflow.utils.task_group import MappedTaskGroup, TaskGroup
from airflow.utils.types import NOTSET, ArgNotSet

if TYPE_CHECKING:
    from pydantic import BaseModel

    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

    HAS_KUBERNETES: bool
    try:
        from kubernetes.client import models as k8s

        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
    except ImportError:
        pass

log = logging.getLogger(__name__)

_OPERATOR_EXTRA_LINKS: set[str] = {
    "airflow.operators.trigger_dagrun.TriggerDagRunLink",
    "airflow.sensors.external_task.ExternalDagLink",
    # Deprecated names, so that existing serialized dags load straight away.
    "airflow.sensors.external_task.ExternalTaskSensorLink",
    "airflow.operators.dagrun_operator.TriggerDagRunLink",
    "airflow.sensors.external_task_sensor.ExternalTaskSensorLink",
}


@cache
def get_operator_extra_links() -> set[str]:
    """Get the operator extra links.

    This includes both the built-in ones, and those come from the providers.
    """
    _OPERATOR_EXTRA_LINKS.update(ProvidersManager().extra_links_class_names)
    return _OPERATOR_EXTRA_LINKS


@cache
def _get_default_mapped_partial() -> dict[str, Any]:
    """Get default partial kwargs in a mapped operator.

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


def encode_timezone(var: Timezone) -> str | int:
    """Encode a Pendulum Timezone for serialization.

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


def decode_timezone(var: str | int) -> Timezone:
    """Decode a previously serialized Pendulum Timezone."""
    return pendulum.tz.timezone(var)


def _get_registered_timetable(importable_string: str) -> type[Timetable] | None:
    from airflow import plugins_manager

    if importable_string.startswith("airflow.timetables."):
        return import_string(importable_string)
    plugins_manager.initialize_timetables_plugins()
    if plugins_manager.timetable_classes:
        return plugins_manager.timetable_classes.get(importable_string)
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


def _encode_timetable(var: Timetable) -> dict[str, Any]:
    """Encode a timetable instance.

    This delegates most of the serialization work to the type, so the behavior
    can be completely controlled by a custom subclass.
    """
    timetable_class = type(var)
    importable_string = qualname(timetable_class)
    if _get_registered_timetable(importable_string) is None:
        raise _TimetableNotRegistered(importable_string)
    return {Encoding.TYPE: importable_string, Encoding.VAR: var.serialize()}


def _decode_timetable(var: dict[str, Any]) -> Timetable:
    """Decode a previously serialized timetable.

    Most of the deserialization logic is delegated to the actual type, which
    we import from string.
    """
    importable_string = var[Encoding.TYPE]
    timetable_class = _get_registered_timetable(importable_string)
    if timetable_class is None:
        raise _TimetableNotRegistered(importable_string)
    return timetable_class.deserialize(var[Encoding.VAR])


class _XComRef(NamedTuple):
    """Used to store info needed to create XComArg.

    We can't turn it in to a XComArg until we've loaded _all_ the tasks, so when
    deserializing an operator, we need to create something in its place, and
    post-process it in ``deserialize_dag``.
    """

    data: dict

    def deref(self, dag: DAG) -> XComArg:
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
    """Used to store info needed to create a mapped operator's expand input.

    This references a ``ExpandInput`` type, but replaces ``XComArg`` objects
    with ``_XComRef`` (see documentation on the latter type for reasoning).
    """

    key: str
    value: _ExpandInputSerializedValue

    @classmethod
    def validate_expand_input_value(cls, value: _ExpandInputOriginalValue) -> None:
        """Validate we've covered all ``ExpandInput.value`` types.

        This function does not actually do anything, but is called during
        serialization so Mypy will *statically* check we have handled all
        possible ExpandInput cases.
        """

    def deref(self, dag: DAG) -> ExpandInput:
        """De-reference into a concrete ExpandInput object.

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

    SERIALIZER_VERSION = 1

    @classmethod
    def to_json(cls, var: DAG | BaseOperator | dict | list | set | tuple) -> str:
        """Stringifies DAGs and operators contained by var and returns a JSON string of var."""
        return json.dumps(cls.to_dict(var), ensure_ascii=True)

    @classmethod
    def to_dict(cls, var: DAG | BaseOperator | dict | list | set | tuple) -> dict:
        """Stringifies DAGs and operators contained by var and returns a dict of var."""
        # Don't call on this class directly - only SerializedDAG or
        # SerializedBaseOperator should be used as the "entrypoint"
        raise NotImplementedError()

    @classmethod
    def from_json(cls, serialized_obj: str) -> BaseSerialization | dict | list | set | tuple:
        """Deserializes json_str and reconstructs all DAGs and operators it contains."""
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
        """Types excluded from serialization."""
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
        cls, object_to_serialize: BaseOperator | MappedOperator | DAG, decorated_fields: set
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
                task_type = getattr(object_to_serialize, "_task_type", None)
                if value != task_type:
                    serialized_object[key] = cls.serialize(value)
            elif key in decorated_fields:
                serialized_object[key] = cls.serialize(value)
            elif key == "timetable" and value is not None:
                serialized_object[key] = _encode_timetable(value)
            else:
                value = cls.serialize(value)
                if isinstance(value, dict) and Encoding.TYPE in value:
                    value = value[Encoding.VAR]
                serialized_object[key] = value
        return serialized_object

    @classmethod
    def serialize(
        cls, var: Any, *, strict: bool = False, use_pydantic_models: bool = False
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
        if use_pydantic_models and not _ENABLE_AIP_44:
            raise RuntimeError(
                "Setting use_pydantic_models = True requires AIP-44 (in progress) feature flag to be true. "
                "This parameter will be removed eventually when new serialization is used by AIP-44"
            )
        if cls._is_primitive(var):
            # enum.IntEnum is an int instance, it causes json dumps error so we use its value.
            if isinstance(var, enum.Enum):
                return var.value
            return var
        elif isinstance(var, dict):
            return cls._encode(
                {
                    str(k): cls.serialize(v, strict=strict, use_pydantic_models=use_pydantic_models)
                    for k, v in var.items()
                },
                type_=DAT.DICT,
            )
        elif isinstance(var, list):
            return [cls.serialize(v, strict=strict, use_pydantic_models=use_pydantic_models) for v in var]
        elif var.__class__.__name__ == "V1Pod" and _has_kubernetes() and isinstance(var, k8s.V1Pod):
            json_pod = PodGenerator.serialize_pod(var)
            return cls._encode(json_pod, type_=DAT.POD)
        elif isinstance(var, DAG):
            return SerializedDAG.serialize_dag(var)
        elif isinstance(var, Resources):
            return var.to_dict()
        elif isinstance(var, MappedOperator):
            return SerializedBaseOperator.serialize_mapped_operator(var)
        elif isinstance(var, BaseOperator):
            return SerializedBaseOperator.serialize_operator(var)
        elif isinstance(var, cls._datetime_types):
            return cls._encode(var.timestamp(), type_=DAT.DATETIME)
        elif isinstance(var, datetime.timedelta):
            return cls._encode(var.total_seconds(), type_=DAT.TIMEDELTA)
        elif isinstance(var, Timezone):
            return cls._encode(encode_timezone(var), type_=DAT.TIMEZONE)
        elif isinstance(var, relativedelta.relativedelta):
            return cls._encode(encode_relativedelta(var), type_=DAT.RELATIVEDELTA)
        elif callable(var):
            return str(get_python_source(var))
        elif isinstance(var, set):
            # FIXME: casts set to list in customized serialization in future.
            try:
                return cls._encode(
                    sorted(
                        cls.serialize(v, strict=strict, use_pydantic_models=use_pydantic_models) for v in var
                    ),
                    type_=DAT.SET,
                )
            except TypeError:
                return cls._encode(
                    [cls.serialize(v, strict=strict, use_pydantic_models=use_pydantic_models) for v in var],
                    type_=DAT.SET,
                )
        elif isinstance(var, tuple):
            # FIXME: casts tuple to list in customized serialization in future.
            return cls._encode(
                [cls.serialize(v, strict=strict, use_pydantic_models=use_pydantic_models) for v in var],
                type_=DAT.TUPLE,
            )
        elif isinstance(var, TaskGroup):
            return TaskGroupSerialization.serialize_task_group(var)
        elif isinstance(var, Param):
            return cls._encode(cls._serialize_param(var), type_=DAT.PARAM)
        elif isinstance(var, XComArg):
            return cls._encode(serialize_xcom_arg(var), type_=DAT.XCOM_REF)
        elif isinstance(var, Dataset):
            return cls._encode(dict(uri=var.uri, extra=var.extra), type_=DAT.DATASET)
        elif isinstance(var, SimpleTaskInstance):
            return cls._encode(
                cls.serialize(var.__dict__, strict=strict, use_pydantic_models=use_pydantic_models),
                type_=DAT.SIMPLE_TASK_INSTANCE,
            )
        elif use_pydantic_models and _ENABLE_AIP_44:

            def _pydantic_model_dump(model_cls: type[BaseModel], var: Any) -> dict[str, Any]:
                try:
                    return model_cls.model_validate(var).model_dump(mode="json")  # type: ignore[attr-defined]
                except AttributeError:  # Pydantic 1.x compatibility.
                    return model_cls.from_orm(var).dict()  # type: ignore[attr-defined]

            if isinstance(var, Job):
                return cls._encode(_pydantic_model_dump(JobPydantic, var), type_=DAT.BASE_JOB)
            elif isinstance(var, TaskInstance):
                return cls._encode(_pydantic_model_dump(TaskInstancePydantic, var), type_=DAT.TASK_INSTANCE)
            elif isinstance(var, DagRun):
                return cls._encode(_pydantic_model_dump(DagRunPydantic, var), type_=DAT.DAG_RUN)
            elif isinstance(var, Dataset):
                return cls._encode(_pydantic_model_dump(DatasetPydantic, var), type_=DAT.DATA_SET)
            else:
                return cls.default_serialization(strict, var)
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
    def deserialize(cls, encoded_var: Any, use_pydantic_models=False) -> Any:
        """
        Deserialize an object; helper function of depth first search for deserialization.

        :meta private:
        """
        # JSON primitives (except for dict) are not encoded.
        if use_pydantic_models and not _ENABLE_AIP_44:
            raise RuntimeError(
                "Setting use_pydantic_models = True requires AIP-44 (in progress) feature flag to be true. "
                "This parameter will be removed eventually when new serialization is used by AIP-44"
            )
        if cls._is_primitive(encoded_var):
            return encoded_var
        elif isinstance(encoded_var, list):
            return [cls.deserialize(v, use_pydantic_models) for v in encoded_var]

        if not isinstance(encoded_var, dict):
            raise ValueError(f"The encoded_var should be dict and is {type(encoded_var)}")
        var = encoded_var[Encoding.VAR]
        type_ = encoded_var[Encoding.TYPE]

        if type_ == DAT.DICT:
            return {k: cls.deserialize(v, use_pydantic_models) for k, v in var.items()}
        elif type_ == DAT.DAG:
            return SerializedDAG.deserialize_dag(var)
        elif type_ == DAT.OP:
            return SerializedBaseOperator.deserialize_operator(var)
        elif type_ == DAT.DATETIME:
            return pendulum.from_timestamp(var)
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
        elif type_ == DAT.SET:
            return {cls.deserialize(v, use_pydantic_models) for v in var}
        elif type_ == DAT.TUPLE:
            return tuple(cls.deserialize(v, use_pydantic_models) for v in var)
        elif type_ == DAT.PARAM:
            return cls._deserialize_param(var)
        elif type_ == DAT.XCOM_REF:
            return _XComRef(var)  # Delay deserializing XComArg objects until we have the entire DAG.
        elif type_ == DAT.DATASET:
            return Dataset(**var)
        elif type_ == DAT.SIMPLE_TASK_INSTANCE:
            return SimpleTaskInstance(**cls.deserialize(var))
        elif use_pydantic_models and _ENABLE_AIP_44:
            if type_ == DAT.BASE_JOB:
                return JobPydantic.parse_obj(var)
            elif type_ == DAT.TASK_INSTANCE:
                return TaskInstancePydantic.parse_obj(var)
            elif type_ == DAT.DAG_RUN:
                return DagRunPydantic.parse_obj(var)
            elif type_ == DAT.DATA_SET:
                return DatasetPydantic.parse_obj(var)
        elif type_ == DAT.ARG_NOT_SET:
            return NOTSET
        else:
            raise TypeError(f"Invalid type {type_!s} in deserialization.")

    _deserialize_datetime = pendulum.from_timestamp
    _deserialize_timezone = pendulum.tz.timezone

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
        if attrname in cls._CONSTRUCTOR_PARAMS and (
            cls._CONSTRUCTOR_PARAMS[attrname] is value or (value in [{}, []])
        ):
            return True
        return False

    @classmethod
    def _serialize_param(cls, param: Param):
        return dict(
            __class=f"{param.__module__}.{param.__class__.__name__}",
            default=cls.serialize(param.value),
            description=cls.serialize(param.description),
            schema=cls.serialize(param.schema),
        )

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
            if attr not in param_dict:
                continue
            val = param_dict[attr]
            if is_serialized(val):
                deserialized_val = cls.deserialize(param_dict[attr])
                kwargs[attr] = deserialized_val
            else:
                kwargs[attr] = val
        return class_(**kwargs)

    @classmethod
    def _serialize_params_dict(cls, params: ParamsDict | dict):
        """Serialize Params dict for a DAG or task."""
        serialized_params = {}
        for k, v in params.items():
            # TODO: As of now, we would allow serialization of params which are of type Param only.
            try:
                class_identity = f"{v.__module__}.{v.__class__.__name__}"
            except AttributeError:
                class_identity = ""
            if class_identity == "airflow.models.param.Param":
                serialized_params[k] = cls._serialize_param(v)
            else:
                raise ValueError(
                    f"Params to a DAG or a Task can be only of type airflow.models.param.Param, "
                    f"but param {k!r} is {v.__class__}"
                )
        return serialized_params

    @classmethod
    def _deserialize_params_dict(cls, encoded_params: dict) -> ParamsDict:
        """Deserialize a DAG's Params dict."""
        op_params = {}
        for k, v in encoded_params.items():
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
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator
        from airflow.sensors.external_task import ExternalTaskSensor

        deps = []
        if isinstance(task, TriggerDagRunOperator):
            deps.append(
                DagDependency(
                    source=task.dag_id,
                    target=getattr(task, "trigger_dag_id"),
                    dependency_type="trigger",
                    dependency_id=task.task_id,
                )
            )
        elif isinstance(task, ExternalTaskSensor):
            deps.append(
                DagDependency(
                    source=getattr(task, "external_dag_id"),
                    target=task.dag_id,
                    dependency_type="sensor",
                    dependency_id=task.task_id,
                )
            )
        for obj in task.outlets or []:
            if isinstance(obj, Dataset):
                deps.append(
                    DagDependency(
                        source=task.dag_id,
                        target="dataset",
                        dependency_type="dataset",
                        dependency_id=obj.uri,
                    )
                )
        return deps

    @staticmethod
    def detect_dag_dependencies(dag: DAG | None) -> Iterable[DagDependency]:
        """Detect dependencies set directly on the DAG object."""
        if not dag:
            return
        for x in dag.dataset_triggers:
            yield DagDependency(
                source="dataset",
                target=dag.dag_id,
                dependency_type="dataset",
                dependency_id=x.uri,
            )


class SerializedBaseOperator(BaseOperator, BaseSerialization):
    """A JSON serializable representation of operator.

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
        for k, v in signature(BaseOperator.__init__).parameters.items()
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
        return self._operator_name

    @operator_name.setter
    def operator_name(self, operator_name: str):
        self._operator_name = operator_name

    @classmethod
    def serialize_mapped_operator(cls, op: MappedOperator) -> dict[str, Any]:
        serialized_op = cls._serialize_node(op, include_deps=op.deps != MappedOperator.deps_for(BaseOperator))
        # Handle expand_input and op_kwargs_expand_input.
        expansion_kwargs = op._get_specified_expand_input()
        if TYPE_CHECKING:  # Let Mypy check the input type for us!
            _ExpandInputRef.validate_expand_input_value(expansion_kwargs.value)
        serialized_op[op._expand_input_attr] = {
            "type": get_map_type_key(expansion_kwargs),
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
    def serialize_operator(cls, op: BaseOperator) -> dict[str, Any]:
        return cls._serialize_node(op, include_deps=op.deps is not BaseOperator.deps)

    @classmethod
    def _serialize_node(cls, op: BaseOperator | MappedOperator, include_deps: bool) -> dict[str, Any]:
        """Serialize operator into a JSON object."""
        serialize_op = cls.serialize_to_json(op, cls._decorated_fields)
        serialize_op["_task_type"] = getattr(op, "_task_type", type(op).__name__)
        serialize_op["_task_module"] = getattr(op, "_task_module", type(op).__module__)
        if op.operator_name != serialize_op["_task_type"]:
            serialize_op["_operator_name"] = op.operator_name

        # Used to determine if an Operator is inherited from EmptyOperator
        serialize_op["_is_empty"] = op.inherits_from_empty_operator

        if op.operator_extra_links:
            serialize_op["_operator_extra_links"] = cls._serialize_operator_extra_links(
                op.operator_extra_links.__get__(op)
                if isinstance(op.operator_extra_links, property)
                else op.operator_extra_links
            )

        if include_deps:
            serialize_op["deps"] = cls._serialize_deps(op.deps)

        # Store all template_fields as they are if there are JSON Serializable
        # If not, store them as strings
        # And raise an exception if the field is not templateable
        forbidden_fields = set(inspect.signature(BaseOperator.__init__).parameters.keys())
        if op.template_fields:
            for template_field in op.template_fields:
                if template_field in forbidden_fields:
                    raise AirflowException(f"Cannot template BaseOperator fields: {template_field}")
                value = getattr(op, template_field, None)
                if not cls._is_excluded(value, template_field, op):
                    serialize_op[template_field] = serialize_template_field(value)

        if op.params:
            serialize_op["params"] = cls._serialize_params_dict(op.params)

        return serialize_op

    @classmethod
    def _serialize_deps(cls, op_deps: Iterable[BaseTIDep]) -> list[str]:
        from airflow import plugins_manager

        plugins_manager.initialize_ti_deps_plugins()
        if plugins_manager.registered_ti_dep_classes is None:
            raise AirflowException("Can not load plugins")

        deps = []
        for dep in op_deps:
            klass = type(dep)
            module_name = klass.__module__
            qualname = f"{module_name}.{klass.__name__}"
            if (
                not qualname.startswith("airflow.ti_deps.deps.")
                and qualname not in plugins_manager.registered_ti_dep_classes
            ):
                raise SerializationError(
                    f"Custom dep class {qualname} not serialized, please register it through plugins."
                )
            deps.append(qualname)
        # deps needs to be sorted here, because op_deps is a set, which is unstable when traversing,
        # and the same call may get different results.
        # When calling json.dumps(self.data, sort_keys=True) to generate dag_hash, misjudgment will occur
        return sorted(deps)

    @classmethod
    def populate_operator(cls, op: Operator, encoded_op: dict[str, Any]) -> None:
        """Populate operator attributes with serialized values.

        This covers simple attributes that don't reference other things in the
        DAG. Setting references (such as ``op.dag`` and task dependencies) is
        done in ``set_task_dag_references`` instead, which is called after the
        DAG is hydrated.
        """
        if "label" not in encoded_op:
            # Handle deserialization of old data before the introduction of TaskGroup
            encoded_op["label"] = encoded_op["task_id"]

        # Extra Operator Links defined in Plugins
        op_extra_links_from_plugin = {}

        if "_operator_name" not in encoded_op:
            encoded_op["_operator_name"] = encoded_op["_task_type"]

        # We don't want to load Extra Operator links in Scheduler
        if cls._load_operator_extra_links:
            from airflow import plugins_manager

            plugins_manager.initialize_extra_operators_links_plugins()

            if plugins_manager.operator_extra_links is None:
                raise AirflowException("Can not load plugins")

            for ope in plugins_manager.operator_extra_links:
                for operator in ope.operators:
                    if (
                        operator.__name__ == encoded_op["_task_type"]
                        and operator.__module__ == encoded_op["_task_module"]
                    ):
                        op_extra_links_from_plugin.update({ope.name: ope})

            # If OperatorLinks are defined in Plugins but not in the Operator that is being Serialized
            # set the Operator links attribute
            # The case for "If OperatorLinks are defined in the operator that is being Serialized"
            # is handled in the deserialization loop where it matches k == "_operator_extra_links"
            if op_extra_links_from_plugin and "_operator_extra_links" not in encoded_op:
                setattr(op, "operator_extra_links", list(op_extra_links_from_plugin.values()))

        for k, v in encoded_op.items():
            # Todo: TODO: Remove in Airflow 3.0 when dummy operator is removed
            if k == "_is_dummy":
                k = "_is_empty"

            if k in ("_outlets", "_inlets"):
                # `_outlets` -> `outlets`
                k = k[1:]
            if k == "_downstream_task_ids":
                # Upgrade from old format/name
                k = "downstream_task_ids"
            if k == "label":
                # Label shouldn't be set anymore --  it's computed from task_id now
                continue
            elif k == "downstream_task_ids":
                v = set(v)
            elif k == "subdag":
                v = SerializedDAG.deserialize_dag(v)
            elif k in {"retry_delay", "execution_timeout", "sla", "max_retry_delay"}:
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

            elif k == "deps":
                v = cls._deserialize_deps(v)
            elif k == "params":
                v = cls._deserialize_params_dict(v)
                if op.params:  # Merge existing params if needed.
                    v, new = op.params, v
                    v.update(new)
            elif k == "partial_kwargs":
                v = {arg: cls.deserialize(value) for arg, value in v.items()}
            elif k in {"expand_input", "op_kwargs_expand_input"}:
                v = _ExpandInputRef(v["type"], cls.deserialize(v["value"]))
            elif k in cls._decorated_fields or k not in op.get_serialized_fields():
                v = cls.deserialize(v)
            elif k in ("outlets", "inlets"):
                v = cls.deserialize(v)
            elif k == "on_failure_fail_dagrun":
                k = "_on_failure_fail_dagrun"
            # else use v as it is

            setattr(op, k, v)

        for k in op.get_serialized_fields() - encoded_op.keys() - cls._CONSTRUCTOR_PARAMS.keys():
            # TODO: refactor deserialization of BaseOperator and MappedOperaotr (split it out), then check
            # could go away.
            if not hasattr(op, k):
                setattr(op, k, None)

        # Set all the template_field to None that were not present in Serialized JSON
        for field in op.template_fields:
            if not hasattr(op, field):
                setattr(op, field, None)

        # Used to determine if an Operator is inherited from EmptyOperator
        setattr(op, "_is_empty", bool(encoded_op.get("_is_empty", False)))

    @staticmethod
    def set_task_dag_references(task: Operator, dag: DAG) -> None:
        """Handle DAG references on an operator.

        The operator should have been mostly populated earlier by calling
        ``populate_operator``. This function further fixes object references
        that were not possible before the task's containing DAG is hydrated.
        """
        task.dag = dag

        for date_attr in ("start_date", "end_date"):
            if getattr(task, date_attr, None) is None:
                setattr(task, date_attr, getattr(dag, date_attr, None))

        if task.subdag is not None:
            task.subdag.parent_dag = dag

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
            op_data = {k: v for k, v in encoded_op.items() if k in BaseOperator.get_serialized_fields()}
            try:
                operator_name = encoded_op["_operator_name"]
            except KeyError:
                operator_name = encoded_op["_task_type"]
            op = MappedOperator(
                operator_class=op_data,
                expand_input=EXPAND_INPUT_EMPTY,
                partial_kwargs={},
                task_id=encoded_op["task_id"],
                params={},
                deps=MappedOperator.deps_for(BaseOperator),
                operator_extra_links=BaseOperator.operator_extra_links,
                template_ext=BaseOperator.template_ext,
                template_fields=BaseOperator.template_fields,
                template_fields_renderers=BaseOperator.template_fields_renderers,
                ui_color=BaseOperator.ui_color,
                ui_fgcolor=BaseOperator.ui_fgcolor,
                is_empty=False,
                task_module=encoded_op["_task_module"],
                task_type=encoded_op["_task_type"],
                operator_name=operator_name,
                dag=None,
                task_group=None,
                start_date=None,
                end_date=None,
                disallow_kwargs_override=encoded_op["_disallow_kwargs_override"],
                expand_input_attr=encoded_op["_expand_input_attr"],
            )
        else:
            op = SerializedBaseOperator(task_id=encoded_op["task_id"])

        cls.populate_operator(op, encoded_op)
        return op

    @classmethod
    def detect_dependencies(cls, op: Operator) -> set[DagDependency]:
        """Detect between DAG dependencies for the operator."""

        def get_custom_dep() -> list[DagDependency]:
            """
            If custom dependency detector is configured, use it.

            TODO: Remove this logic in 3.0.
            """
            custom_dependency_detector_cls = conf.getimport("scheduler", "dependency_detector", fallback=None)
            if not (
                custom_dependency_detector_cls is None or custom_dependency_detector_cls is DependencyDetector
            ):
                warnings.warn(
                    "Use of a custom dependency detector is deprecated. "
                    "Support will be removed in a future release.",
                    RemovedInAirflow3Warning,
                )
                dep = custom_dependency_detector_cls().detect_task_dependencies(op)
                if type(dep) is DagDependency:
                    return [dep]
            return []

        dependency_detector = DependencyDetector()
        deps = set(dependency_detector.detect_task_dependencies(op))
        deps.update(get_custom_dep())  # todo: remove in 3.0
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
    def _deserialize_deps(cls, deps: list[str]) -> set[BaseTIDep]:
        from airflow import plugins_manager

        plugins_manager.initialize_ti_deps_plugins()
        if plugins_manager.registered_ti_dep_classes is None:
            raise AirflowException("Can not load plugins")

        instances = set()
        for qn in set(deps):
            if (
                not qn.startswith("airflow.ti_deps.deps.")
                and qn not in plugins_manager.registered_ti_dep_classes
            ):
                raise SerializationError(
                    f"Custom dep class {qn} not deserialized, please register it through plugins."
                )

            try:
                instances.add(import_string(qn)())
            except ImportError:
                log.warning("Error importing dep %r", qn, exc_info=True)
        return instances

    @classmethod
    def _deserialize_operator_extra_links(cls, encoded_op_links: list) -> dict[str, BaseOperatorLink]:
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

        for _operator_links_source in encoded_op_links:
            # Get the key, value pair as Tuple where key is OperatorLink ClassName
            # and value is the dictionary containing the arguments passed to the OperatorLink
            #
            # Example of a single iteration:
            #
            #   _operator_links_source =
            #   {
            #       'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink': {
            #           'index': 0
            #       }
            #   },
            #
            #   list(_operator_links_source.items()) =
            #   [
            #       (
            #           'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink',
            #           {'index': 0}
            #       )
            #   ]
            #
            #   list(_operator_links_source.items())[0] =
            #   (
            #       'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink',
            #       {
            #           'index': 0
            #       }
            #   )

            _operator_link_class_path, data = next(iter(_operator_links_source.items()))
            if _operator_link_class_path in get_operator_extra_links():
                single_op_link_class = import_string(_operator_link_class_path)
            elif _operator_link_class_path in plugins_manager.registered_operator_link_classes:
                single_op_link_class = plugins_manager.registered_operator_link_classes[
                    _operator_link_class_path
                ]
            else:
                log.error("Operator Link class %r not registered", _operator_link_class_path)
                return {}

            op_predefined_extra_link: BaseOperatorLink = cattr.structure(data, single_op_link_class)

            op_predefined_extra_links.update({op_predefined_extra_link.name: op_predefined_extra_link})

        return op_predefined_extra_links

    @classmethod
    def _serialize_operator_extra_links(cls, operator_extra_links: Iterable[BaseOperatorLink]):
        """
        Serialize Operator Links.

        Store the import path of the OperatorLink and the arguments passed to it.
        For example:
        ``[{'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink': {}}]``

        :param operator_extra_links: Operator Link
        :return: Serialized Operator Link
        """
        serialize_operator_extra_links = []
        for operator_extra_link in operator_extra_links:
            op_link_arguments = cattr.unstructure(operator_extra_link)
            if not isinstance(op_link_arguments, dict):
                op_link_arguments = {}

            module_path = (
                f"{operator_extra_link.__class__.__module__}.{operator_extra_link.__class__.__name__}"
            )
            serialize_operator_extra_links.append({module_path: op_link_arguments})

        return serialize_operator_extra_links

    @classmethod
    def serialize(cls, var: Any, *, strict: bool = False, use_pydantic_models: bool = False) -> Any:
        # the wonders of multiple inheritance BaseOperator defines an instance method
        return BaseSerialization.serialize(var=var, strict=strict, use_pydantic_models=use_pydantic_models)

    @classmethod
    def deserialize(cls, encoded_var: Any, use_pydantic_models: bool = False) -> Any:
        return BaseSerialization.deserialize(encoded_var=encoded_var, use_pydantic_models=use_pydantic_models)


class SerializedDAG(DAG, BaseSerialization):
    """
    A JSON serializable representation of DAG.

    A stringified DAG can only be used in the scope of scheduler and webserver, because fields
    that are not serializable, such as functions and customer defined classes, are casted to
    strings.

    Compared with SimpleDAG: SerializedDAG contains all information for webserver.
    Compared with DagPickle: DagPickle contains all information for worker, but some DAGs are
    not pickle-able. SerializedDAG works for all DAGs.
    """

    _decorated_fields = {"schedule_interval", "default_args", "_access_control"}

    @staticmethod
    def __get_constructor_defaults():
        param_to_attr = {
            "max_active_tasks": "_max_active_tasks",
            "description": "_description",
            "default_view": "_default_view",
            "access_control": "_access_control",
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

            # If schedule_interval is backed by timetable, serialize only
            # timetable; vice versa for a timetable backed by schedule_interval.
            if dag.timetable.summary == dag.schedule_interval:
                del serialized_dag["schedule_interval"]
            else:
                del serialized_dag["timetable"]

            serialized_dag["tasks"] = [cls.serialize(task) for _, task in dag.task_dict.items()]

            dag_deps = {
                dep
                for task in dag.task_dict.values()
                for dep in SerializedBaseOperator.detect_dependencies(task)
            }
            dag_deps.update(DependencyDetector.detect_dag_dependencies(dag))
            serialized_dag["dag_dependencies"] = [x.__dict__ for x in sorted(dag_deps)]
            serialized_dag["_task_group"] = TaskGroupSerialization.serialize_task_group(dag.task_group)

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
        dag = SerializedDAG(dag_id=encoded_dag["_dag_id"])

        for k, v in encoded_dag.items():
            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "tasks":
                SerializedBaseOperator._load_operator_extra_links = cls._load_operator_extra_links

                v = {task["task_id"]: SerializedBaseOperator.deserialize_operator(task) for task in v}
                k = "task_dict"
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
                v = _decode_timetable(v)
            elif k in cls._decorated_fields:
                v = cls.deserialize(v)
            elif k == "params":
                v = cls._deserialize_params_dict(v)
            elif k == "dataset_triggers":
                v = cls.deserialize(v)
            # else use v as it is

            setattr(dag, k, v)

        # A DAG is always serialized with only one of schedule_interval and
        # timetable. This back-populates the other to ensure the two attributes
        # line up correctly on the DAG instance.
        if "timetable" in encoded_dag:
            dag.schedule_interval = dag.timetable.summary
        else:
            dag.timetable = create_timetable(dag.schedule_interval, dag.timezone)

        # Set _task_group
        if "_task_group" in encoded_dag:
            dag._task_group = TaskGroupSerialization.deserialize_task_group(
                encoded_dag["_task_group"],
                None,
                dag.task_dict,
                dag,
            )
        else:
            # This must be old data that had no task_group. Create a root TaskGroup and add
            # all tasks to it.
            dag._task_group = TaskGroup.create_root(dag)
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
    def to_dict(cls, var: Any) -> dict:
        """Stringifies DAGs and operators contained by var and returns a dict of var."""
        json_dict = {"__version": cls.SERIALIZER_VERSION, "dag": cls.serialize_dag(var)}

        # Validate Serialized DAG with Json Schema. Raises Error if it mismatches
        cls.validate_schema(json_dict)
        return json_dict

    @classmethod
    def from_dict(cls, serialized_obj: dict) -> SerializedDAG:
        """Deserializes a python dict in to the DAG and operators it contains."""
        ver = serialized_obj.get("__version", "<not present>")
        if ver != cls.SERIALIZER_VERSION:
            raise ValueError(f"Unsure how to deserialize version {ver!r}")
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
                "type": get_map_type_key(expand_input),
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
            label: set_ref(task_dict[val])
            if _type == DAT.OP
            else cls.deserialize_task_group(val, group, task_dict, dag=dag)
            for label, (_type, val) in encoded_group["children"].items()
        }
        group.upstream_group_ids.update(cls.deserialize(encoded_group["upstream_group_ids"]))
        group.downstream_group_ids.update(cls.deserialize(encoded_group["downstream_group_ids"]))
        group.upstream_task_ids.update(cls.deserialize(encoded_group["upstream_task_ids"]))
        group.downstream_task_ids.update(cls.deserialize(encoded_group["downstream_task_ids"]))
        return group


@dataclass(frozen=True, order=True)
class DagDependency:
    """
    Dataclass for representing dependencies between DAGs.

    These are calculated during serialization and attached to serialized DAGs.
    """

    source: str
    target: str
    dependency_type: str
    dependency_id: str | None = None

    @property
    def node_id(self):
        """Node ID for graph rendering."""
        val = f"{self.dependency_type}"
        if not self.dependency_type == "dataset":
            val += f":{self.source}:{self.target}"
        if self.dependency_id:
            val += f":{self.dependency_id}"
        return val


def _has_kubernetes() -> bool:
    global HAS_KUBERNETES
    if "HAS_KUBERNETES" in globals():
        return HAS_KUBERNETES

    # Loading kube modules is expensive, so delay it until the last moment

    try:
        from kubernetes.client import models as k8s

        try:
            from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
        except ImportError:
            from airflow.kubernetes.pre_7_4_0_compatibility.pod_generator import (  # type: ignore[assignment]
                PodGenerator,
            )

        globals()["k8s"] = k8s
        globals()["PodGenerator"] = PodGenerator

        # isort: on
        HAS_KUBERNETES = True
    except ImportError:
        HAS_KUBERNETES = False
    return HAS_KUBERNETES
