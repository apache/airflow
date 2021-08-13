# -*- coding: utf-8 -*-
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

"""Serialized DAG and BaseOperator"""

import datetime
import enum
import logging
import six
from typing import TYPE_CHECKING, Optional, Union, Dict, List

import cattr
import pendulum
from dateutil import relativedelta

from airflow import DAG, AirflowException
from airflow.models.baseoperator import BaseOperator, BaseOperatorLink
from airflow.models.connection import Connection
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.helpers import serialize_template_field
from airflow.serialization.json_schema import Validator, load_dag_schema
from airflow.settings import json
from airflow.utils.module_loading import import_string
from airflow.www.utils import get_python_source

try:
    from inspect import signature
except ImportError:
    from funcsigs import signature  # type: ignore

if TYPE_CHECKING:
    from inspect import Parameter

log = logging.getLogger(__name__)


BUILTIN_OPERATOR_EXTRA_LINKS = [
    "airflow.contrib.operators.bigquery_operator.BigQueryConsoleLink",
    "airflow.contrib.operators.bigquery_operator.BigQueryConsoleIndexableLink",
    "airflow.contrib.operators.qubole_operator.QDSLink",
    # providers new paths
    "airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink",
    "airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink",
    "airflow.providers.qubole.operators.qubole.QDSLink"
]  # type: List[str]


class BaseSerialization:
    """BaseSerialization provides utils for serialization."""

    # JSON primitive types.
    _primitive_types = (int, bool, float) + six.string_types

    # Time types.
    # datetime.date and datetime.time are converted to strings.
    _datetime_types = (datetime.datetime,)

    # Object types that are always excluded in serialization.
    _excluded_types = (logging.Logger, Connection, type)

    _json_schema = None     # type: Optional[Validator]

    _CONSTRUCTOR_PARAMS = {}  # type: Dict[str, Parameter]

    SERIALIZER_VERSION = 1

    @classmethod
    def to_json(cls, var):
        # type: (Union[DAG, BaseOperator, dict, list, set, tuple]) -> str
        """Stringifies DAGs and operators contained by var and returns a JSON string of var.
        """
        return json.dumps(cls.to_dict(var), ensure_ascii=True)

    @classmethod
    def to_dict(cls, var):
        # type: (Union[DAG, BaseOperator, dict, list, set, tuple]) -> dict
        """Stringifies DAGs and operators contained by var and returns a dict of var.
        """
        # Don't call on this class directly - only SerializedDAG or
        # SerializedBaseOperator should be used as the "entrypoint"
        raise NotImplementedError()

    @classmethod
    def from_json(cls, serialized_obj):
        # type: (str) -> Union['BaseSerialization', dict, list, set, tuple]
        """Deserializes json_str and reconstructs all DAGs and operators it contains."""
        return cls.from_dict(json.loads(serialized_obj))

    @classmethod
    def from_dict(cls, serialized_obj):
        # type: (dict) -> Union['BaseSerialization', dict, list, set, tuple]
        """Deserializes a python dict stored with type decorators and
        reconstructs all DAGs and operators it contains."""
        return cls._deserialize(serialized_obj)

    @classmethod
    def validate_schema(cls, serialized_obj):
        # type: (Union[str, dict]) -> None
        """Validate serialized_obj satisfies JSON schema."""
        if cls._json_schema is None:
            raise AirflowException('JSON schema of {:s} is not set.'.format(cls.__name__))

        if isinstance(serialized_obj, dict):
            cls._json_schema.validate(serialized_obj)
        elif isinstance(serialized_obj, str):
            cls._json_schema.validate(json.loads(serialized_obj))
        else:
            raise TypeError("Invalid type: Only dict and str are supported.")

    @staticmethod
    def _encode(x, type_):
        """Encode data by a JSON dict."""
        return {Encoding.VAR: x, Encoding.TYPE: type_}

    @classmethod
    def _is_primitive(cls, var):
        """Primitive types."""
        return var is None or isinstance(var, cls._primitive_types)

    @classmethod
    def _is_excluded(cls, var, attrname, instance):
        """Types excluded from serialization."""

        if var is None:
            if not cls._is_constructor_param(attrname, instance):
                # Any instance attribute, that is not a constructor argument, we exclude None as the default
                return True

            return cls._value_is_hardcoded_default(attrname, var, instance)
        return (
            isinstance(var, cls._excluded_types) or
            cls._value_is_hardcoded_default(attrname, var, instance)
        )

    @classmethod
    def serialize_to_json(cls, object_to_serialize, decorated_fields):
        """Serializes an object to json"""
        serialized_object = {}
        keys_to_serialize = object_to_serialize.get_serialized_fields()
        for key in keys_to_serialize:
            # None is ignored in serialized form and is added back in deserialization.
            value = getattr(object_to_serialize, key, None)
            if cls._is_excluded(value, key, object_to_serialize):
                continue

            if key in decorated_fields:
                serialized_object[key] = cls._serialize(value)
            else:
                value = cls._serialize(value)
                if isinstance(value, dict) and "__type" in value:
                    value = value["__var"]
                serialized_object[key] = value
        return serialized_object

    @classmethod
    def _serialize(cls, var):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for serialization.

        visited_dags stores DAGs that are being serialized or have been serialized,
        for:
        (1) preventing deadlock loop caused by task.dag, task._dag, and dag.parent_dag;
        (2) replacing the fields in (1) with serialized counterparts.

        The serialization protocol is:

        (1) keeping JSON supported types: primitives, dict, list;
        (2) encoding other types as ``{TYPE: 'foo', VAR: 'bar'}``, the deserialization
            step decode VAR according to TYPE;
        (3) Operator has a special field CLASS to record the original class
            name for displaying in UI.
        """
        try:
            if cls._is_primitive(var):
                # enum.IntEnum is an int instance, it causes json dumps error so we use its value.
                if isinstance(var, enum.Enum):
                    return var.value
                return var
            elif isinstance(var, dict):
                return cls._encode(
                    {str(k): cls._serialize(v) for k, v in var.items()},
                    type_=DAT.DICT)
            elif isinstance(var, list):
                return [cls._serialize(v) for v in var]
            elif isinstance(var, DAG):
                return SerializedDAG.serialize_dag(var)
            elif isinstance(var, BaseOperator):
                return SerializedBaseOperator.serialize_operator(var)
            elif isinstance(var, cls._datetime_types):
                return cls._encode(pendulum.instance(var).timestamp(), type_=DAT.DATETIME)
            elif isinstance(var, datetime.timedelta):
                return cls._encode(var.total_seconds(), type_=DAT.TIMEDELTA)
            elif isinstance(var, (pendulum.tz.Timezone, pendulum.tz.timezone_info.TimezoneInfo)):
                return cls._encode(str(var.name), type_=DAT.TIMEZONE)
            elif isinstance(var, relativedelta.relativedelta):
                encoded = {k: v for k, v in var.__dict__.items() if not k.startswith("_") and v}
                if var.weekday and var.weekday.n:
                    # Every n'th Friday for example
                    encoded['weekday'] = [var.weekday.weekday, var.weekday.n]
                elif var.weekday:
                    encoded['weekday'] = [var.weekday.weekday]
                return cls._encode(encoded, type_=DAT.RELATIVEDELTA)
            elif callable(var):
                return str(get_python_source(var, return_none_if_x_none=True))
            elif isinstance(var, set):
                # FIXME: casts set to list in customized serialization in future.
                return cls._encode(
                    [cls._serialize(v) for v in var], type_=DAT.SET)
            elif isinstance(var, tuple):
                # FIXME: casts tuple to list in customized serialization in future.
                return cls._encode(
                    [cls._serialize(v) for v in var], type_=DAT.TUPLE)
            else:
                log.debug('Cast type %s to str in serialization.', type(var))
                return str(var)
        except Exception:  # pylint: disable=broad-except
            log.warning('Failed to stringify.', exc_info=True)
            return FAILED

    @classmethod
    def _deserialize(cls, encoded_var):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for deserialization."""
        # JSON primitives (except for dict) are not encoded.
        if cls._is_primitive(encoded_var):
            return encoded_var
        elif isinstance(encoded_var, list):
            return [cls._deserialize(v) for v in encoded_var]

        assert isinstance(encoded_var, dict)
        var = encoded_var[Encoding.VAR]
        type_ = encoded_var[Encoding.TYPE]

        if type_ == DAT.DICT:
            return {k: cls._deserialize(v) for k, v in var.items()}
        elif type_ == DAT.DAG:
            return SerializedDAG.deserialize_dag(var)
        elif type_ == DAT.OP:
            return SerializedBaseOperator.deserialize_operator(var)
        elif type_ == DAT.DATETIME:
            return pendulum.from_timestamp(var)
        elif type_ == DAT.TIMEDELTA:
            return datetime.timedelta(seconds=var)
        elif type_ == DAT.TIMEZONE:
            return pendulum.timezone(var)
        elif type_ == DAT.RELATIVEDELTA:
            if 'weekday' in var:
                var['weekday'] = relativedelta.weekday(*var['weekday'])
            return relativedelta.relativedelta(**var)
        elif type_ == DAT.SET:
            return {cls._deserialize(v) for v in var}
        elif type_ == DAT.TUPLE:
            return tuple([cls._deserialize(v) for v in var])
        else:
            raise TypeError('Invalid type {!s} in deserialization.'.format(type_))

    _deserialize_datetime = pendulum.from_timestamp

    @classmethod
    def _deserialize_timezone(cls, name):
        return pendulum.timezone(name)

    @classmethod
    def _deserialize_timedelta(cls, seconds):
        return datetime.timedelta(seconds=seconds)

    @classmethod
    def _is_constructor_param(cls, attrname, instance):
        # pylint: disable=unused-argument
        return attrname in cls._CONSTRUCTOR_PARAMS

    @classmethod
    def _value_is_hardcoded_default(cls, attrname, value, instance):
        """
        Return true if ``value`` is the hard-coded default for the given attribute.
        This takes in to account cases where the ``concurrency`` parameter is
        stored in the ``_concurrency`` attribute.
        And by using `is` here only and not `==` this copes with the case a
        user explicitly specifies an attribute with the same "value" as the
        default. (This is because ``"default" is "default"`` will be False as
        they are different strings with the same characters.)

        Also returns True if the value is an empty list or empty dict. This is done
        to account for the case where the default value of the field is None but has the
        ``field = field or {}`` set.
        """
        # pylint: disable=unused-argument
        if attrname in cls._CONSTRUCTOR_PARAMS and \
                (cls._CONSTRUCTOR_PARAMS[attrname] is value or (value in [{}, []])):
            return True
        return False


class SerializedBaseOperator(BaseOperator, BaseSerialization):
    """A JSON serializable representation of operator.

    All operators are casted to SerializedBaseOperator after deserialization.
    Class specific attributes used by UI are move to object attributes.
    """
    _decorated_fields = {'executor_config', }

    _CONSTRUCTOR_PARAMS = {
        k: v.default for k, v in signature(BaseOperator).parameters.items()
        if v.default is not v.empty
    }

    def __init__(self, *args, **kwargs):
        super(SerializedBaseOperator, self).__init__(*args, **kwargs)
        # task_type is used by UI to display the correct class type, because UI only
        # receives BaseOperator from deserialized DAGs.
        self._task_type = 'BaseOperator'
        # Move class attributes into object attributes.
        self.ui_color = BaseOperator.ui_color
        self.ui_fgcolor = BaseOperator.ui_fgcolor
        self.template_fields = BaseOperator.template_fields
        # subdag parameter is only set for SubDagOperator.
        # Setting it to None by default as other Operators do not have that field
        self.subdag = None
        self.operator_extra_links = BaseOperator.operator_extra_links

    @property
    def task_type(self):
        # type: () -> str
        # Overwrites task_type of BaseOperator to use _task_type instead of
        # __class__.__name__.
        return self._task_type

    @task_type.setter
    def task_type(self, task_type):
        # type: (str) -> None
        self._task_type = task_type

    @classmethod
    def serialize_operator(cls, op):
        # type: (BaseOperator) -> dict
        """Serializes operator into a JSON object.
        """
        serialize_op = cls.serialize_to_json(op, cls._decorated_fields)
        # Adds a new task_type field to record the original operator class.
        serialize_op['_task_type'] = op.__class__.__name__
        serialize_op['_task_module'] = op.__class__.__module__
        if op.operator_extra_links:
            serialize_op['_operator_extra_links'] = \
                cls._serialize_operator_extra_links(op.operator_extra_links)

        # Store all template_fields as they are if there are JSON Serializable
        # If not, store them as strings
        if op.template_fields:
            for template_field in op.template_fields:
                value = getattr(op, template_field, None)
                if not cls._is_excluded(value, template_field, op):
                    serialize_op[template_field] = serialize_template_field(value)

        return serialize_op

    @classmethod
    def deserialize_operator(cls, encoded_op):
        # type: (dict) -> BaseOperator
        """Deserializes an operator from a JSON object.
        """
        from airflow.plugins_manager import operator_extra_links

        op = SerializedBaseOperator(task_id=encoded_op['task_id'])

        # Extra Operator Links defined in Plugins
        op_extra_links_from_plugin = {}

        for ope in operator_extra_links:
            for operator in ope.operators:
                if operator.__name__ == encoded_op["_task_type"] and \
                        operator.__module__ == encoded_op["_task_module"]:
                    op_extra_links_from_plugin.update({ope.name: ope})

        # If OperatorLinks are defined in Plugins but not in the Operator that is being Serialized
        # set the Operator links attribute
        # The case for "If OperatorLinks are defined in the operator that is being Serialized"
        # is handled in the deserialization loop where it matches k == "_operator_extra_links"
        if op_extra_links_from_plugin and "_operator_extra_links" not in encoded_op:
            setattr(op, "operator_extra_links", list(op_extra_links_from_plugin.values()))

        for k, v in encoded_op.items():

            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "subdag":
                v = SerializedDAG.deserialize_dag(v)
            elif k in {"retry_delay", "execution_timeout"}:
                v = cls._deserialize_timedelta(v)
            elif k in encoded_op["template_fields"]:
                pass
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
            elif k == "_operator_extra_links":
                op_predefined_extra_links = cls._deserialize_operator_extra_links(v)

                # If OperatorLinks with the same name exists, Links via Plugin have higher precedence
                op_predefined_extra_links.update(op_extra_links_from_plugin)

                v = list(op_predefined_extra_links.values())
                k = "operator_extra_links"
            elif k in cls._decorated_fields or k not in op.get_serialized_fields():
                v = cls._deserialize(v)
            # else use v as it is

            setattr(op, k, v)

        for k in op.get_serialized_fields() - set(encoded_op.keys()) - set(
                cls._CONSTRUCTOR_PARAMS.keys()):
            setattr(op, k, None)

        # Set all the template_field to None that were not present in Serialized JSON
        for field in op.template_fields:
            if not hasattr(op, field):
                setattr(op, field, None)

        return op

    @classmethod
    def _is_excluded(cls, var, attrname, op):
        if var is not None and op.has_dag() and attrname.endswith("_date"):
            # If this date is the same as the matching field in the dag, then
            # don't store it again at the task level.
            dag_date = getattr(op.dag, attrname, None)
            if var is dag_date or var == dag_date:
                return True
        return super(SerializedBaseOperator, cls)._is_excluded(var, attrname, op)

    @classmethod
    def _deserialize_operator_extra_links(
        cls,
        encoded_op_links
    ):
        # type: (list) -> Dict[str, BaseOperatorLink]
        """
        Deserialize Operator Links if the Classes  are registered in Airflow Plugins.
        Error is raised if the OperatorLink is not found in Plugins too.

        :param encoded_op_links: Serialized Operator Link
        :return: De-Serialized Operator Link
        """
        from airflow.plugins_manager import registered_operator_link_classes

        op_predefined_extra_links = {}

        for _operator_links_source in encoded_op_links:
            # Get the key, value pair as Tuple where key is OperatorLink ClassName
            # and value is the dictionary containing the arguments passed to the OperatorLink
            #
            # Example of a single iteration:
            #
            #   _operator_links_source =
            #   {'airflow.gcp.operators.bigquery.BigQueryConsoleIndexableLink': {'index': 0}},
            #
            #   list(_operator_links_source.items()) =
            #   [('airflow.gcp.operators.bigquery.BigQueryConsoleIndexableLink', {'index': 0})]
            #
            #   list(_operator_links_source.items())[0] =
            #   ('airflow.gcp.operators.bigquery.BigQueryConsoleIndexableLink', {'index': 0})

            _operator_link_class_path, data = list(_operator_links_source.items())[0]
            if _operator_link_class_path in BUILTIN_OPERATOR_EXTRA_LINKS:
                single_op_link_class = import_string(_operator_link_class_path)
            elif _operator_link_class_path in registered_operator_link_classes:
                single_op_link_class = registered_operator_link_classes[_operator_link_class_path]
            else:
                log.error("Operator Link class %r not registered", _operator_link_class_path)
                return {}

            op_predefined_extra_link = cattr.structure(
                data, single_op_link_class)    # type: BaseOperatorLink

            op_predefined_extra_links.update(
                {op_predefined_extra_link.name: op_predefined_extra_link}
            )

        return op_predefined_extra_links

    @classmethod
    def _serialize_operator_extra_links(
        cls,
        operator_extra_links
    ):
        """
        Serialize Operator Links. Store the import path of the OperatorLink and the arguments
        passed to it. Example ``[{'airflow.gcp.operators.bigquery.BigQueryConsoleLink': {}}]``

        :param operator_extra_links: Operator Link
        :return: Serialized Operator Link
        """
        serialize_operator_extra_links = []
        for operator_extra_link in operator_extra_links:
            op_link_arguments = cattr.unstructure(operator_extra_link)
            if not isinstance(op_link_arguments, dict):
                op_link_arguments = {}
            serialize_operator_extra_links.append(
                {
                    "{}.{}".format(operator_extra_link.__class__.__module__,
                                   operator_extra_link.__class__.__name__): op_link_arguments
                }
            )

        return serialize_operator_extra_links


class SerializedDAG(DAG, BaseSerialization):
    """A JSON serializable representation of DAG.

    A stringified DAG can only be used in the scope of scheduler and webserver, because fields
    that are not serializable, such as functions and customer defined classes, are casted to
    strings.

    Compared with SimpleDAG: SerializedDAG contains all information for webserver.
    Compared with DagPickle: DagPickle contains all information for worker, but some DAGs are
    not pickle-able. SerializedDAG works for all DAGs.
    """

    _decorated_fields = {'schedule_interval', 'default_args', '_access_control'}

    @staticmethod
    def __get_constructor_defaults():  # pylint: disable=no-method-argument
        param_to_attr = {
            'concurrency': '_concurrency',
            'description': '_description',
            'default_view': '_default_view',
            'access_control': '_access_control',
        }
        return {
            param_to_attr.get(k, k): v.default for k, v in signature(DAG).parameters.items()
            if v.default is not v.empty
        }

    _CONSTRUCTOR_PARAMS = __get_constructor_defaults.__func__()  # type: ignore
    del __get_constructor_defaults

    _json_schema = load_dag_schema()

    @classmethod
    def serialize_dag(cls, dag):
        # type: (DAG) -> dict
        """Serializes a DAG into a JSON object.
        """
        serialize_dag = cls.serialize_to_json(dag, cls._decorated_fields)

        serialize_dag["tasks"] = [cls._serialize(task) for _, task in dag.task_dict.items()]
        return serialize_dag

    @classmethod
    def deserialize_dag(cls, encoded_dag):
        # type: (dict) -> 'SerializedDAG'
        """Deserializes a DAG from a JSON object.
        """

        dag = SerializedDAG(dag_id=encoded_dag['_dag_id'])

        for k, v in encoded_dag.items():
            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "tasks":
                v = {
                    task["task_id"]: SerializedBaseOperator.deserialize_operator(task) for task in v
                }
                k = "task_dict"
            elif k == "timezone":
                v = cls._deserialize_timezone(v)
            elif k in {"dagrun_timeout"}:
                v = cls._deserialize_timedelta(v)
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
            elif k in cls._decorated_fields:
                v = cls._deserialize(v)
            # else use v as it is

            setattr(dag, k, v)

        keys_to_set_none = dag.get_serialized_fields() - set(encoded_dag.keys()) - set(
            cls._CONSTRUCTOR_PARAMS.keys())
        for k in keys_to_set_none:
            setattr(dag, k, None)

        setattr(dag, 'full_filepath', dag.fileloc)
        for task in dag.task_dict.values():
            task.dag = dag
            serializable_task = task  # type: BaseOperator

            for date_attr in ["start_date", "end_date"]:
                if getattr(serializable_task, date_attr) is None:
                    setattr(serializable_task, date_attr, getattr(dag, date_attr))

            if serializable_task.subdag is not None:
                setattr(serializable_task.subdag, 'parent_dag', dag)
                serializable_task.subdag.is_subdag = True

            for task_id in serializable_task.downstream_task_ids:
                # Bypass set_upstream etc here - it does more than we want
                # noinspection PyProtectedMember
                dag.task_dict[task_id]._upstream_task_ids.add(serializable_task.task_id)  # noqa: E501 # pylint: disable=protected-access

        return dag

    @classmethod
    def to_dict(cls, var):
        # type: (...) -> dict
        """Stringifies DAGs and operators contained by var and returns a dict of var.
        """
        json_dict = {
            "__version": cls.SERIALIZER_VERSION,
            "dag": cls.serialize_dag(var)
        }

        # Validate Serialized DAG with Json Schema. Raises Error if it mismatches
        cls.validate_schema(json_dict)
        return json_dict

    @classmethod
    def from_dict(cls, serialized_obj):
        # type: (dict) -> 'SerializedDAG'
        """Deserializes a python dict in to the DAG and operators it contains."""
        ver = serialized_obj.get('__version', '<not present>')
        if ver != cls.SERIALIZER_VERSION:
            raise ValueError("Unsure how to deserialize version {!r}".format(ver))
        return cls.deserialize_dag(serialized_obj['dag'])


# Serialization failure returns 'failed'.
FAILED = 'serialization_failed'
