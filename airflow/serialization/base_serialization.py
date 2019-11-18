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

"""Utils for DAG serialization with JSON."""

import datetime
import enum
import logging
from inspect import Parameter
from typing import Dict, Optional, Set, Union

import pendulum
from dateutil import relativedelta

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.json_schema import Validator
from airflow.settings import json
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.utils import get_python_source

LOG = LoggingMixin().log

# Serialization failure returns 'failed'.
FAILED = 'serialization_failed'


class BaseSerialization:
    """BaseSerialization provides utils for serialization."""

    # JSON primitive types.
    _primitive_types = (int, bool, float, str)

    # Time types.
    # datetime.date and datetime.time are converted to strings.
    _datetime_types = (datetime.datetime,)

    # Object types that are always excluded in serialization.
    # FIXME: not needed if _included_fields of DAG and operator are customized.
    _excluded_types = (logging.Logger, Connection, type)

    _json_schema = None  # type: Optional[Validator]

    _CONSTRUCTOR_PARAMS = {}  # type: Dict[str, Parameter]

    SERIALIZER_VERSION = 1

    @classmethod
    def to_json(cls, var: Union[DAG, BaseOperator, dict, list, set, tuple]) -> str:
        """Stringifies DAGs and operators contained by var and returns a JSON string of var.
        """
        return json.dumps(cls.to_dict(var), ensure_ascii=True)

    @classmethod
    def to_dict(cls, var: Union[DAG, BaseOperator, dict, list, set, tuple]) -> dict:
        """Stringifies DAGs and operators contained by var and returns a dict of var.
        """
        # Don't call on this class directly - only SerializedDAG or
        # SerializedBaseOperator should be used as the "entrypoint"
        raise NotImplementedError()

    @classmethod
    def from_json(cls, serialized_obj: str) -> Union['BaseSerialization', dict, list, set, tuple]:
        """Deserializes json_str and reconstructs all DAGs and operators it contains."""
        return cls.from_dict(json.loads(serialized_obj))

    @classmethod
    def from_dict(cls, serialized_obj: dict) -> Union['BaseSerialization', dict, list, set, tuple]:
        """Deserializes a python dict stored with type decorators and
        reconstructs all DAGs and operators it contains."""
        return cls._deserialize(serialized_obj)

    @classmethod
    def validate_schema(cls, serialized_obj: Union[str, dict]):
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
        # pylint: disable=unused-argument
        return (
            var is None or
            isinstance(var, cls._excluded_types) or
            cls._value_is_hardcoded_default(attrname, var)
        )

    @classmethod
    def serialize_to_json(cls, object_to_serialize: Union[BaseOperator, DAG], decorated_fields: Set):
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

        The serialization protocol is:

        (1) keeping JSON supported types: primitives, dict, list;
        (2) encoding other types as ``{TYPE: 'foo', VAR: 'bar'}``, the deserialization
            step decode VAR according to TYPE;
        (3) Operator has a special field CLASS to record the original class
            name for displaying in UI.
        """
        from airflow.serialization.serialized_dag import SerializedDAG
        from airflow.serialization.serialized_baseoperator import SerializedBaseOperator
        try:
            if cls._is_primitive(var):
                # enum.IntEnum is an int instance, it causes json dumps error so we use its value.
                if isinstance(var, enum.Enum):
                    return var.value
                return var
            elif isinstance(var, dict):
                return cls._encode(
                    {str(k): cls._serialize(v) for k, v in var.items()},
                    type_=DAT.DICT
                )
            elif isinstance(var, list):
                return [cls._serialize(v) for v in var]
            elif isinstance(var, DAG):
                return SerializedDAG.serialize_dag(var)
            elif isinstance(var, BaseOperator):
                return SerializedBaseOperator.serialize_operator(var)
            elif isinstance(var, cls._datetime_types):
                return cls._encode(var.timestamp(), type_=DAT.DATETIME)
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
                return str(get_python_source(var))
            elif isinstance(var, set):
                # FIXME: casts set to list in customized serialization in future.
                return cls._encode(
                    [cls._serialize(v) for v in var], type_=DAT.SET)
            elif isinstance(var, tuple):
                # FIXME: casts tuple to list in customized serialization in future.
                return cls._encode(
                    [cls._serialize(v) for v in var], type_=DAT.TUPLE)
            else:
                LOG.debug('Cast type %s to str in serialization.', type(var))
                return str(var)
        except Exception:  # pylint: disable=broad-except
            LOG.warning('Failed to stringify.', exc_info=True)
            return FAILED

    @classmethod
    def _deserialize(cls, encoded_var):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for deserialization."""
        from airflow.serialization.serialized_dag import SerializedDAG
        from airflow.serialization.serialized_baseoperator import SerializedBaseOperator
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
    _deserialize_timezone = pendulum.timezone

    @classmethod
    def _deserialize_timedelta(cls, seconds):
        return datetime.timedelta(seconds=seconds)

    @classmethod
    def _value_is_hardcoded_default(cls, attrname, value):
        """
        Return true if ``value`` is the hard-coded default for the given attribute.

        This takes in to account cases where the ``concurrency`` parameter is
        stored in the ``_concurrency`` attribute.

        And by using `is` here only and not `==` this copes with the case a
        user explicitly specifies an attribute with the same "value" as the
        default. (This is because ``"default" is "default"`` will be False as
        they are different strings with the same characters.)
        """
        if attrname in cls._CONSTRUCTOR_PARAMS and cls._CONSTRUCTOR_PARAMS[attrname].default is value:
            return True
        return False
