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
import json
import logging
from typing import Dict, Optional, TYPE_CHECKING, Union

import dateutil.parser
import jsonschema
import pendulum

import airflow
from airflow.dag.serialization.enum import DagAttributeTypes as DAT, Encoding
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, DAG
from airflow.models.connection import Connection
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.utils import get_python_source


if TYPE_CHECKING:
    from airflow.dag.serialization.serialized_baseoperator import SerializedBaseOperator  # noqa: F401, E501; # pylint: disable=cyclic-import
    from airflow.dag.serialization.serialized_dag import SerializedDAG  # noqa: F401, E501; # pylint: disable=cyclic-import

LOG = LoggingMixin().log

# Serialization failure returns 'failed'.
FAILED = 'serialization_failed'


class Serialization:
    """Serialization provides utils for serialization."""

    # JSON primitive types.
    _primitive_types = (int, bool, float, str)

    # Time types.
    _datetime_types = (datetime.datetime, datetime.date, datetime.time)

    # Object types that are always excluded in serialization.
    # FIXME: not needed if _included_fields of DAG and operator are customized.
    _excluded_types = (logging.Logger, Connection, type)

    _json_schema = None     # type: Optional[Dict]

    @classmethod
    def to_json(cls, var: Union[DAG, BaseOperator, dict, list, set, tuple]) -> str:
        """Stringifies DAGs and operators contained by var and returns a JSON string of var.
        """
        return json.dumps(cls._serialize(var, {}), ensure_ascii=True)

    @classmethod
    def from_json(cls, json_str: str) -> Union[
            'SerializedDAG', 'SerializedBaseOperator', dict, list, set, tuple]:
        """Deserializes json_str and reconstructs all DAGs and operators it contains."""
        return cls._deserialize(json.loads(json_str), {})

    @classmethod
    def validate_json(cls, json_str: str):
        """Validate json_str satisfies JSON schema."""
        if cls._json_schema is None:
            raise AirflowException('JSON schema of {:s} is not set.'.format(cls.__name__))
        jsonschema.validate(json.loads(json_str), cls._json_schema)

    @staticmethod
    def _encode(x, type_):
        """Encode data by a JSON dict."""
        return {Encoding.VAR: x, Encoding.TYPE: type_}

    @classmethod
    def _serialize_object(cls, var, visited_dags, included_fields):
        """Helper function to serialize an object as a JSON dict."""
        new_var = {}
        for k in included_fields:
            # None is ignored in serialized form and is added back in deserialization.
            v = getattr(var, k, None)
            if not cls._is_excluded(v):
                new_var[k] = cls._serialize(v, visited_dags)
        return new_var

    @classmethod
    def _deserialize_object(cls, var, new_var, included_fields, visited_dags):
        """Deserialize and copy the attributes of dict var to a new object new_var.

        It does not create new_var because if var is a DAG, new_var should be added into visited_dag
        ahead of calling this function.
        """
        for k in included_fields:
            if k in var:
                setattr(new_var, k, cls._deserialize(var[k], visited_dags))
            else:
                setattr(new_var, k, None)

    @classmethod
    def _is_primitive(cls, var):
        """Primitive types."""
        return var is None or isinstance(var, cls._primitive_types)

    @classmethod
    def _is_excluded(cls, var):
        """Types excluded from serialization."""
        return var is None or isinstance(var, cls._excluded_types)

    @classmethod
    def _serialize(cls, var, visited_dags):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for serialization.

        visited_dags stores DAGs that are being serialized or have been serialized,
        for:
        (1) preventing deadlock loop caused by task.dag, task._dag, and dag.parent_dag;
        (2) replacing the fields in (1) with serialized counterparts.

        The serialization protocol is:
        (1) keeping JSON supported types: primitives, dict, list;
        (2) encoding other types as {TYPE: 'foo', VAR: 'bar'}, the deserialization
            step decode VAR according to TYPE;
        (3) Operator has a special field CLASS to record the original class
            name for displaying in UI.
        """
        try:
            if cls._is_primitive(var):
                return var
            elif isinstance(var, dict):
                return cls._encode(
                    {str(k): cls._serialize(v, visited_dags)
                     for k, v in var.items()},
                    type_=DAT.DICT)
            elif isinstance(var, list):
                return [cls._serialize(v, visited_dags) for v in var]
            elif isinstance(var, DAG):
                return airflow.dag.serialization.SerializedDAG.serialize_dag(
                    var, visited_dags)
            elif isinstance(var, BaseOperator):
                return airflow.dag.serialization.SerializedBaseOperator.serialize_operator(
                    var, visited_dags)
            elif isinstance(var, cls._datetime_types):
                return cls._encode(var.isoformat(), type_=DAT.DATETIME)
            elif isinstance(var, datetime.timedelta):
                return cls._encode(var.total_seconds(), type_=DAT.TIMEDELTA)
            elif isinstance(var, pendulum.tz.Timezone):
                return cls._encode(str(var.name), type_=DAT.TIMEZONE)
            elif callable(var):
                return str(get_python_source(var))
            elif isinstance(var, set):
                # FIXME: casts set to list in customized serilization in future.
                return cls._encode(
                    [cls._serialize(v, visited_dags) for v in var], type_=DAT.SET)
            elif isinstance(var, tuple):
                # FIXME: casts tuple to list in customized serilization in future.
                return cls._encode(
                    [cls._serialize(v, visited_dags) for v in var], type_=DAT.TUPLE)
            else:
                LOG.debug('Cast type %s to str in serialization.', type(var))
                return str(var)
        except Exception:  # pylint: disable=broad-except
            LOG.warning('Failed to stringify.', exc_info=True)
            return FAILED

    @classmethod
    def _deserialize(cls, encoded_var, visited_dags):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for deserialization."""
        try:
            # JSON primitives (except for dict) are not encoded.
            if cls._is_primitive(encoded_var):
                return encoded_var
            elif isinstance(encoded_var, list):
                return [cls._deserialize(v, visited_dags) for v in encoded_var]

            assert isinstance(encoded_var, dict)
            var = encoded_var[Encoding.VAR]
            type_ = encoded_var[Encoding.TYPE]

            if type_ == DAT.DICT:
                return {k: cls._deserialize(v, visited_dags) for k, v in var.items()}
            elif type_ == DAT.DAG:
                if isinstance(var, dict):
                    return airflow.dag.serialization.SerializedDAG.deserialize_dag(
                        var, visited_dags)
                elif isinstance(var, str) and var in visited_dags:
                    # dag_id is stored in the serailized form for a visited DAGs.
                    return visited_dags[var]
                LOG.warning('Invalid DAG %s in deserialization.', var)
                return None
            elif type_ == DAT.OP:
                return airflow.dag.serialization.SerializedBaseOperator.deserialize_operator(
                    var, visited_dags)
            elif type_ == DAT.DATETIME:
                return dateutil.parser.parse(var)
            elif type_ == DAT.TIMEDELTA:
                return datetime.timedelta(seconds=var)
            elif type_ == DAT.TIMEZONE:
                return pendulum.tz.timezone(name=var)
            elif type_ == DAT.SET:
                return {cls._deserialize(v, visited_dags) for v in var}
            elif type_ == DAT.TUPLE:
                return tuple([cls._deserialize(v, visited_dags) for v in var])
            else:
                LOG.warning('Invalid type %s in deserialization.', type_)
                return None
        except Exception:  # pylint: disable=broad-except
            LOG.warning('Failed to deserialize %s.', encoded_var, exc_info=True)
            return None
