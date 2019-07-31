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

"""DAG serialization with JSON."""

import json
import logging

import datetime
import dateutil.parser
import jinja2
import pendulum

from airflow import models
from airflow.www.utils import get_python_source


# JSON primitive types.
_primitive_types = (int, bool, float, str)

_datetime_types = (datetime.datetime, datetime.date, datetime.time)

# Object types that are always excluded.
_excluded_types = (logging.Logger, models.connection.Connection, type)

# Stringified DADs and operators contain exactly these fields.
# TODO(coufong): to customize included fields and keep only necessary fields.
_dag_included_fields = list(vars(models.DAG(dag_id='test')).keys())
_op_included_fields = list(vars(models.BaseOperator(task_id='test')).keys()) + [
    '_dag', 'ui_color', 'ui_fgcolor', 'template_fields']

# Encoding constants.
TYPE = '__type'
CLASS = '__class'
VAR = '__var'

# Supported types. primitives and list are not encoded.
DAG = 'dag'
OP = 'operator'
DATETIME = 'datetime'
TIMEDELTA = 'timedelta'
TIMEZONE = 'timezone'
DICT = 'dict'
SET = 'set'
TUPLE = 'tuple'

# Constants.
BASE_OPERATOR_CLASS = 'airflow.models.baseoperator.BaseOperator'
# Serialization failure returns 'failed'.
FAILED = 'failed'


def _is_primitive(x):
    return x is None or isinstance(x, _primitive_types)


def _is_excluded(x):
    return x is None or isinstance(x, _excluded_types)


def _encode(x, type_, class_=None):
    return ({VAR: x, TYPE: type_} if class_ is None
            else {VAR: x, TYPE: type_, CLASS: class_})


def _serialize_object(x, visited_dags, included_fields):
    """helper fn to serialize an object as a JSON dict."""
    new_x = {}
    for k in included_fields:
        # None is ignored in serialized form and is added back in deserialization.
        v = getattr(x, k, None)
        if not _is_excluded(v):
            new_x[k] = _serialize(v, visited_dags)
    return new_x


def _serialize_dag(x, visited_dags):
    if x.dag_id in visited_dags:
        return {TYPE: DAG, VAR: str(x.dag_id)}

    new_x = {TYPE: DAG}
    visited_dags[x.dag_id] = new_x
    new_x[VAR] = _serialize_object(
        x, visited_dags, included_fields=_dag_included_fields)
    return new_x


def _serialize_operator(x, visited_dags):
    return _encode(
        _serialize_object(
            x, visited_dags, included_fields=_op_included_fields),
        type_=OP,
        class_=x.__class__.__name__
    )


def _serialize(x, visited_dags):  # pylint: disable=too-many-return-statements
    """Helper function of depth first search for serialization.

    visited_dags stores DAGs that are being stringifying for have been stringified,
    for:
      (1) preventing deadlock loop caused by task.dag, task._dag, and dag.parent_dag;
      (2) replacing the fields in (1) with serialized counterparts.

    The serialization protocol is:
      (1) keeping JSON supported types: primitives, dict, list;
      (2) encoding other types as {'_type', 'foo', '_var', 'bar'}, the deserialization
          step decode '_var' according to '_type';
      (3) Airflow Operator has a special field '_class' to record the original class
          name for displaying in UI.
    """
    try:
        if _is_primitive(x):
            return x
        elif isinstance(x, dict):
            return _encode({k: _serialize(v, visited_dags) for k, v in x.items()}, type_=DICT)
        elif isinstance(x, list):
            return [_serialize(v, visited_dags) for v in x]
        elif isinstance(x, models.DAG):
            return _serialize_dag(x, visited_dags)
        elif isinstance(x, models.BaseOperator):
            return _serialize_operator(x, visited_dags)
        elif isinstance(x, _datetime_types):
            return _encode(x.isoformat(), type_=DATETIME)
        elif isinstance(x, datetime.timedelta):
            return _encode(x.total_seconds(), type_=TIMEDELTA)
        elif isinstance(x, pendulum.tz.Timezone):
            return _encode(str(x.name), type_=TIMEZONE)
        elif callable(x):
            return str(get_python_source(x))
        elif isinstance(x, set):
            return _encode([_serialize(v, visited_dags) for v in x], type_=SET)
        elif isinstance(x, tuple):
            return _encode([_serialize(v, visited_dags) for v in x], type_=TUPLE)
        else:
            logging.debug('Cast type %s to str in serialization.', type(x))
            return str(x)
    except Exception:  # pylint: disable=broad-except
        logging.warning('Failed to stringify.', exc_info=True)
        return FAILED


def _copy_attr(x, new_x, included_fields, visited_dags):
    """Copy the attributes of dict x to a new object new_x."""
    for k in included_fields:
        if k in x:
            setattr(new_x, k, _deserialize(x[k], visited_dags))
        else:
            setattr(new_x, k, None)


def _deserialize_dag(x, visited_dags):
    if isinstance(x, dict):
        dag = models.DAG(dag_id=x['_dag_id'])
        visited_dags[dag.dag_id] = dag  # pylint: disable=protected-access
        _copy_attr(x, dag, _dag_included_fields, visited_dags)
        dag.is_stringified = True
        return dag
    elif isinstance(x, str):
        if x in visited_dags:
            return visited_dags[x]
        logging.warning('DAG %s not found in serialization.', x)
    else:
        logging.warning('Invalid type %s for decoded DAG.', type(x))
    return FAILED


def _deserialize_operator(x, operator_class, visited_dags):
    op = object.__new__(models.BaseOperator)
    _copy_attr(x, op, _op_included_fields, visited_dags)
    op.operator_class = operator_class
    op.is_stringified = True
    return op


def _deserialize(encoded_x, visited_dags):  # pylint: disable=too-many-return-statements
    """Helper function of depth first search for deserialization."""
    try:
        # JSON primitives (except for dict) are not encoded.
        if _is_primitive(encoded_x):
            return encoded_x
        elif isinstance(encoded_x, list):
            return [_deserialize(v, visited_dags) for v in encoded_x]

        assert isinstance(encoded_x, dict)
        var = encoded_x[VAR]
        type_ = encoded_x[TYPE]

        if type_ == DICT:
            return {k: _deserialize(v, visited_dags) for k, v in var.items()}
        elif type_ == DAG:
            return _deserialize_dag(var, visited_dags)
        elif type_ == OP:
            return _deserialize_operator(
                var,
                encoded_x[CLASS] if CLASS in encoded_x else BASE_OPERATOR_CLASS,
                visited_dags)
        elif type_ == DATETIME:
            return dateutil.parser.parse(var)
        elif type_ == TIMEDELTA:
            return datetime.timedelta(seconds=var)
        elif type_ == TIMEZONE:
            return pendulum.tz.timezone(name=var)
        elif type_ == SET:
            return {_deserialize(v, visited_dags) for v in var}
        elif type_ == TUPLE:
            return tuple([_deserialize(v, visited_dags) for v in var])
        else:
            logging.warning('Invalid type %s in deserialization.', type_)
            return FAILED
    except Exception:  # pylint: disable=broad-except
        logging.warning('Failed to deserialize %s.', encoded_x, exc_info=True)
        return FAILED


def serialize(x):
    """Stringifies DAGs and operators contained by x and returns a JSON string of x."""
    return json.dumps(_serialize(x, {}))


def deserialize(encoded_x):
    """Deserializes encoded_x and reconstructs all DAGs and operators it contains."""
    # Setting strict to False for control characters ('\n') in function source code.
    return _deserialize(json.loads(encoded_x, strict=False), {})
