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

from enum import Enum
from enum import unique
import json
import logging
import datetime
import dateutil.parser
import pendulum

from airflow.models import BaseOperator
from airflow.models import DAG
from airflow.models.connection import Connection
from airflow.www.utils import get_python_source


# Serialization failure returns 'failed'.
FAILED = 'serialization_failed'


# Fields of an encoded object in serialization.
@unique
class Encoding(str, Enum):
    """Enum of encoding constants."""
    TYPE = '__type'
    VAR = '__var'


# Supported types for encoding. primitives and list are not encoded.
@unique
class DagTypes(str, Enum):
    """Enum of supported attribute types of DAG."""
    DAG = 'dag'
    OP = 'operator'
    DATETIME = 'datetime'
    TIMEDELTA = 'timedelta'
    TIMEZONE = 'timezone'
    DICT = 'dict'
    SET = 'set'
    TUPLE = 'tuple'


class Serialization():
    """Serialization provides utils for serialization."""

    # JSON primitive types.
    _primitive_types = (int, bool, float, str)
    # Time types.
    _datetime_types = (datetime.datetime, datetime.date, datetime.time)
    # Object types that are always excluded.
    # TODO(coufon): not needed if _dag_included_fields and _op_included_fields are customized.
    _excluded_types = (logging.Logger, Connection, type)

    @classmethod
    def to_json(cls, x):
        """Stringifies DAGs and operators contained by x and returns a JSON string of x."""
        return json.dumps(cls._serialize(x, {}), ensure_ascii=True)

    @classmethod
    def from_json(cls, encoded_x):
        """Deserializes encoded_x and reconstructs all DAGs and operators it contains."""
        return cls._deserialize(json.loads(encoded_x), {})

    @classmethod
    def encode(cls, x, type_):
        """Encode data by a JSON dict."""
        return {Encoding.VAR: x, Encoding.TYPE: type_}

    @classmethod
    def serialize_object(cls, x, visited_dags, included_fields):
        """Helper function to serialize an object as a JSON dict."""
        new_x = {}
        for k in included_fields:
            # None is ignored in serialized form and is added back in deserialization.
            v = getattr(x, k, None)
            if not cls._is_excluded(v):
                new_x[k] = cls._serialize(v, visited_dags)
        return new_x

    @classmethod
    def deserialize_object(cls, x, new_x, included_fields, visited_dags):
        """Deserialize and copy the attributes of dict x to a new object new_x.

        It does not create new_x because if x is a DAG, new_x should be added into visited_dag
        ahead of calling this function.
        """
        for k in included_fields:
            if k in x:
                setattr(new_x, k, cls._deserialize(x[k], visited_dags))
            else:
                setattr(new_x, k, None)

    @classmethod
    def _is_primitive(cls, x):
        """Primitive types."""
        return x is None or isinstance(x, cls._primitive_types)

    @classmethod
    def _is_excluded(cls, x):
        """Types excluded from serialization.

        TODO(coufon): not needed if _dag_included_fields and _op_included_fields are customized.
        """
        return x is None or isinstance(x, cls._excluded_types)

    @classmethod
    def _serialize(cls, x, visited_dags):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for serialization.

        visited_dags stores DAGs that are being stringifying for have been stringified,
        for:
        (1) preventing deadlock loop caused by task.dag, task._dag, and dag.parent_dag;
        (2) replacing the fields in (1) with serialized counterparts.

        The serialization protocol is:
        (1) keeping JSON supported types: primitives, dict, list;
        (2) encoding other types as {TYPE, 'foo', VAR, 'bar'}, the deserialization
            step decode VAR according to TYPE;
        (3) Operator has a special field CLASS to record the original class
            name for displaying in UI.
        """
        try:
            if cls._is_primitive(x):
                return x
            elif isinstance(x, dict):
                return cls.encode(
                    {str(k): cls._serialize(v, visited_dags)
                     for k, v in x.items()},
                    type_=DagTypes.DICT)
            elif isinstance(x, list):
                return [cls._serialize(v, visited_dags) for v in x]
            elif isinstance(x, DAG):
                return SerializedDAG.serialize_dag(x, visited_dags)
            elif isinstance(x, BaseOperator):
                return SerializedOperator.serialize_operator(x, visited_dags)
            elif isinstance(x, cls._datetime_types):
                return cls.encode(x.isoformat(), type_=DagTypes.DATETIME)
            elif isinstance(x, datetime.timedelta):
                return cls.encode(x.total_seconds(), type_=DagTypes.TIMEDELTA)
            elif isinstance(x, pendulum.tz.Timezone):
                return cls.encode(str(x.name), type_=DagTypes.TIMEZONE)
            elif callable(x):
                return str(get_python_source(x))
            elif isinstance(x, set):
                return cls.encode(
                    [cls._serialize(v, visited_dags) for v in x], type_=DagTypes.SET)
            elif isinstance(x, tuple):
                return cls.encode(
                    [cls._serialize(v, visited_dags) for v in x], type_=DagTypes.TUPLE)
            else:
                logging.debug('Cast type %s to str in serialization.', type(x))
                return str(x)
        except Exception:  # pylint: disable=broad-except
            logging.warning('Failed to stringify.', exc_info=True)
            return FAILED

    @classmethod
    def _deserialize(cls, encoded_x, visited_dags):  # pylint: disable=too-many-return-statements
        """Helper function of depth first search for deserialization."""
        try:
            # JSON primitives (except for dict) are not encoded.
            if cls._is_primitive(encoded_x):
                return encoded_x
            elif isinstance(encoded_x, list):
                return [cls._deserialize(v, visited_dags) for v in encoded_x]

            assert isinstance(encoded_x, dict)
            var = encoded_x[Encoding.VAR]
            type_ = encoded_x[Encoding.TYPE]

            if type_ == DagTypes.DICT:
                return {k: cls._deserialize(v, visited_dags) for k, v in var.items()}
            elif type_ == DagTypes.DAG:
                if isinstance(var, dict):
                    return SerializedDAG.deserialize_dag(var, visited_dags)
                elif isinstance(var, str) and var in visited_dags:
                    # dag_id is stored in the serailized form for a visited DAGs.
                    return visited_dags[var]
                logging.warning('Invalid DAG %s in deserialization.', var)
                return None
            elif type_ == DagTypes.OP:
                return SerializedOperator.deserialize_operator(var, visited_dags)
            elif type_ == DagTypes.DATETIME:
                return dateutil.parser.parse(var)
            elif type_ == DagTypes.TIMEDELTA:
                return datetime.timedelta(seconds=var)
            elif type_ == DagTypes.TIMEZONE:
                return pendulum.tz.timezone(name=var)
            elif type_ == DagTypes.SET:
                return {cls._deserialize(v, visited_dags) for v in var}
            elif type_ == DagTypes.TUPLE:
                return tuple([cls._deserialize(v, visited_dags) for v in var])
            else:
                logging.warning('Invalid type %s in deserialization.', type_)
                return None
        except Exception:  # pylint: disable=broad-except
            logging.warning('Failed to deserialize %s.',
                            encoded_x, exc_info=True)
            return None


class SerializedDAG(DAG, Serialization):
    """A JSON serializable representation of DAG.

    A stringified DAG can only be used in the scope of scheduler and webserver, because fields
    that are not serializable, such as functions and customer defined classes, are casted to
    strings.
    Compared with SimpleDAG: SerializedDAG contains all information for webserver.
    Compared with DagPickle: DagPickle contains all information for worker, but some DAGs are
    not pickable. SerializedDAG works for all DAGs.
    """
    # Stringified DAGs and operators contain exactly these fields.
    # TODO(coufon): to customize included fields and keep only necessary fields.
    _dag_included_fields = list(vars(DAG(dag_id='test')).keys())

    @classmethod
    def serialize_dag(cls, dag, visited_dags):
        """Returns a serialized DAG.

        :param dag: a DAG.
        :type dag: airflow.models.DAG
        """
        if dag.dag_id in visited_dags:
            return {Encoding.TYPE: DagTypes.DAG, Encoding.VAR: str(dag.dag_id)}

        new_dag = {Encoding.TYPE: DagTypes.DAG}
        visited_dags[dag.dag_id] = new_dag
        new_dag[Encoding.VAR] = cls.serialize_object(
            dag, visited_dags, included_fields=cls._dag_included_fields)
        return new_dag

    @classmethod
    def deserialize_dag(cls, encoded_dag, visited_dags):
        """Returns a deserialized DAG.

        :param encoded_dag: a JSON dict of serialized DAG.
        :type encoded_dag: dict
        """
        dag = SerializedDAG(dag_id=encoded_dag['_dag_id'])
        visited_dags[dag.dag_id] = dag
        cls.deserialize_object(encoded_dag, dag, cls._dag_included_fields, visited_dags)
        return dag


class SerializedOperator(BaseOperator, Serialization):
    """A JSON serializable representation of operator.

    All operators are casted to SerializedOperator after deserialization.
    Class specific attributes used by UI are move to object attributes.
    """
    _op_included_fields = list(vars(BaseOperator(task_id='test')).keys()) + [
        '_dag', '_task_type', 'ui_color', 'ui_fgcolor', 'template_fields']

    def __init__(self, *args, **kwargs):
        BaseOperator.__init__(*args, **kwargs)
        # task_type is used by UI to display the correct class type, because UI only
        # receives BaseOperator from deserialized DAGs.
        self._task_type = 'BaseOperator'
        # Move class attributes into object attributes.
        self.ui_color = BaseOperator.ui_color
        self.ui_fgcolor = BaseOperator.ui_fgcolor
        self.template_fields = BaseOperator.template_fields

    @property
    def task_type(self):
        # Overwrites task_type of BaseOperator to use _task_type instead of
        # __class__.__name__.
        return self._task_type

    @task_type.setter
    def task_type(self, task_type):
        self._task_type = task_type

    @classmethod
    def serialize_operator(cls, op, visited_dags):
        """Returns a serializeed operator.

        :param op: an operator.
        :type op: subclasses of airflow.models.BaseOperator
        """
        serialize_op = cls.serialize_object(op, visited_dags,
            included_fields=SerializedOperator._op_included_fields)
        # Adds a new task_type field to record the original operator class.
        serialize_op['_task_type'] = op.__class__.__name__
        return cls.encode(serialize_op, type_=DagTypes.OP)

    @classmethod
    def deserialize_operator(cls, encoded_op, visited_dags):
        """Returns a deserialized operator.

        :param encoded_op: a JSON dict of serialized operator.
        :type encoded_op: dict
        """
        op = object.__new__(SerializedOperator)
        cls.deserialize_object(
            encoded_op, op, SerializedOperator._op_included_fields, visited_dags)
        return op
