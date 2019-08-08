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

"""Operator serialization with JSON."""

from airflow.dag.serialization.enum import DagAttributeTypes as DAT
from airflow.dag.serialization.json_schema import make_operator_schema
from airflow.dag.serialization.serialization import Serialization
from airflow.models import BaseOperator


class SerializedBaseOperator(BaseOperator, Serialization):
    """A JSON serializable representation of operator.

    All operators are casted to SerializedBaseOperator after deserialization.
    Class specific attributes used by UI are move to object attributes.
    """
    _included_fields = list(vars(BaseOperator(task_id='test')).keys()) + [
        '_dag', '_task_type', 'ui_color', 'ui_fgcolor', 'template_fields']

    _json_schema = make_operator_schema()

    def __init__(self, *args, **kwargs):
        BaseOperator.__init__(self, *args, **kwargs)
        # task_type is used by UI to display the correct class type, because UI only
        # receives BaseOperator from deserialized DAGs.
        self._task_type = 'BaseOperator'
        # Move class attributes into object attributes.
        self.ui_color = BaseOperator.ui_color
        self.ui_fgcolor = BaseOperator.ui_fgcolor
        self.template_fields = BaseOperator.template_fields

    @property
    def task_type(self) -> str:
        # Overwrites task_type of BaseOperator to use _task_type instead of
        # __class__.__name__.
        return self._task_type

    @task_type.setter
    def task_type(self, task_type: str):
        self._task_type = task_type

    @classmethod
    def serialize_operator(cls, op: BaseOperator, visited_dags: dict) -> dict:
        """Serializes operator into a JSON object.
        """
        serialize_op = cls._serialize_object(
            op, visited_dags, included_fields=SerializedBaseOperator._included_fields)
        # Adds a new task_type field to record the original operator class.
        serialize_op['_task_type'] = op.__class__.__name__
        return cls._encode(serialize_op, type_=DAT.OP)

    @classmethod
    def deserialize_operator(cls, encoded_op: dict, visited_dags: dict) -> BaseOperator:
        """Deserializes an operator from a JSON object.
        """
        op = SerializedBaseOperator(task_id=encoded_op['task_id'])
        cls._deserialize_object(
            encoded_op, op, SerializedBaseOperator._included_fields, visited_dags)
        return op
