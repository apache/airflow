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
from inspect import signature

from airflow.models.baseoperator import BaseOperator
from airflow.serialization.base_serialization import BaseSerialization


class SerializedBaseOperator(BaseOperator, BaseSerialization):
    """A JSON serializable representation of operator.

    All operators are casted to SerializedBaseOperator after deserialization.
    Class specific attributes used by UI are move to object attributes.
    """

    _decorated_fields = {'executor_config', }

    _CONSTRUCTOR_PARAMS = {
        k: v for k, v in signature(BaseOperator).parameters.items()
        if v.default is not v.empty and v.default is not None
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
    def task_type(self) -> str:
        # Overwrites task_type of BaseOperator to use _task_type instead of
        # __class__.__name__.
        return self._task_type

    @task_type.setter
    def task_type(self, task_type: str):
        self._task_type = task_type

    @classmethod
    def serialize_operator(cls, op: BaseOperator) -> dict:
        """Serializes operator into a JSON object.
        """
        serialize_op = cls.serialize_to_json(op, cls._decorated_fields)
        serialize_op['_task_type'] = op.__class__.__name__
        serialize_op['_task_module'] = op.__class__.__module__
        return serialize_op

    @classmethod
    def deserialize_operator(cls, encoded_op: dict) -> BaseOperator:
        """Deserializes an operator from a JSON object.
        """
        from airflow.serialization.serialized_dag import SerializedDAG
        from airflow.plugins_manager import operator_extra_links

        op = SerializedBaseOperator(task_id=encoded_op['task_id'])

        # Extra Operator Links
        op_extra_links_from_plugin = {}

        for ope in operator_extra_links:
            for operator in ope.operators:
                if operator.__name__ == encoded_op["_task_type"] and \
                        operator.__module__ == encoded_op["_task_module"]:
                    op_extra_links_from_plugin.update({ope.name: ope})

        setattr(op, "operator_extra_links", list(op_extra_links_from_plugin.values()))

        for k, v in encoded_op.items():

            if k == "_downstream_task_ids":
                v = set(v)
            elif k == "subdag":
                v = SerializedDAG.deserialize_dag(v)
            elif k in {"retry_delay", "execution_timeout"}:
                v = cls._deserialize_timedelta(v)
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
            elif k in cls._decorated_fields or k not in op.get_serialized_fields():
                v = cls._deserialize(v)
            # else use v as it is

            setattr(op, k, v)

        for k in op.get_serialized_fields() - encoded_op.keys() - cls._CONSTRUCTOR_PARAMS.keys():
            setattr(op, k, None)

        return op

    @classmethod
    def _is_excluded(cls, var, attrname, op):
        if var is not None and op.has_dag() and attrname.endswith("_date"):
            # If this date is the same as the matching field in the dag, then
            # don't store it again at the task level.
            dag_date = getattr(op.dag, attrname, None)
            if var is dag_date or var == dag_date:
                return True
        if attrname in {"executor_config", "params"} and not var:
            # Don't store empty executor config or params dicts.
            return True
        return super()._is_excluded(var, attrname, op)
