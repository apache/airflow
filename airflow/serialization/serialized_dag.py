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
from inspect import signature

from airflow.models.dag import DAG
from airflow.serialization.base_serialization import BaseSerialization
from airflow.serialization.json_schema import load_dag_schema


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

    _decorated_fields = {'schedule_interval', 'default_args'}

    @staticmethod
    def __get_constructor_defaults():  # pylint: disable=no-method-argument
        param_to_attr = {
            'concurrency': '_concurrency',
            'description': '_description',
            'default_view': '_default_view',
            'access_control': '_access_control',
        }
        return {
            param_to_attr.get(k, k): v for k, v in signature(DAG).parameters.items()
            if v.default is not v.empty and v.default is not None
        }
    _CONSTRUCTOR_PARAMS = __get_constructor_defaults.__func__()  # type: ignore
    del __get_constructor_defaults

    _json_schema = load_dag_schema()

    @classmethod
    def serialize_dag(cls, dag: DAG) -> dict:
        """Serializes a DAG into a JSON object.
        """
        serialize_dag = cls.serialize_to_json(dag, cls._decorated_fields)

        serialize_dag["tasks"] = [cls._serialize(task) for _, task in dag.task_dict.items()]
        return serialize_dag

    @classmethod
    def deserialize_dag(cls, encoded_dag: dict) -> "SerializedDAG":
        """Deserializes a DAG from a JSON object.
        """
        from airflow.serialization.serialized_baseoperator import SerializedBaseOperator

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
            elif k in {"retry_delay", "execution_timeout"}:
                v = cls._deserialize_timedelta(v)
            elif k.endswith("_date"):
                v = cls._deserialize_datetime(v)
            elif k in cls._decorated_fields:
                v = cls._deserialize(v)
            # else use v as it is

            setattr(dag, k, v)

        keys_to_set_none = dag.get_serialized_fields() - encoded_dag.keys() - cls._CONSTRUCTOR_PARAMS.keys()
        for k in keys_to_set_none:
            setattr(dag, k, None)

        setattr(dag, 'full_filepath', dag.fileloc)
        for task in dag.task_dict.values():
            task.dag = dag
            serializable_task: SerializedBaseOperator = task

            for date_attr in ["start_date", "end_date"]:
                if getattr(serializable_task, date_attr) is None:
                    setattr(serializable_task, date_attr, getattr(dag, date_attr))

            if serializable_task.subdag is not None:
                setattr(serializable_task.subdag, 'parent_dag', dag)
                serializable_task.subdag.is_subdag = True

            for task_id in serializable_task.downstream_task_ids:
                # Bypass set_upstream etc here - it does more than we want
                # noinspection PyProtectedMember
                dag.task_dict[task_id]._upstream_task_ids.add(task_id)  # pylint: disable=protected-access

        return dag

    @classmethod
    def to_dict(cls, var) -> dict:
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
    def from_dict(cls, serialized_obj: dict) -> 'SerializedDAG':
        """Deserializes a python dict in to the DAG and operators it contains."""
        ver = serialized_obj.get('__version', '<not present>')
        if ver != cls.SERIALIZER_VERSION:
            raise ValueError("Unsure how to deserialize version {!r}".format(ver))
        return cls.deserialize_dag(serialized_obj['dag'])
