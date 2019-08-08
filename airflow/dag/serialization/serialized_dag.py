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

from airflow.dag.serialization.enum import DagAttributeTypes as DAT, Encoding
from airflow.dag.serialization.json_schema import make_dag_schema
from airflow.dag.serialization.serialization import Serialization
from airflow.models import DAG


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
    # FIXME: to customize included fields and keep only necessary fields.
    _included_fields = list(vars(DAG(dag_id='test')).keys())

    _json_schema = make_dag_schema()

    @classmethod
    def serialize_dag(cls, dag: DAG, visited_dags: dict) -> dict:
        """Serializes a DAG into a JSON object.
        """
        if dag.dag_id in visited_dags:
            return {Encoding.TYPE: DAT.DAG, Encoding.VAR: str(dag.dag_id)}

        new_dag = {Encoding.TYPE: DAT.DAG}
        visited_dags[dag.dag_id] = new_dag
        new_dag[Encoding.VAR] = cls._serialize_object(
            dag, visited_dags, included_fields=cls._included_fields)
        return new_dag

    @classmethod
    def deserialize_dag(cls, encoded_dag: dict, visited_dags: dict) -> DAG:
        """Deserializes a DAG from a JSON object.
        """
        dag = SerializedDAG(dag_id=encoded_dag['_dag_id'])
        visited_dags[dag.dag_id] = dag
        cls._deserialize_object(encoded_dag, dag, cls._included_fields, visited_dags)
        return dag
