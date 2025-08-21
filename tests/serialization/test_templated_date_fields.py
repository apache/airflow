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
from __future__ import annotations

from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG
from airflow.utils import timezone


class _DummyOperator(BaseOperator):
    def execute(self, context):  # pragma: no cover - not executed in tests
        return None


TEMPLATE_STR = "{{ (data_interval_end - macros.timedelta(days=4)).strftime('%Y-%m-%d') }}"


def test_operator_deserialize_ignores_templated_date_strings():
    """
    Ensure that deserializing an operator with a templated string in a *_date field
    does not attempt datetime coercion that would crash with a TypeError.

    This would fail prior to the guard that checks the value type is numeric.
    """
    with DAG(
        dag_id="test_templated_date_in_op",
        schedule=None,
        start_date=timezone.datetime(2021, 1, 1),
    ):
        op = _DummyOperator(task_id="t1")

    op.end_date = TEMPLATE_STR

    serialized = SerializedBaseOperator.serialize_operator(op)
    assert serialized.get("end_date") == TEMPLATE_STR

    # Would raise TypeError before the fix; should round-trip now
    deserialized = SerializedBaseOperator.deserialize_operator(serialized)
    assert getattr(deserialized, "end_date") == TEMPLATE_STR


def test_dag_deserialize_ignores_templated_date_strings():
    """
    Ensure that deserializing a DAG with a templated string in a *_date field
    does not attempt datetime coercion that would crash with a TypeError.

    This would fail prior to the guard that checks the value type is numeric.
    """
    dag = DAG(
        dag_id="test_templated_date_in_dag",
        schedule=None,
        start_date=timezone.datetime(2021, 1, 1),
    )

    dag.end_date = TEMPLATE_STR

    serialized = SerializedDAG.serialize_dag(dag)
    assert serialized.get("end_date") == TEMPLATE_STR

    # Would raise TypeError before the fix; should round-trip now
    deserialized = SerializedDAG.deserialize_dag(serialized)
    assert getattr(deserialized, "end_date") == TEMPLATE_STR
