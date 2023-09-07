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

import pytest

from airflow import DAG
from airflow.exceptions import AirflowDagTaskOutOfBoundsValue
from airflow.operators.empty import EmptyOperator
from airflow.utils.dag_parameters_overflow import check_values_overflow
from tests.models import DEFAULT_DATE


class TestDagTaskParameterOverflow:
    def test_priority_weight_empty(self):
        # test empty DAG no overflow
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        assert not check_values_overflow(dag)

    def test_priority_weight_single_task(self):
        # test single task with no specific priority weight (no overflow)
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        with dag:
            EmptyOperator(task_id="stage1")

        assert not check_values_overflow(dag)

    @pytest.mark.backend("postgres")
    def test_priority_weight_sum_up_overflow(self):
        # Test that priority_weight_total sum up overflows
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})
        with dag:
            op1 = EmptyOperator(task_id="stage1", priority_weight=10)
            op2 = EmptyOperator(task_id="stage2", priority_weight=2147483647)
            op1.set_downstream(op2)
        with pytest.raises(AirflowDagTaskOutOfBoundsValue):
            assert not check_values_overflow(dag)

    @pytest.mark.backend("postgres")
    def test_priority_weight_negative_overflow(self):
        # Test that priority_weight_total overflows
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})
        with dag:
            EmptyOperator(task_id="stage1", priority_weight=-3147483648)
        with pytest.raises(AirflowDagTaskOutOfBoundsValue):
            assert not check_values_overflow(dag)

    @pytest.mark.backend("postgres")
    def test_priority_weight_positive_overflow(self):
        # Test that priority_weight_total overflows
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})
        with dag:
            EmptyOperator(task_id="stage1", priority_weight=2147483648)
        with pytest.raises(AirflowDagTaskOutOfBoundsValue):
            assert not check_values_overflow(dag)
