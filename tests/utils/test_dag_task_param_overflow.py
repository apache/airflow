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

from airflow.exceptions import AirflowDagTaskOutOfBoundsValue
from airflow.models import dag as dag_module, dagbag as dagbag_module
from airflow.operators.empty import EmptyOperator
from airflow.utils.dag_parameters_overflow import (
    _WEIGHT_LOWER_BOUND,
    _WEIGHT_UPPER_BOUND,
    check_values_overflow,
)
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestDagTaskParameterOverflow:
    def test_priority_weight_default(self, dag_maker, mocker):
        spy = mocker.spy(dagbag_module, "check_values_overflow")
        with dag_maker(dag_id="test_priority_weight_empty") as dag:
            EmptyOperator(task_id="empty")
        spy.assert_called_once_with(dag)

    @pytest.mark.parametrize(
        "priority_weight",
        [
            42,
            pytest.param(_WEIGHT_LOWER_BOUND, id="lower-bound"),
            pytest.param(_WEIGHT_UPPER_BOUND, id="upper-bound"),
        ],
    )
    def test_priority_weight_absolute(self, priority_weight, dag_maker, mocker):
        spy = mocker.spy(dagbag_module, "check_values_overflow")
        with dag_maker(dag_id="test_priority_weight_empty") as dag:
            EmptyOperator(task_id="empty", priority_weight=priority_weight)
        spy.assert_called_once_with(dag)

    @pytest.mark.parametrize(
        "priority_weight",
        [
            pytest.param(_WEIGHT_LOWER_BOUND - 1, id="less-than-lower-bound"),
            pytest.param(_WEIGHT_UPPER_BOUND + 1, id="greater-than-upper-bound"),
        ],
    )
    def test_priority_weight_absolute_overflow(self, priority_weight, dag_maker, mocker):
        spy = mocker.spy(dagbag_module, "check_values_overflow")
        error_message = f"'empty' has priority weight {priority_weight}"
        with pytest.raises(AirflowDagTaskOutOfBoundsValue, match=error_message):
            with dag_maker(dag_id="test_priority_weight_empty") as dag:
                EmptyOperator(task_id="empty", priority_weight=priority_weight)
        spy.assert_called_once_with(dag)

    @pytest.mark.parametrize(
        "priority, bound_priority",
        [
            pytest.param(-1, _WEIGHT_LOWER_BOUND, id="less-than-lower-bound"),
            pytest.param(1, _WEIGHT_UPPER_BOUND, id="greater-than-upper-bound"),
        ],
    )
    def test_priority_weight_sum_up_overflow(self, priority: int, bound_priority: int, dag_maker, mocker):
        spy = mocker.spy(dagbag_module, "check_values_overflow")
        with pytest.raises(AirflowDagTaskOutOfBoundsValue) as err_ctx:
            with dag_maker(dag_id="test_priority_weight_sum_up_overflow") as dag:
                op1 = EmptyOperator(task_id="stage1", priority_weight=priority)
                op2 = EmptyOperator(task_id="stage2", priority_weight=priority)
                op3 = EmptyOperator(task_id="stage3", priority_weight=bound_priority)
                op1 >> op2 >> op3
        spy.assert_called_once_with(dag)
        exc_msg = err_ctx.value.args[0]
        assert "Tasks in dag 'test_priority_weight_sum_up_overflow' exceeds" in exc_msg
        assert "Task 'stage1' has priority weight" in exc_msg
        assert "Task 'stage2' has priority weight" in exc_msg
        assert "Task 'stage3' has priority weight" not in exc_msg

    @pytest.mark.parametrize(
        "priority_weight",
        [
            42,
            pytest.param(_WEIGHT_LOWER_BOUND, id="lower-bound"),
            pytest.param(_WEIGHT_UPPER_BOUND, id="upper-bound"),
            pytest.param(_WEIGHT_LOWER_BOUND - 1, id="less-than-lower-bound"),
            pytest.param(_WEIGHT_UPPER_BOUND + 1, id="greater-than-upper-bound"),
            pytest.param(10**100, id="positive-googol"),
            pytest.param(-(10**100), id="negative-googol"),
        ],
    )
    def test_priority_weight_database_rule(self, priority_weight, dag_maker, mocker):
        """Test that in `database` check rule doesn't matter values until it saved into the database."""
        with conf_vars({("core", "priority_weight_check_rule"): "ignore"}):
            with dag_module.DAG("test_priority_weight_database_rule", schedule=None) as dag:
                EmptyOperator(task_id="empty", priority_weight=priority_weight)
            check_values_overflow(dag)
