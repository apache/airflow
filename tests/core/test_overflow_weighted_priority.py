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

import contextlib
import warnings

import pytest

from airflow.models.baseoperator import BaseOperator
from airflow.operators.empty import EmptyOperator
from tests.test_utils.db import clear_db_dags, clear_db_serialized_dags

INT32_MAX = 2147483647
INT32_MIN = -2147483648


@contextlib.contextmanager
def _warning_not_expected():
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "error", message=".*exceeds allowed priority weight range.*", category=UserWarning
        )
        yield


@pytest.fixture
def _clear_dags():
    clear_db_dags()
    clear_db_serialized_dags()
    yield
    clear_db_dags()
    clear_db_serialized_dags()


class TestDagTaskParameterOverflow:
    @_warning_not_expected()
    def test_priority_weight_default(self):
        assert EmptyOperator(task_id="empty").priority_weight_total

    @pytest.mark.parametrize(
        "priority_weight",
        [
            42,
            pytest.param(INT32_MIN, id="lower-bound"),
            pytest.param(INT32_MAX, id="upper-bound"),
        ],
    )
    @_warning_not_expected()
    def test_priority_weight_absolute(self, priority_weight):
        EmptyOperator(task_id="empty", priority_weight=priority_weight)

    @pytest.mark.parametrize(
        "priority_weight, priority_weight_total",
        [
            pytest.param(INT32_MIN - 1, INT32_MIN, id="less-than-lower-bound"),
            pytest.param(INT32_MAX + 1, INT32_MAX, id="greater-than-upper-bound"),
        ],
    )
    def test_priority_weight_absolute_overflow(self, priority_weight, priority_weight_total):
        op = EmptyOperator(task_id="empty", priority_weight=priority_weight)
        with pytest.warns(UserWarning, match="exceeds allowed priority weight range"):
            assert op.priority_weight_total == priority_weight_total

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "priority, bound_priority",
        [
            pytest.param(-10, INT32_MIN, id="less-than-lower-bound"),
            pytest.param(10, INT32_MAX, id="greater-than-upper-bound"),
        ],
    )
    def test_priority_weight_sum_up_overflow(
        self, priority: int, bound_priority: int, dag_maker, _clear_dags
    ):
        class TestOp(BaseOperator):
            def __init__(self, value, **kwargs):
                super().__init__(**kwargs)
                self.value = value

        with dag_maker(dag_id="test_priority_weight_sum_up_overflow"):
            op1 = EmptyOperator(task_id="op1", priority_weight=priority)
            op2 = TestOp.partial(task_id="op2", priority_weight=bound_priority).expand(value=[1, 2, 3])
            op3 = EmptyOperator(task_id="op3", priority_weight=priority)
            op1 >> op2 >> op3

        with pytest.warns(UserWarning, match="exceeds allowed priority weight range"):
            dr = dag_maker.create_dagrun()

        tis_priorities = {ti.task_id: ti.priority_weight for ti in dr.task_instances}
        assert tis_priorities["op3"] == priority
        assert tis_priorities["op2"] == bound_priority
        assert tis_priorities["op1"] == bound_priority
