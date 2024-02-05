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
from __future__ import annotations

import pytest

from airflow.operators.empty import EmptyOperator

pytestmark = pytest.mark.db_test


class TestTaskmin:
    def test_set_upstream_list(self, dag_maker):
        with dag_maker("test_set_upstream_list"):
            op_a = EmptyOperator(task_id="a")
            op_b = EmptyOperator(task_id="b")
            op_c = EmptyOperator(task_id="c")
            op_d = EmptyOperator(task_id="d")

            [op_d, op_c << op_b] << op_a

        assert [op_a] == op_b.upstream_list
        assert [op_a] == op_d.upstream_list
        assert [op_b] == op_c.upstream_list

    def test_set_downstream_list(self, dag_maker):
        with dag_maker("test_set_downstream_list"):
            op_a = EmptyOperator(task_id="a")
            op_b = EmptyOperator(task_id="b")
            op_c = EmptyOperator(task_id="c")
            op_d = EmptyOperator(task_id="d")

            op_a >> [op_b >> op_c, op_d]

        assert [] == op_b.upstream_list
        assert [op_a] == op_d.upstream_list
        assert {op_a, op_b} == set(op_c.upstream_list)
