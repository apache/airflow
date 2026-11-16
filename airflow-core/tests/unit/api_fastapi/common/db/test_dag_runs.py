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
from sqlalchemy import select

from airflow.api_fastapi.common.db.dag_runs import filter_runs_of_dag
from airflow.api_fastapi.common.parameters import SortParam
from airflow.models.dagrun import DagRun


@pytest.mark.parametrize(
    ("order_by", "expected_where", "expected_order_by"),
    [
        (
            ["-run_after"],
            "WHERE dag_run.dag_id BETWEEN 'a_dag' AND 'a_dag'",
            "ORDER BY dag_run.dag_id DESC, dag_run.run_after DESC, dag_run.id DESC",
        ),
        (
            ["-start_date"],
            "WHERE dag_run.dag_id = 'a_dag'",
            "ORDER BY dag_run.start_date DESC, dag_run.id DESC",
        ),
    ],
)
def test_filter_runs_of_dag_leads_a_run_after_sort_with_dag_id(order_by, expected_where, expected_order_by):
    sort = SortParam(["run_after", "start_date"], DagRun).set_value(order_by)

    statement = sort.to_orm(filter_runs_of_dag(select(DagRun.id), sort, dag_id="a_dag"))

    sql = str(statement.compile(compile_kwargs={"literal_binds": True}))
    assert expected_where in sql
    assert expected_order_by in sql
