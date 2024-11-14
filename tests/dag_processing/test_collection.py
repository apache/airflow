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

import warnings

from sqlalchemy.exc import SAWarning

from airflow.dag_processing.collection import _get_latest_runs_stmt


def test_statement_latest_runs_one_dag():
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=SAWarning)

        stmt = _get_latest_runs_stmt(["fake-dag"])
        compiled_stmt = str(stmt.compile())
        actual = [x.strip() for x in compiled_stmt.splitlines()]
        expected = [
            "SELECT dag_run.logical_date, dag_run.id, dag_run.dag_id, "
            "dag_run.data_interval_start, dag_run.data_interval_end",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_1 AND dag_run.logical_date = ("
            "SELECT max(dag_run.logical_date) AS max_execution_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_2 AND dag_run.run_type IN (__[POSTCOMPILE_run_type_1]))",
        ]
        assert actual == expected, compiled_stmt


def test_statement_latest_runs_many_dag():
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=SAWarning)

        stmt = _get_latest_runs_stmt(["fake-dag-1", "fake-dag-2"])
        compiled_stmt = str(stmt.compile())
        actual = [x.strip() for x in compiled_stmt.splitlines()]
        expected = [
            "SELECT dag_run.logical_date, dag_run.id, dag_run.dag_id, "
            "dag_run.data_interval_start, dag_run.data_interval_end",
            "FROM dag_run, (SELECT dag_run.dag_id AS dag_id, "
            "max(dag_run.logical_date) AS max_execution_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id IN (__[POSTCOMPILE_dag_id_1]) "
            "AND dag_run.run_type IN (__[POSTCOMPILE_run_type_1]) GROUP BY dag_run.dag_id) AS anon_1",
            "WHERE dag_run.dag_id = anon_1.dag_id AND dag_run.logical_date = anon_1.max_execution_date",
        ]
        assert actual == expected, compiled_stmt
