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

from unittest.mock import MagicMock

import pytest

from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.ydb.operators.ydb import YDBExecuteQueryOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2024, 1, 1)


@pytest.fixture(scope="module", autouse=True)
def ydb_connections():
    """Create YDB connection which use for testing purpose."""
    c = Connection(
        conn_id="ydb_default", conn_type="ydb", host="grpc://ydb", port=2136, extra={"database": "/local"}
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AIRFLOW_CONN_YDB_DEFAULT", c.as_json())
        yield


@pytest.mark.integration("ydb")
class TestYDBExecuteQueryOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_ydb_dag_id", default_args=args)

        self.mock_context = MagicMock()

    def test_execute_hello(self):
        operator = YDBExecuteQueryOperator(
            task_id="simple_sql", sql="select 987", is_ddl=False, handler=fetch_all_handler
        )

        results = operator.execute(self.mock_context)
        assert results == [(987,)]
