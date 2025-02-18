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
import ydb

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

    def test_bulk_upsert(self):
        create_table_op = YDBExecuteQueryOperator(
            task_id="create",
            sql="""
            CREATE TABLE team (
                id INT,
                name TEXT,
                age UINT32,
                PRIMARY KEY (id)
            );""",
            is_ddl=True,
        )

        create_table_op.execute(self.mock_context)

        age_sum_op = YDBExecuteQueryOperator(task_id="age_sum", sql="SELECT SUM(age) as age_sum FROM team")

        hook = age_sum_op.get_db_hook()
        column_types = (
            ydb.BulkUpsertColumns()
            .add_column("id", ydb.OptionalType(ydb.PrimitiveType.Int32))
            .add_column("name", ydb.OptionalType(ydb.PrimitiveType.Utf8))
            .add_column("age", ydb.OptionalType(ydb.PrimitiveType.Uint32))
        )

        rows = [
            {"id": 1, "name": "rabbits", "age": 17},
            {"id": 2, "name": "bears", "age": 22},
            {"id": 3, "name": "foxes", "age": 9},
        ]
        hook.bulk_upsert("team", rows=rows, column_types=column_types)

        result = age_sum_op.execute(self.mock_context)
        assert result == [(48,)]
