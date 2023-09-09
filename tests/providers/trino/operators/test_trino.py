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

from unittest import mock

import pytest
from openlineage.client.facet import SchemaDatasetFacet, SchemaField, SqlJobFacet
from openlineage.client.run import Dataset

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.connection import Connection
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.trino.operators.trino import TrinoOperator

TRINO_CONN_ID = "test_trino"
TASK_ID = "test_trino_task"


class TestTrinoOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_execute(self, mock_get_db_hook):
        """Asserts that the run method is called when a TrinoOperator task is executed"""

        with pytest.warns(AirflowProviderDeprecationWarning, match="This class is deprecated.*"):
            op = TrinoOperator(
                task_id=TASK_ID,
                sql="SELECT 1;",
                trino_conn_id=TRINO_CONN_ID,
                handler=list,
            )
        op.execute(None)

        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT 1;",
            autocommit=False,
            handler=list,
            parameters=None,
            return_last=True,
        )


def test_execute_openlineage_events():
    DB_NAME = "tpch"
    DB_SCHEMA_NAME = "sf1"

    class TrinoHookForTests(TrinoHook):
        get_conn = mock.MagicMock(name="conn")
        get_connection = mock.MagicMock()

        def get_first(self, *_):
            return [f"{DB_NAME}.{DB_SCHEMA_NAME}"]

    dbapi_hook = TrinoHookForTests()

    class TrinoOperatorForTest(TrinoOperator):
        def get_db_hook(self):
            return dbapi_hook

    sql = "SELECT name FROM tpch.sf1.customer LIMIT 3"
    op = TrinoOperatorForTest(task_id="trino-operator", sql=sql)
    rows = [
        (DB_SCHEMA_NAME, "customer", "custkey", 1, "bigint", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "name", 2, "varchar(25)", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "address", 3, "varchar(40)", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "nationkey", 4, "bigint", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "phone", 5, "varchar(15)", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "acctbal", 6, "double", DB_NAME),
    ]
    dbapi_hook.get_connection.return_value = Connection(
        conn_id="trino_default",
        conn_type="trino",
        host="trino",
        port=8080,
    )
    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    lineage = op.get_openlineage_facets_on_start()
    assert lineage.inputs == [
        Dataset(
            namespace="trino://trino:8080",
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.customer",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaField(name="custkey", type="bigint"),
                        SchemaField(name="name", type="varchar(25)"),
                        SchemaField(name="address", type="varchar(40)"),
                        SchemaField(name="nationkey", type="bigint"),
                        SchemaField(name="phone", type="varchar(15)"),
                        SchemaField(name="acctbal", type="double"),
                    ]
                )
            },
        )
    ]

    assert len(lineage.outputs) == 0

    assert lineage.job_facets == {"sql": SqlJobFacet(query=sql)}
