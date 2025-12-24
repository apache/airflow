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

import inspect
from contextlib import closing
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import MagicMock

import pytest
from more_itertools import flatten

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

from tests_common.test_utils.compat import GenericTransfer
from tests_common.test_utils.operators.run_deferrable import execute_operator, mock_context
from tests_common.test_utils.providers import get_provider_min_airflow_version

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"
pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"
INSERT_ARGS = {
    "commit_every": 1000,  # Number of rows inserted in each batch
    "executemany": True,  # Enable batch inserts
    "fast_executemany": True,  # Boost performance for MSSQL inserts
    "replace": True,  # Used for upserts/merges if needed
}
counter = 0


@pytest.mark.backend("mysql")
class TestMySql:
    def teardown_method(self):
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        drop_tables = {"test_mysql_to_mysql", "test_airflow"}
        with closing(MySqlHook().get_conn()) as conn:
            for table in drop_tables:
                # Previous version tried to run execute directly on dbapi call, which was accidentally working
                with closing(conn.cursor()) as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    @pytest.mark.parametrize(
        "client",
        [
            "mysqlclient",
            "mysql-connector-python",
        ],
    )
    def test_mysql_to_mysql(self, client, dag_maker):
        class MySqlContext:
            def __init__(self, client):
                self.client = client
                self.connection = MySqlHook.get_connection(MySqlHook.default_conn_name)
                self.init_client = self.connection.extra_dejson.get("client", "mysqlclient")

            def __enter__(self):
                self.connection.set_extra(f'{{"client": "{self.client}"}}')

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.connection.set_extra(f'{{"client": "{self.init_client}"}}')

        with MySqlContext(client):
            sql = "SELECT * FROM connection;"
            with dag_maker(f"TEST_DAG_ID_{client}", start_date=DEFAULT_DATE):
                op = GenericTransfer(
                    task_id="test_m2m",
                    preoperator=[
                        "DROP TABLE IF EXISTS test_mysql_to_mysql",
                        "CREATE TABLE IF NOT EXISTS test_mysql_to_mysql LIKE connection",
                    ],
                    source_conn_id="airflow_db",
                    destination_conn_id="airflow_db",
                    destination_table="test_mysql_to_mysql",
                    sql=sql,
                )

            dag_maker.run_ti(op.task_id)

    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
    def test_mysql_to_mysql_replace(self, mock_insert, dag_maker):
        sql = "SELECT * FROM connection LIMIT 10;"
        with dag_maker("TEST_DAG_ID", start_date=DEFAULT_DATE):
            op = GenericTransfer(
                task_id="test_m2m",
                preoperator=[
                    "DROP TABLE IF EXISTS test_mysql_to_mysql",
                    "CREATE TABLE IF NOT EXISTS test_mysql_to_mysql LIKE connection",
                ],
                source_conn_id="airflow_db",
                destination_conn_id="airflow_db",
                destination_table="test_mysql_to_mysql",
                sql=sql,
                insert_args={"replace": True},
            )

        dag_maker.run_ti(op.task_id)
        assert mock_insert.called
        _, kwargs = mock_insert.call_args
        assert "replace" in kwargs


@pytest.mark.backend("postgres")
class TestPostgres:
    def teardown_method(self):
        tables_to_drop = ["test_postgres_to_postgres", "test_airflow"]
        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                for table in tables_to_drop:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    def test_postgres_to_postgres(self, dag_maker):
        sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 100;"
        with dag_maker(default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True):
            _ = GenericTransfer(
                task_id="test_p2p",
                preoperator=[
                    "DROP TABLE IF EXISTS test_postgres_to_postgres",
                    "CREATE TABLE IF NOT EXISTS test_postgres_to_postgres (LIKE INFORMATION_SCHEMA.TABLES)",
                ],
                source_conn_id="postgres_default",
                destination_conn_id="postgres_default",
                destination_table="test_postgres_to_postgres",
                sql=sql,
            )
        dr = dag_maker.create_dagrun()
        dag_maker.run_ti("test_p2p", dr)

    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
    def test_postgres_to_postgres_replace(self, mock_insert, dag_maker):
        sql = "SELECT id, conn_id, conn_type FROM connection LIMIT 10;"
        with dag_maker(default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, serialized=True):
            _ = GenericTransfer(
                task_id="test_p2p",
                preoperator=[
                    "DROP TABLE IF EXISTS test_postgres_to_postgres",
                    "CREATE TABLE IF NOT EXISTS test_postgres_to_postgres (LIKE connection INCLUDING INDEXES)",
                ],
                source_conn_id="postgres_default",
                destination_conn_id="postgres_default",
                destination_table="test_postgres_to_postgres",
                sql=sql,
                insert_args={
                    "replace": True,
                    "target_fields": ("id", "conn_id", "conn_type"),
                    "replace_index": "id",
                },
            )
        dr = dag_maker.create_dagrun()
        dag_maker.run_ti("test_p2p", dr)
        assert mock_insert.called
        _, kwargs = mock_insert.call_args
        assert "replace" in kwargs


class TestGenericTransfer:
    mocked_source_hook = MagicMock(conn_name_attr="my_source_conn_id", spec=DbApiHook)
    mocked_destination_hook = MagicMock(conn_name_attr="my_destination_conn_id", spec=DbApiHook)
    mocked_hooks = {
        "my_source_conn_id": mocked_source_hook,
        "my_destination_conn_id": mocked_destination_hook,
    }

    @classmethod
    def get_hook(cls, conn_id: str, hook_params: dict | None = None):
        return cls.mocked_hooks[conn_id]

    @classmethod
    def get_connection(cls, conn_id: str):
        mocked_hook = cls.get_hook(conn_id=conn_id)
        mocked_conn = MagicMock(conn_id=conn_id, spec=Connection)
        mocked_conn.get_hook.return_value = mocked_hook
        return mocked_conn

    def setup_method(self):
        # Reset mock states before each test
        self.mocked_source_hook.reset_mock()
        self.mocked_destination_hook.reset_mock()

        # Set up the side effect for paginated read
        records = [
            [[1, 2], [11, 12], [3, 4], [13, 14]],
            [[3, 4], [13, 14]],
        ]

        def get_records_side_effect(sql: str):
            if records:
                if "LIMIT" not in sql:
                    return list(flatten(records))
                return records.pop(0)
            return []

        self.mocked_source_hook.get_records.side_effect = get_records_side_effect

    def test_templated_fields(self):
        dag = DAG(
            "test_dag",
            schedule=None,
            start_date=datetime(2024, 10, 10),
            render_template_as_native_obj=True,
        )
        operator = GenericTransfer(
            task_id="test_task",
            sql="{{ sql }}",
            destination_table="{{ destination_table }}",
            source_conn_id="{{ source_conn_id }}",
            destination_conn_id="{{ destination_conn_id }}",
            preoperator="{{ preoperator }}",
            insert_args="{{ insert_args }}",
            page_size="{{ page_size }}",
            paginated_sql_statement_clause="{{ paginated_sql_statement_clause }}",
            dag=dag,
        )
        operator.render_template_fields(
            {
                "sql": "my_sql",
                "destination_table": "my_destination_table",
                "source_conn_id": "my_source_conn_id",
                "destination_conn_id": "my_destination_conn_id",
                "preoperator": "my_preoperator",
                "insert_args": {"commit_every": 5000, "executemany": True, "replace": True},
                "page_size": 1000,
                "paginated_sql_statement_clause": "{} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY;",
            }
        )
        assert operator.sql == "my_sql"
        assert operator.destination_table == "my_destination_table"
        assert operator.source_conn_id == "my_source_conn_id"
        assert operator.destination_conn_id == "my_destination_conn_id"
        assert operator.preoperator == "my_preoperator"
        assert operator.insert_args == {"commit_every": 5000, "executemany": True, "replace": True}
        assert operator.page_size == 1000
        assert operator.paginated_sql_statement_clause == "{} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY;"

    def test_non_paginated_read(self):
        with mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection", side_effect=self.get_connection):
            with mock.patch(f"{BASEHOOK_PATCH_PATH}.get_hook", side_effect=self.get_hook):
                operator = GenericTransfer(
                    task_id="transfer_table",
                    source_conn_id="my_source_conn_id",
                    destination_conn_id="my_destination_conn_id",
                    sql="SELECT * FROM HR.EMPLOYEES",
                    destination_table="NEW_HR.EMPLOYEES",
                    insert_args=INSERT_ARGS,
                    execution_timeout=timedelta(hours=1),
                )

                operator.execute(context=mock_context(task=operator))

        assert self.mocked_source_hook.get_records.call_count == 1
        assert self.mocked_source_hook.get_records.call_args_list[0].args[0] == "SELECT * FROM HR.EMPLOYEES"
        assert self.mocked_destination_hook.insert_rows.call_count == 1
        assert self.mocked_destination_hook.insert_rows.call_args_list[0].kwargs == {
            **INSERT_ARGS,
            **{"rows": [[1, 2], [11, 12], [3, 4], [13, 14], [3, 4], [13, 14]], "table": "NEW_HR.EMPLOYEES"},
        }

    def test_non_paginated_read_for_multiple_sql_statements(self):
        with mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection", side_effect=self.get_connection):
            with mock.patch(f"{BASEHOOK_PATCH_PATH}.get_hook", side_effect=self.get_hook):
                operator = GenericTransfer(
                    task_id="transfer_table",
                    source_conn_id="my_source_conn_id",
                    destination_conn_id="my_destination_conn_id",
                    sql=["SELECT * FROM HR.EMPLOYEES", "SELECT * FROM HR.PEOPLE"],
                    destination_table="NEW_HR.EMPLOYEES",
                    insert_args=INSERT_ARGS,
                    execution_timeout=timedelta(hours=1),
                )

                operator.execute(context=mock_context(task=operator))

            assert self.mocked_source_hook.get_records.call_count == 2
            assert [call.args[0] for call in self.mocked_source_hook.get_records.call_args_list] == [
                "SELECT * FROM HR.EMPLOYEES",
                "SELECT * FROM HR.PEOPLE",
            ]
            assert self.mocked_destination_hook.insert_rows.call_count == 2
            assert self.mocked_destination_hook.insert_rows.call_args_list[0].kwargs == {
                **INSERT_ARGS,
                "rows": [[1, 2], [11, 12], [3, 4], [13, 14], [3, 4], [13, 14]],
                "table": "NEW_HR.EMPLOYEES",
            }
            assert self.mocked_destination_hook.insert_rows.call_args_list[1].kwargs == {
                **INSERT_ARGS,
                "rows": [[1, 2], [11, 12], [3, 4], [13, 14], [3, 4], [13, 14]],
                "table": "NEW_HR.EMPLOYEES",
            }

    def test_paginated_read(self):
        """
        This unit test is based on the example described in the medium article:
        https://medium.com/apache-airflow/transfering-data-from-sap-hana-to-mssql-using-the-airflow-generictransfer-d29f147a9f1f
        """

        with mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection", side_effect=self.get_connection):
            with mock.patch(f"{BASEHOOK_PATCH_PATH}.get_hook", side_effect=self.get_hook):
                operator = GenericTransfer(
                    task_id="transfer_table",
                    source_conn_id="my_source_conn_id",
                    destination_conn_id="my_destination_conn_id",
                    sql="SELECT * FROM HR.EMPLOYEES",
                    destination_table="NEW_HR.EMPLOYEES",
                    page_size=1000,  # Fetch data in chunks of 1000 rows for pagination
                    insert_args=INSERT_ARGS,
                    execution_timeout=timedelta(hours=1),
                )

                results, events = execute_operator(operator)

                assert not results
                assert len(events) == 3
                assert events[0].payload["results"] == [[1, 2], [11, 12], [3, 4], [13, 14]]
                assert events[1].payload["results"] == [[3, 4], [13, 14]]
                assert not events[2].payload["results"]

        assert self.mocked_source_hook.get_records.call_count == 3
        assert (
            self.mocked_source_hook.get_records.call_args_list[0].args[0]
            == "SELECT * FROM HR.EMPLOYEES LIMIT 1000 OFFSET 0"
        )
        assert self.mocked_destination_hook.insert_rows.call_count == 2
        assert self.mocked_destination_hook.insert_rows.call_args_list[0].kwargs == {
            **INSERT_ARGS,
            **{"rows": [[1, 2], [11, 12], [3, 4], [13, 14]], "table": "NEW_HR.EMPLOYEES"},
        }
        assert self.mocked_destination_hook.insert_rows.call_args_list[1].kwargs == {
            **INSERT_ARGS,
            **{"rows": [[3, 4], [13, 14]], "table": "NEW_HR.EMPLOYEES"},
        }

    def test_when_provider_min_airflow_version_is_3_0_or_higher_remove_obsolete_method(self):
        """
        Once this test starts failing due to the fact that the minimum Airflow version is now 3.0.0 or higher
        for this provider, you should remove the obsolete get_hook method in the GenericTransfer and use the
        one from BaseHook and remove this test.  This test was added to make sure to not forget to remove the
        fallback code for backward compatibility with Airflow 2.8.x which isn't need anymore once this
        provider depends on Airflow 3.0.0 or higher.
        """
        min_airflow_version = get_provider_min_airflow_version("apache-airflow-providers-standard")

        # Check if the current Airflow version is 3.0.0 or higher
        if min_airflow_version[0] >= 3:
            method_source = inspect.getsource(GenericTransfer.get_hook)
            raise AirflowProviderDeprecationWarning(
                f"Remove obsolete get_hook method in GenericTransfer:\n\r\n\r\t\t\t{method_source}"
            )
