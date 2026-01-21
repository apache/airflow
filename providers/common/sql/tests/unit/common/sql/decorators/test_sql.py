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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import Connection, task
    from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models import Connection  # type: ignore[assignment]
    from airflow.utils.types import NOTSET as SET_DURING_EXECUTION  # type: ignore[assignment]

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import timezone
else:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

DEFAULT_DATE = timezone.datetime(2023, 1, 1)
CONN_ID: str = "test-sql-decorator"


class TestSqlDecorator:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker, create_connection_without_db):
        # Create a connection to be used in testing
        conn = Connection(conn_id=CONN_ID, conn_type="sqlite", host=":memory:")
        create_connection_without_db(conn)

        self.dag_maker = dag_maker

        with dag_maker(dag_id="sql_deco_dag") as dag:
            ...

        self.dag = dag

    def execute_task(self, task):
        session = self.dag_maker.session
        dag_run = self.dag_maker.create_dagrun(run_id=f"sql_deco_test_{DEFAULT_DATE.date()}", session=session)
        ti = dag_run.get_task_instance(task.operator.task_id, session=session)
        return_val = task.operator.execute(context={"ti": ti})

        return ti, return_val

    def test_sql_decorator_init(self):
        """Test the initialization of the @task.sql decorator."""
        with self.dag_maker:

            @task.sql(conn_id=CONN_ID)
            def sql(): ...

            sql_task = sql()

        assert sql_task.operator.task_id == "sql"
        assert sql_task.operator.conn_id == CONN_ID

    def test_sql_query(self):
        """Test the value returned by function matches the .sql parameter."""
        with self.dag_maker:

            @task.sql(conn_id=CONN_ID)
            def sql():
                return "SELECT 1"

            sql_task = sql()

        assert sql_task.operator.sql == SET_DURING_EXECUTION

        ti, return_value = self.execute_task(sql_task)

        print(f"sql_task.operator.sql: {sql_task.operator.sql}")

        assert sql_task.operator.sql == "SELECT 1"

    def test_sql_query_return(self):
        """Test the value returned when executing the task."""
        with self.dag_maker:

            @task.sql(conn_id=CONN_ID)
            def sql():
                return "SELECT 1"

            sql_task = sql()

        _, return_value = self.execute_task(sql_task)

        assert isinstance(return_value, list)
        assert len(return_value) == 1
        assert return_value[0] == (1,)
