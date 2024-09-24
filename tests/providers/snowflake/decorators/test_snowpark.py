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

from typing import TYPE_CHECKING
from unittest import mock

from airflow.decorators import task
from airflow.utils import timezone

if TYPE_CHECKING:
    from snowflake.snowpark import Session

DEFAULT_DATE = timezone.datetime(2024, 9, 1)
TEST_DAG_ID = "test_snowpark_decorator"
TASK_ID = "snowpark_task"
CONN_ID = "snowflake_default"


class TestSnowparkDecorator:
    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_decorator_no_param(self, mock_snowflake_hook, dag_maker):
        number = 11

        @task.snowpark(
            task_id=f"{TASK_ID}_1",
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
        )
        def func1(session: Session):
            assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value
            return number

        @task.snowpark(
            task_id=f"{TASK_ID}_2",
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
        )
        def func2():
            return number

        with dag_maker(dag_id=TEST_DAG_ID):
            rets = [func1(), func2()]

        dr = dag_maker.create_dagrun()
        for ret in rets:
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in dr.get_task_instances():
            assert ti.xcom_pull() == number
        assert mock_snowflake_hook.call_count == 2
        assert mock_snowflake_hook.return_value.get_snowpark_session.call_count == 2

    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_decorator_with_param(self, mock_snowflake_hook, dag_maker):
        number = 11

        @task.snowpark(
            task_id=f"{TASK_ID}_1",
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
        )
        def func1(session: Session, number: int):
            assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value
            return number

        @task.snowpark(
            task_id=f"{TASK_ID}_2",
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
        )
        def func2(number: int, session: Session):
            assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value
            return number

        @task.snowpark(
            task_id=f"{TASK_ID}_3",
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
        )
        def func3(number: int):
            return number

        with dag_maker(dag_id=TEST_DAG_ID):
            rets = [func1(number=number), func2(number=number), func3(number=number)]

        dr = dag_maker.create_dagrun()
        for ret in rets:
            ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in dr.get_task_instances():
            assert ti.xcom_pull() == number
        assert mock_snowflake_hook.call_count == 3
        assert mock_snowflake_hook.return_value.get_snowpark_session.call_count == 3

    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_decorator_no_return(self, mock_snowflake_hook, dag_maker):
        @task.snowpark(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
        )
        def func(session: Session):
            assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value

        with dag_maker(dag_id=TEST_DAG_ID):
            ret = func()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in dr.get_task_instances():
            assert ti.xcom_pull() is None
        mock_snowflake_hook.assert_called_once()
        mock_snowflake_hook.return_value.get_snowpark_session.assert_called_once()

    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_decorator_multiple_output(self, mock_snowflake_hook, dag_maker):
        @task.snowpark(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
            multiple_outputs=True,
        )
        def func(session: Session):
            assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value
            return {"a": 1, "b": "2"}

        with dag_maker(dag_id=TEST_DAG_ID):
            ret = func()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(key="a") == 1
        assert ti.xcom_pull(key="b") == "2"
        assert ti.xcom_pull() == {"a": 1, "b": "2"}
        mock_snowflake_hook.assert_called_once()
        mock_snowflake_hook.return_value.get_snowpark_session.assert_called_once()

    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_decorator_session_tag(self, mock_snowflake_hook, dag_maker):
        mock_session = mock_snowflake_hook.return_value.get_snowpark_session.return_value
        mock_session.query_tag = {}

        # Mock the update_query_tag function to combine with another dict
        def update_query_tag(new_tags):
            mock_session.query_tag.update(new_tags)

        mock_session.update_query_tag = mock.Mock(side_effect=update_query_tag)

        @task.snowpark(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            role="test_role",
            authenticator="externalbrowser",
        )
        def func(session: Session):
            return session.query_tag

        with dag_maker(dag_id=TEST_DAG_ID):
            ret = func()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        query_tag = ti.xcom_pull()
        assert query_tag == {
            "dag_id": TEST_DAG_ID,
            "dag_run_id": dr.run_id,
            "task_id": TASK_ID,
            "operator": "_SnowparkDecoratedOperator",
        }
