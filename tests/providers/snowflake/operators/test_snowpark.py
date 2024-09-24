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

from airflow.providers.snowflake.operators.snowpark import SnowparkOperator
from airflow.utils import timezone

if TYPE_CHECKING:
    from snowflake.snowpark import Session

DEFAULT_DATE = timezone.datetime(2024, 9, 1)
TEST_DAG_ID = "test_snowpark_operator"
TASK_ID = "snowpark_task"
CONN_ID = "snowflake_default"


class TestSnowparkOperator:
    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_operator_no_param(self, mock_snowflake_hook, dag_maker):
        number = 11

        with dag_maker(dag_id=TEST_DAG_ID) as dag:

            def func1(session: Session):
                assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value
                return number

            def func2():
                return number

            operators = [
                SnowparkOperator(
                    task_id=f"{TASK_ID}_{i}",
                    snowflake_conn_id=CONN_ID,
                    python_callable=func,
                    warehouse="test_warehouse",
                    database="test_database",
                    schema="test_schema",
                    role="test_role",
                    authenticator="externalbrowser",
                    dag=dag,
                )
                for i, func in enumerate([func1, func2])
            ]

        dr = dag_maker.create_dagrun()
        for operator in operators:
            operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in dr.get_task_instances():
            assert ti.xcom_pull() == number
        assert mock_snowflake_hook.call_count == 2
        assert mock_snowflake_hook.return_value.get_snowpark_session.call_count == 2

    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_operator_with_param(self, mock_snowflake_hook, dag_maker):
        number = 11

        with dag_maker(dag_id=TEST_DAG_ID) as dag:

            def func1(session: Session, number: int):
                assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value
                return number

            def func2(number: int, session: Session):
                assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value
                return number

            def func3(number: int):
                return number

            operators = [
                SnowparkOperator(
                    task_id=f"{TASK_ID}_{i}",
                    snowflake_conn_id=CONN_ID,
                    python_callable=func,
                    op_kwargs={"number": number},
                    warehouse="test_warehouse",
                    database="test_database",
                    schema="test_schema",
                    role="test_role",
                    authenticator="externalbrowser",
                    dag=dag,
                )
                for i, func in enumerate([func1, func2, func3])
            ]

        dr = dag_maker.create_dagrun()
        for operator in operators:
            operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in dr.get_task_instances():
            assert ti.xcom_pull() == number
        assert mock_snowflake_hook.call_count == 3
        assert mock_snowflake_hook.return_value.get_snowpark_session.call_count == 3

    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_operator_no_return(self, mock_snowflake_hook, dag_maker):
        with dag_maker(dag_id=TEST_DAG_ID) as dag:

            def func(session: Session):
                assert session == mock_snowflake_hook.return_value.get_snowpark_session.return_value

            operator = SnowparkOperator(
                task_id=TASK_ID,
                snowflake_conn_id=CONN_ID,
                python_callable=func,
                warehouse="test_warehouse",
                database="test_database",
                schema="test_schema",
                role="test_role",
                authenticator="externalbrowser",
                dag=dag,
            )

        dr = dag_maker.create_dagrun()
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        for ti in dr.get_task_instances():
            assert ti.xcom_pull() is None
        mock_snowflake_hook.assert_called_once()
        mock_snowflake_hook.return_value.get_snowpark_session.assert_called_once()

    @mock.patch("airflow.providers.snowflake.operators.snowpark.SnowflakeHook")
    def test_snowpark_operator_session_tag(self, mock_snowflake_hook, dag_maker):
        mock_session = mock_snowflake_hook.return_value.get_snowpark_session.return_value
        mock_session.query_tag = {}

        # Mock the update_query_tag function to combine with another dict
        def update_query_tag(new_tags):
            mock_session.query_tag.update(new_tags)

        mock_session.update_query_tag = mock.Mock(side_effect=update_query_tag)

        with dag_maker(dag_id=TEST_DAG_ID) as dag:

            def func(session: Session):
                return session.query_tag

            operator = SnowparkOperator(
                task_id=TASK_ID,
                snowflake_conn_id=CONN_ID,
                python_callable=func,
                warehouse="test_warehouse",
                database="test_database",
                schema="test_schema",
                role="test_role",
                authenticator="externalbrowser",
                dag=dag,
            )

        dr = dag_maker.create_dagrun()
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        query_tag = ti.xcom_pull()
        assert query_tag == {
            "dag_id": TEST_DAG_ID,
            "dag_run_id": dr.run_id,
            "task_id": TASK_ID,
            "operator": "SnowparkOperator",
        }
