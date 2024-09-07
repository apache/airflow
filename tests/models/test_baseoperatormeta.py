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

import datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from airflow.configuration import conf
from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowRescheduleException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator, ExecutorSafeguard
from airflow.providers.standard.core.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState, State

if TYPE_CHECKING:
    from airflow.utils.context import Context


class HelloWorldOperator(BaseOperator):
    def execute(self, context: Context) -> Any:
        return f"Hello {self.owner}!"


class ExtendedHelloWorldOperator(HelloWorldOperator):
    def execute(self, context: Context) -> Any:
        return super().execute(context)


class TestExecutorSafeguard:
    def setup_method(self):
        ExecutorSafeguard.test_mode = False

    def teardown_method(self, method):
        ExecutorSafeguard.test_mode = conf.getboolean("core", "unit_test_mode")

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.db_test
    @patch.object(HelloWorldOperator, "log")
    def test_executor_when_classic_operator_called_from_dag(self, mock_log, dag_maker):
        with dag_maker() as dag:
            HelloWorldOperator(task_id="hello_operator")

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        mock_log.warning.assert_not_called()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.db_test
    @patch.object(HelloWorldOperator, "log")
    def test_executor_when_extended_classic_operator_called_from_dag(
        self,
        mock_log,
        dag_maker,
    ):
        with dag_maker() as dag:
            ExtendedHelloWorldOperator(task_id="hello_operator")

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        mock_log.warning.assert_not_called()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.parametrize(
        "state, exception, retries",
        [
            (State.FAILED, AirflowException, 0),
            (State.SKIPPED, AirflowSkipException, 0),
            (State.SUCCESS, None, 0),
            (State.UP_FOR_RESCHEDULE, AirflowRescheduleException(timezone.utcnow()), 0),
            (State.UP_FOR_RETRY, AirflowException, 1),
        ],
    )
    @pytest.mark.db_test
    def test_executor_when_python_operator_raises_exception_called_from_dag(
        self, session, dag_maker, state, exception, retries
    ):
        with dag_maker():

            def _raise_if_exception():
                if exception:
                    raise exception

            task = PythonOperator(
                task_id="hello_operator",
                python_callable=_raise_if_exception,
                retries=retries,
                retry_delay=datetime.timedelta(seconds=2),
            )

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.next_method = "execute"
        ti.next_kwargs = {}
        session.merge(ti)
        session.commit()

        ti.task = task
        if state in [State.FAILED, State.UP_FOR_RETRY]:
            with pytest.raises(exception):
                ti.run()
        else:
            ti.run()
        ti.refresh_from_db()

        assert ti.next_method is None
        assert ti.next_kwargs is None
        assert ti.state == state

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.db_test
    def test_executor_when_classic_operator_called_from_decorated_task_with_allow_nested_operators_false(
        self, dag_maker
    ):
        with dag_maker() as dag:

            @task(task_id="task_id", dag=dag)
            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator", allow_nested_operators=False)
                return operator.execute(context=context)

            say_hello()

        dag_run = dag.test()
        assert dag_run.state == DagRunState.FAILED

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.db_test
    @patch.object(HelloWorldOperator, "log")
    def test_executor_when_classic_operator_called_from_decorated_task_without_allow_nested_operators(
        self,
        mock_log,
        dag_maker,
    ):
        with dag_maker() as dag:

            @task(task_id="task_id", dag=dag)
            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator")
                return operator.execute(context=context)

            say_hello()

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        mock_log.warning.assert_called_once_with(
            "HelloWorldOperator.execute cannot be called outside TaskInstance!"
        )

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.db_test
    def test_executor_when_classic_operator_called_from_python_operator_with_allow_nested_operators_false(
        self,
        dag_maker,
    ):
        with dag_maker() as dag:

            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator", allow_nested_operators=False)
                return operator.execute(context=context)

            PythonOperator(
                task_id="say_hello",
                dag=dag,
                python_callable=say_hello,
            )

        dag_run = dag.test()
        assert dag_run.state == DagRunState.FAILED

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.db_test
    @patch.object(HelloWorldOperator, "log")
    def test_executor_when_classic_operator_called_from_python_operator_without_allow_nested_operators(
        self,
        mock_log,
        dag_maker,
    ):
        with dag_maker() as dag:

            def say_hello(**context):
                operator = HelloWorldOperator(task_id="hello_operator")
                return operator.execute(context=context)

            PythonOperator(
                task_id="say_hello",
                dag=dag,
                python_callable=say_hello,
            )

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        mock_log.warning.assert_called_once_with(
            "HelloWorldOperator.execute cannot be called outside TaskInstance!"
        )
