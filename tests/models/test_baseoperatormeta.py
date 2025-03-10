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
import threading
from threading import local
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from airflow.decorators import task
from airflow.exceptions import AirflowException, AirflowRescheduleException, AirflowSkipException
from airflow.models.baseoperator import BaseOperator, ExecutorSafeguard, chain
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState, State

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


class HelloWorldOperator(BaseOperator):
    def execute(self, context: Context) -> Any:
        return f"Hello {self.owner}!"


class ExtendedHelloWorldOperator(HelloWorldOperator):
    def execute(self, context: Context) -> Any:
        return super().execute(context)


class Dummy:
    pass


class DeeplyExtendedHelloWorldOperator(ExtendedHelloWorldOperator, Dummy):
    def execute(self, context: Context) -> Any:
        return super().execute(context)


class UnusedOperator(BaseOperator):
    def execute(self, context: Context) -> Any:
        return f"Hello {self.owner}!"


class ExtendedAndUsedOperator(UnusedOperator):
    def execute(self, context: Context) -> Any:
        return super().execute(context)


class TestExecutorSafeguard:
    def setup_method(self):
        self._test_mode_before = ExecutorSafeguard.test_mode
        self._sentinel_before = ExecutorSafeguard._sentinel
        self._sentinel_callers_before = ExecutorSafeguard._sentinel.callers
        ExecutorSafeguard.test_mode = False
        ExecutorSafeguard._sentinel = local()
        ExecutorSafeguard._sentinel.callers = {}

    def teardown_method(self, method):
        ExecutorSafeguard.test_mode = self._test_mode_before
        ExecutorSafeguard._sentinel = self._sentinel_before
        ExecutorSafeguard._sentinel.callers = self._sentinel_callers_before

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "operator", [ExtendedHelloWorldOperator, ExtendedAndUsedOperator, HelloWorldOperator]
    )
    def test_executor_when_classic_operator_called_from_dag(self, dag_maker, operator):
        operator.log = MagicMock()
        with dag_maker() as dag:
            operator(task_id="hello_operator")

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        operator.log.warning.assert_not_called()

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

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "operator_class", [ExtendedHelloWorldOperator, ExtendedAndUsedOperator, HelloWorldOperator]
    )
    def test_executor_when_classic_operator_called_from_decorated_task_with_allow_nested_operators_false(
        self, dag_maker, operator_class
    ):
        with dag_maker() as dag:

            @task(task_id="task_id", dag=dag)
            def say_hello(**context):
                operator = operator_class(task_id="hello_operator", allow_nested_operators=False)
                return operator.execute(context=context)

            say_hello()

        dag_run = dag.test()
        assert dag_run.state == DagRunState.FAILED

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "operator_class", [ExtendedHelloWorldOperator, ExtendedAndUsedOperator, HelloWorldOperator]
    )
    def test_executor_when_classic_operator_called_from_decorated_task_without_allow_nested_operators(
        self,
        dag_maker,
        operator_class,
    ):
        operator_class.log = MagicMock()

        with dag_maker() as dag:

            @task(task_id="task_id", dag=dag)
            def say_hello(**context):
                operator = operator_class(task_id="hello_operator")
                return operator.execute(context=context)

            say_hello()

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        name = operator_class.__name__
        operator_class.log.warning.assert_called_once_with(
            f"{name}.execute cannot be called outside TaskInstance!"
        )

    @pytest.mark.db_test
    def test_executor_when_classic_operator_called_from_decorated_task_and_classic(
        self,
        dag_maker,
    ):
        """
        Test that the combination of wrong and correct usage of operators leads to the
        correct warning messages count.
        """
        hello_world_log = MagicMock()
        extended_hello_world_log = MagicMock()
        unused_log = MagicMock()
        extended_and_used_log = MagicMock()
        HelloWorldOperator.log = hello_world_log  # type: ignore
        ExtendedHelloWorldOperator.log = extended_hello_world_log  # type: ignore
        UnusedOperator.log = unused_log  # type: ignore
        ExtendedAndUsedOperator.log = extended_and_used_log  # type: ignore

        with dag_maker() as dag:

            @task(task_id="hello_from_task", dag=dag)
            def hello_from_task(**context):
                operator = HelloWorldOperator(task_id="hello_from_task")
                return operator.execute(context=context)

            @task(task_id="extended_hello_from_task", dag=dag)
            def extended_hello_from_task(**context):
                operator = ExtendedHelloWorldOperator(task_id="extended_hello_from_task")
                return operator.execute(context=context)

            @task(task_id="extended_and_used_from_task", dag=dag)
            def extended_and_used_from_task(**context):
                operator = ExtendedAndUsedOperator(task_id="extended_and_used_from_task")
                return operator.execute(context=context)

            hello = hello_from_task()
            extended_hello = extended_hello_from_task()
            extended_and_used_hello = extended_and_used_from_task()

            hello_op = HelloWorldOperator(task_id="hello_from_dag")
            extended_hello_op = ExtendedHelloWorldOperator(task_id="extended_hello_from_dag")
            extended_and_used_op = ExtendedAndUsedOperator(task_id="extended_and_used_from_dag")

            chain(
                hello,
                hello_op,
                extended_hello,
                extended_hello_op,
                extended_and_used_hello,
                extended_and_used_op,
            )

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS

        msg = "HelloWorldOperator.execute cannot be called outside TaskInstance!"
        hello_world_log.warning.assert_called_once_with(msg)

        msg = "ExtendedHelloWorldOperator.execute cannot be called outside TaskInstance!"
        extended_hello_world_log.warning.assert_called_once_with(msg)

        msg = "ExtendedAndUsedOperator.execute cannot be called outside TaskInstance!"
        extended_and_used_log.warning.assert_called_once_with(msg)

        assert unused_log.warning.call_count == 0

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

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "operator_class", [ExtendedHelloWorldOperator, ExtendedAndUsedOperator, HelloWorldOperator]
    )
    def test_executor_when_classic_operator_called_from_python_operator_without_allow_nested_operators(
        self,
        dag_maker,
        operator_class,
    ):
        operator_class.log = MagicMock()
        with dag_maker() as dag:

            def say_hello(**context):
                operator = operator_class(task_id="hello_operator")
                return operator.execute(context=context)

            PythonOperator(
                task_id="say_hello",
                dag=dag,
                python_callable=say_hello,
            )

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        name = operator_class.__name__
        operator_class.log.warning.assert_called_once_with(
            f"{name}.execute cannot be called outside TaskInstance!"
        )

    @pytest.mark.db_test
    def test_safeguard_sentinel_callers_propagation(self, dag_maker):
        """Test that only original caller is remaining in the sentinel callers"""
        DeeplyExtendedHelloWorldOperator.log = MagicMock()  # type: ignore
        with dag_maker() as dag:
            DeeplyExtendedHelloWorldOperator(task_id="hello_operator")

        dag_run = dag.test()
        assert dag_run.state == DagRunState.SUCCESS
        DeeplyExtendedHelloWorldOperator.log.warning.assert_not_called()

        remaining_callers = list(ExecutorSafeguard._sentinel.callers)
        expected_callers = ["DeeplyExtendedHelloWorldOperator__sentinel"]
        assert remaining_callers == expected_callers

    def test_thread_local_executor_safeguard(self):
        class TestExecutorSafeguardThread(threading.Thread):
            def __init__(self):
                threading.Thread.__init__(self)
                self.executor_safeguard = ExecutorSafeguard()

            def run(self):
                class Wrapper:
                    def wrapper_test_func(self, *args, **kwargs):
                        print("test")

                wrap_func = self.executor_safeguard.decorator(Wrapper.wrapper_test_func)
                wrap_func(Wrapper(), Wrapper__sentinel="abc")

        # Test thread local caller value is set properly
        TestExecutorSafeguardThread().start()
