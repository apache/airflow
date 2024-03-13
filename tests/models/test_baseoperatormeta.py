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

from typing import Any
from unittest.mock import MagicMock, patch

import pendulum
import pytest
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import DagRun, Operator, TaskInstance
from airflow.models.baseoperator import BaseOperator, ExecutorSafeguard
from airflow.operators.python import task
from airflow.utils.context import Context
from airflow.utils.state import DagRunState


class HelloWorldOperator(BaseOperator):
    called = False

    def execute(self, context: Context) -> Any:
        HelloWorldOperator.called = True
        return f"Hello {self.owner}!"


class IterableSession(Session):
    def __next__(self):
        pass


@pytest.fixture
def mock_task_instance(mocker):
    email = "dev@airflow.apache.org"
    task = MagicMock(spec=BaseOperator)
    task.iter_mapped_dependants = iter([])
    task.email = email
    context = {
        "params": {},
        "task": task,
        "email_for_state": lambda email: email,
        "callbacks": {},
        "context": {},
    }
    mocker.patch.object(TaskInstance, "get_task_instance", return_value=None)
    mocker.patch.object(TaskInstance, "get_template_context", return_value=context)
    mocker.patch.object(TaskInstance, "render_templates", return_value=None)
    mocker.patch.object(TaskInstance, "clear_xcom_data", return_value=None)
    mocker.patch.object(TaskInstance, "check_and_change_state_before_execution", return_value=True)
    mocker.patch.object(TaskInstance, "fetch_handle_failure_context", return_value=context)
    mocker.patch.object(TaskInstance, "email_alert", return_value=None)


@pytest.fixture
def mock_session(mocker):
    session = mocker.patch("sqlalchemy.orm.Session", spec=IterableSession)
    session.__iter__.return_value = iter({})
    return session


class TestExecutorSafeguard:
    def setup_method(self):
        HelloWorldOperator.called = False
        ExecutorSafeguard.test_mode = False

    def teardown_method(self, method):
        ExecutorSafeguard.test_mode = conf.getboolean("core", "unit_test_mode")

    @classmethod
    def create_task_instance(cls, operator: Operator) -> TaskInstance:
        task_instance = TaskInstance(task=operator, run_id="run_id")
        task_instance.task_id = "hello_operator"
        task_instance.dag_id = "hello_world"
        task_instance.dag_run = DagRun(
            run_id="run_id",
            dag_id="hello_world",
            execution_date=pendulum.now(),
            state=DagRunState.RUNNING,
        )
        return task_instance

    def test_executor_when_called_directly(self):
        with pytest.raises(AirflowException, match="HelloWorldOperator.execute cannot be called directly!"):
            dag = DAG(dag_id="hello_world")
            context = MagicMock(spec=Context)

            HelloWorldOperator(task_id="task_id", dag=dag).execute(context=context)

    def test_executor_when_called_directly_but_allow_mixing_is_enabled(self):
        dag = DAG(dag_id="hello_world")
        context = MagicMock(spec=Context)
        operator = HelloWorldOperator(task_id="task_id", dag=dag, allow_mixing=True)

        with patch.object(operator.log, "warning") as mock_warning:
            operator.execute(context=context)

            mock_warning.assert_called_once_with("HelloWorldOperator.execute cannot be called directly!")

    def test_executor_when_called_from_decorated_task(self, mock_session, mock_task_instance):
        dag = DAG(dag_id="hello_world")
        context = MagicMock(spec=Context)
        operator = HelloWorldOperator(task_id="hello_operator", dag=dag)

        @task(task_id="task_id", dag=dag)
        def say_hello(**context):
            return operator.execute(context=context)

        assert not operator.called

        with pytest.raises(
            AirflowException, match="_PythonDecoratedOperator.execute cannot be called from execute!"
        ):
            task_instance = self.create_task_instance(operator=say_hello(context=context).operator)
            task_instance.run(test_mode=True, session=mock_session())

    def test_executor_when_called_from_task_instance(self, mock_session, mock_task_instance):
        dag = DAG(dag_id="hello_world")
        operator = HelloWorldOperator(task_id="hello_operator", dag=dag)

        assert not operator.called

        with patch("airflow.settings.Session", return_value=mock_session), patch(
            "airflow.models.taskinstance._record_task_map_for_downstreams", return_value=None
        ):
            task_instance = self.create_task_instance(operator=operator)
            task_instance.run(test_mode=True, session=mock_session())

            assert operator.called
