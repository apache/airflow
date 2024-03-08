from typing import Any
from unittest.mock import MagicMock, patch

import pendulum
import pytest
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import DagRun, Operator, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import task
from airflow.utils.context import Context
from airflow.utils.state import DagRunState
from tests.test_utils.config import conf_vars


class HelloWorldOperator(BaseOperator):
    called = False

    def execute(self, context: Context) -> Any:
        HelloWorldOperator.called = True
        return f"Hello {self.owner}!"


class IterableSession(Session):
    def __next__(self):
        pass


@pytest.fixture
def mock_task_instance_methods(mocker):
    mocker.patch.object(TaskInstance, "get_task_instance", return_value=None)
    mocker.patch.object(TaskInstance, "get_template_context", return_value={"params": {}})
    mocker.patch.object(TaskInstance, "render_templates", return_value=None)
    mocker.patch.object(TaskInstance, "clear_xcom_data", return_value=None)
    mocker.patch.object(TaskInstance, "check_and_change_state_before_execution", return_value=True)


class TestBaseOperatorMeta:
    def setup_method(self):
        HelloWorldOperator.called = False

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
        with pytest.raises(AirflowException, match="Method execute cannot be called from inner!"):
            dag = DAG(dag_id="hello_world")
            context = MagicMock(spec=Context)

            with conf_vars({("core", "unit_test_mode"): "False"}):
                HelloWorldOperator(task_id="task_id", retries=0, dag=dag).execute(context=context)

    @patch("sqlalchemy.orm.Session.__init__")
    def test_executor_when_called_from_decorated_task(
        self, mock_session: MagicMock, mock_task_instance_methods
    ):
        session = MagicMock(spec=IterableSession)
        mock_session.return_value = session
        session.__iter__.return_value = iter({})

        dag = DAG(dag_id="hello_world")
        context = MagicMock(spec=Context)
        operator = HelloWorldOperator(task_id="hello_operator", retries=0, dag=dag)

        @task(task_id="task_id", dag=dag)
        def say_hello(**context):
            return operator.execute(context=context)

        assert not operator.called

        task_instance = self.create_task_instance(operator=say_hello(context=context).operator)

        with conf_vars({("core", "unit_test_mode"): "False"}):
            task_instance.run(test_mode=True, session=session)

        assert not operator.called

    @patch("sqlalchemy.orm.Session.__init__")
    def test_executor_when_called_from_task_instance(
        self, mock_session: MagicMock, mock_task_instance_methods
    ):
        session = MagicMock(spec=IterableSession)
        mock_session.return_value = session
        session.__iter__.return_value = iter({})

        dag = DAG(dag_id="hello_world")
        operator = HelloWorldOperator(task_id="hello_operator", retries=0, dag=dag)

        assert not operator.called

        task_instance = self.create_task_instance(operator=operator)

        with conf_vars({("core", "unit_test_mode"): "False"}):
            task_instance.run(test_mode=True, session=session)

        assert operator.called
