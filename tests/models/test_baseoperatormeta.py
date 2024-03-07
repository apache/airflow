from typing import Any
from unittest.mock import Mock, patch, MagicMock

import pendulum
from pytest import raises
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance, DagRun
from airflow.models.baseoperator import BaseOperator
from airflow.operators.python import PythonOperator, task
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


class TestBaseOperatorMeta:
    @conf_vars({("core", "unit_test_mode"): "False"})
    def test_executor_when_called_directly(self):
        with raises(AirflowException, match="Method execute cannot be called from inner!"):
            dag = DAG(dag_id="hello_world")
            context = MagicMock(spec=Context)

            HelloWorldOperator(task_id="task_id", retries=0, dag=dag).execute(context=context)

    @patch("sqlalchemy.orm.Session.__init__")
    @conf_vars({("core", "unit_test_mode"): "False"})
    def test_executor_when_called_from_decorated_task(self, mock_session: MagicMock):
        session = MagicMock(spec=IterableSession)
        mock_session.return_value = session
        session.__iter__.return_value = iter({})

        dag = DAG(dag_id="hello_world")
        context = {"params": {}}
        TaskInstance.get_task_instance = Mock(return_value=None)
        TaskInstance.get_template_context = Mock(return_value=context)
        TaskInstance.render_templates = Mock(return_value=None)
        TaskInstance.clear_xcom_data = Mock(return_value=None)
        TaskInstance.get_task_instance = Mock(return_value=None)

        operator = HelloWorldOperator(task_id="hello_operator", retries=0, dag=dag)

        @task(task_id="task_id", dag=dag)
        def say_hello(**context):
            return operator.execute(context=context)

        assert not operator.called

        task_instance = TaskInstance(task=say_hello(context=context).operator, run_id="run_id")
        task_instance.task_id = "hello_operator"
        task_instance.dag_id = "hello_world"
        task_instance.dag_run = DagRun(run_id="run_id", dag_id="hello_world",
                                       execution_date=pendulum.now(),
                                       state=DagRunState.RUNNING)
        task_instance._run_raw_task(test_mode=True, session=session)

        assert not operator.called

    @patch("sqlalchemy.orm.Session.__init__")
    @conf_vars({("core", "unit_test_mode"): "False"})
    def test_executor_when_called_from_task_instance(self, mock_session: MagicMock):
        session = MagicMock(spec=IterableSession)
        mock_session.return_value = session
        session.__iter__.return_value = iter({})

        dag = DAG(dag_id="hello_world")
        context = {"params": {}}
        TaskInstance.get_task_instance = Mock(return_value=None)
        TaskInstance.get_template_context = Mock(return_value=context)
        TaskInstance.render_templates = Mock(return_value=None)
        TaskInstance.clear_xcom_data = Mock(return_value=None)
        TaskInstance.get_task_instance = Mock(return_value=None)

        operator = HelloWorldOperator(task_id="hello_operator", retries=0, dag=dag)

        assert not operator.called

        task_instance = TaskInstance(task=operator, run_id="run_id")
        task_instance.task_id = "hello_operator"
        task_instance.dag_id = "hello_world"
        task_instance.dag_run = DagRun(run_id="run_id", dag_id="hello_world", execution_date=pendulum.now(),
                                       state=DagRunState.RUNNING)
        task_instance._run_raw_task(test_mode=True, session=session)

        assert operator.called
