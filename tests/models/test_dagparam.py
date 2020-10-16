import unittest
from datetime import datetime, timedelta

from airflow.jobs.scheduler_job import TI
from airflow.models import DagRun
from airflow.models.dag import DAG
from airflow.models.dagparam import dag as dag_decorator
from airflow.operators.python import (
    PythonOperator,
    task)
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": timezone.utcnow(),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
DEFAULT_DATE = timezone.datetime(2016, 1, 1)

VALUE = 42


class TestDagParamRuntime(unittest.TestCase):

    def tearDown(self):
        super().tearDown()
        clear_db_runs()

    def test_xcom_pass_to_op(self):
        with DAG(dag_id="test_xcom_pass_to_op", default_args=DEFAULT_ARGS) as dag:
            value = dag.param('value', default=VALUE)
            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        xcom_arg.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == VALUE


class TestDagDecorator:
    @conf_vars({("core", "executor"): "DebugExecutor"})
    def test_xcom_pass_to_op(self):

        @dag_decorator(default_args=DEFAULT_ARGS)
        def test_pipeline(some_param, other_param=VALUE):

            @task
            def some_task(param):
                return param

            @task
            def another_task(param):
                return param

            some_task(some_param)
            another_task(other_param)

        d = test_pipeline(VALUE)

        d.run()
