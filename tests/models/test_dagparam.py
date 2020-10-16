import unittest
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import (
    task)
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs


class TestDagParamRuntime(unittest.TestCase):
    DEFAULT_ARGS = {
        "owner": "test",
        "depends_on_past": True,
        "start_date": timezone.utcnow(),
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }
    VALUE = 42
    DEFAULT_DATE = timezone.datetime(2016, 1, 1)

    def tearDown(self):
        super().tearDown()
        clear_db_runs()

    def test_xcom_pass_to_op(self):
        with DAG(dag_id="test_xcom_pass_to_op", default_args=self.DEFAULT_ARGS) as dag:
            value = dag.param('value', default=self.VALUE)

            @task
            def return_num(num):
                return num

            xcom_arg = return_num(value)

        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=self.DEFAULT_DATE,
            state=State.RUNNING
        )

        xcom_arg.operator.run(start_date=self.DEFAULT_DATE, end_date=self.DEFAULT_DATE)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == self.VALUE
