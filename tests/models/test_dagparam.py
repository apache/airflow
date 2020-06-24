from airflow.models.dag import DAG
from airflow.utils.types import DagRunType
from airflow.models.dagparam import dag
from airflow.operators.python import (
    PythonOperator
)
from tests.test_utils.config import conf_vars
from datetime import datetime, timedelta, timezone

DEFAULT_ARGS = {
    "owner": "test",
    "depends_on_past": True,
    "start_date": datetime.today(),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}
DEFAULT_DATE = timezone.datetime(2016, 1, 1)

VALUE = 42

class TestDagParamRuntime:
    def test_xcom_pass_to_op(self):
        with DAG(dag_id="test_xcom_pass_to_op", default_args=DEFAULT_ARGS) as dag:
            value = dag.param('value', default=VALUE)
            operator = PythonOperator(
                python_callable=id,
                op_args=[value],
                task_id="return_value_1",
                do_xcom_push=True,
            )
        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING
        )

        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() == VALUE


class TestDagDecorator:
    @conf_vars({("core", "executor"): "DebugExecutor"})
    def test_xcom_pass_to_op(self):

        @dag(default_args=DEFAULT_ARGS)
        def test_pipeline(some_param, other_param=VALUE):
            operator = PythonOperator(
                python_callable=id,
                op_args=[some_param],
                task_id="some_param"
            )

            other_param = PythonOperator(
                python_callable=id,
                op_args=[some_param],
                task_id="some_param"
            )

        d = test_pipeline(VALUE)

        d.run()
