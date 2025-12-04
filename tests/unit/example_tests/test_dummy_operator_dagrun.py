
from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.utils.state import TaskInstanceState

from tests.test_utils.operators.dummy_operator import DummySuccessOperator


def test_dummy_operator_with_dagrun():
    with DAG(
        dag_id="test_dummy_operator_dagrun",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=None,
    ) as dag:
        DummySuccessOperator(task_id="dummy_task")

    dagrun = dag.create_dagrun(
        run_id="test_run",
        execution_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        state=TaskInstanceState.RUNNING,
    )

    ti = dagrun.get_task_instance(task_id="dummy_task")
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids="dummy_task") == {"ok": True}
