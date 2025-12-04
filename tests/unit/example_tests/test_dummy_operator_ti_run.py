from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import TaskInstanceState
from tests.test_utils.operators.dummy_operator import DummySuccessOperator


def test_dummy_operator_with_ti_run():
    with DAG(
        dag_id="test_dummy_operator_ti_run",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        schedule=None,
    ) as dag:
        task = DummySuccessOperator(task_id="dummy_task")

    ti = TaskInstance(task=task, run_id="test_run")
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS
