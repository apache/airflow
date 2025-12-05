Testing Operators with pytest
-----------------------------

The ``dag.test()`` example shown in older documentation works only when executed
inside a DAG file, but often does *not* work inside pytest because DAG
serialization is inactive in standalone test files.

Below are two recommended, runnable patterns for unit-testing custom operators
with pytest. Both examples work with Airflow 3.x.

1. Using ``TaskInstance.run()``
2. Using ``dag.create_dagrun()``


Example: DummySuccessOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This small operator is used only for demonstrating how to test operators.

.. code-block:: python

    from __future__ import annotations

    from airflow.models.baseoperator import BaseOperator


    class DummySuccessOperator(BaseOperator):
        """Very small operator used only for unit test examples."""

        def execute(self, context):
            return {"ok": True}


Example 1: Testing using ``TaskInstance.run()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This pattern constructs a DAG, creates a ``TaskInstance`` manually,
and runs it directly.

.. code-block:: python

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
        assert ti.xcom_pull(task_ids="dummy_task") == {"ok": True}


Example 2: Testing using ``dag.create_dagrun()``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This pattern creates a full DAG run and then runs the TaskInstance
associated with that DAG run.

.. code-block:: python

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
            task = DummySuccessOperator(task_id="dummy_task")

        dagrun = dag.create_dagrun(
            run_id="test_run",
            state="success",
            execution_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
            start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
            data_interval=(pendulum.datetime(2024, 1, 1, tz="UTC"),
                           pendulum.datetime(2024, 1, 1, tz="UTC")),
            logical_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        )

        ti = dagrun.get_task_instance("dummy_task")
        ti.run(ignore_ti_state=True)

        assert ti.state == TaskInstanceState.SUCCESS
        assert ti.xcom_pull(task_ids="dummy_task") == {"ok": True}


Notes
~~~~~

* ``dag.test()`` should not be used inside pytest for Airflow 3.x because
  DAG serialization is inactive.
* Both examples above run completely inside pytest without requiring
  any DAG serialization or scheduler.
* Use mocking for external services if needed.
