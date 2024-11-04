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
from functools import reduce
from typing import TYPE_CHECKING, Mapping
from unittest import mock
from unittest.mock import call

import pendulum
import pytest

from airflow import settings
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.decorators import setup, task, task_group, teardown
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG, DagModel
from airflow.models.dagrun import DagRun, DagRunNote
from airflow.models.taskinstance import TaskInstance, TaskInstanceNote, clear_task_instances
from airflow.models.taskmap import TaskMap
from airflow.models.taskreschedule import TaskReschedule
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.stats import Stats
from airflow.triggers.base import StartTriggerArgs
from airflow.utils import timezone
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType

from tests.models import DEFAULT_DATE as _DEFAULT_DATE
from tests_common.test_utils import db
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.mock_operators import MockOperator

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

TI = TaskInstance
DEFAULT_DATE = pendulum.instance(_DEFAULT_DATE)


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DagBag

    return DagBag(include_examples=True)


class TestDagRun:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self):
        self._clean_db()
        yield
        self._clean_db()

    @staticmethod
    def _clean_db():
        db.clear_db_runs()
        db.clear_db_pools()
        db.clear_db_dags()
        db.clear_db_variables()
        db.clear_db_assets()
        db.clear_db_xcom()

    def create_dag_run(
        self,
        dag: DAG,
        *,
        task_states: Mapping[str, TaskInstanceState] | None = None,
        execution_date: datetime.datetime | None = None,
        is_backfill: bool = False,
        state: DagRunState = DagRunState.RUNNING,
        session: Session,
    ):
        now = timezone.utcnow()
        execution_date = pendulum.instance(execution_date or now)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        if is_backfill:
            run_type = DagRunType.BACKFILL_JOB
            data_interval = dag.infer_automated_data_interval(execution_date)
        else:
            run_type = DagRunType.MANUAL
            data_interval = dag.timetable.infer_manual_data_interval(run_after=execution_date)
        dag_run = dag.create_dagrun(
            run_type=run_type,
            execution_date=execution_date,
            data_interval=data_interval,
            start_date=now,
            state=state,
            external_trigger=False,
            **triggered_by_kwargs,  # type: ignore
        )

        if task_states is not None:
            for task_id, task_state in task_states.items():
                ti = dag_run.get_task_instance(task_id)
                ti.set_state(task_state, session)
            session.flush()

        return dag_run

    @pytest.mark.parametrize("state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_clear_task_instances_for_backfill_unfinished_dagrun(self, state, session):
        now = timezone.utcnow()
        dag_id = "test_clear_task_instances_for_backfill_dagrun"
        dag = DAG(dag_id=dag_id, schedule=datetime.timedelta(days=1), start_date=now)
        dag_run = self.create_dag_run(dag, execution_date=now, is_backfill=True, state=state, session=session)

        task0 = EmptyOperator(task_id="backfill_task_0", owner="test", dag=dag)
        ti0 = TI(task=task0, run_id=dag_run.run_id)
        ti0.run()

        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.commit()
        ti0.refresh_from_db()
        dr0 = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date == now).first()
        assert dr0.state == state
        assert dr0.clear_number < 1

    @pytest.mark.parametrize("state", [DagRunState.SUCCESS, DagRunState.FAILED])
    def test_clear_task_instances_for_backfill_finished_dagrun(self, state, session):
        now = timezone.utcnow()
        dag_id = "test_clear_task_instances_for_backfill_dagrun"
        dag = DAG(dag_id=dag_id, schedule=datetime.timedelta(days=1), start_date=now)
        dag_run = self.create_dag_run(dag, execution_date=now, is_backfill=True, state=state, session=session)

        task0 = EmptyOperator(task_id="backfill_task_0", owner="test", dag=dag)
        ti0 = TI(task=task0, run_id=dag_run.run_id)
        ti0.run()

        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.commit()
        ti0.refresh_from_db()
        dr0 = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date == now).first()
        assert dr0.state == DagRunState.QUEUED
        assert dr0.clear_number == 1

    def test_dagrun_find(self, session):
        now = timezone.utcnow()

        dag_id1 = "test_dagrun_find_externally_triggered"
        dag_run = DagRun(
            dag_id=dag_id1,
            run_id=dag_id1,
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=True,
        )
        session.add(dag_run)

        dag_id2 = "test_dagrun_find_not_externally_triggered"
        dag_run = DagRun(
            dag_id=dag_id2,
            run_id=dag_id2,
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=False,
        )
        session.add(dag_run)

        session.commit()

        assert 1 == len(DagRun.find(dag_id=dag_id1, external_trigger=True))
        assert 1 == len(DagRun.find(run_id=dag_id1))
        assert 2 == len(DagRun.find(run_id=[dag_id1, dag_id2]))
        assert 2 == len(DagRun.find(execution_date=[now, now]))
        assert 2 == len(DagRun.find(execution_date=now))
        assert 0 == len(DagRun.find(dag_id=dag_id1, external_trigger=False))
        assert 0 == len(DagRun.find(dag_id=dag_id2, external_trigger=True))
        assert 1 == len(DagRun.find(dag_id=dag_id2, external_trigger=False))

    def test_dagrun_find_duplicate(self, session):
        now = timezone.utcnow()

        dag_id = "test_dagrun_find_duplicate"
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=dag_id,
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=True,
        )
        session.add(dag_run)

        session.commit()

        assert DagRun.find_duplicate(dag_id=dag_id, run_id=dag_id, execution_date=now) is not None
        assert DagRun.find_duplicate(dag_id=dag_id, run_id=dag_id, execution_date=None) is not None
        assert DagRun.find_duplicate(dag_id=dag_id, run_id=None, execution_date=now) is not None
        assert DagRun.find_duplicate(dag_id=dag_id, run_id=None, execution_date=None) is None

    def test_dagrun_success_when_all_skipped(self, session):
        """
        Tests that a DAG run succeeds when all tasks are skipped
        """
        dag = DAG(
            dag_id="test_dagrun_success_when_all_skipped",
            schedule=datetime.timedelta(days=1),
            start_date=timezone.datetime(2017, 1, 1),
        )
        dag_task1 = ShortCircuitOperator(
            task_id="test_short_circuit_false", dag=dag, python_callable=lambda: False
        )
        dag_task2 = EmptyOperator(task_id="test_state_skipped1", dag=dag)
        dag_task3 = EmptyOperator(task_id="test_state_skipped2", dag=dag)
        dag_task1.set_downstream(dag_task2)
        dag_task2.set_downstream(dag_task3)

        initial_task_states = {
            "test_short_circuit_false": TaskInstanceState.SUCCESS,
            "test_state_skipped1": TaskInstanceState.SKIPPED,
            "test_state_skipped2": TaskInstanceState.SKIPPED,
        }

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        dag_run.update_state()
        assert DagRunState.SUCCESS == dag_run.state

    def test_dagrun_not_stuck_in_running_when_all_tasks_instances_are_removed(self, session):
        """
        Tests that a DAG run succeeds when all tasks are removed
        """
        dag = DAG(
            dag_id="test_dagrun_success_when_all_skipped",
            schedule=datetime.timedelta(days=1),
            start_date=timezone.datetime(2017, 1, 1),
        )
        dag_task1 = ShortCircuitOperator(
            task_id="test_short_circuit_false", dag=dag, python_callable=lambda: False
        )
        dag_task2 = EmptyOperator(task_id="test_state_skipped1", dag=dag)
        dag_task3 = EmptyOperator(task_id="test_state_skipped2", dag=dag)
        dag_task1.set_downstream(dag_task2)
        dag_task2.set_downstream(dag_task3)

        initial_task_states = {
            "test_short_circuit_false": TaskInstanceState.REMOVED,
            "test_state_skipped1": TaskInstanceState.REMOVED,
            "test_state_skipped2": TaskInstanceState.REMOVED,
        }

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        dag_run.update_state()
        assert DagRunState.SUCCESS == dag_run.state

    def test_dagrun_success_conditions(self, session):
        dag = DAG(
            "test_dagrun_success_conditions",
            schedule=datetime.timedelta(days=1),
            start_date=DEFAULT_DATE,
            default_args={"owner": "owner1"},
        )

        # A -> B
        # A -> C -> D
        # ordered: B, D, C, A or D, B, C, A or D, C, B, A
        with dag:
            op1 = EmptyOperator(task_id="A")
            op2 = EmptyOperator(task_id="B")
            op3 = EmptyOperator(task_id="C")
            op4 = EmptyOperator(task_id="D")
            op1.set_upstream([op2, op3])
            op3.set_upstream(op4)

        dag.clear()

        now = pendulum.now("UTC")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id="test_dagrun_success_conditions",
            state=DagRunState.RUNNING,
            execution_date=now,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=now),
            start_date=now,
            **triggered_by_kwargs,
        )

        # op1 = root
        ti_op1 = dr.get_task_instance(task_id=op1.task_id)
        ti_op1.set_state(state=TaskInstanceState.SUCCESS, session=session)

        ti_op2 = dr.get_task_instance(task_id=op2.task_id)
        ti_op3 = dr.get_task_instance(task_id=op3.task_id)
        ti_op4 = dr.get_task_instance(task_id=op4.task_id)

        # root is successful, but unfinished tasks
        dr.update_state()
        assert DagRunState.RUNNING == dr.state

        # one has failed, but root is successful
        ti_op2.set_state(state=TaskInstanceState.FAILED, session=session)
        ti_op3.set_state(state=TaskInstanceState.SUCCESS, session=session)
        ti_op4.set_state(state=TaskInstanceState.SUCCESS, session=session)
        dr.update_state()
        assert DagRunState.SUCCESS == dr.state

    def test_dagrun_deadlock(self, session):
        dag = DAG(
            "text_dagrun_deadlock",
            schedule=datetime.timedelta(days=1),
            start_date=DEFAULT_DATE,
            default_args={"owner": "owner1"},
        )

        with dag:
            op1 = EmptyOperator(task_id="A")
            op2 = EmptyOperator(task_id="B")
            op2.trigger_rule = TriggerRule.ONE_FAILED
            op2.set_upstream(op1)

        dag.clear()
        now = pendulum.now("UTC")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id="test_dagrun_deadlock",
            state=DagRunState.RUNNING,
            execution_date=now,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=now),
            start_date=now,
            session=session,
            **triggered_by_kwargs,
        )

        ti_op1: TI = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti_op2: TI = dr.get_task_instance(task_id=op2.task_id, session=session)
        ti_op1.set_state(state=TaskInstanceState.SUCCESS, session=session)
        ti_op2.set_state(state=None, session=session)

        dr.update_state(session=session)
        assert dr.state == DagRunState.RUNNING

        ti_op2.set_state(state=None, session=session)
        op2.trigger_rule = "invalid"  # type: ignore
        dr.update_state(session=session)
        assert dr.state == DagRunState.FAILED

    def test_dagrun_no_deadlock_with_restarting(self, session):
        dag = DAG(
            "test_dagrun_no_deadlock_with_restarting",
            schedule=datetime.timedelta(days=1),
            start_date=DEFAULT_DATE,
        )
        with dag:
            op1 = EmptyOperator(task_id="upstream_task")
            op2 = EmptyOperator(task_id="downstream_task")
            op2.set_upstream(op1)

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id="test_dagrun_no_deadlock_with_shutdown",
            state=DagRunState.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            start_date=DEFAULT_DATE,
            **triggered_by_kwargs,
        )
        upstream_ti = dr.get_task_instance(task_id="upstream_task")
        upstream_ti.set_state(TaskInstanceState.RESTARTING, session=session)

        dr.update_state()
        assert dr.state == DagRunState.RUNNING

    def test_dagrun_no_deadlock_with_depends_on_past(self, session):
        dag = DAG("test_dagrun_no_deadlock", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE)
        with dag:
            EmptyOperator(task_id="dop", depends_on_past=True)
            EmptyOperator(task_id="tc", max_active_tis_per_dag=1)

        dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id="test_dagrun_no_deadlock_1",
            state=DagRunState.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            start_date=DEFAULT_DATE,
            **triggered_by_kwargs,
        )
        next_date = DEFAULT_DATE + datetime.timedelta(days=1)
        dr2 = dag.create_dagrun(
            run_id="test_dagrun_no_deadlock_2",
            state=DagRunState.RUNNING,
            execution_date=next_date,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=next_date),
            start_date=next_date,
            **triggered_by_kwargs,
        )
        ti1_op1 = dr.get_task_instance(task_id="dop")
        dr2.get_task_instance(task_id="dop")
        ti2_op1 = dr.get_task_instance(task_id="tc")
        dr.get_task_instance(task_id="tc")
        ti1_op1.set_state(state=TaskInstanceState.RUNNING, session=session)
        dr.update_state()
        dr2.update_state()
        assert dr.state == DagRunState.RUNNING
        assert dr2.state == DagRunState.RUNNING

        ti2_op1.set_state(state=TaskInstanceState.RUNNING, session=session)
        dr.update_state()
        dr2.update_state()
        assert dr.state == DagRunState.RUNNING
        assert dr2.state == DagRunState.RUNNING

    def test_dagrun_success_callback(self, session):
        def on_success_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_success_callback"

        dag = DAG(
            dag_id="test_dagrun_success_callback",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_success_callback=on_success_callable,
        )
        dag_task1 = EmptyOperator(task_id="test_state_succeeded1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_state_succeeded2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SUCCESS,
            "test_state_succeeded2": TaskInstanceState.SUCCESS,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        _, callback = dag_run.update_state()
        assert DagRunState.SUCCESS == dag_run.state
        # Callbacks are not added until handle_callback = False is passed to dag_run.update_state()
        assert callback is None

    def test_dagrun_failure_callback(self, session):
        def on_failure_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_failure_callback"

        dag = DAG(
            dag_id="test_dagrun_failure_callback",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_failure_callback=on_failure_callable,
        )
        dag_task1 = EmptyOperator(task_id="test_state_succeeded1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_state_failed2", dag=dag)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SUCCESS,
            "test_state_failed2": TaskInstanceState.FAILED,
        }
        dag_task1.set_downstream(dag_task2)

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        _, callback = dag_run.update_state()
        assert DagRunState.FAILED == dag_run.state
        # Callbacks are not added until handle_callback = False is passed to dag_run.update_state()
        assert callback is None

    def test_on_success_callback_when_task_skipped(self, session):
        mock_on_success = mock.MagicMock()
        mock_on_success.__name__ = "mock_on_success"

        dag = DAG(
            dag_id="test_dagrun_update_state_with_handle_callback_success",
            start_date=datetime.datetime(2017, 1, 1),
            on_success_callback=mock_on_success,
            schedule=datetime.timedelta(days=1),
        )

        _ = EmptyOperator(task_id="test_state_succeeded1", dag=dag)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SKIPPED,
        }

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        _, _ = dag_run.update_state(execute_callbacks=True)
        task = dag_run.get_task_instances()[0]

        assert task.state == TaskInstanceState.SKIPPED
        assert DagRunState.SUCCESS == dag_run.state
        mock_on_success.assert_called_once()

    def test_dagrun_update_state_with_handle_callback_success(self, session):
        def on_success_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_update_state_with_handle_callback_success"

        dag = DAG(
            dag_id="test_dagrun_update_state_with_handle_callback_success",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_success_callback=on_success_callable,
        )
        DAG.bulk_write_to_db(dags=[dag], processor_subdir="/tmp/test", session=session)

        dag_task1 = EmptyOperator(task_id="test_state_succeeded1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_state_succeeded2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SUCCESS,
            "test_state_succeeded2": TaskInstanceState.SUCCESS,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)

        _, callback = dag_run.update_state(execute_callbacks=False)
        assert DagRunState.SUCCESS == dag_run.state
        # Callbacks are not added until handle_callback = False is passed to dag_run.update_state()

        assert callback == DagCallbackRequest(
            full_filepath=dag_run.dag.fileloc,
            dag_id="test_dagrun_update_state_with_handle_callback_success",
            run_id=dag_run.run_id,
            is_failure_callback=False,
            processor_subdir="/tmp/test",
            msg="success",
        )

    def test_dagrun_update_state_with_handle_callback_failure(self, session):
        def on_failure_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_update_state_with_handle_callback_failure"

        dag = DAG(
            dag_id="test_dagrun_update_state_with_handle_callback_failure",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_failure_callback=on_failure_callable,
        )
        DAG.bulk_write_to_db(dags=[dag], processor_subdir="/tmp/test", session=session)

        dag_task1 = EmptyOperator(task_id="test_state_succeeded1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_state_failed2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SUCCESS,
            "test_state_failed2": TaskInstanceState.FAILED,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)

        _, callback = dag_run.update_state(execute_callbacks=False)
        assert DagRunState.FAILED == dag_run.state
        # Callbacks are not added until handle_callback = False is passed to dag_run.update_state()

        assert callback == DagCallbackRequest(
            full_filepath=dag_run.dag.fileloc,
            dag_id="test_dagrun_update_state_with_handle_callback_failure",
            run_id=dag_run.run_id,
            is_failure_callback=True,
            processor_subdir="/tmp/test",
            msg="task_failure",
        )

    def test_dagrun_set_state_end_date(self, session):
        dag = DAG(
            "test_dagrun_set_state_end_date",
            schedule=datetime.timedelta(days=1),
            start_date=DEFAULT_DATE,
            default_args={"owner": "owner1"},
        )

        dag.clear()

        now = pendulum.now("UTC")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id="test_dagrun_set_state_end_date",
            state=DagRunState.RUNNING,
            execution_date=now,
            data_interval=dag.timetable.infer_manual_data_interval(now),
            start_date=now,
            **triggered_by_kwargs,
        )

        # Initial end_date should be NULL
        # DagRunState.SUCCESS and DagRunState.FAILED are all ending state and should set end_date
        # DagRunState.RUNNING set end_date back to NULL
        session.add(dr)
        session.commit()
        assert dr.end_date is None

        dr.set_state(DagRunState.SUCCESS)
        session.merge(dr)
        session.commit()

        dr_database = session.query(DagRun).filter(DagRun.run_id == "test_dagrun_set_state_end_date").one()
        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

        dr.set_state(DagRunState.RUNNING)
        session.merge(dr)
        session.commit()

        dr_database = session.query(DagRun).filter(DagRun.run_id == "test_dagrun_set_state_end_date").one()

        assert dr_database.end_date is None

        dr.set_state(DagRunState.FAILED)
        session.merge(dr)
        session.commit()
        dr_database = session.query(DagRun).filter(DagRun.run_id == "test_dagrun_set_state_end_date").one()

        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

    def test_dagrun_update_state_end_date(self, session):
        dag = DAG(
            "test_dagrun_update_state_end_date",
            schedule=datetime.timedelta(days=1),
            start_date=DEFAULT_DATE,
            default_args={"owner": "owner1"},
        )

        # A -> B
        with dag:
            op1 = EmptyOperator(task_id="A")
            op2 = EmptyOperator(task_id="B")
            op1.set_upstream(op2)

        dag.clear()

        now = pendulum.now("UTC")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_id="test_dagrun_update_state_end_date",
            state=DagRunState.RUNNING,
            execution_date=now,
            data_interval=dag.timetable.infer_manual_data_interval(now),
            start_date=now,
            **triggered_by_kwargs,
        )

        # Initial end_date should be NULL
        # DagRunState.SUCCESS and DagRunState.FAILED are all ending state and should set end_date
        # DagRunState.RUNNING set end_date back to NULL
        session.merge(dr)
        session.commit()
        assert dr.end_date is None

        ti_op1 = dr.get_task_instance(task_id=op1.task_id)
        ti_op1.set_state(state=TaskInstanceState.SUCCESS, session=session)
        ti_op2 = dr.get_task_instance(task_id=op2.task_id)
        ti_op2.set_state(state=TaskInstanceState.SUCCESS, session=session)

        dr.update_state()

        dr_database = session.query(DagRun).filter(DagRun.run_id == "test_dagrun_update_state_end_date").one()
        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

        ti_op1.set_state(state=TaskInstanceState.RUNNING, session=session)
        ti_op2.set_state(state=TaskInstanceState.RUNNING, session=session)
        dr.update_state()

        dr_database = session.query(DagRun).filter(DagRun.run_id == "test_dagrun_update_state_end_date").one()

        assert dr._state == DagRunState.RUNNING
        assert dr.end_date is None
        assert dr_database.end_date is None

        ti_op1.set_state(state=TaskInstanceState.FAILED, session=session)
        ti_op2.set_state(state=TaskInstanceState.FAILED, session=session)
        dr.update_state()

        dr_database = session.query(DagRun).filter(DagRun.run_id == "test_dagrun_update_state_end_date").one()

        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

    def test_get_task_instance_on_empty_dagrun(self, session):
        """
        Make sure that a proper value is returned when a dagrun has no task instances
        """
        dag = DAG(
            dag_id="test_get_task_instance_on_empty_dagrun",
            schedule=datetime.timedelta(days=1),
            start_date=timezone.datetime(2017, 1, 1),
        )
        ShortCircuitOperator(task_id="test_short_circuit_false", dag=dag, python_callable=lambda: False)

        now = timezone.utcnow()

        # Don't use create_dagrun since it will create the task instances too which we
        # don't want
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_id="test_get_task_instance_on_empty_dagrun",
            run_type=DagRunType.MANUAL,
            execution_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
            external_trigger=False,
        )
        session.add(dag_run)
        session.commit()

        ti = dag_run.get_task_instance("test_short_circuit_false")
        assert ti is None

    def test_get_latest_runs(self, session):
        dag = DAG(dag_id="test_latest_runs_1", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE)
        self.create_dag_run(dag, execution_date=timezone.datetime(2015, 1, 1), session=session)
        self.create_dag_run(dag, execution_date=timezone.datetime(2015, 1, 2), session=session)
        dagruns = DagRun.get_latest_runs(session)
        session.close()
        for dagrun in dagruns:
            if dagrun.dag_id == "test_latest_runs_1":
                assert dagrun.execution_date == timezone.datetime(2015, 1, 2)

    def test_removed_task_instances_can_be_restored(self, session):
        def with_all_tasks_removed(dag):
            return DAG(dag_id=dag.dag_id, schedule=datetime.timedelta(days=1), start_date=dag.start_date)

        dag = DAG("test_task_restoration", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE)
        dag.add_task(EmptyOperator(task_id="flaky_task", owner="test"))

        dagrun = self.create_dag_run(dag, session=session)
        flaky_ti = dagrun.get_task_instances()[0]
        assert "flaky_task" == flaky_ti.task_id
        assert flaky_ti.state is None

        dagrun.dag = with_all_tasks_removed(dag)

        dagrun.verify_integrity()
        flaky_ti.refresh_from_db()
        assert flaky_ti.state is None

        dagrun.dag.add_task(EmptyOperator(task_id="flaky_task", owner="test"))

        dagrun.verify_integrity()
        flaky_ti.refresh_from_db()
        assert flaky_ti.state is None

    def test_already_added_task_instances_can_be_ignored(self, session):
        dag = DAG("triggered_dag", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE)
        dag.add_task(EmptyOperator(task_id="first_task", owner="test"))

        dagrun = self.create_dag_run(dag, session=session)
        first_ti = dagrun.get_task_instances()[0]
        assert "first_task" == first_ti.task_id
        assert first_ti.state is None

        # Lets assume that the above TI was added into DB by webserver, but if scheduler
        # is running the same method at the same time it would find 0 TIs for this dag
        # and proceeds further to create TIs. Hence mocking DagRun.get_task_instances
        # method to return an empty list of TIs.
        with mock.patch.object(DagRun, "get_task_instances") as mock_gtis:
            mock_gtis.return_value = []
            dagrun.verify_integrity()
            first_ti.refresh_from_db()
            assert first_ti.state is None

    @pytest.mark.parametrize("state", State.task_states)
    @mock.patch.object(settings, "task_instance_mutation_hook", autospec=True)
    def test_task_instance_mutation_hook(self, mock_hook, session, state):
        def mutate_task_instance(task_instance):
            if task_instance.queue == "queue1":
                task_instance.queue = "queue2"
            else:
                task_instance.queue = "queue1"

        mock_hook.side_effect = mutate_task_instance

        dag = DAG(
            "test_task_instance_mutation_hook",
            schedule=datetime.timedelta(days=1),
            start_date=DEFAULT_DATE,
        )
        dag.add_task(EmptyOperator(task_id="task_to_mutate", owner="test", queue="queue1"))

        dagrun = self.create_dag_run(dag, session=session)
        task = dagrun.get_task_instances()[0]
        task.state = state
        session.merge(task)
        session.commit()
        assert task.queue == "queue2"

        dagrun.verify_integrity()
        task = dagrun.get_task_instances()[0]
        assert task.queue == "queue1"

    @pytest.mark.parametrize(
        "prev_ti_state, is_ti_success",
        [
            (TaskInstanceState.SUCCESS, True),
            (TaskInstanceState.SKIPPED, True),
            (TaskInstanceState.RUNNING, False),
            (TaskInstanceState.FAILED, False),
            (None, False),
        ],
    )
    def test_depends_on_past(self, dagbag, session, prev_ti_state, is_ti_success):
        dag_id = "test_depends_on_past"

        dag = dagbag.get_dag(dag_id)
        task = dag.tasks[0]

        dag_run_1 = self.create_dag_run(
            dag,
            execution_date=timezone.datetime(2016, 1, 1, 0, 0, 0),
            is_backfill=True,
            session=session,
        )
        dag_run_2 = self.create_dag_run(
            dag,
            execution_date=timezone.datetime(2016, 1, 2, 0, 0, 0),
            is_backfill=True,
            session=session,
        )

        prev_ti = TI(task, run_id=dag_run_1.run_id)
        prev_ti.refresh_from_db()
        ti = TI(task, run_id=dag_run_2.run_id)
        ti.refresh_from_db()

        prev_ti.set_state(prev_ti_state)
        ti.set_state(TaskInstanceState.QUEUED)
        ti.run()
        assert (ti.state == TaskInstanceState.SUCCESS) == is_ti_success

    @pytest.mark.parametrize(
        "prev_ti_state, is_ti_success",
        [
            (TaskInstanceState.SUCCESS, True),
            (TaskInstanceState.SKIPPED, True),
            (TaskInstanceState.RUNNING, False),
            (TaskInstanceState.FAILED, False),
            (None, False),
        ],
    )
    def test_wait_for_downstream(self, dagbag, session, prev_ti_state, is_ti_success):
        dag_id = "test_wait_for_downstream"
        dag = dagbag.get_dag(dag_id)
        upstream, downstream = dag.tasks

        # For ti.set_state() to work, the DagRun has to exist,
        # Otherwise ti.previous_ti returns an unpersisted TI
        dag_run_1 = self.create_dag_run(
            dag,
            execution_date=timezone.datetime(2016, 1, 1, 0, 0, 0),
            is_backfill=True,
            session=session,
        )
        dag_run_2 = self.create_dag_run(
            dag,
            execution_date=timezone.datetime(2016, 1, 2, 0, 0, 0),
            is_backfill=True,
            session=session,
        )

        prev_ti_downstream = TI(task=downstream, run_id=dag_run_1.run_id)
        prev_ti_downstream.refresh_from_db()
        ti = TI(task=upstream, run_id=dag_run_2.run_id)
        ti.refresh_from_db()
        prev_ti = ti.get_previous_ti()
        prev_ti.set_state(TaskInstanceState.SUCCESS)
        assert prev_ti.state == TaskInstanceState.SUCCESS

        prev_ti_downstream.set_state(prev_ti_state)
        ti.set_state(TaskInstanceState.QUEUED)
        ti.run()
        assert (ti.state == TaskInstanceState.SUCCESS) == is_ti_success

    @pytest.mark.parametrize("state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_next_dagruns_to_examine_only_unpaused(self, session, state):
        """
        Check that "next_dagruns_to_examine" ignores runs from paused/inactive DAGs
        and gets running/queued dagruns
        """

        dag = DAG(dag_id="test_dags", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE)
        EmptyOperator(task_id="dummy", dag=dag, owner="airflow")

        orm_dag = DagModel(
            dag_id=dag.dag_id,
            has_task_concurrency_limits=False,
            next_dagrun=DEFAULT_DATE,
            next_dagrun_create_after=DEFAULT_DATE + datetime.timedelta(days=1),
            is_active=True,
        )
        session.add(orm_dag)
        session.flush()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=state,
            execution_date=DEFAULT_DATE,
            data_interval=dag.infer_automated_data_interval(DEFAULT_DATE),
            start_date=DEFAULT_DATE if state == DagRunState.RUNNING else None,
            session=session,
            **triggered_by_kwargs,
        )

        if state == DagRunState.RUNNING:
            func = DagRun.get_running_dag_runs_to_examine
        else:
            func = DagRun.get_queued_dag_runs_to_set_running
        runs = func(session).all()

        assert runs == [dr]

        orm_dag.is_paused = True
        session.flush()

        runs = func(session).all()
        assert runs == []

    @mock.patch.object(Stats, "timing")
    def test_no_scheduling_delay_for_nonscheduled_runs(self, stats_mock, session):
        """
        Tests that dag scheduling delay stat is not called if the dagrun is not a scheduled run.
        This case is manual run. Simple test for coherence check.
        """
        dag = DAG(dag_id="test_dagrun_stats", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE)
        dag_task = EmptyOperator(task_id="dummy", dag=dag)

        initial_task_states = {
            dag_task.task_id: TaskInstanceState.SUCCESS,
        }

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        dag_run.update_state()
        assert call(f"dagrun.{dag.dag_id}.first_task_scheduling_delay") not in stats_mock.mock_calls

    @pytest.mark.parametrize(
        "schedule, expected",
        [
            ("*/5 * * * *", True),
            (None, False),
            ("@once", False),
        ],
    )
    def test_emit_scheduling_delay(self, session, schedule, expected):
        """
        Tests that dag scheduling delay stat is set properly once running scheduled dag.
        dag_run.update_state() invokes the _emit_true_scheduling_delay_stats_for_finished_state method.
        """
        dag = DAG(dag_id="test_emit_dag_stats", start_date=DEFAULT_DATE, schedule=schedule)
        dag_task = EmptyOperator(task_id="dummy", dag=dag, owner="airflow")
        expected_stat_tags = {"dag_id": f"{dag.dag_id}", "run_type": DagRunType.SCHEDULED}

        try:
            info = dag.next_dagrun_info(None)
            orm_dag_kwargs = {"dag_id": dag.dag_id, "has_task_concurrency_limits": False, "is_active": True}
            if info is not None:
                orm_dag_kwargs.update(
                    {
                        "next_dagrun": info.logical_date,
                        "next_dagrun_data_interval": info.data_interval,
                        "next_dagrun_create_after": info.run_after,
                    },
                )
            orm_dag = DagModel(**orm_dag_kwargs)
            session.add(orm_dag)
            session.flush()
            triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
            dag_run = dag.create_dagrun(
                run_type=DagRunType.SCHEDULED,
                state=DagRunState.SUCCESS,
                execution_date=dag.start_date,
                data_interval=dag.infer_automated_data_interval(dag.start_date),
                start_date=dag.start_date,
                session=session,
                **triggered_by_kwargs,
            )
            ti = dag_run.get_task_instance(dag_task.task_id, session)
            ti.set_state(TaskInstanceState.SUCCESS, session)
            session.flush()

            with mock.patch.object(Stats, "timing") as stats_mock:
                dag_run.update_state(session)

            metric_name = f"dagrun.{dag.dag_id}.first_task_scheduling_delay"

            if expected:
                true_delay = ti.start_date - dag_run.data_interval_end
                sched_delay_stat_call = call(metric_name, true_delay, tags=expected_stat_tags)
                sched_delay_stat_call_with_tags = call(
                    "dagrun.first_task_scheduling_delay", true_delay, tags=expected_stat_tags
                )
                assert sched_delay_stat_call in stats_mock.mock_calls
                assert sched_delay_stat_call_with_tags in stats_mock.mock_calls
            else:
                # Assert that we never passed the metric
                sched_delay_stat_call = call(
                    metric_name,
                    mock.ANY,
                )
                assert sched_delay_stat_call not in stats_mock.mock_calls
        finally:
            # Don't write anything to the DB
            session.rollback()
            session.close()

    def test_states_sets(self, session):
        """
        Tests that adding State.failed_states and State.success_states work as expected.
        """
        dag = DAG(dag_id="test_dagrun_states", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE)
        dag_task_success = EmptyOperator(task_id="dummy", dag=dag)
        dag_task_failed = EmptyOperator(task_id="dummy2", dag=dag)

        initial_task_states = {
            dag_task_success.task_id: TaskInstanceState.SUCCESS,
            dag_task_failed.task_id: TaskInstanceState.FAILED,
        }
        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        ti_success = dag_run.get_task_instance(dag_task_success.task_id)
        ti_failed = dag_run.get_task_instance(dag_task_failed.task_id)
        assert ti_success.state in State.success_states
        assert ti_failed.state in State.failed_states

    def test_update_state_one_unfinished(self, dag_maker, session):
        """
        Previously this lived in test_scheduler_job.py

        It only really tested the behavior of DagRun.update_state.

        As far as I can tell, it checks that if you null out the state on a TI of a finished dag,
        and then you call ``update_state``, then the DR will be set to running.
        """
        with dag_maker(session=session) as dag:
            PythonOperator(task_id="t1", python_callable=lambda: print)
            PythonOperator(task_id="t2", python_callable=lambda: print)
        dr = dag.create_dagrun(
            state=DagRunState.FAILED,
            triggered_by=DagRunTriggeredByType.TEST,
            run_id="abc123",
            session=session,
        )
        for ti in dr.get_task_instances(session=session):
            ti.state = TaskInstanceState.FAILED
        session.commit()
        session.expunge_all()
        dr = session.get(DagRun, dr.id)
        assert dr.state == DagRunState.FAILED
        ti = dr.get_task_instance("t1", session=session)
        ti.state = State.NONE
        session.commit()
        dr = session.get(DagRun, dr.id)
        assert dr.state == DagRunState.FAILED
        dr.dag = dag
        dr.update_state(session=session)
        session.commit()
        dr = session.get(DagRun, dr.id)
        assert dr.state == State.RUNNING


@pytest.mark.parametrize(
    ("run_type", "expected_tis"),
    [
        pytest.param(DagRunType.MANUAL, 1, id="manual"),
        pytest.param(DagRunType.BACKFILL_JOB, 3, id="backfill"),
    ],
)
@mock.patch.object(Stats, "incr")
def test_verify_integrity_task_start_and_end_date(Stats_incr, session, run_type, expected_tis):
    """Test that tasks with specific dates are only created for backfill runs"""

    with DAG("test", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE) as dag:
        EmptyOperator(task_id="without")
        EmptyOperator(task_id="with_start_date", start_date=DEFAULT_DATE + datetime.timedelta(1))
        EmptyOperator(task_id="with_end_date", end_date=DEFAULT_DATE - datetime.timedelta(1))

    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_type=run_type,
        execution_date=DEFAULT_DATE,
        run_id=DagRun.generate_run_id(run_type, DEFAULT_DATE),
    )
    dag_run.dag = dag

    session.add(dag_run)
    session.flush()
    dag_run.verify_integrity(session=session)

    tis = dag_run.task_instances
    assert len(tis) == expected_tis

    Stats_incr.assert_any_call(
        "task_instance_created_EmptyOperator", expected_tis, tags={"dag_id": "test", "run_type": run_type}
    )
    Stats_incr.assert_any_call(
        "task_instance_created",
        expected_tis,
        tags={"dag_id": "test", "run_type": run_type, "task_type": "EmptyOperator"},
    )


@pytest.mark.parametrize("is_noop", [True, False])
def test_expand_mapped_task_instance_at_create(is_noop, dag_maker, session):
    with mock.patch("airflow.settings.task_instance_mutation_hook") as mock_mut:
        mock_mut.is_noop = is_noop
        literal = [1, 2, 3, 4]
        with dag_maker(session=session, dag_id="test_dag"):
            mapped = MockOperator.partial(task_id="task_2").expand(arg2=literal)

        dr = dag_maker.create_dagrun()
        indices = (
            session.query(TI.map_index)
            .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
            .order_by(TI.map_index)
            .all()
        )
        assert indices == [(0,), (1,), (2,), (3,)]


@pytest.mark.need_serialized_dag
@pytest.mark.parametrize("is_noop", [True, False])
def test_expand_mapped_task_instance_task_decorator(is_noop, dag_maker, session):
    with mock.patch("airflow.settings.task_instance_mutation_hook") as mock_mut:
        mock_mut.is_noop = is_noop

        @task
        def mynameis(arg):
            print(arg)

        literal = [1, 2, 3, 4]
        with dag_maker(session=session, dag_id="test_dag"):
            mynameis.expand(arg=literal)

        dr = dag_maker.create_dagrun()
        indices = (
            session.query(TI.map_index)
            .filter_by(task_id="mynameis", dag_id=dr.dag_id, run_id=dr.run_id)
            .order_by(TI.map_index)
            .all()
        )
        assert indices == [(0,), (1,), (2,), (3,)]


def test_mapped_literal_verify_integrity(dag_maker, session):
    """Test that when the length of a mapped literal changes we remove extra TIs"""

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()

    # Now "change" the DAG and we should see verify_integrity REMOVE some TIs
    dag._remove_task("task_2")

    with dag:
        mapped = task_2.expand(arg2=[1, 2]).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag
    dr.verify_integrity()

    indices = (
        session.query(TI.map_index, TI.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
        .all()
    )

    assert indices == [(0, None), (1, None), (2, TaskInstanceState.REMOVED), (3, TaskInstanceState.REMOVED)]


def test_mapped_literal_to_xcom_arg_verify_integrity(dag_maker, session):
    """Test that when we change from literal to a XComArg the TIs are removed"""

    with dag_maker(session=session) as dag:
        t1 = BaseOperator(task_id="task_1")

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()

    # Now "change" the DAG and we should see verify_integrity REMOVE some TIs
    dag._remove_task("task_2")

    with dag:
        mapped = task_2.expand(arg2=t1.output).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag
    dr.verify_integrity()

    indices = (
        session.query(TI.map_index, TI.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
        .all()
    )

    assert indices == [
        (0, TaskInstanceState.REMOVED),
        (1, TaskInstanceState.REMOVED),
        (2, TaskInstanceState.REMOVED),
        (3, TaskInstanceState.REMOVED),
    ]


def test_mapped_literal_length_increase_adds_additional_ti(dag_maker, session):
    """Test that when the length of mapped literal increases, additional ti is added"""

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
    ]

    # Now "increase" the length of literal
    dag._remove_task("task_2")

    with dag:
        task_2.expand(arg2=[1, 2, 3, 4, 5]).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag
    # Every mapped task is revised at task_instance_scheduling_decision
    dr.task_instance_scheduling_decisions()

    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
        (4, State.NONE),
    ]


def test_mapped_literal_length_reduction_adds_removed_state(dag_maker, session):
    """Test that when the length of mapped literal reduces, removed state is added"""

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
    ]

    # Now "reduce" the length of literal
    dag._remove_task("task_2")

    with dag:
        task_2.expand(arg2=[1, 2]).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag
    # Since we change the literal on the dag file itself, the dag_hash will
    # change which will have the scheduler verify the dr integrity
    dr.verify_integrity()

    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.REMOVED),
        (3, State.REMOVED),
    ]


def test_mapped_length_increase_at_runtime_adds_additional_tis(dag_maker, session):
    """Test that when the length of mapped literal increases at runtime, additional ti is added"""
    from airflow.models import Variable

    Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        return Variable.get("arg1", deserialize_json=True)

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
    ]

    # Now "clear" and "increase" the length of literal
    dag.clear()
    Variable.set(key="arg1", value=[1, 2, 3, 4])

    with dag:
        task_2.expand(arg2=task_1()).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag

    # Run the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1")
    task1 = dag.get_task("task_1")
    ti.refresh_from_task(task1)
    ti.run()

    # this would be called by the localtask job
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()

    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
    ]


def test_mapped_literal_length_reduction_at_runtime_adds_removed_state(dag_maker, session):
    """
    Test that when the length of mapped literal reduces at runtime, the missing task instances
    are marked as removed
    """
    from airflow.models import Variable

    Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        return Variable.get("arg1", deserialize_json=True)

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
    ]

    # Now "clear" and "reduce" the length of literal
    dag.clear()
    Variable.set(key="arg1", value=[1, 2])

    with dag:
        task_2.expand(arg2=task_1()).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag

    # Run the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1")
    task1 = dag.get_task("task_1")
    ti.refresh_from_task(task1)
    ti.run()

    # this would be called by the localtask job
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()

    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, TaskInstanceState.REMOVED),
    ]


def test_mapped_literal_faulty_state_in_db(dag_maker, session):
    """
    This test tries to recreate a faulty state in the database and checks if we can recover from it.
    The state that happens is that there exists mapped task instances and the unmapped task instance.
    So we have instances with map_index [-1, 0, 1]. The -1 task instances should be removed in this case.
    """

    with dag_maker(session=session) as dag:

        @task
        def task_1():
            return [1, 2]

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    decision = dr.task_instance_scheduling_decisions()
    assert len(decision.schedulable_tis) == 2

    # We insert a faulty record
    session.add(TaskInstance(task=dag.get_task("task_2"), run_id=dr.run_id))
    session.flush()

    decision = dr.task_instance_scheduling_decisions()
    assert len(decision.schedulable_tis) == 2


def test_mapped_literal_length_with_no_change_at_runtime_doesnt_call_verify_integrity(dag_maker, session):
    """
    Test that when there's no change to mapped task indexes at runtime, the dagrun.verify_integrity
    is not called
    """
    from airflow.models import Variable

    Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        return Variable.get("arg1", deserialize_json=True)

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
    ]

    # Now "clear" and no change to length
    dag.clear()
    Variable.set(key="arg1", value=[1, 2, 3])

    with dag:
        task_2.expand(arg2=task_1()).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag

    # Run the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1")
    task1 = dag.get_task("task_1")
    ti.refresh_from_task(task1)
    ti.run()

    # this would be called by the localtask job
    # Verify that DagRun.verify_integrity is not called
    with mock.patch("airflow.models.dagrun.DagRun.verify_integrity") as mock_verify_integrity:
        dr.task_instance_scheduling_decisions()
        mock_verify_integrity.assert_not_called()


def test_calls_to_verify_integrity_with_mapped_task_increase_at_runtime(dag_maker, session):
    """
    Test increase in mapped task at runtime with calls to dagrun.verify_integrity
    """
    from airflow.models import Variable

    Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        return Variable.get("arg1", deserialize_json=True)

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
    ]
    # Now "clear" and "increase" the length of literal
    dag.clear()
    Variable.set(key="arg1", value=[1, 2, 3, 4, 5])

    with dag:
        task_2.expand(arg2=task_1()).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag

    # Run the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1")
    task1 = dag.get_task("task_1")
    ti.refresh_from_task(task1)
    ti.run()
    task2 = dag.get_task("task_2")
    for ti in dr.get_task_instances():
        if ti.map_index < 0:
            ti.task = task1
        else:
            ti.task = task2
        session.merge(ti)
    session.flush()
    # create the additional task
    dr.task_instance_scheduling_decisions()
    # Run verify_integrity as a whole and assert new tasks were added
    dr.verify_integrity()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
        (4, State.NONE),
    ]
    ti3 = dr.get_task_instance(task_id="task_2", map_index=3)
    ti3.task = task2
    ti3.state = TaskInstanceState.FAILED
    session.merge(ti3)
    session.flush()
    # assert repeated calls did not change the instances
    dr.verify_integrity()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, TaskInstanceState.FAILED),
        (4, State.NONE),
    ]


def test_calls_to_verify_integrity_with_mapped_task_reduction_at_runtime(dag_maker, session):
    """
    Test reduction in mapped task at runtime with calls to dagrun.verify_integrity
    """
    from airflow.models import Variable

    Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        return Variable.get("arg1", deserialize_json=True)

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
    ]
    # Now "clear" and "reduce" the length of literal
    dag.clear()
    Variable.set(key="arg1", value=[1])

    with dag:
        task_2.expand(arg2=task_1()).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag

    # Run the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1")
    task1 = dag.get_task("task_1")
    ti.refresh_from_task(task1)
    ti.run()
    task2 = dag.get_task("task_2")
    for ti in dr.get_task_instances():
        if ti.map_index < 0:
            ti.task = task1
        else:
            ti.task = task2
            ti.state = TaskInstanceState.SUCCESS
        session.merge(ti)
    session.flush()

    # Run verify_integrity as a whole and assert some tasks were removed
    dr.verify_integrity()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, TaskInstanceState.SUCCESS),
        (1, TaskInstanceState.REMOVED),
        (2, TaskInstanceState.REMOVED),
    ]

    # assert repeated calls did not change the instances
    dr.verify_integrity()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, TaskInstanceState.SUCCESS),
        (1, TaskInstanceState.REMOVED),
        (2, TaskInstanceState.REMOVED),
    ]


def test_calls_to_verify_integrity_with_mapped_task_with_no_changes_at_runtime(dag_maker, session):
    """
    Test no change in mapped task at runtime with calls to dagrun.verify_integrity
    """
    from airflow.models import Variable

    Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        return Variable.get("arg1", deserialize_json=True)

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
    ]
    # Now "clear" and return the same length
    dag.clear()
    Variable.set(key="arg1", value=[1, 2, 3])

    with dag:
        task_2.expand(arg2=task_1()).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag

    # Run the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1")
    task1 = dag.get_task("task_1")
    ti.refresh_from_task(task1)
    ti.run()
    task2 = dag.get_task("task_2")
    for ti in dr.get_task_instances():
        if ti.map_index < 0:
            ti.task = task1
        else:
            ti.task = task2
            ti.state = TaskInstanceState.SUCCESS
        session.merge(ti)
    session.flush()

    # Run verify_integrity as a whole and assert no changes
    dr.verify_integrity()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, TaskInstanceState.SUCCESS),
        (1, TaskInstanceState.SUCCESS),
        (2, TaskInstanceState.SUCCESS),
    ]

    # assert repeated calls did not change the instances
    dr.verify_integrity()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, TaskInstanceState.SUCCESS),
        (1, TaskInstanceState.SUCCESS),
        (2, TaskInstanceState.SUCCESS),
    ]


def test_calls_to_verify_integrity_with_mapped_task_zero_length_at_runtime(dag_maker, session, caplog):
    """
    Test zero length reduction in mapped task at runtime with calls to dagrun.verify_integrity
    """
    import logging

    from airflow.models import Variable

    Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        return Variable.get("arg1", deserialize_json=True)

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1")
    ti.run()
    dr.task_instance_scheduling_decisions()
    tis = dr.get_task_instances()
    indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
    ]
    ti1 = next(i for i in tis if i.map_index == 0)
    # Now "clear" and "reduce" the length to empty list
    dag.clear()
    Variable.set(key="arg1", value=[])

    with dag:
        task_2.expand(arg2=task_1()).operator

    # At this point, we need to test that the change works on the serialized
    # DAG (which is what the scheduler operates on)
    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

    dr.dag = serialized_dag

    # Run the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1")
    task1 = dag.get_task("task_1")
    ti.refresh_from_task(task1)
    ti.run()
    task2 = dag.get_task("task_2")
    for ti in dr.get_task_instances():
        if ti.map_index < 0:
            ti.task = task1
        else:
            ti.task = task2
        session.merge(ti)
    session.flush()
    with caplog.at_level(logging.DEBUG):
        # Run verify_integrity as a whole and assert the tasks were removed
        dr.verify_integrity()
        tis = dr.get_task_instances()
        indices = [(ti.map_index, ti.state) for ti in tis if ti.map_index >= 0]
        assert sorted(indices) == [
            (0, TaskInstanceState.REMOVED),
            (1, TaskInstanceState.REMOVED),
            (2, TaskInstanceState.REMOVED),
        ]
        assert (
            f"Removing task '{ti1}' as the map_index is longer than the resolved mapping list (0)"
            in caplog.text
        )


@pytest.mark.need_serialized_dag
def test_mapped_mixed_literal_not_expanded_at_create(dag_maker, session):
    literal = [1, 2, 3, 4]
    with dag_maker(session=session):
        task = BaseOperator(task_id="task_1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg1=literal, arg2=task.output)

    dr = dag_maker.create_dagrun()
    query = (
        session.query(TI.map_index, TI.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
    )

    assert query.all() == [(-1, None)]

    # Verify_integrity shouldn't change the result now that the TIs exist
    dr.verify_integrity(session=session)
    assert query.all() == [(-1, None)]


def test_mapped_task_group_expands_at_create(dag_maker, session):
    literal = [[1, 2], [3, 4]]

    with dag_maker(session=session):

        @task_group
        def tg(x):
            # Normal operator in mapped task group, expands to 2 tis.
            MockOperator(task_id="t1")
            # Mapped operator expands *again* against mapped task group arguments to 4 tis.
            with pytest.raises(NotImplementedError) as ctx:
                MockOperator.partial(task_id="t2").expand(arg1=literal)
            assert str(ctx.value) == "operator expansion in an expanded task group is not yet supported"
            # Normal operator referencing mapped task group arguments does not further expand, only 2 tis.
            MockOperator(task_id="t3", arg1=x)
            # It can expand *again* (since each item in x is a list) but this is not done at parse time.
            with pytest.raises(NotImplementedError) as ctx:
                MockOperator.partial(task_id="t4").expand(arg1=x)
            assert str(ctx.value) == "operator expansion in an expanded task group is not yet supported"

        tg.expand(x=literal)

    dr = dag_maker.create_dagrun()
    query = (
        session.query(TI.task_id, TI.map_index, TI.state)
        .filter_by(dag_id=dr.dag_id, run_id=dr.run_id)
        .order_by(TI.task_id, TI.map_index)
    )
    assert query.all() == [
        ("tg.t1", 0, None),
        ("tg.t1", 1, None),
        # ("tg.t2", 0, None),
        # ("tg.t2", 1, None),
        # ("tg.t2", 2, None),
        # ("tg.t2", 3, None),
        ("tg.t3", 0, None),
        ("tg.t3", 1, None),
        # ("tg.t4", -1, None),
    ]


def test_mapped_task_group_empty_operator(dag_maker, session):
    """
    Test that dynamic task inside a dynamic task group only marks
    the corresponding downstream EmptyOperator as success.
    """

    literal = [1, 2, 3]

    with dag_maker(session=session) as dag:

        @task_group
        def tg(x):
            @task
            def t1(x):
                return x

            t2 = EmptyOperator(task_id="t2")

            @task
            def t3(x):
                return x

            t1(x) >> t2 >> t3(x)

        tg.expand(x=literal)

    dr = dag_maker.create_dagrun()

    t2_task = dag.get_task("tg.t2")
    t2_0 = dr.get_task_instance(task_id="tg.t2", map_index=0)
    t2_0.refresh_from_task(t2_task)
    assert t2_0.state is None

    t2_1 = dr.get_task_instance(task_id="tg.t2", map_index=1)
    t2_1.refresh_from_task(t2_task)
    assert t2_1.state is None

    dr.schedule_tis([t2_0])

    t2_0 = dr.get_task_instance(task_id="tg.t2", map_index=0)
    assert t2_0.state == TaskInstanceState.SUCCESS

    t2_1 = dr.get_task_instance(task_id="tg.t2", map_index=1)
    assert t2_1.state is None


def test_ti_scheduling_mapped_zero_length(dag_maker, session):
    with dag_maker(session=session):
        task = BaseOperator(task_id="task_1")
        mapped = MockOperator.partial(task_id="task_2").expand(arg2=task.output)

    dr: DagRun = dag_maker.create_dagrun()
    ti1, ti2 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
    ti1.state = TaskInstanceState.SUCCESS
    session.add(
        TaskMap(dag_id=dr.dag_id, task_id=ti1.task_id, run_id=dr.run_id, map_index=-1, length=0, keys=None)
    )
    session.flush()

    decision = dr.task_instance_scheduling_decisions(session=session)

    # ti1 finished execution. ti2 goes directly to finished state because it's
    # expanded against a zero-length XCom.
    assert decision.finished_tis == [ti1, ti2]

    indices = (
        session.query(TI.map_index, TI.state)
        .filter_by(task_id=mapped.task_id, dag_id=mapped.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
        .all()
    )

    assert indices == [(-1, TaskInstanceState.SKIPPED)]


@pytest.mark.parametrize("trigger_rule", [TriggerRule.ALL_DONE, TriggerRule.ALL_SUCCESS])
def test_mapped_task_upstream_failed(dag_maker, session, trigger_rule):
    from airflow.providers.standard.operators.python import PythonOperator

    with dag_maker(session=session) as dag:

        @dag.task
        def make_list():
            return [f'echo "{a!r}"' for a in [1, 2, {"a": "b"}]]

        def consumer(*args):
            print(repr(args))

        PythonOperator.partial(
            task_id="consumer",
            trigger_rule=trigger_rule,
            python_callable=consumer,
        ).expand(op_args=make_list())

    dr = dag_maker.create_dagrun()
    _, make_list_ti = sorted(dr.task_instances, key=lambda ti: ti.task_id)
    make_list_ti.state = TaskInstanceState.FAILED
    session.flush()

    tis, _ = dr.update_state(execute_callbacks=False, session=session)
    assert tis == []
    tis = sorted(dr.task_instances, key=lambda ti: ti.task_id)

    assert sorted((ti.task_id, ti.map_index, ti.state) for ti in tis) == [
        ("consumer", -1, TaskInstanceState.UPSTREAM_FAILED),
        ("make_list", -1, TaskInstanceState.FAILED),
    ]
    # Bug/possible source of optimization: The DR isn't marked as failed until
    # in the loop that marks the last task as UPSTREAM_FAILED
    tis, _ = dr.update_state(execute_callbacks=False, session=session)
    assert tis == []
    assert dr.state == DagRunState.FAILED


def test_mapped_task_all_finish_before_downstream(dag_maker, session):
    result = None

    with dag_maker(session=session) as dag:

        @dag.task
        def make_list():
            return [1, 2]

        @dag.task
        def double(value):
            return value * 2

        @dag.task
        def consumer(value):
            nonlocal result
            result = list(value)

        consumer(value=double.expand(value=make_list()))

    dr: DagRun = dag_maker.create_dagrun()

    def _task_ids(tis):
        return [ti.task_id for ti in tis]

    # The first task is always make_list.
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["make_list"]

    # After make_list is run, double is expanded.
    decision.schedulable_tis[0].run(verbose=False, session=session)
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["double", "double"]

    # Running just one of the mapped tis does not make downstream schedulable.
    decision.schedulable_tis[0].run(verbose=False, session=session)
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["double"]

    # Downstream is schedulable after all mapped tis are run.
    decision.schedulable_tis[0].run(verbose=False, session=session)
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["consumer"]

    # We should be able to get all values aggregated from mapped upstreams.
    decision.schedulable_tis[0].run(verbose=False, session=session)
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert decision.schedulable_tis == []
    assert result == [2, 4]


def test_schedule_tis_map_index(dag_maker, session):
    with dag_maker(session=session, dag_id="test"):
        task = BaseOperator(task_id="task_1")

    dr = DagRun(dag_id="test", run_id="test", run_type=DagRunType.MANUAL)
    ti0 = TI(task=task, run_id=dr.run_id, map_index=0, state=TaskInstanceState.SUCCESS)
    ti1 = TI(task=task, run_id=dr.run_id, map_index=1, state=None)
    ti2 = TI(task=task, run_id=dr.run_id, map_index=2, state=TaskInstanceState.SUCCESS)
    session.add_all((dr, ti0, ti1, ti2))
    session.flush()

    assert dr.schedule_tis((ti1,), session=session) == 1

    session.refresh(ti0)
    session.refresh(ti1)
    session.refresh(ti2)
    assert ti0.state == TaskInstanceState.SUCCESS
    assert ti1.state == TaskInstanceState.SCHEDULED
    assert ti2.state == TaskInstanceState.SUCCESS


def test_schedule_tis_start_trigger(dag_maker, session):
    """
    Test that an operator with start_trigger_args set can be directly deferred during scheduling.
    """

    class TestOperator(BaseOperator):
        start_trigger_args = StartTriggerArgs(
            trigger_cls="airflow.triggers.testing.SuccessTrigger",
            trigger_kwargs=None,
            next_method="execute_complete",
            timeout=None,
        )
        start_from_trigger = True

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.start_trigger_args.trigger_kwargs = {}

        def execute_complete(self):
            pass

    with dag_maker(session=session):
        task = TestOperator(task_id="test_task")

    dr: DagRun = dag_maker.create_dagrun()

    ti = TI(task=task, run_id=dr.run_id, state=None)
    assert ti.state is None
    dr.schedule_tis((ti,), session=session)
    assert ti.state == TaskInstanceState.DEFERRED


def test_schedule_tis_empty_operator_try_number(dag_maker, session: Session):
    """
    When empty operator is not actually run, then we need to increment the try_number,
    since ordinarily it's incremented when scheduled, but empty operator is generally not scheduled.
    """

    with dag_maker(session=session):
        BashOperator(task_id="real_task", bash_command="echo 1")
        EmptyOperator(task_id="empty_task")

    dr: DagRun = dag_maker.create_dagrun(session=session)
    session.commit()
    tis = dr.task_instances
    dr.schedule_tis(tis, session=session)
    session.commit()
    session.expunge_all()
    tis = dr.get_task_instances(session=session)
    real_ti = next(x for x in tis if x.task_id == "real_task")
    empty_ti = next(x for x in tis if x.task_id == "empty_task")
    assert real_ti.try_number == 1
    assert empty_ti.try_number == 1


def test_schedule_tis_start_trigger_through_expand(dag_maker, session):
    """
    Test that an operator with start_trigger_args set can be directly deferred during scheduling.
    """

    class TestOperator(BaseOperator):
        start_trigger_args = StartTriggerArgs(
            trigger_cls="airflow.triggers.testing.SuccessTrigger",
            trigger_kwargs={},
            next_method="execute_complete",
            timeout=None,
        )
        start_from_trigger = False

        def __init__(self, *args, start_from_trigger: bool = False, **kwargs):
            super().__init__(*args, **kwargs)
            self.start_from_trigger = start_from_trigger

        def execute_complete(self):
            pass

    with dag_maker(session=session):
        TestOperator.partial(task_id="test_task").expand(start_from_trigger=[True, False])

    dr: DagRun = dag_maker.create_dagrun()

    dr.schedule_tis(dr.task_instances, session=session)
    tis = [(ti.state, ti.map_index) for ti in dr.task_instances]
    assert tis[0] == (TaskInstanceState.DEFERRED, 0)
    assert tis[1] == (None, 1)


def test_mapped_expand_kwargs(dag_maker):
    with dag_maker():

        @task
        def task_0():
            return {"arg1": "a", "arg2": "b"}

        @task
        def task_1(args_0):
            return [args_0, {"arg1": "y"}, {"arg2": "z"}]

        args_0 = task_0()
        args_list = task_1(args_0=args_0)

        MockOperator.partial(task_id="task_2").expand_kwargs(args_list)
        MockOperator.partial(task_id="task_3").expand_kwargs(
            [{"arg1": "a", "arg2": "b"}, {"arg1": "y"}, {"arg2": "z"}],
        )
        MockOperator.partial(task_id="task_4").expand_kwargs([args_0, {"arg1": "y"}, {"arg2": "z"}])

    dr: DagRun = dag_maker.create_dagrun()
    tis = {(ti.task_id, ti.map_index): ti for ti in dr.task_instances}

    # task_2 is not expanded yet since it relies on one single XCom input.
    # task_3 and task_4 received a pure literal and can expanded right away.
    # task_4 relies on an XCom input in the list, but can also be expanded.
    assert sorted(map_index for (task_id, map_index) in tis if task_id == "task_2") == [-1]
    assert sorted(map_index for (task_id, map_index) in tis if task_id == "task_3") == [0, 1, 2]
    assert sorted(map_index for (task_id, map_index) in tis if task_id == "task_4") == [0, 1, 2]

    tis[("task_0", -1)].run()
    tis[("task_1", -1)].run()

    # With the upstreams available, everything should get expanded now.
    decision = dr.task_instance_scheduling_decisions()
    assert {(ti.task_id, ti.map_index): ti.state for ti in decision.schedulable_tis} == {
        ("task_2", 0): None,
        ("task_2", 1): None,
        ("task_2", 2): None,
        ("task_3", 0): None,
        ("task_3", 1): None,
        ("task_3", 2): None,
        ("task_4", 0): None,
        ("task_4", 1): None,
        ("task_4", 2): None,
    }


def test_mapped_skip_upstream_not_deadlock(dag_maker):
    with dag_maker() as dag:

        @dag.task
        def add_one(x: int):
            return x + 1

        @dag.task
        def say_hi():
            print("Hi")

        added_values = add_one.expand(x=[])
        added_more_values = add_one.expand(x=[])
        say_hi() >> added_values
        added_values >> added_more_values

    dr = dag_maker.create_dagrun()

    session = dag_maker.session
    tis = {ti.task_id: ti for ti in dr.task_instances}

    tis["say_hi"].state = TaskInstanceState.SUCCESS
    session.flush()

    dr.update_state(session=session)  # expands the mapped tasks
    dr.update_state(session=session)  # marks the task as skipped
    dr.update_state(session=session)  # marks dagrun as success
    assert dr.state == DagRunState.SUCCESS
    assert tis["add_one__1"].state == TaskInstanceState.SKIPPED


def test_schedulable_task_exist_when_rerun_removed_upstream_mapped_task(session, dag_maker):
    from airflow.decorators import task

    @task
    def do_something(i):
        return 1

    @task
    def do_something_else(i):
        return 1

    with dag_maker():
        nums = do_something.expand(i=[i + 1 for i in range(5)])
        do_something_else.expand(i=nums)

    dr = dag_maker.create_dagrun()

    ti = dr.get_task_instance("do_something_else", session=session)
    ti.map_index = 0
    task = ti.task
    for map_index in range(1, 5):
        ti = TI(task, run_id=dr.run_id, map_index=map_index)
        session.add(ti)
        ti.dag_run = dr
    session.flush()
    tis = dr.get_task_instances()
    for ti in tis:
        if ti.task_id == "do_something":
            if ti.map_index > 2:
                ti.state = TaskInstanceState.REMOVED
            else:
                ti.state = TaskInstanceState.SUCCESS
            session.merge(ti)
    session.commit()
    # The Upstream is done with 2 removed tis and 3 success tis
    (tis, _) = dr.update_state()
    assert len(tis)
    assert dr.state != DagRunState.FAILED


@pytest.mark.parametrize(
    "partial_params, mapped_params, expected",
    [
        pytest.param(None, [{"a": 1}], [[("a", 1)]], id="simple"),
        pytest.param({"b": 2}, [{"a": 1}], [[("a", 1), ("b", 2)]], id="merge"),
        pytest.param({"b": 2}, [{"a": 1, "b": 3}], [[("a", 1), ("b", 3)]], id="override"),
    ],
)
def test_mapped_expand_against_params(dag_maker, partial_params, mapped_params, expected):
    results = []

    class PullOperator(BaseOperator):
        def execute(self, context):
            results.append(sorted(context["params"].items()))

    with dag_maker():
        PullOperator.partial(task_id="t", params=partial_params).expand(params=mapped_params)

    dr: DagRun = dag_maker.create_dagrun()
    decision = dr.task_instance_scheduling_decisions()

    for ti in decision.schedulable_tis:
        ti.run()

    assert sorted(results) == expected


def test_mapped_task_group_expands(dag_maker, session):
    with dag_maker(session=session):

        @task_group
        def tg(x, y):
            return MockOperator(task_id="task_2", arg1=x, arg2=y)

        task_1 = BaseOperator(task_id="task_1")
        tg.expand(x=task_1.output, y=[1, 2, 3])

    dr: DagRun = dag_maker.create_dagrun()

    # Not expanding task_2 yet since it depends on result from task_1.
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert {(ti.task_id, ti.map_index, ti.state) for ti in decision.tis} == {
        ("task_1", -1, None),
        ("tg.task_2", -1, None),
    }

    # Simulate task_1 execution to produce TaskMap.
    (ti_1,) = decision.schedulable_tis
    assert ti_1.task_id == "task_1"
    ti_1.state = TaskInstanceState.SUCCESS
    session.add(TaskMap.from_task_instance_xcom(ti_1, ["a", "b"]))
    session.flush()

    # Now task_2 in mapped tagk group is expanded.
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert {(ti.task_id, ti.map_index, ti.state) for ti in decision.schedulable_tis} == {
        ("tg.task_2", 0, None),
        ("tg.task_2", 1, None),
        ("tg.task_2", 2, None),
        ("tg.task_2", 3, None),
        ("tg.task_2", 4, None),
        ("tg.task_2", 5, None),
    }


def test_operator_mapped_task_group_receives_value(dag_maker, session):
    with dag_maker(session=session):

        @task
        def t(value, *, ti=None):
            results[(ti.task_id, ti.map_index)] = value
            return value

        @task_group
        def tg(va):
            # Each expanded group has one t1 and t2 each.
            t1 = t.override(task_id="t1")(va)
            t2 = t.override(task_id="t2")(t1)

            with pytest.raises(NotImplementedError) as ctx:
                t.override(task_id="t4").expand(value=va)
            assert str(ctx.value) == "operator expansion in an expanded task group is not yet supported"

            return t2

        # The group is mapped by 3.
        t2 = tg.expand(va=[["a", "b"], [4], ["z"]])

        # Aggregates results from task group.
        t.override(task_id="t3")(t2)

    dr: DagRun = dag_maker.create_dagrun()

    results = {}
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run()
    assert results == {("tg.t1", 0): ["a", "b"], ("tg.t1", 1): [4], ("tg.t1", 2): ["z"]}

    results = {}
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run()
    assert results == {("tg.t2", 0): ["a", "b"], ("tg.t2", 1): [4], ("tg.t2", 2): ["z"]}

    results = {}
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        ti.run()
    assert len(results) == 1
    assert list(results[("t3", -1)]) == [["a", "b"], [4], ["z"]]


def test_mapping_against_empty_list(dag_maker, session):
    with dag_maker(session=session):

        @task
        def add_one(x: int):
            return x + 1

        @task
        def say_hi():
            print("Hi")

        @task
        def say_bye():
            print("Bye")

        added_values = add_one.expand(x=[])
        added_more_values = add_one.expand(x=[])
        added_more_more_values = add_one.expand(x=[])
        say_hi() >> say_bye() >> added_values
        added_values >> added_more_values >> added_more_more_values

    dr: DagRun = dag_maker.create_dagrun()

    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    say_hi_ti = tis["say_hi"]
    say_bye_ti = tis["say_bye"]
    say_hi_ti.state = TaskInstanceState.SUCCESS
    say_bye_ti.state = TaskInstanceState.SUCCESS
    session.merge(say_hi_ti)
    session.merge(say_bye_ti)
    session.flush()

    dr.update_state(session=session)
    dr.update_state(session=session)  # marks first empty mapped task as skipped
    dr.update_state(session=session)  # marks second empty mapped task as skipped
    dr.update_state(session=session)  # marks the third empty mapped task as skipped and dagrun as success
    tis = {ti.task_id: ti.state for ti in dr.get_task_instances(session=session)}
    assert tis["say_hi"] == TaskInstanceState.SUCCESS
    assert tis["say_bye"] == TaskInstanceState.SUCCESS
    assert tis["add_one"] == TaskInstanceState.SKIPPED
    assert tis["add_one__1"] == TaskInstanceState.SKIPPED
    assert tis["add_one__2"] == TaskInstanceState.SKIPPED
    assert dr.state == State.SUCCESS


def test_mapped_task_depends_on_past(dag_maker, session):
    with dag_maker(session=session):

        @task(depends_on_past=True)
        def print_value(value):
            print(value)

        print_value.expand_kwargs([{"value": i} for i in range(2)])

    dr1: DagRun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
    dr2: DagRun = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)

    # print_value in dr2 is not ready yet since the task depends on past.
    decision = dr2.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 0

    # Run print_value in dr1.
    decision = dr1.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 2
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    # Now print_value in dr2 can run
    decision = dr2.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 2
    for ti in decision.schedulable_tis:
        ti.run(session=session)

    # Both runs are finished now.
    decision = dr1.task_instance_scheduling_decisions(session=session)
    assert len(decision.unfinished_tis) == 0
    decision = dr2.task_instance_scheduling_decisions(session=session)
    assert len(decision.unfinished_tis) == 0


def test_clearing_task_and_moving_from_non_mapped_to_mapped(dag_maker, session):
    """
    Test that clearing a task and moving from non-mapped to mapped clears existing
    references in XCom, TaskInstanceNote, TaskReschedule and
    RenderedTaskInstanceFields. To be able to test this, RenderedTaskInstanceFields
    was not used in the test since it would require that the task is expanded first.
    """

    from airflow.models.xcom import XCom

    @task
    def printx(x):
        print(x)

    with dag_maker() as dag:
        printx.expand(x=[1])

    dr1: DagRun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
    ti = dr1.get_task_instances()[0]
    filter_kwargs = dict(dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, map_index=ti.map_index)
    ti = session.query(TaskInstance).filter_by(**filter_kwargs).one()

    tr = TaskReschedule(
        task_id=ti.task_id,
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        try_number=ti.try_number,
        start_date=timezone.datetime(2017, 1, 1),
        end_date=timezone.datetime(2017, 1, 2),
        reschedule_date=timezone.datetime(2017, 1, 1),
    )

    # mimicking a case where task moved from non-mapped to mapped
    # in that case, it would have map_index of -1 even though mapped
    ti.map_index = -1
    ti.note = "sample note"
    session.merge(ti)
    session.flush()
    # Purposely omitted RenderedTaskInstanceFields because the ti need
    # to be expanded but here we are mimicking and made it map_index -1
    session.add(tr)
    XCom.set(key="test", value="value", task_id=ti.task_id, dag_id=dag.dag_id, run_id=ti.run_id)
    session.commit()
    for table in [TaskInstanceNote, TaskReschedule, XCom]:
        assert session.query(table).count() == 1
    dr1.task_instance_scheduling_decisions(session)
    for table in [TaskInstanceNote, TaskReschedule, XCom]:
        assert session.query(table).count() == 0


def test_dagrun_with_note(dag_maker, session):
    with dag_maker():

        @task
        def the_task():
            print("Hi")

        the_task()

    dr: DagRun = dag_maker.create_dagrun()
    dr.note = "dag run with note"

    session.add(dr)
    session.commit()

    dr_note = session.query(DagRunNote).filter(DagRunNote.dag_run_id == dr.id).one()
    assert dr_note.content == "dag run with note"

    session.delete(dr)
    session.commit()

    assert session.query(DagRun).filter(DagRun.id == dr.id).one_or_none() is None
    assert session.query(DagRunNote).filter(DagRunNote.dag_run_id == dr.id).one_or_none() is None


@pytest.mark.parametrize(
    "dag_run_state, on_failure_fail_dagrun", [[DagRunState.SUCCESS, False], [DagRunState.FAILED, True]]
)
def test_teardown_failure_behaviour_on_dagrun(dag_maker, session, dag_run_state, on_failure_fail_dagrun):
    with dag_maker():

        @teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)
        def teardowntask():
            print(1)

        @task
        def mytask():
            print(1)

        mytask() >> teardowntask()

    dr = dag_maker.create_dagrun()
    ti1 = dr.get_task_instance(task_id="mytask")
    td1 = dr.get_task_instance(task_id="teardowntask")
    ti1.state = State.SUCCESS
    td1.state = State.FAILED
    session.merge(ti1)
    session.merge(td1)
    session.flush()
    dr.update_state()
    session.flush()
    dr = session.query(DagRun).one()
    assert dr.state == dag_run_state


@pytest.mark.parametrize(
    "dag_run_state, on_failure_fail_dagrun", [[DagRunState.SUCCESS, False], [DagRunState.FAILED, True]]
)
def test_teardown_failure_on_non_leaf_behaviour_on_dagrun(
    dag_maker, session, dag_run_state, on_failure_fail_dagrun
):
    with dag_maker():

        @teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)
        def teardowntask():
            print(1)

        @teardown
        def teardowntask2():
            print(1)

        @task
        def mytask():
            print(1)

        mytask() >> teardowntask() >> teardowntask2()

    dr = dag_maker.create_dagrun()
    ti1 = dr.get_task_instance(task_id="mytask")
    td1 = dr.get_task_instance(task_id="teardowntask")
    td2 = dr.get_task_instance(task_id="teardowntask2")
    ti1.state = State.SUCCESS
    td1.state = State.FAILED
    td2.state = State.FAILED
    session.merge(ti1)
    session.merge(td1)
    session.merge(td2)
    session.flush()
    dr.update_state()
    session.flush()
    dr = session.query(DagRun).one()
    assert dr.state == dag_run_state


def test_work_task_failure_when_setup_teardown_are_successful(dag_maker, session):
    with dag_maker():

        @setup
        def setuptask():
            print(2)

        @teardown
        def teardown_task():
            print(1)

        @task
        def mytask():
            print(1)

        with setuptask() >> teardown_task():
            mytask()

    dr = dag_maker.create_dagrun()
    s1 = dr.get_task_instance(task_id="setuptask")
    td1 = dr.get_task_instance(task_id="teardown_task")
    t1 = dr.get_task_instance(task_id="mytask")
    s1.state = TaskInstanceState.SUCCESS
    td1.state = TaskInstanceState.SUCCESS
    t1.state = TaskInstanceState.FAILED
    session.merge(s1)
    session.merge(td1)
    session.merge(t1)
    session.flush()
    dr.update_state()
    session.flush()
    dr = session.query(DagRun).one()
    assert dr.state == DagRunState.FAILED


def test_failure_of_leaf_task_not_connected_to_teardown_task(dag_maker, session):
    with dag_maker():

        @setup
        def setuptask():
            print(2)

        @teardown
        def teardown_task():
            print(1)

        @task
        def mytask():
            print(1)

        setuptask()
        teardown_task()
        mytask()

    dr = dag_maker.create_dagrun()
    s1 = dr.get_task_instance(task_id="setuptask")
    td1 = dr.get_task_instance(task_id="teardown_task")
    t1 = dr.get_task_instance(task_id="mytask")
    s1.state = TaskInstanceState.SUCCESS
    td1.state = TaskInstanceState.SUCCESS
    t1.state = TaskInstanceState.FAILED
    session.merge(s1)
    session.merge(td1)
    session.merge(t1)
    session.flush()
    dr.update_state()
    session.flush()
    dr = session.query(DagRun).one()
    assert dr.state == DagRunState.FAILED


@pytest.mark.parametrize(
    "input, expected",
    [
        (["s1 >> w1 >> t1"], {"w1"}),  # t1 ignored
        (["s1 >> w1 >> t1", "s1 >> t1"], {"w1"}),  # t1 ignored; properly wired to setup
        (["s1 >> w1"], {"w1"}),  # no teardown
        (["s1 >> w1 >> t1_"], {"t1_"}),  # t1_ is natural leaf and OFFD=True;
        (["s1 >> w1 >> t1_", "s1 >> t1_"], {"t1_"}),  # t1_ is natural leaf and OFFD=True; wired to setup
        (["s1 >> w1 >> t1_ >> w2", "s1 >> t1_"], {"w2"}),  # t1_ is not a natural leaf so excluded anyway
        (["t1 >> t2"], {"t2"}),  # all teardowns -- default to "leaves"
        (["w1 >> t1_ >> t2"], {"t1_"}),  # teardown to teardown
    ],
)
def test_tis_considered_for_state(dag_maker, session, input, expected):
    """
    We use a convenience notation to wire up test scenarios:

    t<num> -- teardown task
    t<num>_ -- teardown task with on_failure_fail_dagrun = True
    s<num> -- setup task
    w<num> -- work task (a.k.a. normal task)

    In the test input, each line is a statement. We'll automatically create the tasks and wire them up
    as indicated in the test input.
    """

    @teardown
    def teardown_task():
        print(1)

    @task
    def work_task():
        print(1)

    @setup
    def setup_task():
        print(1)

    def make_task(task_id, dag):
        """
        Task factory helper.

        Will give a setup, teardown, work, or teardown-with-dagrun-failure task depending on input.
        """
        if task_id.startswith("s"):
            factory = setup_task
        elif task_id.startswith("w"):
            factory = work_task
        elif task_id.endswith("_"):
            factory = teardown_task.override(on_failure_fail_dagrun=True)
        else:
            factory = teardown_task
        return dag.task_dict.get(task_id) or factory.override(task_id=task_id)()

    with dag_maker() as dag:
        for line in input:
            tasks = [make_task(x, dag) for x in line.split(" >> ")]
            reduce(lambda x, y: x >> y, tasks)

    dr = dag_maker.create_dagrun()
    tis = dr.task_instance_scheduling_decisions(session).tis
    tis_for_state = {x.task_id for x in dr._tis_for_dagrun_state(dag=dag, tis=tis)}
    assert tis_for_state == expected


@pytest.mark.parametrize(
    "pattern, run_id, result",
    [
        ["^[A-Z]", "ABC", True],
        ["^[A-Z]", "abc", False],
        ["^[0-9]", "123", True],
        # The below params tests that user configuration does not affect internally generated
        # run_ids
        ["", "scheduled__2023-01-01T00:00:00+00:00", True],
        ["", "manual__2023-01-01T00:00:00+00:00", True],
        ["", "asset_triggered__2023-01-01T00:00:00+00:00", True],
        ["", "scheduled_2023-01-01T00", False],
        ["", "manual_2023-01-01T00", False],
        ["", "asset_triggered_2023-01-01T00", False],
        ["^[0-9]", "scheduled__2023-01-01T00:00:00+00:00", True],
        ["^[0-9]", "manual__2023-01-01T00:00:00+00:00", True],
        ["^[a-z]", "asset_triggered__2023-01-01T00:00:00+00:00", True],
    ],
)
def test_dag_run_id_config(session, dag_maker, pattern, run_id, result):
    with conf_vars({("scheduler", "allowed_run_id_pattern"): pattern}):
        with dag_maker():
            ...
        if result:
            dag_maker.create_dagrun(run_id=run_id)
        else:
            with pytest.raises(AirflowException):
                dag_maker.create_dagrun(run_id=run_id)
