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
from collections.abc import Mapping
from functools import reduce
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import call

import pendulum
import pytest
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow import settings
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG, DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun, DagRunNote
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance, TaskInstanceNote, clear_task_instances
from airflow.models.taskmap import TaskMap
from airflow.models.taskreschedule import TaskReschedule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import setup, task, task_group, teardown
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.stats import Stats
from airflow.triggers.base import StartTriggerArgs
from airflow.utils import timezone
from airflow.utils.span_status import SpanStatus
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.thread_safe_dict import ThreadSafeDict
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.mock_operators import MockOperator
from unit.models import DEFAULT_DATE as _DEFAULT_DATE

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


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
        db.clear_db_dags()

    def create_dag_run(
        self,
        dag: DAG,
        *,
        task_states: Mapping[str, TaskInstanceState] | None = None,
        logical_date: datetime.datetime | None = None,
        is_backfill: bool = False,
        state: DagRunState = DagRunState.RUNNING,
        session: Session,
    ):
        now = timezone.utcnow()
        logical_date = pendulum.instance(logical_date or now)
        if is_backfill:
            run_type = DagRunType.BACKFILL_JOB
            data_interval = dag.infer_automated_data_interval(logical_date)
        else:
            run_type = DagRunType.MANUAL
            data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
        dag_run = dag.create_dagrun(
            run_id=dag.timetable.generate_run_id(
                run_type=run_type,
                run_after=logical_date,
                data_interval=data_interval,
            ),
            run_type=run_type,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=data_interval.end,
            start_date=now,
            state=state,
            triggered_by=DagRunTriggeredByType.TEST,
            session=session,
        )

        if task_states is not None:
            for task_id, task_state in task_states.items():
                ti = dag_run.get_task_instance(task_id)
                if TYPE_CHECKING:
                    assert ti
                ti.set_state(task_state, session)
            session.flush()

        return dag_run

    def test_clear_task_instances_for_backfill_running_dagrun(self, dag_maker, session):
        now = timezone.utcnow()
        state = DagRunState.RUNNING
        dag_id = "test_clear_task_instances_for_backfill_running_dagrun"
        with dag_maker(dag_id=dag_id) as dag:
            EmptyOperator(task_id="backfill_task_0")
        self.create_dag_run(dag, logical_date=now, is_backfill=True, state=state, session=session)

        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.flush()
        dr0 = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.logical_date == now).first()
        assert dr0.state == state
        assert dr0.clear_number < 1

    @pytest.mark.parametrize("state", [DagRunState.SUCCESS, DagRunState.FAILED])
    def test_clear_task_instances_for_backfill_finished_dagrun(self, dag_maker, state, session):
        now = timezone.utcnow()
        dag_id = "test_clear_task_instances_for_backfill_finished_dagrun"
        with dag_maker(dag_id=dag_id) as dag:
            EmptyOperator(task_id="backfill_task_0")
        self.create_dag_run(dag, logical_date=now, is_backfill=True, state=state, session=session)

        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.flush()
        dr0 = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.logical_date == now).first()
        assert dr0.state == DagRunState.QUEUED
        assert dr0.clear_number == 1

    def test_dagrun_find(self, session):
        now = timezone.utcnow()

        dag_id1 = "test_dagrun_find_externally_triggered"
        dag_run = DagRun(
            dag_id=dag_id1,
            run_id=dag_id1,
            run_type=DagRunType.MANUAL,
            logical_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
        )
        session.add(dag_run)

        dag_id2 = "test_dagrun_find_not_externally_triggered"
        dag_run = DagRun(
            dag_id=dag_id2,
            run_id=dag_id2,
            run_type=DagRunType.SCHEDULED,
            logical_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
        )
        session.add(dag_run)

        session.commit()

        assert len(DagRun.find(dag_id=dag_id1, run_type=DagRunType.MANUAL)) == 1
        assert len(DagRun.find(run_id=dag_id1)) == 1
        assert len(DagRun.find(run_id=[dag_id1, dag_id2])) == 2
        assert len(DagRun.find(logical_date=[now, now])) == 2
        assert len(DagRun.find(logical_date=now)) == 2
        assert len(DagRun.find(dag_id=dag_id1, run_type=DagRunType.SCHEDULED)) == 0
        assert len(DagRun.find(dag_id=dag_id2, run_type=DagRunType.MANUAL)) == 0
        assert len(DagRun.find(dag_id=dag_id2)) == 1

    def test_dagrun_find_duplicate(self, session):
        now = timezone.utcnow()

        dag_id = "test_dagrun_find_duplicate"
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=dag_id,
            run_type=DagRunType.MANUAL,
            logical_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
        )
        session.add(dag_run)

        session.commit()

        assert DagRun.find_duplicate(dag_id=dag_id, run_id=dag_id) is not None
        assert DagRun.find_duplicate(dag_id=dag_id, run_id=dag_id) is not None
        assert DagRun.find_duplicate(dag_id=dag_id, run_id=None) is None

    def test_dagrun_success_when_all_skipped(self, dag_maker, session):
        """
        Tests that a DAG run succeeds when all tasks are skipped
        """
        with dag_maker(
            dag_id="test_dagrun_success_when_all_skipped",
            schedule=datetime.timedelta(days=1),
            start_date=timezone.datetime(2017, 1, 1),
        ) as dag:
            ...
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
        assert dag_run.state == DagRunState.SUCCESS

    def test_dagrun_not_stuck_in_running_when_all_tasks_instances_are_removed(self, dag_maker, session):
        """
        Tests that a DAG run succeeds when all tasks are removed
        """
        with dag_maker(
            dag_id="test_dagrun_success_when_all_skipped",
            schedule=datetime.timedelta(days=1),
            start_date=timezone.datetime(2017, 1, 1),
        ) as dag:
            ...
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
        assert dag_run.state == DagRunState.SUCCESS

    def test_dagrun_success_conditions(self, dag_maker, session):
        # A -> B
        # A -> C -> D
        # ordered: B, D, C, A or D, B, C, A or D, C, B, A
        with dag_maker(schedule=datetime.timedelta(days=1), session=session):
            op1 = EmptyOperator(task_id="A")
            op2 = EmptyOperator(task_id="B")
            op3 = EmptyOperator(task_id="C")
            op4 = EmptyOperator(task_id="D")
            op1.set_upstream([op2, op3])
            op3.set_upstream(op4)

        dr = dag_maker.create_dagrun()

        # op1 = root
        ti_op1 = dr.get_task_instance(task_id=op1.task_id)
        ti_op1.set_state(state=TaskInstanceState.SUCCESS, session=session)

        ti_op2 = dr.get_task_instance(task_id=op2.task_id)
        ti_op3 = dr.get_task_instance(task_id=op3.task_id)
        ti_op4 = dr.get_task_instance(task_id=op4.task_id)

        # root is successful, but unfinished tasks
        dr.update_state()
        assert dr.state == DagRunState.RUNNING

        # one has failed, but root is successful
        ti_op2.set_state(state=TaskInstanceState.FAILED, session=session)
        ti_op3.set_state(state=TaskInstanceState.SUCCESS, session=session)
        ti_op4.set_state(state=TaskInstanceState.SUCCESS, session=session)
        dr.update_state()
        assert dr.state == DagRunState.SUCCESS

    def test_dagrun_deadlock(self, dag_maker, session):
        with dag_maker(schedule=datetime.timedelta(days=1), session=session):
            op1 = EmptyOperator(task_id="A")
            op2 = EmptyOperator(task_id="B")
            op2.trigger_rule = TriggerRule.ONE_FAILED
            op2.set_upstream(op1)

        dr = dag_maker.create_dagrun()

        ti_op1: TI = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti_op2: TI = dr.get_task_instance(task_id=op2.task_id, session=session)
        ti_op1.set_state(state=TaskInstanceState.SUCCESS, session=session)
        ti_op2.set_state(state=None, session=session)

        dr.update_state(session=session)
        assert dr.state == DagRunState.RUNNING

        ti_op2.set_state(state=None, session=session)
        ti_op2.task.trigger_rule = "invalid"  # type: ignore
        dr.update_state(session=session)
        assert dr.state == DagRunState.FAILED

    def test_dagrun_no_deadlock_with_restarting(self, dag_maker, session):
        with dag_maker(schedule=datetime.timedelta(days=1)):
            op1 = EmptyOperator(task_id="upstream_task")
            op2 = EmptyOperator(task_id="downstream_task")
            op2.set_upstream(op1)

        dr = dag_maker.create_dagrun()
        upstream_ti = dr.get_task_instance(task_id="upstream_task")
        upstream_ti.set_state(TaskInstanceState.RESTARTING, session=session)

        dr.update_state()
        assert dr.state == DagRunState.RUNNING

    def test_dagrun_no_deadlock_with_depends_on_past(self, dag_maker, session):
        with dag_maker(schedule=datetime.timedelta(days=1)):
            EmptyOperator(task_id="dop", depends_on_past=True)
            EmptyOperator(task_id="tc", max_active_tis_per_dag=1)

        dr = dag_maker.create_dagrun(
            run_id="test_dagrun_no_deadlock_1",
            run_type=DagRunType.SCHEDULED,
            start_date=DEFAULT_DATE,
        )
        next_date = DEFAULT_DATE + datetime.timedelta(days=1)
        dr2 = dag_maker.create_dagrun(
            run_id="test_dagrun_no_deadlock_2",
            start_date=DEFAULT_DATE + datetime.timedelta(days=1),
            logical_date=next_date,
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

    def test_dagrun_success_callback(self, dag_maker, session):
        def on_success_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_success_callback"

        with dag_maker(
            dag_id="test_dagrun_success_callback",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_success_callback=on_success_callable,
        ) as dag:
            ...
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
        assert dag_run.state == DagRunState.SUCCESS
        # Callbacks are not added until handle_callback = False is passed to dag_run.update_state()
        assert callback is None

    def test_dagrun_failure_callback(self, dag_maker, session):
        def on_failure_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_failure_callback"

        with dag_maker(
            dag_id="test_dagrun_failure_callback",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_failure_callback=on_failure_callable,
        ) as dag:
            ...
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
        assert dag_run.state == DagRunState.FAILED
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
        dag.sync_to_db()
        SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SKIPPED,
        }

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        _, _ = dag_run.update_state(execute_callbacks=True)
        task = dag_run.get_task_instances()[0]

        assert task.state == TaskInstanceState.SKIPPED
        assert dag_run.state == DagRunState.SUCCESS
        mock_on_success.assert_called_once()

    def test_start_dr_spans_if_needed_new_span(self, testing_dag_bundle, dag_maker, session):
        with dag_maker(
            dag_id="test_start_dr_spans_if_needed_new_span",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
        ) as dag:
            ...
        DAG.bulk_write_to_db("testing", None, dags=[dag], session=session)

        dag_task1 = EmptyOperator(task_id="test_task1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_task2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_task1": TaskInstanceState.QUEUED,
            "test_task2": TaskInstanceState.QUEUED,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)

        active_spans = ThreadSafeDict()
        dag_run.set_active_spans(active_spans)

        tis = dag_run.get_task_instances()

        assert dag_run.active_spans is not None
        assert dag_run.active_spans.get(dag_run.run_id) is None
        assert dag_run.span_status == SpanStatus.NOT_STARTED

        dag_run.start_dr_spans_if_needed(tis=tis)

        assert dag_run.span_status == SpanStatus.ACTIVE
        assert dag_run.active_spans.get(dag_run.run_id) is not None

    def test_start_dr_spans_if_needed_span_with_continuance(self, testing_dag_bundle, dag_maker, session):
        with dag_maker(
            dag_id="test_start_dr_spans_if_needed_span_with_continuance",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
        ) as dag:
            ...
        DAG.bulk_write_to_db("testing", None, dags=[dag], session=session)

        dag_task1 = EmptyOperator(task_id="test_task1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_task2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_task1": TaskInstanceState.RUNNING,
            "test_task2": TaskInstanceState.QUEUED,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)

        active_spans = ThreadSafeDict()
        dag_run.set_active_spans(active_spans)

        dag_run.span_status = SpanStatus.NEEDS_CONTINUANCE

        tis = dag_run.get_task_instances()

        first_ti = tis[0]
        first_ti.span_status = SpanStatus.NEEDS_CONTINUANCE

        assert dag_run.active_spans is not None
        assert dag_run.active_spans.get(dag_run.run_id) is None
        assert dag_run.active_spans.get(first_ti.key) is None
        assert dag_run.span_status == SpanStatus.NEEDS_CONTINUANCE
        assert first_ti.span_status == SpanStatus.NEEDS_CONTINUANCE

        dag_run.start_dr_spans_if_needed(tis=tis)

        assert dag_run.span_status == SpanStatus.ACTIVE
        assert first_ti.span_status == SpanStatus.ACTIVE
        assert dag_run.active_spans.get(dag_run.run_id) is not None
        assert dag_run.active_spans.get(first_ti.key) is not None

    def test_end_dr_span_if_needed(self, testing_dag_bundle, dag_maker, session):
        with dag_maker(
            dag_id="test_end_dr_span_if_needed",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
        ) as dag:
            ...
        DAG.bulk_write_to_db("testing", None, dags=[dag], session=session)

        dag_task1 = EmptyOperator(task_id="test_task1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_task2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_task1": TaskInstanceState.SUCCESS,
            "test_task2": TaskInstanceState.SUCCESS,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)

        active_spans = ThreadSafeDict()
        dag_run.set_active_spans(active_spans)

        from airflow.traces.tracer import Trace

        dr_span = Trace.start_root_span(span_name="test_span", start_as_current=False)

        active_spans.set(dag_run.run_id, dr_span)

        assert dag_run.active_spans is not None
        assert dag_run.active_spans.get(dag_run.run_id) is not None

        dag_run.end_dr_span_if_needed()

        assert dag_run.span_status == SpanStatus.ENDED
        assert dag_run.active_spans.get(dag_run.run_id) is None

    def test_end_dr_span_if_needed_with_span_from_another_scheduler(
        self, testing_dag_bundle, dag_maker, session
    ):
        with dag_maker(
            dag_id="test_end_dr_span_if_needed_with_span_from_another_scheduler",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
        ) as dag:
            ...
        DAG.bulk_write_to_db("testing", None, dags=[dag], session=session)

        dag_task1 = EmptyOperator(task_id="test_task1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_task2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_task1": TaskInstanceState.SUCCESS,
            "test_task2": TaskInstanceState.SUCCESS,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)

        active_spans = ThreadSafeDict()
        dag_run.set_active_spans(active_spans)

        dag_run.span_status = SpanStatus.ACTIVE

        assert dag_run.active_spans is not None
        assert dag_run.active_spans.get(dag_run.run_id) is None

        dag_run.end_dr_span_if_needed()

        assert dag_run.span_status == SpanStatus.SHOULD_END

    def test_dagrun_update_state_with_handle_callback_success(self, testing_dag_bundle, dag_maker, session):
        def on_success_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_update_state_with_handle_callback_success"

        relative_fileloc = "test_dagrun_update_state_with_handle_callback_success.py"
        with dag_maker(
            dag_id="test_dagrun_update_state_with_handle_callback_success",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_success_callback=on_success_callable,
        ) as dag:
            ...
        DAG.bulk_write_to_db("testing", None, dags=[dag], session=session)
        dm = DagModel.get_dagmodel(dag.dag_id, session=session)
        dm.relative_fileloc = relative_fileloc
        session.merge(dm)
        session.commit()

        dag_task1 = EmptyOperator(task_id="test_state_succeeded1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_state_succeeded2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SUCCESS,
            "test_state_succeeded2": TaskInstanceState.SUCCESS,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dag.relative_fileloc = relative_fileloc
        SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)
        session.commit()

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        dag_run.dag_model = dm

        _, callback = dag_run.update_state(execute_callbacks=False)
        assert dag_run.state == DagRunState.SUCCESS
        # Callbacks are not added until handle_callback = False is passed to dag_run.update_state()
        assert callback == DagCallbackRequest(
            filepath=dag_run.dag.relative_fileloc,
            dag_id="test_dagrun_update_state_with_handle_callback_success",
            run_id=dag_run.run_id,
            is_failure_callback=False,
            bundle_name="testing",
            bundle_version=None,
            msg="success",
        )

    def test_dagrun_update_state_with_handle_callback_failure(self, testing_dag_bundle, dag_maker, session):
        def on_failure_callable(context):
            assert context["dag_run"].dag_id == "test_dagrun_update_state_with_handle_callback_failure"

        relative_fileloc = "test_dagrun_update_state_with_handle_callback_failure.py"
        with dag_maker(
            dag_id="test_dagrun_update_state_with_handle_callback_failure",
            schedule=datetime.timedelta(days=1),
            start_date=datetime.datetime(2017, 1, 1),
            on_failure_callback=on_failure_callable,
        ) as dag:
            ...
        DAG.bulk_write_to_db("testing", None, dags=[dag], session=session)
        dm = DagModel.get_dagmodel(dag.dag_id, session=session)
        dm.relative_fileloc = relative_fileloc
        session.merge(dm)
        session.commit()

        dag_task1 = EmptyOperator(task_id="test_state_succeeded1", dag=dag)
        dag_task2 = EmptyOperator(task_id="test_state_failed2", dag=dag)
        dag_task1.set_downstream(dag_task2)

        initial_task_states = {
            "test_state_succeeded1": TaskInstanceState.SUCCESS,
            "test_state_failed2": TaskInstanceState.FAILED,
        }

        # Scheduler uses Serialized DAG -- so use that instead of the Actual DAG
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dag.relative_fileloc = relative_fileloc
        SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)
        session.commit()

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        dag_run.dag_model = dm

        _, callback = dag_run.update_state(execute_callbacks=False)
        assert dag_run.state == DagRunState.FAILED
        # Callbacks are not added until handle_callback = False is passed to dag_run.update_state()

        assert callback == DagCallbackRequest(
            filepath=dag.relative_fileloc,
            dag_id="test_dagrun_update_state_with_handle_callback_failure",
            run_id=dag_run.run_id,
            is_failure_callback=True,
            msg="task_failure",
            bundle_name="testing",
            bundle_version=None,
        )

    def test_dagrun_set_state_end_date(self, dag_maker, session):
        with dag_maker(schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE):
            pass

        dr = dag_maker.create_dagrun()

        # Initial end_date should be NULL
        # DagRunState.SUCCESS and DagRunState.FAILED are all ending state and should set end_date
        # DagRunState.RUNNING set end_date back to NULL
        session.add(dr)
        session.commit()
        assert dr.end_date is None

        dr.set_state(DagRunState.SUCCESS)
        session.merge(dr)
        session.commit()

        dr_database = session.query(DagRun).filter(DagRun.run_id == dr.run_id).one()
        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

        dr.set_state(DagRunState.RUNNING)
        session.merge(dr)
        session.commit()

        dr_database = session.query(DagRun).filter(DagRun.run_id == dr.run_id).one()

        assert dr_database.end_date is None

        dr.set_state(DagRunState.FAILED)
        session.merge(dr)
        session.commit()
        dr_database = session.query(DagRun).filter(DagRun.run_id == dr.run_id).one()

        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

    def test_dagrun_update_state_end_date(self, dag_maker, session):
        # A -> B
        with dag_maker(schedule=datetime.timedelta(days=1)):
            op1 = EmptyOperator(task_id="A")
            op2 = EmptyOperator(task_id="B")
            op1.set_upstream(op2)

        dr = dag_maker.create_dagrun()

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

        dr_database = session.query(DagRun).filter(DagRun.run_id == dr.run_id).one()
        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

        ti_op1.set_state(state=TaskInstanceState.RUNNING, session=session)
        ti_op2.set_state(state=TaskInstanceState.RUNNING, session=session)
        dr.update_state()

        dr_database = session.query(DagRun).filter(DagRun.run_id == dr.run_id).one()

        assert dr._state == DagRunState.RUNNING
        assert dr.end_date is None
        assert dr_database.end_date is None

        ti_op1.set_state(state=TaskInstanceState.FAILED, session=session)
        ti_op2.set_state(state=TaskInstanceState.FAILED, session=session)
        dr.update_state()

        dr_database = session.query(DagRun).filter(DagRun.run_id == dr.run_id).one()

        assert dr_database.end_date is not None
        assert dr.end_date == dr_database.end_date

    def test_get_task_instance_on_empty_dagrun(self, dag_maker, session):
        """
        Make sure that a proper value is returned when a dagrun has no task instances
        """
        with dag_maker(
            dag_id="test_get_task_instance_on_empty_dagrun",
            schedule=datetime.timedelta(days=1),
            start_date=timezone.datetime(2017, 1, 1),
        ) as dag:
            ...
        ShortCircuitOperator(task_id="test_short_circuit_false", dag=dag, python_callable=lambda: False)

        now = timezone.utcnow()

        # Don't use create_dagrun since it will create the task instances too which we
        # don't want
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_id="test_get_task_instance_on_empty_dagrun",
            run_type=DagRunType.MANUAL,
            logical_date=now,
            start_date=now,
            state=DagRunState.RUNNING,
        )
        session.add(dag_run)
        session.commit()

        ti = dag_run.get_task_instance("test_short_circuit_false")
        assert ti is None

    def test_get_latest_runs(self, dag_maker, session):
        with dag_maker(
            dag_id="test_latest_runs_1", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE
        ) as dag:
            ...
        self.create_dag_run(dag, logical_date=timezone.datetime(2015, 1, 1), session=session)
        self.create_dag_run(dag, logical_date=timezone.datetime(2015, 1, 2), session=session)
        dagruns = DagRun.get_latest_runs(session)
        session.close()
        for dagrun in dagruns:
            if dagrun.dag_id == "test_latest_runs_1":
                assert dagrun.logical_date == timezone.datetime(2015, 1, 2)

    def test_removed_task_instances_can_be_restored(self, dag_maker, session):
        def with_all_tasks_removed(dag):
            with dag_maker(
                dag_id=dag.dag_id, schedule=datetime.timedelta(days=1), start_date=dag.start_date
            ) as dag:
                pass
            return dag

        with dag_maker(
            "test_task_restoration", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE
        ) as dag:
            ...
        dag.add_task(EmptyOperator(task_id="flaky_task", owner="test"))

        dagrun = self.create_dag_run(dag, session=session)
        flaky_ti = dagrun.get_task_instances()[0]
        assert flaky_ti.task_id == "flaky_task"
        assert flaky_ti.state is None

        dagrun.dag = with_all_tasks_removed(dag)
        dag_version_id = DagVersion.get_latest_version(dag.dag_id, session=session).id
        dagrun.verify_integrity(dag_version_id=dag_version_id)
        flaky_ti.refresh_from_db()
        assert flaky_ti.state is None

        dagrun.dag.add_task(EmptyOperator(task_id="flaky_task", owner="test"))

        dagrun.verify_integrity(dag_version_id=dag_version_id)
        flaky_ti.refresh_from_db()
        assert flaky_ti.state is None

    def test_already_added_task_instances_can_be_ignored(self, dag_maker, session):
        with dag_maker("triggered_dag", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE) as dag:
            ...
        dag.add_task(EmptyOperator(task_id="first_task", owner="test"))

        dagrun = self.create_dag_run(dag, session=session)
        first_ti = dagrun.get_task_instances()[0]
        assert first_ti.task_id == "first_task"
        assert first_ti.state is None

        # Lets assume that the above TI was added into DB by webserver, but if scheduler
        # is running the same method at the same time it would find 0 TIs for this dag
        # and proceeds further to create TIs. Hence mocking DagRun.get_task_instances
        # method to return an empty list of TIs.
        with mock.patch.object(DagRun, "get_task_instances") as mock_gtis:
            mock_gtis.return_value = []
            dagrun.verify_integrity(
                dag_version_id=DagVersion.get_latest_version(dag.dag_id, session=session).id
            )
            first_ti.refresh_from_db()
            assert first_ti.state is None

    @pytest.mark.parametrize("state", State.task_states)
    @mock.patch.object(settings, "task_instance_mutation_hook", autospec=True)
    def test_task_instance_mutation_hook(self, mock_hook, dag_maker, session, state):
        def mutate_task_instance(task_instance):
            if task_instance.queue == "queue1":
                task_instance.queue = "queue2"
            else:
                task_instance.queue = "queue1"

        mock_hook.side_effect = mutate_task_instance

        with dag_maker(
            "test_task_instance_mutation_hook",
            schedule=datetime.timedelta(days=1),
            start_date=DEFAULT_DATE,
        ) as dag:
            ...
        dag.add_task(EmptyOperator(task_id="task_to_mutate", owner="test", queue="queue1"))

        dagrun = self.create_dag_run(dag, session=session)
        task = dagrun.get_task_instances()[0]
        task.state = state
        session.merge(task)
        session.commit()
        assert task.queue == "queue2"

        dagrun.verify_integrity(dag_version_id=DagVersion.get_latest_version(dag.dag_id, session=session).id)
        task = dagrun.get_task_instances()[0]
        assert task.queue == "queue1"

    @pytest.mark.parametrize(
        "prev_ti_state, is_ti_schedulable",
        [
            (TaskInstanceState.SUCCESS, True),
            (TaskInstanceState.SKIPPED, True),
            (TaskInstanceState.RUNNING, False),
            (TaskInstanceState.FAILED, False),
            (None, False),
        ],
    )
    def test_depends_on_past(self, dag_maker, session, prev_ti_state, is_ti_schedulable):
        # DAG tests depends_on_past dependencies
        with dag_maker(
            dag_id="test_depends_on_past", schedule=datetime.timedelta(days=1), session=session
        ) as dag:
            BaseOperator(
                task_id="test_dop_task",
                depends_on_past=True,
            )

        task = dag.tasks[0]

        dag_run_1: DagRun = dag_maker.create_dagrun(
            logical_date=timezone.datetime(2016, 1, 1, 0, 0, 0),
            run_type=DagRunType.SCHEDULED,
        )
        dag_run_2: DagRun = dag_maker.create_dagrun(
            logical_date=timezone.datetime(2016, 1, 2, 0, 0, 0),
            run_type=DagRunType.SCHEDULED,
        )

        prev_ti = TI(task, run_id=dag_run_1.run_id)
        prev_ti.refresh_from_db(session=session)
        prev_ti.set_state(prev_ti_state, session=session)
        session.flush()
        ti = TI(task, run_id=dag_run_2.run_id)
        ti.refresh_from_db(session=session)

        decision = dag_run_2.task_instance_scheduling_decisions(session=session)
        schedulable_tis = [ti.task_id for ti in decision.schedulable_tis]
        assert ("test_dop_task" in schedulable_tis) == is_ti_schedulable

    @pytest.mark.parametrize(
        "prev_ti_state, is_ti_schedulable",
        [
            (TaskInstanceState.SUCCESS, True),
            (TaskInstanceState.SKIPPED, True),
            (TaskInstanceState.RUNNING, False),
            (TaskInstanceState.FAILED, False),
            (None, False),
        ],
    )
    def test_wait_for_downstream(self, dag_maker, session, prev_ti_state, is_ti_schedulable):
        dag_id = "test_wait_for_downstream"

        with dag_maker(dag_id=dag_id, session=session) as dag:
            dag_wfd_upstream = EmptyOperator(
                task_id="upstream_task",
                wait_for_downstream=True,
            )
            dag_wfd_downstream = EmptyOperator(task_id="downstream_task")
            dag_wfd_upstream >> dag_wfd_downstream
        upstream, downstream = dag.tasks

        # For ti.set_state() to work, the DagRun has to exist,
        # Otherwise ti.previous_ti returns an unpersisted TI
        dag_run_1: DagRun = dag_maker.create_dagrun(
            logical_date=timezone.datetime(2016, 1, 1, 0, 0, 0),
            run_type=DagRunType.SCHEDULED,
        )
        dag_run_2: DagRun = dag_maker.create_dagrun(
            logical_date=timezone.datetime(2016, 1, 2, 0, 0, 0),
            run_type=DagRunType.SCHEDULED,
        )

        ti = dag_run_2.get_task_instance(task_id=upstream.task_id, session=session)
        prev_ti_downstream = dag_run_1.get_task_instance(task_id=downstream.task_id, session=session)
        prev_ti_upstream = ti.get_previous_ti(session=session)
        assert ti
        assert prev_ti_upstream
        assert prev_ti_downstream
        prev_ti_upstream.state = TaskInstanceState.SUCCESS

        prev_ti_downstream.state = prev_ti_state
        session.flush()

        decision = dag_run_2.task_instance_scheduling_decisions(session=session)
        schedulable_tis = [ti.task_id for ti in decision.schedulable_tis]
        assert (upstream.task_id in schedulable_tis) == is_ti_schedulable

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
            is_stale=False,
        )
        session.add(orm_dag)
        session.flush()
        SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)
        dr = dag.create_dagrun(
            run_id=dag.timetable.generate_run_id(
                run_type=DagRunType.SCHEDULED,
                run_after=DEFAULT_DATE,
                data_interval=dag.infer_automated_data_interval(DEFAULT_DATE),
            ),
            run_type=DagRunType.SCHEDULED,
            state=state,
            logical_date=DEFAULT_DATE,
            data_interval=dag.infer_automated_data_interval(DEFAULT_DATE),
            run_after=DEFAULT_DATE,
            start_date=DEFAULT_DATE if state == DagRunState.RUNNING else None,
            session=session,
            triggered_by=DagRunTriggeredByType.TEST,
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

        dag.sync_to_db(session=session)
        SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)

        initial_task_states = {
            dag_task.task_id: TaskInstanceState.SUCCESS,
        }

        dag_run = self.create_dag_run(dag=dag, task_states=initial_task_states, session=session)
        dag_run.update_state(session=session)
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
            orm_dag_kwargs = {"dag_id": dag.dag_id, "has_task_concurrency_limits": False, "is_stale": False}
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
            SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)
            dag_run = dag.create_dagrun(
                run_id=dag.timetable.generate_run_id(
                    run_type=DagRunType.SCHEDULED,
                    run_after=dag.start_date,
                    data_interval=dag.infer_automated_data_interval(dag.start_date),
                ),
                run_type=DagRunType.SCHEDULED,
                state=DagRunState.SUCCESS,
                logical_date=dag.start_date,
                data_interval=dag.infer_automated_data_interval(dag.start_date),
                run_after=dag.start_date,
                start_date=dag.start_date,
                triggered_by=DagRunTriggeredByType.TEST,
                session=session,
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

    def test_states_sets(self, dag_maker, session):
        """
        Tests that adding State.failed_states and State.success_states work as expected.
        """
        with dag_maker(
            dag_id="test_dagrun_states", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE
        ) as dag:
            ...
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
        dr = dag_maker.create_dagrun(state=DagRunState.FAILED)
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

    def test_dag_run_dag_versions_method(self, dag_maker, session):
        with dag_maker(
            "test_dag_run_dag_versions", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE
        ):
            EmptyOperator(task_id="empty")
        dag_run = dag_maker.create_dagrun()

        dm = session.query(DagModel).options(joinedload(DagModel.dag_versions)).one()
        assert dag_run.dag_versions[0].id == dm.dag_versions[0].id

    def test_dag_run_version_number(self, dag_maker, session):
        with dag_maker(
            "test_dag_run_version_number", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE
        ):
            EmptyOperator(task_id="empty") >> EmptyOperator(task_id="empty2")
        dag_run = dag_maker.create_dagrun()
        tis = dag_run.task_instances
        tis[0].set_state(TaskInstanceState.SUCCESS)
        dag_v = DagVersion.write_dag(dag_id=dag_run.dag_id, bundle_name="testing", version_number=2)
        tis[1].dag_version = dag_v
        session.merge(tis[1])
        session.flush()
        dag_run = session.query(DagRun).filter(DagRun.run_id == dag_run.run_id).one()
        # Check that dag_run.version_number returns the version number of
        # the latest task instance dag_version
        assert dag_run.version_number == dag_v.version_number


@pytest.mark.parametrize(
    ("run_type", "expected_tis"),
    [
        pytest.param(DagRunType.MANUAL, 1, id="manual"),
        pytest.param(DagRunType.BACKFILL_JOB, 3, id="backfill"),
    ],
)
@mock.patch.object(Stats, "incr")
def test_verify_integrity_task_start_and_end_date(Stats_incr, dag_maker, session, run_type, expected_tis):
    """Test that tasks with specific dates are only created for backfill runs"""
    with dag_maker("test", schedule=datetime.timedelta(days=1), start_date=DEFAULT_DATE) as dag:
        EmptyOperator(task_id="without")
        EmptyOperator(task_id="with_start_date", start_date=DEFAULT_DATE + datetime.timedelta(1))
        EmptyOperator(task_id="with_end_date", end_date=DEFAULT_DATE - datetime.timedelta(1))

    dag_run = DagRun(
        dag_id=dag.dag_id,
        run_type=run_type,
        logical_date=DEFAULT_DATE,
        run_id=DagRun.generate_run_id(run_type=run_type, logical_date=DEFAULT_DATE, run_after=DEFAULT_DATE),
    )
    dag_run.dag = dag

    session.add(dag_run)
    session.flush()
    dag_version_id = DagVersion.get_latest_version(dag.dag_id, session=session).id
    dag_run.verify_integrity(dag_version_id=dag_version_id, session=session)

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

    @task
    def task_2(arg2): ...

    with dag_maker(session=session):
        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()

    query = (
        select(TI.map_index, TI.state)
        .filter_by(task_id="task_2", dag_id=dr.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
    )
    indices = session.execute(query).all()

    assert indices == [(0, None), (1, None), (2, None), (3, None)]

    # Now "change" the DAG and we should see verify_integrity REMOVE some TIs
    with dag_maker(session=session):
        task_2.expand(arg2=[1, 2])

    # Update it to use the new serialized DAG
    dr.dag = dag_maker.dag
    dag_version_id = DagVersion.get_latest_version(dag_id=dr.dag_id, session=session).id
    dr.verify_integrity(dag_version_id=dag_version_id, session=session)

    indices = session.execute(query).all()
    assert indices == [(0, None), (1, None), (2, TaskInstanceState.REMOVED), (3, TaskInstanceState.REMOVED)]


def test_mapped_literal_to_xcom_arg_verify_integrity(dag_maker, session):
    """Test that when we change from literal to a XComArg the TIs are removed"""

    @task
    def task_2(arg2): ...

    with dag_maker(session=session):
        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()

    with dag_maker(session=session):
        t1 = BaseOperator(task_id="task_1")
        task_2.expand(arg2=t1.output)

    dr.dag = dag_maker.dag
    dag_version_id = DagVersion.get_latest_version(dag_id=dr.dag_id, session=session).id
    dr.verify_integrity(dag_version_id=dag_version_id, session=session)

    indices = (
        session.query(TI.map_index, TI.state)
        .filter_by(task_id="task_2", dag_id=dr.dag_id, run_id=dr.run_id)
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

    @task
    def task_2(arg2): ...

    with dag_maker(session=session):
        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()

    query = (
        select(TI.map_index, TI.state)
        .filter_by(task_id="task_2", dag_id=dr.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
    )
    indices = session.execute(query).all()
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
    ]

    # Now "increase" the length of literal
    with dag_maker(session=session):
        task_2.expand(arg2=[1, 2, 3, 4, 5])

    dr.dag = dag_maker.dag
    # Every mapped task is revised at task_instance_scheduling_decision
    dr.task_instance_scheduling_decisions()

    indices = session.execute(query).all()
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
        (4, State.NONE),
    ]


def test_mapped_literal_length_reduction_adds_removed_state(dag_maker, session):
    """Test that when the length of mapped literal reduces, removed state is added"""

    @task
    def task_2(arg2): ...

    with dag_maker(session=session):
        task_2.expand(arg2=[1, 2, 3, 4])

    dr = dag_maker.create_dagrun()
    query = (
        select(TI.map_index, TI.state)
        .filter_by(task_id="task_2", dag_id=dr.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
    )
    indices = session.execute(query).all()
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.NONE),
        (3, State.NONE),
    ]

    with dag_maker(session=session):
        task_2.expand(arg2=[1, 2])

    dr.dag = dag_maker.dag
    # Since we change the literal on the dag file itself, the dag_hash will
    # change which will have the scheduler verify the dr integrity
    dag_version_id = DagVersion.get_latest_version(dag_id=dr.dag_id, session=session).id
    dr.verify_integrity(dag_version_id=dag_version_id, session=session)

    indices = session.execute(query).all()
    assert sorted(indices) == [
        (0, State.NONE),
        (1, State.NONE),
        (2, State.REMOVED),
        (3, State.REMOVED),
    ]


def test_mapped_length_increase_at_runtime_adds_additional_tis(dag_maker, session):
    """Test that when the length of mapped literal increases at runtime, additional ti is added"""
    # Variable.set(key="arg1", value=[1, 2, 3])

    @task
    def task_1():
        # Behave as if we did this
        # return Variable.get("arg1", deserialize_json=True)
        ...

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr: DagRun = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1", session=session)
    assert ti
    ti.state = TaskInstanceState.SUCCESS
    # Behave as if TI ran after: Variable.set(key="arg1", value=[1, 2, 3])
    session.add(TaskMap.from_task_instance_xcom(ti, [1, 2, 3]))
    session.flush()

    decision = dr.task_instance_scheduling_decisions(session=session)
    indices = [(ti.task_id, ti.map_index) for ti in decision.schedulable_tis]
    assert indices == [("task_2", 0), ("task_2", 1), ("task_2", 2)]

    # Now "clear" and "increase" the length of literal
    dag.clear()

    # "Run" the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1", session=session)
    assert ti
    # Behave as if we did and re-ran the task: Variable.set(key="arg1", value=[1, 2, 3, 4])
    session.merge(TaskMap.from_task_instance_xcom(ti, [1, 2, 3, 4]))
    ti.state = TaskInstanceState.SUCCESS
    session.flush()

    # this would be called by the localtask job
    decision = dr.task_instance_scheduling_decisions(session=session)
    indices = [(ti.task_id, ti.state, ti.map_index) for ti in decision.schedulable_tis]
    assert sorted(indices) == [
        ("task_2", None, 0),
        ("task_2", None, 1),
        ("task_2", None, 2),
        ("task_2", None, 3),
    ]


def test_mapped_literal_length_reduction_at_runtime_adds_removed_state(dag_maker, session):
    """
    Test that when the length of mapped literal reduces at runtime, the missing task instances
    are marked as removed
    """

    @task
    def task_1():
        # return Variable.get("arg1", deserialize_json=True)
        ...

    with dag_maker(session=session) as dag:

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr: DagRun = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1", session=session)
    assert ti
    ti.state = TaskInstanceState.SUCCESS
    # Behave as if TI ran after: Variable.set(key="arg1", value=[1, 2, 3])
    session.add(TaskMap.from_task_instance_xcom(ti, [1, 2, 3]))
    session.flush()

    dr.task_instance_scheduling_decisions(session=session)
    query = (
        select(TI.map_index, TI.state)
        .filter_by(task_id="task_2", dag_id=dr.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
    )
    indices = session.execute(query).all()
    assert indices == [(0, None), (1, None), (2, None)]

    # Now "clear" and "reduce" the length of literal
    dag.clear()

    # "Run" the first task again to get the new lengths
    ti = dr.get_task_instance(task_id="task_1", session=session)
    assert ti
    # Behave as if we did and re-ran the task: Variable.set(key="arg1", value=[1, 2])
    session.merge(TaskMap.from_task_instance_xcom(ti, [1, 2]))
    ti.state = TaskInstanceState.SUCCESS
    session.flush()
    dag_version_id = DagVersion.get_latest_version(dag.dag_id, session=session).id
    dr.verify_integrity(dag_version_id=dag_version_id, session=session)
    indices = session.execute(query).all()
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


def test_calls_to_verify_integrity_with_mapped_task_zero_length_at_runtime(dag_maker, session, caplog):
    """
    Test zero length reduction in mapped task at runtime with calls to dagrun.verify_integrity
    """
    import logging

    with dag_maker(session=session) as dag:

        @task
        def task_1():
            # return Variable.get("arg1", deserialize_json=True)
            ...

        @task
        def task_2(arg2): ...

        task_2.expand(arg2=task_1())

    dr: DagRun = dag_maker.create_dagrun()
    ti = dr.get_task_instance(task_id="task_1", session=session)
    assert ti
    # "Run" task_1
    ti.state = TaskInstanceState.SUCCESS
    # Behave as if TI ran after: Variable.set(key="arg1", value=[1, 2, 3])
    session.add(TaskMap.from_task_instance_xcom(ti, [1, 2, 3]))
    session.flush()

    decision = dr.task_instance_scheduling_decisions(session=session)
    ti_2 = decision.schedulable_tis[0]
    assert ti_2

    query = (
        select(TI.map_index, TI.state)
        .filter_by(task_id="task_2", dag_id=dr.dag_id, run_id=dr.run_id)
        .order_by(TI.map_index)
    )
    indices = session.execute(query).all()
    assert sorted(indices) == [(0, State.NONE), (1, State.NONE), (2, State.NONE)]

    # Now "clear" and "reduce" the length to empty list
    dag.clear()
    # We don't execute task anymore, but this is what we are
    # simulating happened:
    # Variable.set(key="arg1", value=[])
    session.merge(TaskMap.from_task_instance_xcom(ti, []))
    session.flush()

    # Run the first task again to get the new lengths
    with caplog.at_level(logging.DEBUG):
        # Run verify_integrity as a whole and assert the tasks were removed
        dag_version = DagVersion.get_latest_version(dag.dag_id)
        dr.verify_integrity(dag_version_id=dag_version.id, session=session)
    indices = session.execute(query).all()
    assert indices == [
        (0, TaskInstanceState.REMOVED),
        (1, TaskInstanceState.REMOVED),
        (2, TaskInstanceState.REMOVED),
    ]


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
    dag_version_id = DagVersion.get_latest_version(dag_id=dr.dag_id, session=session).id
    dr.verify_integrity(dag_version_id=dag_version_id, session=session)
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
    with dag_maker(session=session) as dag:

        @dag.task
        def make_list():
            return [1, 2]

        @dag.task
        def double(value):
            return value * 2

        @dag.task
        def consumer(value):
            ...
            # result = list(value)

        consumer(value=double.expand(value=make_list()))

    dr: DagRun = dag_maker.create_dagrun()

    def _task_ids(tis):
        return [ti.task_id for ti in tis]

    # The first task is always make_list.
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["make_list"]

    # After make_list is run, double is expanded.
    ti = decision.schedulable_tis[0]
    ti.state = TaskInstanceState.SUCCESS
    session.add(TaskMap.from_task_instance_xcom(ti, [1, 2]))
    session.flush()

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["double", "double"]

    # Running just one of the mapped tis does not make downstream schedulable.
    ti = decision.schedulable_tis[0]
    ti.state = TaskInstanceState.SUCCESS
    session.flush()

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["double"]

    # Downstream is scheduleable after all mapped tis are run.
    ti = decision.schedulable_tis[0]
    ti.state = TaskInstanceState.SUCCESS
    session.flush()
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == ["consumer"]


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


@pytest.mark.xfail(reason="We can't keep this bevaviour with remote workers where scheduler can't reach xcom")
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
    from airflow.sdk import task

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
        pytest.param(None, [{"a": 1}], 1, id="simple"),
        pytest.param({"b": 2}, [{"a": 1}], 1, id="merge"),
        pytest.param({"b": 2}, [{"a": 1, "b": 3}], 1, id="override"),
    ],
)
def test_mapped_expand_against_params(dag_maker, partial_params, mapped_params, expected):
    with dag_maker():
        BaseOperator.partial(task_id="t", params=partial_params).expand(params=mapped_params)

    dr: DagRun = dag_maker.create_dagrun()
    decision = dr.task_instance_scheduling_decisions()
    assert len(decision.schedulable_tis) == expected


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
        def t(value): ...

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

    results = set()
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        results.add((ti.task_id, ti.map_index))
        ti.state = TaskInstanceState.SUCCESS
    session.flush()
    assert results == {("tg.t1", 0), ("tg.t1", 1), ("tg.t1", 2)}

    results.clear()
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        results.add((ti.task_id, ti.map_index))
        ti.state = TaskInstanceState.SUCCESS
    session.flush()
    assert results == {("tg.t2", 0), ("tg.t2", 1), ("tg.t2", 2)}

    results.clear()
    decision = dr.task_instance_scheduling_decisions(session=session)
    for ti in decision.schedulable_tis:
        results.add((ti.task_id, ti.map_index))
        ti.state = TaskInstanceState.SUCCESS
    session.flush()
    assert results == {("t3", -1)}


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
        ti.state = TaskInstanceState.SUCCESS
    session.flush()

    # Now print_value in dr2 can run
    decision = dr2.task_instance_scheduling_decisions(session=session)
    assert len(decision.schedulable_tis) == 2
    for ti in decision.schedulable_tis:
        ti.state = TaskInstanceState.SUCCESS
    session.flush()

    # Both runs are finished now.
    decision = dr1.task_instance_scheduling_decisions(session=session)
    assert len(decision.unfinished_tis) == 0
    decision = dr2.task_instance_scheduling_decisions(session=session)
    assert len(decision.unfinished_tis) == 0


def test_xcom_map_skip_raised(dag_maker, session):
    result = None

    with dag_maker(session=session) as dag:
        # Note: this doesn't actually run this dag, the callbacks are for reference only.

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def forward(value):
            return value

        @dag.task(trigger_rule=TriggerRule.ALL_DONE)
        def collect(value):
            nonlocal result
            result = list(value)

        def skip_c(v):
            ...
            # if v == "c":
            #     raise AirflowSkipException
            # return {"value": v}

        collect(value=forward.expand_kwargs(push().map(skip_c)))

    dr: DagRun = dag_maker.create_dagrun(session=session)

    def _task_ids(tis):
        return [(ti.task_id, ti.map_index) for ti in tis]

    # Check that when forward w/ map_index=2 ends up skipping, that the collect task can still be
    # scheduled!

    # Run "push".
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == [("push", -1)]
    ti = decision.schedulable_tis[0]
    ti.state = TaskInstanceState.SUCCESS
    session.add(TaskMap.from_task_instance_xcom(ti, push.function()))
    session.flush()

    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == [
        ("forward", 0),
        ("forward", 1),
        ("forward", 2),
    ]
    # Run "forward". "c"/index 2 is skipped. Runtime behaviour checked in test_xcom_map_raise_to_skip in
    # TaskSDK
    for ti, state in zip(
        decision.schedulable_tis,
        [TaskInstanceState.SUCCESS, TaskInstanceState.SUCCESS, TaskInstanceState.SKIPPED],
    ):
        ti.state = state
    session.flush()

    # Now "collect" should only get "a" and "b".
    decision = dr.task_instance_scheduling_decisions(session=session)
    assert _task_ids(decision.schedulable_tis) == [("collect", -1)]


def test_clearing_task_and_moving_from_non_mapped_to_mapped(dag_maker, session):
    """
    Test that clearing a task and moving from non-mapped to mapped clears existing
    references in XCom, TaskInstanceNote, TaskReschedule and
    RenderedTaskInstanceFields. To be able to test this, RenderedTaskInstanceFields
    was not used in the test since it would require that the task is expanded first.
    """
    from airflow.models.xcom import XComModel

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
        ti_id=ti.id,
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
    XComModel.set(key="test", value="value", task_id=ti.task_id, dag_id=dag.dag_id, run_id=ti.run_id)
    session.commit()
    for table in [TaskInstanceNote, TaskReschedule, XComModel]:
        assert session.query(table).count() == 1
    dr1.task_instance_scheduling_decisions(session)
    for table in [TaskInstanceNote, TaskReschedule, XComModel]:
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
            tasks = [make_task(x, dag_maker.dag) for x in line.split(" >> ")]
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
            pass
        run_type = DagRunType.from_run_id(run_id)
        if result:
            dag_maker.create_dagrun(run_id=run_id, run_type=run_type)
        else:
            with pytest.raises(ValueError):
                dag_maker.create_dagrun(run_id=run_id, run_type=run_type)
