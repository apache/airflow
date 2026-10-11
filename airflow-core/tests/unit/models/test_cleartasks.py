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
import random
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from unittest import mock

import pytest
from sqlalchemy import delete, event, func, select, update
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from airflow.api_fastapi.core_api.datamodels.task_instances import PatchTaskInstanceBody
from airflow.api_fastapi.core_api.services.public.dag_run import perform_clear_dag_run
from airflow.api_fastapi.core_api.services.public.task_instances import _patch_selected_task_state
from airflow.cli.commands.dag_command import _bulk_clear_runs
from airflow.exceptions import AirflowClearRunningTaskException
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun, clear_partition_runs
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import (
    LoopClearScope,
    TaskInstance,
    TaskInstance as TI,
    apply_loop_clear_scope,
    clear_loop_task_instances,
    clear_task_instances,
    clear_task_instances_for_runs,
    select_loop_clear_scope,
)
from airflow.models.taskreschedule import TaskReschedule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import task, task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.asserts import count_queries
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.taskinstance import run_task_instance
from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


def test_superseded_execution_is_archived_in_place_without_successor(dag_maker, session):
    with dag_maker("superseded_clear"):
        EmptyOperator(task_id="superseded")
        EmptyOperator(task_id="retried")
    dr = dag_maker.create_dagrun()
    superseded, retried = sorted(dr.task_instances, key=lambda ti: ti.task_id, reverse=True)
    superseded.state = retried.state = TaskInstanceState.SUCCESS
    session.flush()
    superseded_id, retried_id = superseded.id, retried.id

    clear_task_instances([superseded, retried], session, superseded_ti_ids={superseded_id})

    assert (superseded.id, superseded.working_set, superseded.archived_reason) == (
        superseded_id,
        None,
        "superseded",
    )
    assert superseded.state == TaskInstanceState.SUCCESS
    live = session.scalars(select(TI).where(TI.dag_id == dr.dag_id, TI.working_set.is_(True))).all()
    assert [ti.task_id for ti in live] == ["retried"]
    assert live[0].id != retried_id
    assert session.get(TI, retried_id).archived_reason == "retry"


@pytest.mark.parametrize(
    ("state", "expected_state"),
    [
        pytest.param(None, None, id="cleared"),
        pytest.param(TaskInstanceState.UP_FOR_RETRY, TaskInstanceState.UP_FOR_RETRY, id="up_for_retry"),
        pytest.param(TaskInstanceState.QUEUED, TaskInstanceState.QUEUED, id="queued"),
        pytest.param(TaskInstanceState.RUNNING, TaskInstanceState.FAILED, id="running"),
    ],
)
def test_superseding_carries_state_only_for_never_started_successor(
    dag_maker, session, state, expected_state
):
    with dag_maker("superseded_successor"):
        EmptyOperator(task_id="task")
    ti = dag_maker.create_dagrun().task_instances[0]
    ti.state = state
    ti.start_date = DEFAULT_DATE
    ti.end_date = DEFAULT_DATE + datetime.timedelta(seconds=5)
    session.flush()

    ti.archive(reason="superseded", session=session)

    assert ti.state == expected_state


def test_clearing_an_unmapped_task_instance_takes_no_dagrun_lock(dag_maker, session):
    with dag_maker("unmapped_clear_without_run_lock"):
        EmptyOperator(task_id="task")
    ti = dag_maker.create_dagrun().task_instances[0]
    statements = []

    @event.listens_for(session, "do_orm_execute")
    def capture_statement(orm_execute_state):
        statements.append(orm_execute_state.statement)

    try:
        clear_task_instances([ti], session)
    finally:
        event.remove(session, "do_orm_execute", capture_statement)

    assert not [
        statement
        for statement in statements
        if getattr(statement, "_for_update_arg", None) is not None
        and DagRun.__table__ in statement.get_final_froms()
    ]


def test_clearing_a_loop_gate_locks_its_dag_run_once(completed_loop, session):
    dr, loop, _ = completed_loop
    tis = dr.get_task_instances(session=session)
    selected = [
        ti for ti in tis if ti.region_index == 2 and ti.task_id in (loop.gate_task_id, "body.consume")
    ]
    session.commit()
    locks = []

    @event.listens_for(session, "do_orm_execute")
    def capture_statement(orm_execute_state):
        statement = orm_execute_state.statement
        if (
            getattr(statement, "_for_update_arg", None) is not None
            and DagRun.__table__ in statement.get_final_froms()
        ):
            locks.append(statement)

    try:
        clear_task_instances_for_runs(selected, session=session)
    finally:
        event.remove(session, "do_orm_execute", capture_statement)

    assert len(locks) == 1


def test_complete_restart_rejects_outcome_for_ordinary_execution(dag_maker, session):
    with dag_maker("ordinary_restart_outcome"):
        EmptyOperator(task_id="task")
    dr = dag_maker.create_dagrun()
    ti = dr.task_instances[0]
    ti.state = TaskInstanceState.RESTARTING
    session.flush()

    with pytest.raises(ValueError, match="requires a superseded execution"):
        ti.complete_restart(session=session, terminal_outcome=TaskInstanceState.FAILED)


def test_partition_clear_archives_legacy_expansion_once_across_batches(dag_maker, session):
    with dag_maker("legacy_partition_clear", serialized=True):
        MockOperator.partial(task_id="mapped").expand(arg2=list(range(1200)))
    dr = dag_maker.create_dagrun()
    session.execute(delete(TI).where(TI.dag_id == dr.dag_id))
    session.execute(delete(DynamicRegion).where(DynamicRegion.dag_id == dr.dag_id))
    session.add_all(
        TI(
            dag_maker.serialized_dag.get_task("mapped"),
            run_id=dr.run_id,
            dag_version_id=dr.created_dag_version_id,
            region_index=index,
            state=State.SUCCESS,
        )
        for index in range(1200)
    )
    dr.partition_key = "legacy"
    session.flush()

    result = clear_partition_runs(
        dag=None,
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        partition_key=None,
        partition_date_start=None,
        partition_date_end=None,
        clear_tis=True,
        dry_run=False,
        session=session,
    )
    session.flush()

    assert result == (1, 1200)
    assert session.scalar(select(func.count()).select_from(DynamicRegion)) == 1
    archived = session.scalars(
        select(TI)
        .where(TI.dag_id == dr.dag_id, TI.working_set.is_(None))
        .execution_options(include_all_attempts=True)
    ).all()
    assert len(archived) == 1200
    assert {ti.archived_reason for ti in archived} == {"superseded"}
    assert len(session.scalars(select(TI).where(TI.dag_id == dr.dag_id)).all()) == 1


@mock.patch("airflow.models.dagrun._TI_CHUNK_SIZE", 4)
def test_partition_clear_keeps_loop_runs_whole_across_batches(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    dr.partition_key = "loop"
    for ti in tis:
        ti.state = State.SUCCESS
    session.flush()
    original = [(ti.id, ti.task_id, iteration(ti) or 0) for ti in tis]

    result = clear_partition_runs(
        dag=None,
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        partition_key=None,
        partition_date_start=None,
        partition_date_end=None,
        clear_tis=True,
        dry_run=False,
        session=session,
    )
    session.flush()

    assert result == (1, len(original))
    for ti_id, task_id, pass_index in original:
        archived = session.get(TaskInstance, ti_id)
        later_pass = task_id != "outside" and pass_index >= 1
        assert (archived.working_set, archived.archived_reason) == (
            None,
            "superseded" if later_pass else "retry",
        ), (task_id, pass_index)


def test_loop_clear_scope_adds_setups_and_teardowns_of_selected_tasks(dag_maker, session):
    @task_group
    def body():
        EmptyOperator(task_id="work")

    with dag_maker("loop_with_setup_teardown", serialized=True):
        create_loop(body, max_iterations=2)
        setup_t = EmptyOperator(task_id="setup_t").as_setup()
        normal_t = EmptyOperator(task_id="normal_t")
        teardown_t = EmptyOperator(task_id="teardown_t").as_teardown(setups=setup_t)
        setup_t >> normal_t >> teardown_t
    dr = dag_maker.create_dagrun()
    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}

    without = select_loop_clear_scope([tis["normal_t"]], downstream=False, session=session)
    with_pairs = select_loop_clear_scope(
        [tis["normal_t"]], downstream=False, include_setups_and_teardowns=True, session=session
    )

    assert without.retry_ids == {tis["normal_t"].id}
    assert with_pairs.retry_ids == {tis[task_id].id for task_id in ("setup_t", "normal_t", "teardown_t")}


@pytest.mark.parametrize("exclude_task_ids", [frozenset(), frozenset({"body.process"})])
def test_dry_run_clear_lists_later_loop_passes_that_clearing_a_gate_archives(
    loop_run, dag_maker, session, exclude_task_ids
):
    dr, dag, loop, root, tis, iteration = loop_run

    listed = dag_maker.serialized_dag.clear(
        task_ids=[loop.gate_task_id],
        run_id=dr.run_id,
        dry_run=True,
        exclude_task_ids=exclude_task_ids,
        session=session,
    )

    assert {ti.id for ti in listed} == {
        ti.id
        for ti in tis
        if ti.task_id == loop.gate_task_id or (ti.task_id != "outside" and (iteration(ti) or 0) >= 1)
    }
    assert len(listed) == len({ti.id for ti in listed})


def test_loop_clear_scope_query_count_does_not_grow_with_mapped_width(dag_maker, session):
    def select_scope(width):
        with dag_maker(f"scope_queries_{width}", serialized=True):
            MockOperator.partial(task_id="mapped").expand(arg2=list(range(width))) >> EmptyOperator(
                task_id="after"
            )
        dr = dag_maker.create_dagrun(run_id=f"scope_queries_run_{width}")
        selected = [ti for ti in dr.get_task_instances(session=session) if ti.task_id == "mapped"]
        assert len(selected) == width
        with count_queries() as queries:
            scope = select_loop_clear_scope(selected, session=session)
        assert len(scope.retry_ids) == width + 1
        return sum(queries.values())

    assert select_scope(8) == select_scope(2)


class TestClearTasks:
    def test_clear_attached_attempt_keeps_pending_changes_on_archived_uuid(
        self, create_task_instance, session
    ):
        attempt = create_task_instance(state=TaskInstanceState.SUCCESS, session=session)
        old_id = attempt.id
        attempt.try_number = 3
        attempt.external_executor_id = "pending-executor"

        successor = clear_task_instances([attempt], session=session)[0]
        session.commit()

        archived = session.get(TaskInstance, old_id)
        assert (archived.try_number, archived.external_executor_id, archived.working_set) == (
            3,
            "pending-executor",
            None,
        )
        assert (successor.try_number, successor.external_executor_id, successor.working_set) == (
            4,
            None,
            True,
        )

    @pytest.mark.parametrize(
        ("state", "dag_metadata_missing", "archived_state"),
        [
            pytest.param(TaskInstanceState.SUCCESS, False, TaskInstanceState.SUCCESS, id="success"),
            pytest.param(
                TaskInstanceState.QUEUED,
                True,
                TaskInstanceState.FAILED,
                id="queued-without-dag-metadata",
            ),
        ],
    )
    def test_clear_detached_attempt_archives_its_uuid_before_inserting_successor(
        self, create_task_instance, session, mocker, state, dag_metadata_missing, archived_state
    ):
        attempt = create_task_instance(state=state, session=session)
        attempt.try_number = 1
        session.commit()
        old_id = attempt.id
        session.expunge(attempt)
        if dag_metadata_missing:
            mocker.patch.object(DBDagBag, "get_latest_version_of_dag", autospec=True, return_value=None)
            mocker.patch.object(DBDagBag, "get_dag_for_run", autospec=True, return_value=None)

        successor = clear_task_instances([attempt], session=session)[0]
        session.commit()

        archived = session.get(TaskInstance, old_id)
        assert (archived.id, archived.try_number, archived.state, archived.working_set) == (
            old_id,
            1,
            archived_state,
            None,
        )
        assert successor.id != old_id
        assert (successor.try_number, successor.working_set, successor.state) == (2, True, None)

    @pytest.mark.parametrize(("non_current", "expected_rows"), [("deleted", 0), ("archived", 2)])
    def test_clear_rejects_non_current_attempt_without_allocating_successor(
        self, create_task_instance, session, non_current, expected_rows
    ):
        attempt = create_task_instance(state=TaskInstanceState.SUCCESS, session=session)
        attempt.try_number = 1
        session.commit()
        if non_current == "archived":
            attempt.prepare_db_for_next_try(session)
        else:
            session.expunge(attempt)
            session.delete(session.get(TaskInstance, attempt.id))
        session.commit()

        with pytest.raises(ValueError, match="archived task instance cannot be cleared"):
            clear_task_instances([attempt], session=session)

        assert (
            session.scalar(
                select(func.count())
                .select_from(TaskInstance)
                .where(
                    TaskInstance.dag_id == attempt.dag_id,
                    TaskInstance.run_id == attempt.run_id,
                    TaskInstance.task_id == attempt.task_id,
                )
                .execution_options(include_all_attempts=True)
            )
            == expected_rows
        )

    @pytest.mark.parametrize("state", [TaskInstanceState.RUNNING, TaskInstanceState.RESTARTING])
    def test_clear_running_attempt_preserves_identity_until_exit(self, dag_maker, session, state):
        with dag_maker():
            EmptyOperator(task_id="task", retries=2)
        ti = dag_maker.create_dagrun(session=session).task_instances[0]
        ti.state = state
        ti.try_number = 4
        ti.max_tries = 3
        session.flush()
        attempt_id = ti.id

        for _ in range(2):
            clear_task_instances([ti], session=session)
            session.flush()
            assert (ti.id, ti.try_number, ti.state) == (attempt_id, 4, TaskInstanceState.RESTARTING)
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(TaskInstance)
                    .where(TaskInstance.working_set.is_(None))
                    .execution_options(include_all_attempts=True)
                )
                == 0
            )

    @pytest.mark.parametrize(
        "state", [TaskInstanceState.QUEUED, TaskInstanceState.RUNNING, TaskInstanceState.RESTARTING]
    )
    def test_failure_allocates_next_attempt(self, dag_maker, session, state):
        with dag_maker():
            task = EmptyOperator(task_id="task", retries=2)
        dr = dag_maker.create_dagrun(session=session)
        ti = dr.task_instances[0]
        ti.task = task
        ti.state = state
        ti.try_number = 1
        session.flush()
        attempt_id = ti.id

        successor = ti.handle_failure("worker exited", session=session)

        assert ti.id == attempt_id
        assert ti.working_set is None
        assert ti.try_number == 1
        assert successor.state == TaskInstanceState.UP_FOR_RETRY
        assert successor.id != attempt_id
        assert successor.try_number == 2
        dr.schedule_tis([successor], session=session)
        session.expire_all()
        assert successor.try_number == 2

    @pytest.mark.parametrize("retries", [1, 2])
    def test_clear_pending_retry_reuses_attempt_and_bypasses_delay(
        self, create_task_instance, session, time_machine, retries
    ):
        time_machine.move_to(DEFAULT_DATE, tick=False)
        ti = create_task_instance(
            task=PythonSensor(
                task_id="task",
                python_callable=lambda: True,
                retries=retries,
                retry_delay=datetime.timedelta(days=1),
            ),
            state=TaskInstanceState.RUNNING,
            hostname="worker",
            pid=123,
            session=session,
            serialized=False,
        )
        dr = ti.dag_run
        ti.try_number = 1
        ti.start_date = DEFAULT_DATE - datetime.timedelta(minutes=1)
        failed_id = ti.id
        session.flush()

        ti = ti.handle_failure("worker exited", session=session)

        pending_id = ti.id
        assert pending_id != failed_id
        assert (ti.try_number, ti.state) == (2, TaskInstanceState.UP_FOR_RETRY)
        retry_dep = NotInRetryPeriodDep()
        assert not retry_dep.is_met(ti, session=session)
        history_query = (
            select(TaskInstance.id, TaskInstance.try_number)
            .where(TaskInstance.dag_id == ti.dag_id, TaskInstance.working_set.is_(None))
            .execution_options(include_all_attempts=True)
        )
        history_before = session.execute(history_query).mappings().all()
        assert [(row.id, row.try_number) for row in history_before] == [(failed_id, 1)]

        # Clearing again in None must preserve the pending attempt, history and retry budget.
        for _ in range(2):
            clear_task_instances([ti], session=session)
            session.flush()
            session.refresh(ti)
            assert (ti.id, ti.try_number, ti.state, ti.max_tries) == (pending_id, 2, None, 1 + retries)
            assert session.execute(history_query).mappings().all() == history_before
            assert retry_dep.is_met(ti, session=session)

        assert dr.schedule_tis([ti], session=session) == 1
        session.refresh(ti)
        assert (ti.id, ti.try_number, ti.state) == (pending_id, 2, TaskInstanceState.SCHEDULED)
        assert session.execute(history_query).mappings().all() == history_before

    @pytest.mark.parametrize("retries", [0, 2])
    def test_clear_unstarted_task_preserves_identity(self, dag_maker, session, retries):
        with dag_maker():
            PythonSensor(task_id="task", python_callable=lambda: True, retries=retries)
        dr = dag_maker.create_dagrun(session=session)
        ti = dr.task_instances[0]
        attempt_id = ti.id
        assert (ti.state, ti.try_number) == (None, 0)

        for _ in range(2):
            clear_task_instances([ti], session=session)
            session.flush()
            session.refresh(ti)
            assert (ti.id, ti.try_number, ti.state, ti.max_tries) == (attempt_id, 0, None, retries)
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(TaskInstance)
                    .where(TaskInstance.dag_id == ti.dag_id, TaskInstance.working_set.is_(None))
                    .execution_options(include_all_attempts=True)
                )
                == 0
            )

        assert dr.schedule_tis([ti], session=session) == 1
        session.refresh(ti)
        assert (ti.id, ti.try_number, ti.state) == (attempt_id, 1, TaskInstanceState.SCHEDULED)

    @pytest.fixture(autouse=True, scope="class")
    def clean(self):
        db.clear_db_runs()
        db.clear_db_serialized_dags()

        yield

        db.clear_db_runs()
        db.clear_db_serialized_dags()

    def test_clear_task_instances(self, dag_maker):
        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )
        ti0 = dag_maker.run_ti("0", dr)
        ti1 = dag_maker.run_ti("1", dr)

        with create_session() as session:
            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
            cleared = clear_task_instances(qry, session=session)

            ti0.refresh_from_db(session=session)
            ti1.refresh_from_db(session=session)
            successors = {ti.task_id: ti for ti in cleared}

        # Next try to run will be try 2
        assert (ti0.state, ti0.try_number, ti0.working_set) == (TaskInstanceState.SUCCESS, 1, None)
        assert (ti1.state, ti1.try_number, ti1.working_set) == (TaskInstanceState.SUCCESS, 1, None)
        assert (successors["0"].state, successors["0"].try_number, successors["0"].max_tries) == (None, 2, 1)
        assert (successors["1"].state, successors["1"].try_number, successors["1"].max_tries) == (None, 2, 3)

    def test_clear_task_instances_external_executor_id(self, dag_maker):
        with dag_maker(
            "test_clear_task_instances_external_executor_id",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
        ) as dag:
            EmptyOperator(task_id="task0")

        ti0 = dag_maker.create_dagrun().task_instances[0]
        ti0.state = State.SUCCESS
        ti0.external_executor_id = "some_external_executor_id"

        with create_session() as session:
            session.add(ti0)
            session.commit()

            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
            successor = clear_task_instances(qry, session)[0]

            ti0.refresh_from_db()

            assert (ti0.state, ti0.external_executor_id, ti0.working_set) == (
                TaskInstanceState.SUCCESS,
                "some_external_executor_id",
                None,
            )
            assert successor.state is None
            assert successor.external_executor_id is None

    def test_clear_task_instances_next_method(self, dag_maker, session):
        with dag_maker(
            "test_clear_task_instances_next_method",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
        ):
            EmptyOperator(task_id="task0")

        ti0 = dag_maker.create_dagrun().task_instances[0]
        ti0.state = State.DEFERRED
        ti0.next_method = "next_method"
        ti0.next_kwargs = {}

        session.add(ti0)
        session.commit()

        successor = clear_task_instances([ti0], session)[0]

        ti0.refresh_from_db()

        assert (ti0.next_method, ti0.next_kwargs, ti0.working_set) == ("next_method", {}, None)
        assert successor.next_method is None
        assert successor.next_kwargs is None

    @pytest.mark.parametrize(
        ("state", "last_scheduling"), [(DagRunState.QUEUED, None), (DagRunState.RUNNING, DEFAULT_DATE)]
    )
    def test_clear_task_instances_dr_state(self, state, last_scheduling, dag_maker):
        """
        Test that DR state is set to None after clear.
        And that DR.last_scheduling_decision is handled OK.
        start_date is also set to None
        """
        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
            serialized=True,
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1", retries=2)
        dr = dag_maker.create_dagrun(
            state=DagRunState.SUCCESS,
            run_type=DagRunType.SCHEDULED,
        )
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        dr.last_scheduling_decision = DEFAULT_DATE
        ti0.state = TaskInstanceState.SUCCESS
        ti1.state = TaskInstanceState.SUCCESS
        session = dag_maker.session
        session.flush()

        # we use order_by(task_id) here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
        assert (
            session.scalar(
                select(func.count())
                .select_from(TI)
                .where(TI.working_set.is_(None))
                .execution_options(include_all_attempts=True)
            )
            == 0
        )
        clear_task_instances(qry, session, dag_run_state=state)
        session.flush()
        # 2 TIs were cleared so 2 history records should be created
        assert (
            session.scalar(
                select(func.count())
                .select_from(TI)
                .where(TI.working_set.is_(None))
                .execution_options(include_all_attempts=True)
            )
            == 2
        )

        session.refresh(dr)

        assert dr.state == state
        assert dr.start_date is None if state == DagRunState.QUEUED else dr.start_date
        assert dr.last_scheduling_decision == last_scheduling

    @pytest.mark.parametrize("state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_clear_task_instances_on_running_dr(self, state, dag_maker):
        """
        Test that DagRun state, start_date and last_scheduling_decision
        are not changed after clearing TI in an unfinished DagRun.
        However, queued_at and clear_number should still be updated.
        """
        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1", retries=2)
        dr = dag_maker.create_dagrun(
            state=state,
            run_type=DagRunType.SCHEDULED,
        )
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        dr.last_scheduling_decision = DEFAULT_DATE
        ti0.state = TaskInstanceState.SUCCESS
        ti1.state = TaskInstanceState.SUCCESS
        session = dag_maker.session
        session.flush()

        # Store original values to verify they're updated
        original_queued_at = dr.queued_at
        original_clear_number = dr.clear_number

        # we use order_by(task_id) here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
        clear_task_instances(qry, session)
        session.flush()

        session.refresh(dr)

        assert dr.state == state
        if state == DagRunState.QUEUED:
            assert dr.start_date is None
        if state == DagRunState.RUNNING:
            assert dr.start_date
        assert dr.last_scheduling_decision == DEFAULT_DATE

        # Verify queued_at and clear_number are updated even for running/queued dag runs
        assert dr.queued_at is not None
        assert dr.queued_at != original_queued_at
        assert dr.clear_number == original_clear_number + 1

    @pytest.mark.parametrize(
        ("state", "last_scheduling"),
        [
            (DagRunState.SUCCESS, None),
            (DagRunState.SUCCESS, DEFAULT_DATE),
            (DagRunState.FAILED, None),
            (DagRunState.FAILED, DEFAULT_DATE),
        ],
    )
    def test_clear_task_instances_on_finished_dr(self, state, last_scheduling, dag_maker):
        """
        Test that DagRun state, start_date and last_scheduling_decision
        are changed after clearing TI in a finished DagRun.
        """
        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
            serialized=True,
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1", retries=2)
        dr = dag_maker.create_dagrun(
            state=state,
            run_type=DagRunType.SCHEDULED,
        )
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        dr.last_scheduling_decision = DEFAULT_DATE
        ti0.state = TaskInstanceState.SUCCESS
        ti1.state = TaskInstanceState.SUCCESS
        session = dag_maker.session
        session.flush()
        original_queued_at = dr.queued_at

        # we use order_by(task_id) here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
        clear_task_instances(qry, session)
        session.flush()

        session.refresh(dr)

        assert dr.state == DagRunState.QUEUED
        assert dr.start_date is None
        assert dr.last_scheduling_decision is None

        # The initial finished run has queued_at=None, clearing should populate it.
        assert original_queued_at is None
        assert dr.queued_at is not None

    @pytest.mark.parametrize("delete_tasks", [True, False])
    def test_clear_task_instances_maybe_task_removed(self, delete_tasks, dag_maker, session):
        """This verifies the behavior of clear_task_instances re task removal.

        When clearing a TI, if the best available serdag for that task doesn't have the
        task anymore, then it has different logic re setting max tries."""
        with dag_maker("test_clear_task_instances_without_task") as dag:
            EmptyOperator(task_id="task0")
            EmptyOperator(task_id="task1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("task0"))
        ti1.refresh_from_task(dag.get_task("task1"))

        # simulate running this task
        # do the incrementing of try_number ordinarily handled by scheduler
        ti0.try_number += 1
        ti1.try_number += 1
        ti0.state = "success"
        ti1.state = "success"
        dr.state = "success"
        session.commit()

        # apparently max tries starts out at task.retries
        # doesn't really make sense
        # then, it later gets updated depending on what happens
        assert ti0.max_tries == 0
        assert ti1.max_tries == 2

        if delete_tasks:
            # Remove the task from dag.
            dag.task_dict.clear()
            dag.task_group.children.clear()
            assert ti1.max_tries == 2
            sync_dag_to_db(dag, session=session)
            session.refresh(ti1)
            assert ti0.try_number == 1
            assert ti0.max_tries == 0
            assert ti1.try_number == 1
            assert ti1.max_tries == 2
        successors = {ti.task_id: ti for ti in clear_task_instances([ti0, ti1], session)}

        # When no task is found, max_tries will be maximum of original max_tries or try_number.
        session.refresh(ti0)
        session.refresh(ti1)
        assert (ti0.try_number, ti0.state, ti0.working_set) == (1, TaskInstanceState.SUCCESS, None)
        assert (ti1.try_number, ti1.state, ti1.working_set) == (1, TaskInstanceState.SUCCESS, None)
        assert successors["task0"].try_number == 2
        assert successors["task0"].max_tries == 1
        assert successors["task0"].state is None
        assert successors["task1"].try_number == 2
        assert successors["task1"].state is None
        if delete_tasks:
            assert successors["task1"].max_tries == 2
        else:
            assert successors["task1"].max_tries == 3
        session.refresh(dr)
        assert dr.state == "queued"

    def test_clear_task_instances_without_dag_param(self, dag_maker, session):
        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances_without_dag_param",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            session=session,
            catchup=True,
        ) as dag:
            task0 = EmptyOperator(task_id="task0")
            task1 = EmptyOperator(task_id="task1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("task0"))
        ti1.refresh_from_task(dag.get_task("task1"))

        with create_session() as session:
            # do the incrementing of try_number ordinarily handled by scheduler
            ti0.try_number += 1
            ti1.try_number += 1
            session.merge(ti0)
            session.merge(ti1)
            session.commit()

        run_task_instance(ti0, task0)
        run_task_instance(ti1, task1)

        # we use order_by(task_id) here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
        successors = {ti.task_id: ti for ti in clear_task_instances(qry, session)}

        ti0.refresh_from_db(session=session)
        ti1.refresh_from_db(session=session)
        assert (ti0.try_number, ti0.working_set) == (1, None)
        assert (ti1.try_number, ti1.working_set) == (1, None)
        assert (successors["task0"].try_number, successors["task0"].max_tries) == (2, 1)
        assert (successors["task1"].try_number, successors["task1"].max_tries) == (2, 3)

    def test_clear_task_instances_in_multiple_dags(self, dag_maker, session):
        with dag_maker("test_clear_task_instances_in_multiple_dags0", session=session):
            EmptyOperator(task_id="task0")

        dr0 = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        with dag_maker("test_clear_task_instances_in_multiple_dags1", session=session):
            EmptyOperator(task_id="task1", retries=2)

        dr1 = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0 = dr0.task_instances[0]
        ti1 = dr1.task_instances[0]

        ti0.try_number = ti1.try_number = 1
        ti0.state = ti1.state = TaskInstanceState.SUCCESS

        session.commit()

        successors = {ti.dag_id: ti for ti in clear_task_instances([ti0, ti1], session)}

        session.refresh(ti0)
        session.refresh(ti1)

        assert (ti0.try_number, ti0.working_set) == (1, None)
        assert (ti1.try_number, ti1.working_set) == (1, None)
        assert (successors[ti0.dag_id].try_number, successors[ti0.dag_id].max_tries) == (2, 1)
        assert (successors[ti1.dag_id].try_number, successors[ti1.dag_id].max_tries) == (2, 3)

    def test_clear_task_instances_with_task_reschedule(self, dag_maker):
        """Clearing preserves reschedules on the archived attempt and starts the successor without them."""

        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances_with_task_reschedule",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
        ) as dag:
            task0 = PythonSensor(task_id="0", python_callable=lambda: False, mode="reschedule")
            task1 = PythonSensor(task_id="1", python_callable=lambda: False, mode="reschedule")

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        ti1.refresh_from_task(dag.get_task("1"))

        with create_session() as session:
            # do the incrementing of try_number ordinarily handled by scheduler
            ti0.try_number += 1
            ti1.try_number += 1
            session.merge(ti0)
            session.merge(ti1)
            session.commit()

        run_task_instance(ti0, task0)
        run_task_instance(ti1, task1)

        with create_session() as session:

            def count_task_reschedule(ti):
                return session.scalar(
                    select(func.count()).select_from(TaskReschedule).where(TaskReschedule.ti_id == ti.id)
                )

            assert count_task_reschedule(ti0) == 1
            assert count_task_reschedule(ti1) == 1
            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = session.scalars(
                select(TI).where(TI.dag_id == dag.dag_id, TI.task_id == ti0.task_id).order_by(TI.task_id)
            ).all()
            successor = clear_task_instances(qry, session)[0]
            assert count_task_reschedule(ti0) == 1
            assert count_task_reschedule(successor) == 0
            assert count_task_reschedule(ti1) == 1

    @pytest.mark.parametrize(
        ("state", "state_recorded"),
        [
            (TaskInstanceState.SUCCESS, TaskInstanceState.SUCCESS),
            (TaskInstanceState.FAILED, TaskInstanceState.FAILED),
            (TaskInstanceState.SKIPPED, TaskInstanceState.SKIPPED),
            (TaskInstanceState.UP_FOR_RETRY, None),
            (TaskInstanceState.UP_FOR_RESCHEDULE, TaskInstanceState.FAILED),
            (TaskInstanceState.RUNNING, None),
            (TaskInstanceState.QUEUED, TaskInstanceState.FAILED),
            (TaskInstanceState.SCHEDULED, TaskInstanceState.FAILED),
            (None, None),
            (TaskInstanceState.RESTARTING, None),
        ],
    )
    def test_task_instance_history_record(self, state, state_recorded, dag_maker):
        """Test that task instance history record is created with approapriate state"""

        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1", retries=2)
        dr = dag_maker.create_dagrun(
            state=DagRunState.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.state = state
        ti1.state = state
        session = dag_maker.session
        session.flush()
        qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
        clear_task_instances(qry, session)
        session.flush()

        session.refresh(dr)
        ti_history = session.scalars(
            select(TI.state).where(TI.working_set.is_(None)).execution_options(include_all_attempts=True)
        ).all()

        assert ti_history == ([str(state_recorded)] * 2 if state_recorded else [])

    def test_dag_clear(self, dag_maker, session):
        with dag_maker("test_dag_clear") as dag:
            EmptyOperator(task_id="test_dag_clear_task_0")
            EmptyOperator(task_id="test_dag_clear_task_1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)

        ti0.try_number = 1
        ti0.state = TaskInstanceState.SUCCESS
        session.commit()

        dag.clear(session=session)
        session.commit()

        cleared_ti0 = dr.get_task_instance(ti0.task_id, session=session)
        cleared_ti1 = dr.get_task_instance(ti1.task_id, session=session)
        assert (ti0.try_number, ti0.state, ti0.working_set) == (1, TaskInstanceState.SUCCESS, None)
        assert (cleared_ti0.try_number, cleared_ti0.state, cleared_ti0.max_tries) == (2, None, 1)
        assert (cleared_ti1.try_number, cleared_ti1.max_tries) == (0, 2)
        pending_ids = (cleared_ti0.id, cleared_ti1.id)

        dag.clear(session=session)

        cleared_ti0 = dr.get_task_instance(ti0.task_id, session=session)
        cleared_ti1 = dr.get_task_instance(ti1.task_id, session=session)
        assert (cleared_ti0.id, cleared_ti1.id) == pending_ids
        assert (cleared_ti1.max_tries, cleared_ti1.try_number) == (2, 0)
        assert (cleared_ti0.try_number, cleared_ti0.max_tries) == (2, 1)

    def test_dags_clear(self, dag_maker, session):
        sdk_dags, ser_dags, tis = [], [], []
        num_of_dags = 5
        for i in range(num_of_dags):
            with dag_maker(
                f"test_dag_clear_{i}",
                schedule=datetime.timedelta(days=1),
                serialized=True,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            ):
                EmptyOperator(task_id=f"test_task_clear_{i}", owner="test")

            dr = dag_maker.create_dagrun(
                run_id=f"scheduled_{i}",
                logical_date=DEFAULT_DATE,
                state=State.RUNNING,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                run_after=DEFAULT_DATE,
                triggered_by=DagRunTriggeredByType.TEST,
            )
            sdk_dags.append(dag_maker.dag)
            ser_dags.append(serialized_dag := dag_maker.serialized_dag)
            (ti := dr.task_instances[0]).refresh_from_task(serialized_dag.get_task(ti.task_id))
            tis.append(ti)

        # test clear all dags
        for dag, ti in zip(sdk_dags, tis):
            session.get(TaskInstance, ti.id).try_number += 1
            session.commit()
            run_task_instance(ti, dag.get_task(ti.task_id))
            assert ti.state == State.SUCCESS
            assert ti.try_number == 1
            assert ti.max_tries == 0
        session.commit()

        def _get_ti(old_ti):
            return session.scalar(
                select(TI).where(
                    TI.dag_id == old_ti.dag_id,
                    TI.task_id == old_ti.task_id,
                    TI.map_index == old_ti.map_index,
                    TI.run_id == old_ti.run_id,
                )
            )

        SerializedDAG.clear_dags(ser_dags)
        session.commit()
        for i in range(num_of_dags):
            ti = _get_ti(tis[i])
            archived = session.get(TI, tis[i].id)
            assert ti.id != tis[i].id
            assert (archived.try_number, archived.state, archived.working_set) == (1, State.SUCCESS, None)
            assert ti.state == State.NONE
            assert ti.try_number == 2
            assert ti.max_tries == 1

        # test dry_run
        for i, dag in enumerate(ser_dags):
            ti = _get_ti(tis[i])
            ti.refresh_from_task(dag.get_task(ti.task_id))
            # Directly set state to SUCCESS instead of calling ti.run() to avoid timeout
            ti.state = State.SUCCESS
            session.commit()
            assert ti.state == State.SUCCESS
            assert ti.try_number == 2
            assert ti.max_tries == 1
        session.commit()
        SerializedDAG.clear_dags(ser_dags, dry_run=True)
        session.commit()
        for i in range(num_of_dags):
            ti = _get_ti(tis[i])
            assert ti.state == State.SUCCESS
            assert ti.try_number == 2
            assert ti.max_tries == 1

        # test only_failed
        ti_fail = random.choice(tis)
        ti_fail = _get_ti(ti_fail)
        ti_fail.state = State.FAILED
        failed_id = ti_fail.id
        session.commit()

        SerializedDAG.clear_dags(ser_dags, only_failed=True)
        archived_failed = session.get(TI, failed_id)

        for ti_in in tis:
            ti = _get_ti(ti_in)
            if ti.dag_id == ti_fail.dag_id:
                assert ti.id != failed_id
                assert (archived_failed.state, archived_failed.working_set) == (State.FAILED, None)
                assert ti.state == State.NONE
                assert ti.try_number == 3
                assert ti.max_tries == 2
            else:
                assert ti.state == State.SUCCESS
                assert ti.try_number == 2
                assert ti.max_tries == 1

    @pytest.mark.parametrize("run_on_latest_version", [True, False])
    def test_clear_task_instances_with_run_on_latest_version(self, run_on_latest_version, dag_maker, session):
        # Explicitly needs catchup as True as test is creating history runs
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
            bundle_version="v1",
        ) as dag:
            task0 = EmptyOperator(task_id="0")
            task1 = EmptyOperator(task_id="1", retries=2)
        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        ti1.refresh_from_task(dag.get_task("1"))

        run_task_instance(ti0, task0)
        run_task_instance(ti1, task1)
        dr.state = DagRunState.SUCCESS
        session.merge(dr)
        session.flush()

        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
            bundle_version="v2",
        ) as dag:
            EmptyOperator(task_id="0")
        new_dag_version = DagVersion.get_latest_version(dag.dag_id)

        assert old_dag_version.id != new_dag_version.id
        qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
        clear_task_instances(qry, session, run_on_latest_version=run_on_latest_version)
        session.commit()
        dr = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id))
        if run_on_latest_version:
            assert dr.created_dag_version_id == new_dag_version.id
            assert dr.bundle_version == new_dag_version.bundle_version
            assert TaskInstanceState.REMOVED in [ti.state for ti in dr.task_instances]
            for ti in dr.task_instances:
                assert ti.dag_version_id == new_dag_version.id
        else:
            assert dr.created_dag_version_id == old_dag_version.id
            assert dr.bundle_version == old_dag_version.bundle_version
            assert TaskInstanceState.REMOVED not in [ti.state for ti in dr.task_instances]
            for ti in dr.task_instances:
                assert ti.dag_version_id == old_dag_version.id

    def test_clear_task_instances_without_dag_version_forces_latest(self, dag_maker, session):
        """A Dag run carried over from Airflow 2 has no version, so clearing must pin it to the latest."""
        dag_id = "test_clear_no_dag_version"
        dr = self._make_versionless_run(dag_maker, session, dag_id, DagRunState.SUCCESS)

        latest_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0 = session.scalar(select(TI).where(TI.dag_id == dag_id))
        assert ti0.dag_version_id is None, "Pre-condition"
        assert ti0.dag_run.created_dag_version_id is None, "Pre-condition"

        clear_task_instances([ti0], session, run_on_latest_version=False)
        session.commit()

        dr_after = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id))
        assert dr_after.created_dag_version_id == latest_dag_version.id
        assert dr_after.bundle_version == latest_dag_version.bundle_version
        assert dr_after.task_instances[0].dag_version_id == latest_dag_version.id

    def _make_versionless_run(self, dag_maker, session, dag_id, dr_state, task_count=1, sibling_state=None):
        """
        Build a run shaped like Airflow 2 left it: no versions anywhere.

        Task "0" is run for real; any further tasks are left in ``sibling_state``.
        """
        with dag_maker(dag_id, start_date=DEFAULT_DATE, catchup=True, bundle_version="v1") as dag:
            task0 = EmptyOperator(task_id="0")
            for index in range(1, task_count):
                EmptyOperator(task_id=str(index))
        dr = dag_maker.create_dagrun(state=State.RUNNING, run_type=DagRunType.SCHEDULED)

        ti0, *siblings = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        run_task_instance(ti0, task0)
        for sibling in siblings:
            sibling.state = sibling_state
        dr.state = dr_state

        # `airflow db migrate` from Airflow 2 leaves these columns NULL. Write them directly so
        # no ORM relationship syncs the old values back, then expire so the objects are reloaded
        # from the database like they are in a real deployment.
        session.flush()
        session.execute(
            update(DagRun).where(DagRun.id == dr.id).values(created_dag_version_id=None, bundle_version=None)
        )
        session.execute(update(TI).where(TI.dag_id == dag.dag_id).values(dag_version_id=None))
        session.commit()
        session.expire_all()
        return dr

    def test_clear_task_instances_pins_task_instance_restored_by_verify_integrity(self, dag_maker, session):
        """
        A task instance revived by ``verify_integrity`` is given a version too.

        It comes back unfinished but unversioned, and pinning the run stops the scheduler
        backfilling one, so it would never be enqueued.
        """
        dag_id = "test_clear_no_dag_version_restored"
        # Task "1" was dropped from the Dag during the Airflow 2 era and later re-added, so
        # verify_integrity restores it when the finished run is cleared.
        dr = self._make_versionless_run(
            dag_maker,
            session,
            dag_id,
            DagRunState.SUCCESS,
            task_count=2,
            sibling_state=TaskInstanceState.REMOVED,
        )
        latest_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0 = session.scalar(select(TI).where(TI.dag_id == dag_id, TI.task_id == "0"))

        clear_task_instances([ti0], session, run_on_latest_version=False)
        session.commit()

        restored = session.scalar(select(TI).where(TI.dag_id == dag_id, TI.task_id == "1"))
        assert restored.state is None, "verify_integrity should have restored it"
        assert restored.dag_version_id == latest_dag_version.id

    def test_clear_task_instances_pins_unfinished_siblings_on_running_run(self, dag_maker, session):
        """A queued/running run is pinned without verify_integrity, so its siblings need one too."""
        dag_id = "test_clear_no_dag_version_running"
        dr = self._make_versionless_run(
            dag_maker,
            session,
            dag_id,
            DagRunState.RUNNING,
            task_count=2,
            sibling_state=TaskInstanceState.SCHEDULED,
        )
        latest_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0 = session.scalar(select(TI).where(TI.dag_id == dag_id, TI.task_id == "0"))

        clear_task_instances([ti0], session, run_on_latest_version=False)
        session.commit()

        dr_after = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id))
        assert dr_after.created_dag_version_id == latest_dag_version.id
        sibling = session.scalar(select(TI).where(TI.dag_id == dag_id, TI.task_id == "1"))
        assert sibling.dag_version_id == latest_dag_version.id

    def test_clear_task_instances_keeps_run_and_task_versions_together(self, dag_maker, session):
        """A run pinned to a version must not leave its cleared task instances on another."""
        dag_id = "test_clear_backfilled_ti_null_run"
        with dag_maker(dag_id, start_date=DEFAULT_DATE, catchup=True, bundle_version="v1") as dag:
            task0 = EmptyOperator(task_id="0")
        dr = dag_maker.create_dagrun(state=State.RUNNING, run_type=DagRunType.SCHEDULED)
        (ti0,) = dr.task_instances
        ti0.refresh_from_task(dag.get_task("0"))
        run_task_instance(ti0, task0)
        dr.state = DagRunState.SUCCESS
        session.flush()

        # The task instance keeps a version while the run loses its own, so the run counts as
        # version-less and gets forced onto the latest.
        old_dag_version = DagVersion.get_latest_version(dag_id)
        session.execute(update(DagRun).where(DagRun.id == dr.id).values(created_dag_version_id=None))
        session.commit()
        session.expire_all()

        with dag_maker(dag_id, start_date=DEFAULT_DATE, catchup=True, bundle_version="v2"):
            EmptyOperator(task_id="0")
        new_dag_version = DagVersion.get_latest_version(dag_id)
        assert old_dag_version.id != new_dag_version.id, "Pre-condition"

        ti0 = session.scalar(select(TI).where(TI.dag_id == dag_id))
        assert ti0.dag_version_id == old_dag_version.id, "Pre-condition"
        assert ti0.dag_run.created_dag_version_id is None, "Pre-condition"

        clear_task_instances([ti0], session, run_on_latest_version=False)
        session.commit()

        dr_after = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id))
        ti_after = session.scalar(select(TI).where(TI.dag_id == dag_id))
        assert dr_after.created_dag_version_id == new_dag_version.id
        assert ti_after.dag_version_id == dr_after.created_dag_version_id, (
            "the run and its task instance must end up on the same version"
        )
        archived = session.get(TI, ti0.id)
        assert (archived.working_set, archived.dag_version_id) == (None, old_dag_version.id)

    def test_clear_task_instances_moves_versionless_task_to_its_run_version(self, dag_maker, session):
        """A version-less task instance on a pinned run joins the run, not the latest version."""
        dag_id = "test_clear_versionless_ti_pinned_run"
        with dag_maker(dag_id, start_date=DEFAULT_DATE, catchup=True, bundle_version="v1") as dag:
            task0 = EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
        dr = dag_maker.create_dagrun(state=State.RUNNING, run_type=DagRunType.SCHEDULED)
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        run_task_instance(ti0, task0)
        ti1.state = TaskInstanceState.SUCCESS
        dr.state = DagRunState.SUCCESS
        session.flush()

        # An Airflow 2 task instance that an earlier clear left behind: it was already finished, so
        # pinning the run did not give it a version.
        run_dag_version = DagVersion.get_latest_version(dag_id)
        session.execute(update(TI).where(TI.dag_id == dag_id, TI.task_id == "1").values(dag_version_id=None))
        session.commit()
        session.expire_all()

        with dag_maker(dag_id, start_date=DEFAULT_DATE, catchup=True, bundle_version="v2"):
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
        assert DagVersion.get_latest_version(dag_id).id != run_dag_version.id, "Pre-condition"

        ti1 = session.scalar(select(TI).where(TI.dag_id == dag_id, TI.task_id == "1"))
        assert ti1.dag_version_id is None, "Pre-condition"
        assert ti1.dag_run.created_dag_version_id == run_dag_version.id, "Pre-condition"

        clear_task_instances([ti1], session, run_on_latest_version=False)
        session.commit()

        dr_after = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id))
        ti1_after = session.scalar(select(TI).where(TI.dag_id == dag_id, TI.task_id == "1"))
        assert dr_after.created_dag_version_id == run_dag_version.id
        assert ti1_after.dag_version_id == run_dag_version.id, (
            "the run and its task instance must end up on the same version"
        )

    def test_clear_subset_run_on_latest_version_only_updates_cleared_tis(self, dag_maker, session):
        """run_on_latest_version on a finished DR must not rewrite dag_version_id on TIs that were not cleared."""
        with dag_maker(
            "test_clear_subset_latest",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
            bundle_version="v1",
        ) as dag:
            task0 = EmptyOperator(task_id="0")
            task1 = EmptyOperator(task_id="1")
        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        ti1.refresh_from_task(dag.get_task("1"))

        run_task_instance(ti0, task0)
        run_task_instance(ti1, task1)
        dr.state = DagRunState.SUCCESS
        session.merge(dr)
        session.flush()

        with dag_maker(
            "test_clear_subset_latest",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            catchup=True,
            bundle_version="v2",
        ):
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
        new_dag_version = DagVersion.get_latest_version(dr.dag_id)
        assert old_dag_version.id != new_dag_version.id

        clear_task_instances([ti0], session, run_on_latest_version=True)
        session.commit()

        dr_after = session.scalar(select(DagRun).where(DagRun.dag_id == dr.dag_id))
        tis = {ti.task_id: ti for ti in dr_after.task_instances}
        assert tis["0"].dag_version_id == new_dag_version.id
        assert tis["1"].dag_version_id == old_dag_version.id
        assert dr_after.created_dag_version_id == new_dag_version.id

    @pytest.mark.parametrize("run_on_latest_version", [True, False])
    def test_clear_running_dag_run_with_run_on_latest_version(
        self, run_on_latest_version, dag_maker, session
    ):
        with dag_maker(
            "test_clear_running_dr",
            start_date=DEFAULT_DATE,
            catchup=True,
            bundle_version="v1",
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
        dr = dag_maker.create_dagrun(state=State.RUNNING, run_type=DagRunType.SCHEDULED)
        old_dag_version = DagVersion.get_latest_version(dr.dag_id)

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.state = TaskInstanceState.RUNNING
        ti0.try_number = 1
        ti1.state = TaskInstanceState.SUCCESS
        session.merge(ti0)
        session.merge(ti1)
        session.flush()

        with dag_maker(
            "test_clear_running_dr",
            start_date=DEFAULT_DATE,
            catchup=True,
            bundle_version="v2",
        ):
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
            EmptyOperator(task_id="2")
        new_dag_version = DagVersion.get_latest_version(dag.dag_id)
        assert old_dag_version.id != new_dag_version.id

        qry = session.scalars(select(TI).where(TI.dag_id == dag.dag_id).order_by(TI.task_id)).all()
        clear_task_instances(qry, session, run_on_latest_version=run_on_latest_version)
        session.commit()

        dr = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id))
        assert dr.state == DagRunState.RUNNING
        tis = {ti.task_id: ti for ti in dr.task_instances}
        assert tis["0"].state == TaskInstanceState.RESTARTING
        expected_version = new_dag_version if run_on_latest_version else old_dag_version
        assert tis["0"].dag_version_id == old_dag_version.id
        assert tis["1"].dag_version_id == expected_version.id
        assert dr.created_dag_version_id == expected_version.id
        assert dr.bundle_version == expected_version.bundle_version
        assert ("2" in tis) is run_on_latest_version

        attempt_id = tis["0"].id
        old_version_id, expected_version_id = old_dag_version.id, expected_version.id
        session.expunge_all()
        attempt = session.get(TI, attempt_id)
        successor = attempt.complete_restart(session=session)
        session.flush()

        assert attempt.working_set is None
        assert attempt.dag_version_id == old_version_id
        assert successor.dag_version_id == expected_version_id
        assert successor.try_number == 2

    @pytest.mark.parametrize("state", [TaskInstanceState.FAILED, TaskInstanceState.RUNNING])
    @pytest.mark.parametrize("mapped_count", [0, 2])
    def test_clear_latest_keeps_unmapped_task_available_for_expansion(
        self, dag_maker, session, state, mapped_count
    ):
        @task
        def work(arg): ...

        with dag_maker("test_clear_plain_to_mapped", bundle_version="v1", session=session):
            work(1)
        dr = dag_maker.create_dagrun(state=DagRunState.RUNNING)
        attempt = dr.get_task_instance("work", session=session)
        attempt.state = state
        attempt.try_number = 1
        old_version_id = attempt.dag_version_id
        session.flush()

        with dag_maker("test_clear_plain_to_mapped", bundle_version="v2", session=session):
            work.expand(arg=list(range(mapped_count)))
        new_version_id = DagVersion.get_latest_version(dr.dag_id, session=session).id

        (cleared,) = clear_task_instances([attempt], session, run_on_latest_version=True)
        if state == TaskInstanceState.RUNNING:
            assert cleared.state == TaskInstanceState.RESTARTING
            cleared.complete_restart(session=session)

        decision = dr.task_instance_scheduling_decisions(session=session)

        assert sorted(ti.map_index for ti in decision.schedulable_tis) == list(range(mapped_count))
        current_tis = dr.get_task_instances(session=session)
        assert {ti.dag_version_id for ti in current_tis} == {new_version_id}
        assert {ti.state for ti in current_tis} == ({None} if mapped_count else {TaskInstanceState.SKIPPED})
        assert attempt.working_set is None
        assert attempt.dag_version_id == old_version_id

    def test_complete_restart_uses_latest_version_for_unpinned_run(self, dag_maker, session):
        with dag_maker("test_restart_unpinned", session=session):
            EmptyOperator(task_id="work")
        dr = dag_maker.create_dagrun(state=DagRunState.RUNNING)
        attempt = dr.get_task_instance("work", session=session)
        attempt.state = TaskInstanceState.RUNNING
        old_version_id = attempt.dag_version_id
        session.flush()

        clear_task_instances([attempt], session)
        with dag_maker("test_restart_unpinned", session=session):
            EmptyOperator(task_id="work", retries=2)
        new_version_id = DagVersion.get_latest_version(dr.dag_id, session=session).id

        successor = attempt.complete_restart(session=session)

        assert successor.dag_version_id == new_version_id
        assert attempt.dag_version_id == old_version_id
        assert old_version_id != new_version_id

    @pytest.mark.parametrize("dr_state", [DagRunState.SUCCESS, DagRunState.RUNNING])
    def test_clear_run_on_latest_version_without_resetting_dag_run(self, dr_state, dag_maker, session):
        """``reset_dag_runs=False`` leaves the run's state alone but must not leave its version behind."""
        with dag_maker(
            "test_clear_no_reset",
            start_date=DEFAULT_DATE,
            catchup=True,
            bundle_version="v1",
        ) as dag:
            EmptyOperator(task_id="0")
        dr = dag_maker.create_dagrun(state=dr_state, run_type=DagRunType.SCHEDULED)
        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        clear_number = dr.clear_number

        ti = dr.task_instances[0]
        ti.state = TaskInstanceState.FAILED
        session.merge(ti)
        session.flush()

        with dag_maker(
            "test_clear_no_reset",
            start_date=DEFAULT_DATE,
            catchup=True,
            bundle_version="v2",
        ):
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
        new_dag_version = DagVersion.get_latest_version(dag.dag_id)
        assert old_dag_version.id != new_dag_version.id

        clear_task_instances([ti], session, dag_run_state=False, run_on_latest_version=True)
        session.commit()

        dr = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id))
        assert dr.state == dr_state
        assert dr.clear_number == clear_number
        assert dr.created_dag_version_id == new_dag_version.id
        assert dr.bundle_version == "v2"
        tis = {ti.task_id: ti for ti in dr.task_instances}
        assert tis["0"].dag_version_id == new_dag_version.id
        assert "1" in tis

    def test_clear_run_on_latest_version_refreshes_bundle_when_dag_unchanged(self, dag_maker, session):
        """A newer bundle updates the latest DagVersion in place, so its id cannot gate the refresh."""
        with dag_maker(
            "test_clear_bundle_only_change",
            start_date=DEFAULT_DATE,
            catchup=True,
            bundle_version="v1",
        ) as dag:
            EmptyOperator(task_id="0")
        dr = dag_maker.create_dagrun(state=State.RUNNING, run_type=DagRunType.SCHEDULED)
        old_dag_version_id = DagVersion.get_latest_version(dr.dag_id).id
        assert dr.bundle_version == "v1"

        ti = dr.task_instances[0]
        ti.state = TaskInstanceState.FAILED
        session.merge(ti)
        session.flush()

        SerializedDagModel.write_dag(
            LazyDeserializedDAG(data=dag_maker.get_serialized_data()),
            bundle_name="dag_maker",
            bundle_version="v2",
            session=session,
        )
        session.get(DagModel, dag.dag_id).bundle_version = "v2"
        session.flush()
        assert DagVersion.get_latest_version(dag.dag_id).id == old_dag_version_id

        clear_task_instances([ti], session, run_on_latest_version=True)
        session.commit()

        dr = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id))
        assert dr.bundle_version == "v2"

    def test_clear_run_on_latest_version_unpins_disabled_bundle_versioning(self, dag_maker, session):
        with dag_maker(
            "test_clear_disable_bundle_versioning",
            start_date=DEFAULT_DATE,
            catchup=True,
            bundle_version="v1",
        ) as dag:
            EmptyOperator(task_id="0")
        dr = dag_maker.create_dagrun(state=State.RUNNING, run_type=DagRunType.SCHEDULED)
        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        assert dr.bundle_version == "v1"

        ti = dr.task_instances[0]
        ti.state = TaskInstanceState.FAILED
        session.merge(ti)
        session.flush()

        with dag_maker(
            "test_clear_disable_bundle_versioning",
            start_date=DEFAULT_DATE,
            catchup=True,
            bundle_version="v2",
            disable_bundle_versioning=True,
        ):
            EmptyOperator(task_id="0")
        new_dag_version = DagVersion.get_latest_version(dag.dag_id)
        assert old_dag_version.id != new_dag_version.id

        clear_task_instances([ti], session, run_on_latest_version=True)
        session.commit()

        dr = session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id))
        assert dr.created_dag_version_id == new_dag_version.id
        assert dr.bundle_version is None

    def test_clear_only_new_tasks(self, dag_maker, session):
        """Test that only_new queues only newly added tasks without clearing existing ones."""

        with dag_maker(
            "test_clear_new_task_instances",
            bundle_version="v1",
        ) as dag:
            task0 = EmptyOperator(task_id="0")
            task1 = EmptyOperator(task_id="1")
        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        ti1.refresh_from_task(dag.get_task("1"))

        run_task_instance(ti0, task0)
        run_task_instance(ti1, task1)
        dr.state = DagRunState.SUCCESS
        session.merge(dr)
        session.flush()

        with dag_maker(
            "test_clear_new_task_instances",
            bundle_version="v2",
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
            EmptyOperator(task_id="2")
            EmptyOperator(task_id="3")

        new_dag_version = DagVersion.get_latest_version(dag.dag_id)

        assert old_dag_version.id != new_dag_version.id

        count = dag.clear(
            run_id=dr.run_id,
            only_new=True,
            session=session,
        )
        assert count == 2

        session.flush()

        updated_dr = session.scalar(
            select(DagRun).where(DagRun.dag_id == dr.dag_id, DagRun.run_id == dr.run_id)
        )

        assert updated_dr.created_dag_version_id == new_dag_version.id
        assert updated_dr.bundle_version == new_dag_version.bundle_version

        all_tis = sorted(updated_dr.task_instances, key=lambda ti: ti.task_id)
        assert len(all_tis) == 4
        assert [ti.task_id for ti in all_tis] == ["0", "1", "2", "3"]

        ti0_after, ti1_after, ti2, ti3 = all_tis
        assert ti0_after.state == TaskInstanceState.SUCCESS
        assert ti1_after.state == TaskInstanceState.SUCCESS

        assert ti2.state is None
        assert ti3.state is None

        for ti in all_tis:
            assert ti.dag_version_id == new_dag_version.id

    def test_clear_only_new_tasks_dry_run(self, dag_maker, session):
        """Test that only_new with dry_run returns new tasks and changes can be rolled back."""
        with dag_maker(
            "test_clear_new_task_instances_dry_run",
            bundle_version="v1",
        ) as dag:
            task0 = EmptyOperator(task_id="0")
            task1 = EmptyOperator(task_id="1")
        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        ti1.refresh_from_task(dag.get_task("1"))

        run_task_instance(ti0, task0)
        run_task_instance(ti1, task1)
        dr.state = DagRunState.SUCCESS
        session.merge(dr)
        session.flush()

        with dag_maker(
            "test_clear_new_task_instances_dry_run",
            bundle_version="v2",
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")
            EmptyOperator(task_id="2")
            EmptyOperator(task_id="3")

        new_dag_version = DagVersion.get_latest_version(dag.dag_id)

        assert old_dag_version.id != new_dag_version.id

        new_tis = dag.clear(
            run_id=dr.run_id,
            only_new=True,
            dry_run=True,
            session=session,
        )

        assert len(new_tis) == 2
        assert sorted(new_tis) == ["2", "3"]

        session.rollback()
        dr.refresh_from_db(session=session)

        assert dr.created_dag_version_id == old_dag_version.id
        assert len(dr.task_instances) == 2  # should be only the 2 earlier tasks

    def test_clear_only_new_no_new_tasks(self, dag_maker, session):
        """Test that only_new returns 0 when no new tasks are added."""
        with dag_maker(
            "test_clear_no_new_task_instances",
            bundle_version="v1",
        ) as dag:
            task0 = EmptyOperator(task_id="0")
            task1 = EmptyOperator(task_id="1")
        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(dag.get_task("0"))
        ti1.refresh_from_task(dag.get_task("1"))

        run_task_instance(ti0, task0)
        run_task_instance(ti1, task1)
        dr.state = DagRunState.SUCCESS
        session.merge(dr)
        session.flush()

        with dag_maker(
            "test_clear_no_new_task_instances",
            bundle_version="v2",
        ) as dag:
            EmptyOperator(task_id="0")
            EmptyOperator(task_id="1")

        new_dag_version = DagVersion.get_latest_version(dag.dag_id)

        assert old_dag_version.id != new_dag_version.id

        count = dag.clear(
            run_id=dr.run_id,
            only_new=True,
            session=session,
        )

        assert count == 0

    def test_clear_normal_task_includes_setup_and_teardown(self, dag_maker):
        with dag_maker("test_clear_normal_task_includes_setup_and_teardown") as dag:
            setup_t = EmptyOperator(task_id="setup_t").as_setup()
            normal_t = EmptyOperator(task_id="normal_t")
            teardown_t = EmptyOperator(task_id="teardown_t").as_teardown(setups=setup_t)
            setup_t >> normal_t >> teardown_t
        dr = dag_maker.create_dagrun()
        for ti in dr.get_task_instances():
            ti.set_state(TaskInstanceState.SUCCESS)
        dag_maker.session.flush()

        cleared = dag.clear(
            dry_run=True,
            task_ids=["normal_t"],
            run_id=dr.run_id,
            session=dag_maker.session,
        )

        cleared_ids = {ti.task_id for ti in cleared}
        assert cleared_ids == {"setup_t", "normal_t", "teardown_t"}

    def test_clear_setup_includes_paired_teardown(self, dag_maker):
        with dag_maker("test_clear_setup_includes_paired_teardown") as dag:
            setup_t = EmptyOperator(task_id="setup_t").as_setup()
            normal_t = EmptyOperator(task_id="normal_t")
            teardown_t = EmptyOperator(task_id="teardown_t").as_teardown(setups=setup_t)
            setup_t >> normal_t >> teardown_t
        dr = dag_maker.create_dagrun()
        for ti in dr.get_task_instances():
            ti.set_state(TaskInstanceState.SUCCESS)
        dag_maker.session.flush()

        cleared = dag.clear(
            dry_run=True,
            task_ids=["setup_t"],
            run_id=dr.run_id,
            session=dag_maker.session,
        )

        cleared_ids = {ti.task_id for ti in cleared}
        assert cleared_ids == {"setup_t", "teardown_t"}

    def test_clear_teardown_does_not_include_setup(self, dag_maker):
        with dag_maker("test_clear_teardown_does_not_include_setup") as dag:
            setup_t = EmptyOperator(task_id="setup_t").as_setup()
            normal_t = EmptyOperator(task_id="normal_t")
            teardown_t = EmptyOperator(task_id="teardown_t").as_teardown(setups=setup_t)
            setup_t >> normal_t >> teardown_t
        dr = dag_maker.create_dagrun()
        for ti in dr.get_task_instances():
            ti.set_state(TaskInstanceState.SUCCESS)
        dag_maker.session.flush()

        cleared = dag.clear(
            dry_run=True,
            task_ids=["teardown_t"],
            run_id=dr.run_id,
            session=dag_maker.session,
        )

        cleared_ids = {ti.task_id for ti in cleared}
        assert cleared_ids == {"teardown_t"}


@pytest.mark.parametrize("downstream", [False, True])
@pytest.mark.parametrize("later", [False, True])
def test_loop_clear_controls_select_exact_live_executions(loop_run, session, downstream, later):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2 and ti.region_index == 0
    )
    before = [(ti.id, ti.region_id, ti.region_index, ti.state, ti.working_set) for ti in tis]

    scope = select_loop_clear_scope(
        [selected], downstream=downstream, later_loop_iterations=later, session=session
    )

    expected_retry = {selected.id}
    if downstream:
        expected_retry.update(
            ti.id
            for ti in tis
            if ti.task_id == "outside"
            or (iteration(ti) == 2 and ti.task_id in {"body.consume", loop.gate_task_id})
        )
    assert scope.retry_ids == expected_retry
    assert scope.archive_ids == {
        ti.id for ti in tis if ti.task_id != "outside" and iteration(ti) > 2 and downstream and later
    }
    assert [(ti.id, ti.region_id, ti.region_index, ti.state, ti.working_set) for ti in tis] == before
    assert not session.new
    assert not session.deleted
    assert not session.dirty


@pytest.mark.parametrize("whole", [False, True])
def test_whole_mapped_task_selection_stays_in_selected_iteration(loop_run, session, whole):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2 and ti.region_index == 0
    )

    scope = select_loop_clear_scope(
        [selected, selected],
        whole_expansion_ids={selected.id} if whole else (),
        downstream=False,
        session=session,
    )

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == selected.task_id and iteration(ti) == 2 and (whole or ti.region_index == 0)
    }
    assert not scope.archive_ids


def test_direct_gate_clear_archives_later_passes_without_downstream(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    gate = next(ti for ti in tis if ti.task_id == loop.gate_task_id and ti.region_index == 2)

    scope = select_loop_clear_scope([gate], downstream=False, session=session)

    assert scope.retry_ids == {gate.id}
    assert scope.archive_ids == {ti.id for ti in tis if ti.task_id != "outside" and iteration(ti) > 2}


def test_loop_clear_uses_execution_pinned_graph_after_latest_definition_changes(loop_run, dag_maker, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)
    with dag_maker(serialized=True, session=session):
        EmptyOperator(task_id="replacement")

    scope = select_loop_clear_scope([selected], later_loop_iterations=False, session=session)

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == "outside"
        or (iteration(ti) == 2 and ti.task_id in {selected.task_id, loop.gate_task_id})
    }
    assert not scope.archive_ids


@pytest.mark.parametrize("width", [1, 3])
@pytest.mark.parametrize("whole", [False, True])
def test_loop_clear_preserves_mapped_group_relevant_indexes(dag_maker, session, width, whole):
    @task_group
    def mapped(value):
        PythonOperator(task_id="first", python_callable=list, op_kwargs={"value": value}) >> EmptyOperator(
            task_id="last"
        )

    @task_group
    def body():
        mapped.expand(value=list(range(width)))

    with dag_maker(serialized=True):
        loop = create_loop(body, max_iterations=3)
    dr = dag_maker.create_dagrun()
    tis = dr.get_task_instances(session=session)
    selected = next(ti for ti in tis if ti.task_id == "body.mapped.first" and ti.region_index == 0)

    scope = select_loop_clear_scope(
        [selected], whole_expansion_ids={selected.id} if whole else (), session=session
    )

    assert scope.retry_ids == {
        ti.id for ti in tis if ti.task_id == loop.gate_task_id or whole or ti.region_index == 0
    }
    assert not scope.archive_ids


def test_loop_clear_does_not_resurrect_archived_passes_after_repeated_selection(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2)
    first = select_loop_clear_scope([selected], whole_expansion_ids={selected.id}, session=session)
    replacement = DynamicRegion(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        node_id=loop.group_id,
        forked_from_region_id=root.id,
        resumes_from_index=3,
    )
    session.add(replacement)
    session.flush()
    for ti in tis:
        if ti.id in first.archive_ids:
            ti.archive(reason="superseded", session=session)
    session.flush()
    replacement_gate = TaskInstance(
        task=dag.get_task(loop.gate_task_id),
        run_id=dr.run_id,
        dag_version_id=dr.created_dag_version_id,
        region_id=replacement.id,
        region_index=3,
    )
    session.add(replacement_gate)
    session.flush()
    consume = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)

    second = select_loop_clear_scope([consume], later_loop_iterations=False, session=session)
    rewind = select_loop_clear_scope([consume], session=session)

    assert second.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == "outside"
        or (iteration(ti) == 2 and ti.task_id in {consume.task_id, loop.gate_task_id})
    }
    assert not second.archive_ids
    assert rewind.archive_ids == {replacement_gate.id}
    assert not (first.archive_ids & (second.retry_ids | rewind.archive_ids))


def test_loop_clear_rejects_deleted_selected_execution(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.consume")
    selected.archive(reason="superseded", session=session)

    with pytest.raises(ValueError, match="no longer live"):
        select_loop_clear_scope([selected], session=session)


def test_ordinary_seed_without_relatives_selects_only_itself(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "outside")

    scope = select_loop_clear_scope([selected], downstream=False, session=session)
    assert scope.retry_ids == {selected.id}
    assert not scope.archive_ids


def test_mixed_clear_scope_applies_together_and_empty_scope_is_noop(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    outside = next(ti for ti in tis if ti.task_id == "outside")
    gate = next(ti for ti in tis if ti.task_id == loop.gate_task_id and ti.region_index == 2)
    scope = select_loop_clear_scope([outside, gate], downstream=False, session=session)
    outside.state = gate.state = State.SUCCESS
    session.flush()

    cleared = apply_loop_clear_scope(scope, session=session, dag_run_state=False)
    assert apply_loop_clear_scope(LoopClearScope(frozenset(), frozenset()), session=session) == []

    assert {ti.id for ti in cleared if ti.working_set is None} == scope.archive_ids
    assert {ti.id for ti in cleared if ti.working_set is True} == {
        ti.id for ti in dr.get_task_instances(session=session) if ti.id not in {t.id for t in tis}
    }
    assert {ti.archived_reason for ti in cleared if ti.working_set is None} == {"superseded"}
    assert outside.working_set is None
    assert gate.working_set is None
    assert {ti.id for ti in dr.get_task_instances(session=session)} - {t.id for t in tis} == {
        ti.id for ti in cleared if ti.working_set is True
    }


def test_running_clear_rejection_does_not_allocate_fork(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    gate = next(ti for ti in tis if ti.task_id == loop.gate_task_id and ti.region_index == 2)
    gate.state = State.RUNNING
    session.flush()
    scope = select_loop_clear_scope([gate], downstream=False, session=session)

    with pytest.raises(AirflowClearRunningTaskException):
        apply_loop_clear_scope(scope, session=session, prevent_running_task=True)

    assert not session.scalar(
        select(DynamicRegion.id).where(DynamicRegion.forked_from_region_id.is_not(None))
    )


@pytest.mark.parametrize("downstream", [False, True])
def test_loop_upstream_scope_stays_in_iteration_before_downstream_expansion(loop_run, session, downstream):
    dr, dag, loop, root, tis, iteration = loop_run
    consume = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)

    scope = select_loop_clear_scope([consume], upstream=True, downstream=downstream, session=session)

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if (
            ti.task_id != "outside" and iteration(ti) == 2 and (downstream or ti.task_id != loop.gate_task_id)
        )
        or (downstream and ti.task_id == "outside")
    }
    assert scope.archive_ids == {
        ti.id for ti in tis if downstream and ti.task_id != "outside" and iteration(ti) > 2
    }


def test_outside_seed_upstream_selects_all_current_loop_occurrences(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    outside = next(ti for ti in tis if ti.task_id == "outside")

    scope = select_loop_clear_scope([outside], upstream=True, downstream=False, session=session)

    assert scope.retry_ids == {ti.id for ti in tis if ti.task_id == "outside" or iteration(ti) == 0}
    assert scope.archive_ids == {ti.id for ti in tis if ti.task_id != "outside" and iteration(ti) > 0}


@pytest.mark.parametrize("upstream", [False, True])
def test_outside_predecessor_selects_all_mapped_loop_occurrences(dag_maker, session, upstream):
    @task_group
    def body():
        PythonOperator.partial(task_id="process", python_callable=list).expand(op_kwargs=[{}, {}])

    with dag_maker(serialized=True):
        before = EmptyOperator(task_id="before")
        loop = create_loop(body, max_iterations=3)
        before >> loop
    dr = dag_maker.create_dagrun()
    root = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
    child = DynamicRegion(
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        node_id="body.process",
        parent_region_id=root.id,
        parent_region_index=1,
    )
    session.add(child)
    session.flush()
    for loop_task in loop.iter_tasks():
        mapped = loop_task.task_id == "body.process"
        for index in range(2) if mapped else [1]:
            session.add(
                TaskInstance(
                    task=loop_task,
                    run_id=dr.run_id,
                    dag_version_id=dr.created_dag_version_id,
                    region_id=child.id if mapped else root.id,
                    region_index=index,
                )
            )
    session.flush()
    tis = dr.get_task_instances(session=session)
    seed = next(
        ti
        for ti in tis
        if ti.task_id == ("body.process" if upstream else "before")
        and ti.region_index == (0 if upstream else -1)
    )

    scope = select_loop_clear_scope(
        [seed],
        upstream=upstream,
        downstream=True,
        later_loop_iterations=False,
        session=session,
    )

    assert scope.retry_ids == {ti.id for ti in tis}
    assert not scope.archive_ids


def test_loop_clear_refreshes_pending_execution_version_after_concurrent_clear(loop_run, dag_maker, session):
    dr, dag, loop, root, tis, iteration = loop_run
    selected = next(ti for ti in tis if ti.task_id == "body.consume" and ti.region_index == 2)
    old_version = selected.dag_version_id
    selected_id = selected.id

    @task_group
    def body():
        prepare = EmptyOperator(task_id="prepare")
        process = PythonOperator.partial(task_id="process", python_callable=list).expand(op_kwargs=[{}, {}])
        consume = EmptyOperator(task_id="consume")
        prepare >> process >> consume >> EmptyOperator(task_id="new")

    with dag_maker(serialized=True, session=session):
        create_loop(body, max_iterations=5) >> EmptyOperator(task_id="outside")
    new_version = DagVersion.get_latest_version(dr.dag_id, session=session).id
    session.commit()
    assert selected.dag_version_id == old_version
    with create_session(scoped=False) as other_session:
        pending = other_session.get(TaskInstance, selected_id)
        clear_task_instances([pending], other_session, run_on_latest_version=True)
        assert pending.id == selected_id
        assert pending.dag_version_id == new_version
        pending.dag_run.verify_integrity(session=other_session, dag_version_id=new_version)
        new_id = other_session.scalar(
            select(TaskInstance.id).where(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.run_id == dr.run_id,
                TaskInstance.task_id == "body.new",
                TaskInstance.region_index == 2,
            )
        )
        assert new_id is not None
    assert selected.dag_version_id == old_version

    scope = select_loop_clear_scope([selected], later_loop_iterations=False, session=session)

    assert scope.retry_ids == {
        ti.id
        for ti in tis
        if ti.task_id == "outside"
        or (iteration(ti) == 2 and ti.task_id in {selected.task_id, loop.gate_task_id})
    } | {new_id}
    assert selected.dag_version_id == new_version


def test_loop_clear_mixes_whole_and_single_index_for_same_task_in_different_passes(loop_run, session):
    dr, dag, loop, root, tis, iteration = loop_run
    whole = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 1 and ti.region_index == 0
    )
    single = next(
        ti for ti in tis if ti.task_id == "body.process" and iteration(ti) == 2 and ti.region_index == 0
    )

    scope = select_loop_clear_scope(
        [whole, single], whole_expansion_ids={whole.id}, downstream=False, session=session
    )

    assert scope.retry_ids == {ti.id for ti in tis if ti.task_id == whole.task_id and iteration(ti) == 1} | {
        single.id
    }
    assert not scope.archive_ids


@pytest.mark.parametrize("decision", ["stop", "continue"])
def test_repeated_selective_loop_clear_preserves_unselected_work(completed_loop, session, decision):
    dr, loop, complete_pass = completed_loop
    original = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}
    process = session.get(TaskInstance, original["body.process", 2])

    clear_loop_task_instances([process], session=session)
    complete_pass(2, "continue")
    complete_pass(3, "stop")
    first = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}
    consume = session.get(TaskInstance, first["body.consume", 2])
    clear_loop_task_instances([consume], later_loop_iterations=False, session=session)
    complete_pass(2, decision)

    current = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}
    assert current["body.prepare", 2] == original["body.prepare", 2]
    assert current["body.process", 2] == first["body.process", 2] != original["body.process", 2]
    assert current["body.consume", 2] != first["body.consume", 2]
    assert current[loop.gate_task_id, 2] != first[loop.gate_task_id, 2]
    assert {key: value for key, value in current.items() if key[1] == 3} == {
        key: value for key, value in first.items() if key[1] == 3
    }
    assert not any(index == 4 for _, index in current)
    assert session.get(TaskInstance, original["body.prepare", 2]).working_set is True
    original_four = session.get(TaskInstance, original[loop.gate_task_id, 4])
    assert (original_four.working_set, original_four.archived_reason) == (None, "superseded")
    assert session.get(TaskInstance, original["body.process", 2]).archived_reason == "retry"


def clear_run_through_dag_clear(dr, session):
    dr.dag.clear(run_id=dr.run_id, session=session)


def clear_run_through_api_service(dr, session):
    perform_clear_dag_run(
        session=session,
        dag=dr.dag,
        dag_run=dr,
        dag_id=dr.dag_id,
        only_failed=False,
        only_new=False,
        run_on_latest_version=False,
        note=None,
        user=None,
    )


def clear_run_through_partition_clear(dr, session):
    clear_partition_runs(
        dag=dr.dag,
        dag_id=dr.dag_id,
        run_id=dr.run_id,
        partition_key=None,
        partition_date_start=None,
        partition_date_end=None,
        clear_tis=True,
        dry_run=False,
        session=session,
    )


def clear_run_through_cli_bulk_clear(dr, session):
    _bulk_clear_runs(dr.dag_id, [dr.run_id], only_failed=False, only_running=False, session=session)


@pytest.mark.parametrize(
    "clear_run",
    [
        clear_run_through_dag_clear,
        clear_run_through_api_service,
        clear_run_through_partition_clear,
        clear_run_through_cli_bulk_clear,
    ],
)
class TestWholeRunClear:
    def test_archives_every_later_pass_and_regenerates_from_the_first(
        self, completed_loop, session, clear_run
    ):
        dr, loop, complete_pass = completed_loop
        original = {(ti.task_id, ti.region_index): ti.id for ti in dr.get_task_instances(session=session)}

        clear_run(dr, session)

        live = dr.get_task_instances(session=session)
        assert {ti.region_index for ti in live} == {0}
        for (task_id, index), ti_id in original.items():
            archived = session.get(TaskInstance, ti_id)
            if index == 0:
                assert (archived.working_set, archived.archived_reason) == (None, "retry"), task_id
            else:
                assert (archived.working_set, archived.archived_reason) == (None, "superseded"), task_id
        fork = session.scalar(select(DynamicRegion).where(DynamicRegion.forked_from_region_id.is_not(None)))
        assert fork.resumes_from_index == 1

        complete_pass(0, "continue")

        regenerated = [ti for ti in dr.get_task_instances(session=session) if ti.region_index == 1]
        assert len(regenerated) == 4
        assert {ti.region_id for ti in regenerated} == {fork.id}

    def test_rerun_can_shorten_the_loop(self, completed_loop, session, clear_run):
        dr, loop, complete_pass = completed_loop

        clear_run(dr, session)
        complete_pass(0, "stop")

        assert {ti.region_index for ti in dr.get_task_instances(session=session)} == {0}


def test_selected_state_patch_locks_dag_run_before_task_instances(completed_loop, session):
    dr, loop, _ = completed_loop
    selected = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == "body.consume" and ti.region_index == 2
    )
    locked: list[str] = []

    @event.listens_for(session, "do_orm_execute")
    def record_locks(orm_execute_state):
        statement = orm_execute_state.statement
        if isinstance(statement, Select) and statement._for_update_arg is not None:
            locked.append(str(statement))

    try:
        _patch_selected_task_state(
            [selected],
            PatchTaskInstanceBody(new_state="failed"),
            {"new_state": State.FAILED},
            session=session,
            commit=True,
        )
    finally:
        event.remove(session, "do_orm_execute", record_locks)

    tables = ["dag_run" if "FROM dag_run" in sql else "task_instance" for sql in locked]
    assert "task_instance" in tables
    assert tables[0] == "dag_run"


@pytest.mark.backend("mysql", "postgres")
def test_selected_state_patch_and_clear_take_locks_in_the_same_order(completed_loop, session):
    dr, loop, _ = completed_loop
    selected = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == "body.consume" and ti.region_index == 2
    )
    selected_id = selected.id
    next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == loop.gate_task_id and ti.region_index == 2
    ).state = State.UPSTREAM_FAILED
    session.commit()
    bind = session.get_bind()
    clear_holds_run_lock = Event()

    def patch_state():
        with Session(bind=bind) as patch_session:
            locks = 0
            waited = False

            @event.listens_for(patch_session, "do_orm_execute")
            def pause_after_first_lock(orm_execute_state):
                nonlocal locks, waited
                statement = orm_execute_state.statement
                if isinstance(statement, Select) and statement._for_update_arg is not None:
                    locks += 1
                elif locks and not waited:
                    waited = True
                    clear_holds_run_lock.wait(timeout=3)

            _patch_selected_task_state(
                [patch_session.get(TaskInstance, selected_id)],
                PatchTaskInstanceBody(new_state="failed"),
                {"new_state": State.FAILED},
                session=patch_session,
                commit=True,
            )
            patch_session.commit()

    def clear():
        with Session(bind=bind) as clear_session:
            run_locked = False

            @event.listens_for(clear_session, "do_orm_execute")
            def signal_after_run_lock(orm_execute_state):
                nonlocal run_locked
                statement = orm_execute_state.statement
                if run_locked:
                    clear_holds_run_lock.set()
                elif isinstance(statement, Select) and statement._for_update_arg is not None:
                    run_locked = True

            clear_task_instances([clear_session.get(TaskInstance, selected_id)], session=clear_session)
            clear_session.commit()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(patch_state), pool.submit(clear)]
        for future in futures:
            future.result(timeout=30)


def test_ordinary_loop_task_clear_does_not_allocate_fork(completed_loop, session):
    dr, loop, complete_pass = completed_loop
    selected = next(
        ti
        for ti in dr.get_task_instances(session=session)
        if ti.task_id == "body.consume" and ti.region_index == 2
    )
    coordinate = selected.region_id, selected.region_index
    original_id = selected.id

    clear_loop_task_instances([selected], downstream=False, session=session)

    successor = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == dr.dag_id,
            TaskInstance.task_id == "body.consume",
            TaskInstance.region_index == 2,
            TaskInstance.working_set.is_(True),
        )
    ).one()
    assert successor.id != original_id
    assert (successor.region_id, successor.region_index) == coordinate
    assert (selected.working_set, selected.archived_reason) == (None, "retry")
    assert len(session.scalars(select(DynamicRegion).where(DynamicRegion.dag_id == dr.dag_id)).all()) == 1
