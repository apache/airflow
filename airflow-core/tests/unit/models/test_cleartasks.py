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

import pytest
from sqlalchemy import select

from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstance as TI, clear_task_instances
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskreschedule import TaskReschedule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.dag import sync_dag_to_db
from unit.models import DEFAULT_DATE

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


class TestClearTasks:
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
            task0 = EmptyOperator(task_id="0")
            task1 = EmptyOperator(task_id="1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

        ti0.run()
        ti1.run()

        with create_session() as session:
            # do the incrementing of try_number ordinarily handled by scheduler
            ti0.try_number += 1
            ti1.try_number += 1
            ti0 = session.merge(ti0)
            ti1 = session.merge(ti1)
            session.commit()

            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
            clear_task_instances(qry, session)

            ti0.refresh_from_db(session)
            ti1.refresh_from_db(session)

        # Next try to run will be try 2
        assert ti0.state is None
        assert ti0.try_number == 1
        assert ti0.max_tries == 1
        assert ti1.state is None
        assert ti1.try_number == 1
        assert ti1.max_tries == 3

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
            qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
            clear_task_instances(qry, session)

            ti0.refresh_from_db()

            assert ti0.state is None
            assert ti0.external_executor_id is None

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

        clear_task_instances([ti0], session)

        ti0.refresh_from_db()

        assert ti0.next_method is None
        assert ti0.next_kwargs is None

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
        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
        assert session.query(TaskInstanceHistory).count() == 0
        clear_task_instances(qry, session, dag_run_state=state)
        session.flush()
        # 2 TIs were cleared so 2 history records should be created
        assert session.query(TaskInstanceHistory).count() == 2

        session.refresh(dr)

        assert dr.state == state
        assert dr.start_date is None if state == DagRunState.QUEUED else dr.start_date
        assert dr.last_scheduling_decision == last_scheduling

    @pytest.mark.parametrize("state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_clear_task_instances_on_running_dr(self, state, dag_maker):
        """
        Test that DagRun state, start_date and last_scheduling_decision
        are not changed after clearing TI in an unfinished DagRun.
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

        # we use order_by(task_id) here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
        clear_task_instances(qry, session)
        session.flush()

        session.refresh(dr)

        assert dr.state == state
        if state == DagRunState.QUEUED:
            assert dr.start_date is None
        if state == DagRunState.RUNNING:
            assert dr.start_date
        assert dr.last_scheduling_decision == DEFAULT_DATE

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
        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
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
            task0 = EmptyOperator(task_id="task0")
            task1 = EmptyOperator(task_id="task1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

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
        clear_task_instances([ti0, ti1], session)

        # When no task is found, max_tries will be maximum of original max_tries or try_number.
        session.refresh(ti0)
        session.refresh(ti1)
        assert ti0.try_number == 1
        assert ti0.max_tries == 1
        assert ti0.state is None
        assert ti1.try_number == 1
        assert ti1.state is None
        if delete_tasks:
            assert ti1.max_tries == 2
        else:
            assert ti1.max_tries == 3
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
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

        with create_session() as session:
            # do the incrementing of try_number ordinarily handled by scheduler
            ti0.try_number += 1
            ti1.try_number += 1
            session.merge(ti0)
            session.merge(ti1)
            session.commit()

        ti0.run(session=session)
        ti1.run(session=session)

        # we use order_by(task_id) here because for the test DAG structure of ours
        # this is equivalent to topological sort. It would not work in general case
        # but it works for our case because we specifically constructed test DAGS
        # in the way that those two sort methods are equivalent
        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
        clear_task_instances(qry, session)

        ti0.refresh_from_db(session=session)
        ti1.refresh_from_db(session=session)
        assert ti0.try_number == 1
        assert ti0.max_tries == 1
        assert ti1.try_number == 1
        assert ti1.max_tries == 3

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

        # simulate running the task
        # do the incrementing of try_number ordinarily handled by scheduler
        ti0.try_number += 1
        ti1.try_number += 1

        session.commit()

        clear_task_instances([ti0, ti1], session)

        session.refresh(ti0)
        session.refresh(ti1)

        assert ti0.try_number == 1
        assert ti0.max_tries == 1
        assert ti1.try_number == 1
        assert ti1.max_tries == 3

    def test_clear_task_instances_with_task_reschedule(self, dag_maker):
        """Test that TaskReschedules are deleted correctly when TaskInstances are cleared"""

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
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

        with create_session() as session:
            # do the incrementing of try_number ordinarily handled by scheduler
            ti0.try_number += 1
            ti1.try_number += 1
            session.merge(ti0)
            session.merge(ti1)
            session.commit()

        ti0.run()
        ti1.run()

        with create_session() as session:

            def count_task_reschedule(ti):
                return session.query(TaskReschedule).filter(TaskReschedule.ti_id == ti.id).count()

            assert count_task_reschedule(ti0) == 1
            assert count_task_reschedule(ti1) == 1
            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = (
                session.query(TI)
                .filter(TI.dag_id == dag.dag_id, TI.task_id == ti0.task_id)
                .order_by(TI.task_id)
                .all()
            )
            clear_task_instances(qry, session)
            assert count_task_reschedule(ti0) == 0
            assert count_task_reschedule(ti1) == 1

    @pytest.mark.parametrize(
        ("state", "state_recorded"),
        [
            (TaskInstanceState.SUCCESS, TaskInstanceState.SUCCESS),
            (TaskInstanceState.FAILED, TaskInstanceState.FAILED),
            (TaskInstanceState.SKIPPED, TaskInstanceState.SKIPPED),
            (TaskInstanceState.UP_FOR_RETRY, TaskInstanceState.FAILED),
            (TaskInstanceState.UP_FOR_RESCHEDULE, TaskInstanceState.FAILED),
            (TaskInstanceState.RUNNING, TaskInstanceState.FAILED),
            (TaskInstanceState.QUEUED, TaskInstanceState.FAILED),
            (TaskInstanceState.SCHEDULED, TaskInstanceState.FAILED),
            (None, TaskInstanceState.FAILED),
            (TaskInstanceState.RESTARTING, TaskInstanceState.FAILED),
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
        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
        clear_task_instances(qry, session)
        session.flush()

        session.refresh(dr)
        ti_history = session.scalars(select(TaskInstanceHistory.state)).all()

        assert [ti_history[0], ti_history[1]] == [str(state_recorded), str(state_recorded)]

    def test_dag_clear(self, dag_maker, session):
        with dag_maker("test_dag_clear") as dag:
            EmptyOperator(task_id="test_dag_clear_task_0")
            EmptyOperator(task_id="test_dag_clear_task_1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)

        ti0.try_number += 1
        session.commit()

        # Next try to run will be try 1
        assert ti0.try_number == 1

        dag.clear(session=session)
        session.commit()

        assert ti0.try_number == 1
        assert ti0.state == State.NONE
        assert ti0.max_tries == 1
        assert ti1.max_tries == 2

        ti1.try_number += 1
        session.commit()

        assert ti1.try_number == 1
        assert ti1.max_tries == 2

        dag.clear(session=session)

        # after clear dag, we have 2 remaining tries
        assert ti1.max_tries == 3
        assert ti1.try_number == 1
        # after clear dag, ti0 has no remaining tries
        assert ti0.try_number == 1
        assert ti0.max_tries == 1

    def test_dags_clear(self, dag_maker, session):
        dags, tis = [], []
        num_of_dags = 5
        for i in range(num_of_dags):
            with dag_maker(
                f"test_dag_clear_{i}",
                schedule=datetime.timedelta(days=1),
                serialized=True,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            ):
                task = EmptyOperator(task_id=f"test_task_clear_{i}", owner="test")

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
            ti = dr.task_instances[0]
            ti.task = task
            dags.append(dag_maker.serialized_model.dag)
            tis.append(ti)

        # test clear all dags
        for i in range(num_of_dags):
            session.get(TaskInstance, tis[i].id).try_number += 1
            session.commit()
            tis[i].run()
            assert tis[i].state == State.SUCCESS
            assert tis[i].try_number == 1
            assert tis[i].max_tries == 0
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

        SerializedDAG.clear_dags(dags)
        session.commit()
        for i in range(num_of_dags):
            ti = _get_ti(tis[i])
            assert ti.state == State.NONE
            assert ti.try_number == 1
            assert ti.max_tries == 1

        # test dry_run
        for i, dag in enumerate(dags):
            ti = _get_ti(tis[i])
            ti.try_number += 1
            session.commit()
            ti.refresh_from_task(dag.get_task(ti.task_id))
            # Directly set state to SUCCESS instead of calling ti.run() to avoid timeout
            ti.state = State.SUCCESS
            session.commit()
            assert ti.state == State.SUCCESS
            assert ti.try_number == 2
            assert ti.max_tries == 1
        session.commit()
        SerializedDAG.clear_dags(dags, dry_run=True)
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
        session.commit()

        SerializedDAG.clear_dags(dags, only_failed=True)

        for ti_in in tis:
            ti = _get_ti(ti_in)
            if ti.dag_id == ti_fail.dag_id:
                assert ti.state == State.NONE
                assert ti.try_number == 2
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
        ):
            task0 = EmptyOperator(task_id="0")
            task1 = EmptyOperator(task_id="1", retries=2)
        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        old_dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

        ti0.run()
        ti1.run()
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
        qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
        clear_task_instances(qry, session, run_on_latest_version=run_on_latest_version)
        session.commit()
        dr = session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).one()
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
