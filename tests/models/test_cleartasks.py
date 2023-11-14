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

from airflow import settings
from airflow.models.dag import DAG
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance as TI, clear_task_instances
from airflow.models.taskreschedule import TaskReschedule
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunType
from tests.models import DEFAULT_DATE
from tests.test_utils import db

pytestmark = pytest.mark.db_test


class TestClearTasks:
    @pytest.fixture(autouse=True, scope="class")
    def clean(self):
        db.clear_db_runs()

        yield

        db.clear_db_runs()

    def test_clear_task_instances(self, dag_maker):
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
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
            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
            clear_task_instances(qry, session, dag=dag)

        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        assert ti0.state is None
        assert ti0.try_number == 2
        assert ti0.max_tries == 1
        assert ti1.state is None
        assert ti1.try_number == 2
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
            clear_task_instances(qry, session, dag=dag)

            ti0.refresh_from_db()

            assert ti0.state is None
            assert ti0.external_executor_id is None

    def test_clear_task_instances_next_method(self, dag_maker, session):
        with dag_maker(
            "test_clear_task_instances_next_method",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
        ) as dag:
            EmptyOperator(task_id="task0")

        ti0 = dag_maker.create_dagrun().task_instances[0]
        ti0.state = State.DEFERRED
        ti0.next_method = "next_method"
        ti0.next_kwargs = {}

        session.add(ti0)
        session.commit()

        clear_task_instances([ti0], session, dag=dag)

        ti0.refresh_from_db()

        assert ti0.next_method is None
        assert ti0.next_kwargs is None

    @pytest.mark.parametrize(
        ["state", "last_scheduling"], [(DagRunState.QUEUED, None), (DagRunState.RUNNING, DEFAULT_DATE)]
    )
    def test_clear_task_instances_dr_state(self, state, last_scheduling, dag_maker):
        """Test that DR state is set to None after clear.
        And that DR.last_scheduling_decision is handled OK.
        start_date is also set to None
        """
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
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
        clear_task_instances(qry, session, dag_run_state=state, dag=dag)
        session.flush()

        session.refresh(dr)

        assert dr.state == state
        assert dr.start_date is None if state == DagRunState.QUEUED else dr.start_date
        assert dr.last_scheduling_decision == last_scheduling

    @pytest.mark.parametrize("state", [DagRunState.QUEUED, DagRunState.RUNNING])
    def test_clear_task_instances_on_running_dr(self, state, dag_maker):
        """Test that DagRun state, start_date and last_scheduling_decision
        are not changed after clearing TI in an unfinished DagRun.
        """
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
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
        clear_task_instances(qry, session, dag=dag)
        session.flush()

        session.refresh(dr)

        assert dr.state == state
        assert dr.start_date
        assert dr.last_scheduling_decision == DEFAULT_DATE

    @pytest.mark.parametrize(
        ["state", "last_scheduling"],
        [
            (DagRunState.SUCCESS, None),
            (DagRunState.SUCCESS, DEFAULT_DATE),
            (DagRunState.FAILED, None),
            (DagRunState.FAILED, DEFAULT_DATE),
        ],
    )
    def test_clear_task_instances_on_finished_dr(self, state, last_scheduling, dag_maker):
        """Test that DagRun state, start_date and last_scheduling_decision
        are changed after clearing TI in a finished DagRun.
        """
        with dag_maker(
            "test_clear_task_instances",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
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
        clear_task_instances(qry, session, dag=dag)
        session.flush()

        session.refresh(dr)

        assert dr.state == DagRunState.QUEUED
        assert dr.start_date is None
        assert dr.last_scheduling_decision is None

    def test_clear_task_instances_without_task(self, dag_maker):
        with dag_maker(
            "test_clear_task_instances_without_task",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
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

        ti0.run()
        ti1.run()

        # Remove the task from dag.
        dag.task_dict = {}
        assert not dag.has_task(task0.task_id)
        assert not dag.has_task(task1.task_id)

        with create_session() as session:
            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
            clear_task_instances(qry, session, dag=dag)

        # When no task is found, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        assert ti0.try_number == 2
        assert ti0.max_tries == 1
        assert ti1.try_number == 2
        assert ti1.max_tries == 2

    def test_clear_task_instances_without_dag(self, dag_maker):
        # Don't write DAG to the database, so no DAG is found by clear_task_instances().
        with dag_maker(
            "test_clear_task_instances_without_dag",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
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

        ti0.run()
        ti1.run()

        with create_session() as session:
            # we use order_by(task_id) here because for the test DAG structure of ours
            # this is equivalent to topological sort. It would not work in general case
            # but it works for our case because we specifically constructed test DAGS
            # in the way that those two sort methods are equivalent
            qry = session.query(TI).filter(TI.dag_id == dag.dag_id).order_by(TI.task_id).all()
            clear_task_instances(qry, session)

        # When no DAG is found, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        assert ti0.try_number == 2
        assert ti0.max_tries == 1
        assert ti1.try_number == 2
        assert ti1.max_tries == 2

    def test_clear_task_instances_without_dag_param(self, dag_maker, session):
        with dag_maker(
            "test_clear_task_instances_without_dag_param",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            session=session,
        ) as dag:
            task0 = EmptyOperator(task_id="task0")
            task1 = EmptyOperator(task_id="task1", retries=2)

        # Write DAG to the database so it can be found by clear_task_instances().
        SerializedDagModel.write_dag(dag, session=session)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

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
        # Next try to run will be try 2
        assert ti0.try_number == 2
        assert ti0.max_tries == 1
        assert ti1.try_number == 2
        assert ti1.max_tries == 3

    def test_clear_task_instances_in_multiple_dags(self, dag_maker, session):
        with dag_maker(
            "test_clear_task_instances_in_multiple_dags0",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            session=session,
        ) as dag0:
            task0 = EmptyOperator(task_id="task0")

        dr0 = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        with dag_maker(
            "test_clear_task_instances_in_multiple_dags1",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            session=session,
        ) as dag1:
            task1 = EmptyOperator(task_id="task1", retries=2)

        # Write secondary DAG to the database so it can be found by clear_task_instances().
        SerializedDagModel.write_dag(dag1, session=session)

        dr1 = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti0 = dr0.task_instances[0]
        ti1 = dr1.task_instances[0]
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

        ti0.run(session=session)
        ti1.run(session=session)

        qry = session.query(TI).filter(TI.dag_id.in_((dag0.dag_id, dag1.dag_id))).all()
        clear_task_instances(qry, session, dag=dag0)

        ti0.refresh_from_db(session=session)
        ti1.refresh_from_db(session=session)
        # Next try to run will be try 2
        assert ti0.try_number == 2
        assert ti0.max_tries == 1
        assert ti1.try_number == 2
        assert ti1.max_tries == 3

    def test_clear_task_instances_with_task_reschedule(self, dag_maker):
        """Test that TaskReschedules are deleted correctly when TaskInstances are cleared"""

        with dag_maker(
            "test_clear_task_instances_with_task_reschedule",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
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
        ti0.run()
        ti1.run()

        with create_session() as session:

            def count_task_reschedule(task_id):
                return (
                    session.query(TaskReschedule)
                    .filter(
                        TaskReschedule.dag_id == dag.dag_id,
                        TaskReschedule.task_id == task_id,
                        TaskReschedule.run_id == dr.run_id,
                        TaskReschedule.try_number == 1,
                    )
                    .count()
                )

            assert count_task_reschedule(ti0.task_id) == 1
            assert count_task_reschedule(ti1.task_id) == 1
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
            clear_task_instances(qry, session, dag=dag)
            assert count_task_reschedule(ti0.task_id) == 0
            assert count_task_reschedule(ti1.task_id) == 1

    def test_dag_clear(self, dag_maker):
        with dag_maker(
            "test_dag_clear", start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + datetime.timedelta(days=10)
        ) as dag:
            task0 = EmptyOperator(task_id="test_dag_clear_task_0")
            task1 = EmptyOperator(task_id="test_dag_clear_task_1", retries=2)

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )
        session = dag_maker.session

        ti0, ti1 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti0.refresh_from_task(task0)
        ti1.refresh_from_task(task1)

        # Next try to run will be try 1
        assert ti0.try_number == 1
        ti0.run()

        assert ti0.try_number == 2
        dag.clear()
        ti0.refresh_from_db()
        assert ti0.try_number == 2
        assert ti0.state == State.NONE
        assert ti0.max_tries == 1

        assert ti1.max_tries == 2
        ti1.try_number = 1
        session.merge(ti1)
        session.commit()

        # Next try will be 2
        ti1.run()

        assert ti1.try_number == 3
        assert ti1.max_tries == 2

        dag.clear()
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # after clear dag, ti2 should show attempt 3 of 5
        assert ti1.max_tries == 4
        assert ti1.try_number == 3
        # after clear dag, ti1 should show attempt 2 of 2
        assert ti0.try_number == 2
        assert ti0.max_tries == 1

    def test_dags_clear(self):
        # setup
        session = settings.Session()
        dags, tis = [], []
        num_of_dags = 5
        for i in range(num_of_dags):
            dag = DAG(
                f"test_dag_clear_{i}",
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE + datetime.timedelta(days=10),
            )
            task = EmptyOperator(task_id=f"test_task_clear_{i}", owner="test", dag=dag)

            dr = dag.create_dagrun(
                execution_date=DEFAULT_DATE,
                state=State.RUNNING,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
            ti = dr.task_instances[0]
            ti.task = task
            dags.append(dag)
            tis.append(ti)

        # test clear all dags
        for i in range(num_of_dags):
            tis[i].run()
            assert tis[i].state == State.SUCCESS
            assert tis[i].try_number == 2
            assert tis[i].max_tries == 0

        DAG.clear_dags(dags)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            assert tis[i].state == State.NONE
            assert tis[i].try_number == 2
            assert tis[i].max_tries == 1

        # test dry_run
        for i in range(num_of_dags):
            tis[i].run()
            assert tis[i].state == State.SUCCESS
            assert tis[i].try_number == 3
            assert tis[i].max_tries == 1

        DAG.clear_dags(dags, dry_run=True)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            assert tis[i].state == State.SUCCESS
            assert tis[i].try_number == 3
            assert tis[i].max_tries == 1

        # test only_failed
        failed_dag = random.choice(tis)
        failed_dag.state = State.FAILED
        session.merge(failed_dag)
        session.commit()

        DAG.clear_dags(dags, only_failed=True)

        for ti in tis:
            ti.refresh_from_db()
            if ti is failed_dag:
                assert ti.state == State.NONE
                assert ti.try_number == 3
                assert ti.max_tries == 2
            else:
                assert ti.state == State.SUCCESS
                assert ti.try_number == 3
                assert ti.max_tries == 1

    def test_operator_clear(self, dag_maker):
        with dag_maker(
            "test_operator_clear",
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
        ):
            op1 = EmptyOperator(task_id="test1")
            op2 = EmptyOperator(task_id="test2", retries=1)
            op1 >> op2

        dr = dag_maker.create_dagrun(
            state=State.RUNNING,
            run_type=DagRunType.SCHEDULED,
        )

        ti1, ti2 = sorted(dr.task_instances, key=lambda ti: ti.task_id)
        ti1.task = op1
        ti2.task = op2

        ti2.run()
        # Dependency not met
        assert ti2.try_number == 1
        assert ti2.max_tries == 1

        op2.clear(upstream=True)
        ti1.run()
        ti2.run(ignore_ti_state=True)
        assert ti1.try_number == 2
        # max_tries is 0 because there is no task instance in db for ti1
        # so clear won't change the max_tries.
        assert ti1.max_tries == 0
        assert ti2.try_number == 2
        # try_number (0) + retries(1)
        assert ti2.max_tries == 1
