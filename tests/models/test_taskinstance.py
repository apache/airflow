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
import operator
import os
import pathlib
import signal
import sys
import urllib
from traceback import format_exception
from typing import cast
from unittest import mock
from unittest.mock import call, mock_open, patch
from uuid import uuid4

import pendulum
import pytest
from freezegun import freeze_time

from airflow import models, settings
from airflow.decorators import task
from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
    UnmappableXComLengthPushed,
    UnmappableXComTypePushed,
    XComForMappingNotPushed,
)
from airflow.models import (
    DAG,
    Connection,
    DagBag,
    DagRun,
    Pool,
    RenderedTaskInstanceFields,
    TaskInstance as TI,
    TaskReschedule,
    Variable,
    XCom,
)
from airflow.models.dataset import DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.models.expandinput import EXPAND_INPUT_EMPTY
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskfail import TaskFail
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskmap import TaskMap
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.sensors.python import PythonSensor
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.ti_deps.dependencies_states import RUNNABLE_STATES
from airflow.ti_deps.deps.base_ti_dep import TIDepStatus
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.db import merge_conn
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType
from airflow.version import version
from tests.models import DEFAULT_DATE, TEST_DAGS_FOLDER
from tests.test_utils import db
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_connections, clear_db_runs
from tests.test_utils.mock_operators import MockOperator


@pytest.fixture
def test_pool():
    with create_session() as session:
        test_pool = Pool(pool='test_pool', slots=1)
        session.add(test_pool)
        session.flush()
        yield test_pool
        session.rollback()


class CallbackWrapper:
    task_id: str | None = None
    dag_id: str | None = None
    execution_date: datetime.datetime | None = None
    task_state_in_callback: str | None = None
    callback_ran = False

    def wrap_task_instance(self, ti):
        self.task_id = ti.task_id
        self.dag_id = ti.dag_id
        self.execution_date = ti.execution_date
        self.task_state_in_callback = ""
        self.callback_ran = False

    def success_handler(self, context):
        self.callback_ran = True
        self.task_state_in_callback = context['ti'].state


class TestTaskInstance:
    @staticmethod
    def clean_db():
        db.clear_db_dags()
        db.clear_db_pools()
        db.clear_db_runs()
        db.clear_db_task_fail()
        db.clear_rendered_ti_fields()
        db.clear_db_task_reschedule()
        db.clear_db_datasets()

    def setup_method(self):
        self.clean_db()

        # We don't want to store any code for (test) dags created in this file
        with patch.object(settings, "STORE_DAG_CODE", False):
            yield

    def teardown_method(self):
        self.clean_db()

    def test_set_task_dates(self, dag_maker):
        """
        Test that tasks properly take start/end dates from DAGs
        """
        with dag_maker('dag', end_date=DEFAULT_DATE + datetime.timedelta(days=10)) as dag:
            pass

        op1 = EmptyOperator(task_id='op_1')

        assert op1.start_date is None and op1.end_date is None

        # dag should assign its dates to op1 because op1 has no dates
        dag.add_task(op1)
        dag_maker.create_dagrun()
        assert op1.start_date == dag.start_date and op1.end_date == dag.end_date

        op2 = EmptyOperator(
            task_id='op_2',
            start_date=DEFAULT_DATE - datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=11),
        )

        # dag should assign its dates to op2 because they are more restrictive
        dag.add_task(op2)
        assert op2.start_date == dag.start_date and op2.end_date == dag.end_date

        op3 = EmptyOperator(
            task_id='op_3',
            start_date=DEFAULT_DATE + datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=9),
        )
        # op3 should keep its dates because they are more restrictive
        dag.add_task(op3)
        assert op3.start_date == DEFAULT_DATE + datetime.timedelta(days=1)
        assert op3.end_date == DEFAULT_DATE + datetime.timedelta(days=9)

    def test_current_state(self, create_task_instance, session):
        ti = create_task_instance(session=session)
        assert ti.current_state(session=session) is None
        ti.run()
        assert ti.current_state(session=session) == State.SUCCESS

    def test_set_dag(self, dag_maker):
        """
        Test assigning Operators to Dags, including deferred assignment
        """
        with dag_maker('dag') as dag:
            pass
        with dag_maker('dag2') as dag2:
            pass
        op = EmptyOperator(task_id='op_1')

        # no dag assigned
        assert not op.has_dag()
        with pytest.raises(AirflowException):
            getattr(op, 'dag')

        # no improper assignment
        with pytest.raises(TypeError):
            op.dag = 1

        op.dag = dag

        # no reassignment
        with pytest.raises(AirflowException):
            op.dag = dag2

        # but assigning the same dag is ok
        op.dag = dag

        assert op.dag is dag
        assert op in dag.tasks

    def test_infer_dag(self, create_dummy_dag):
        op1 = EmptyOperator(task_id='test_op_1')
        op2 = EmptyOperator(task_id='test_op_2')

        dag, op3 = create_dummy_dag(task_id='test_op_3')

        _, op4 = create_dummy_dag('dag2', task_id='test_op_4')

        # double check dags
        assert [i.has_dag() for i in [op1, op2, op3, op4]] == [False, False, True, True]

        # can't combine operators with no dags
        with pytest.raises(AirflowException):
            op1.set_downstream(op2)

        # op2 should infer dag from op1
        op1.dag = dag
        op1.set_downstream(op2)
        assert op2.dag is dag

        # can't assign across multiple DAGs
        with pytest.raises(AirflowException):
            op1.set_downstream(op4)
        with pytest.raises(AirflowException):
            op1.set_downstream([op3, op4])

    def test_bitshift_compose_operators(self, dag_maker):
        with dag_maker('dag'):
            op1 = EmptyOperator(task_id='test_op_1')
            op2 = EmptyOperator(task_id='test_op_2')
            op3 = EmptyOperator(task_id='test_op_3')

            op1 >> op2 << op3
        dag_maker.create_dagrun()
        # op2 should be downstream of both
        assert op2 in op1.downstream_list
        assert op2 in op3.downstream_list

    def test_init_on_load(self, create_task_instance):
        ti = create_task_instance()
        # ensure log is correctly created for ORM ti
        assert ti.log.name == 'airflow.task'
        assert not ti.test_mode

    @patch.object(DAG, 'get_concurrency_reached')
    def test_requeue_over_dag_concurrency(self, mock_concurrency_reached, create_task_instance):
        mock_concurrency_reached.return_value = True

        ti = create_task_instance(
            dag_id='test_requeue_over_dag_concurrency',
            task_id='test_requeue_over_dag_concurrency_op',
            max_active_runs=1,
            max_active_tasks=2,
            dagrun_state=State.QUEUED,
        )
        ti.run()
        assert ti.state == State.NONE

    def test_requeue_over_max_active_tis_per_dag(self, create_task_instance):
        ti = create_task_instance(
            dag_id='test_requeue_over_max_active_tis_per_dag',
            task_id='test_requeue_over_max_active_tis_per_dag_op',
            max_active_tis_per_dag=0,
            max_active_runs=1,
            max_active_tasks=2,
            dagrun_state=State.QUEUED,
        )

        ti.run()
        assert ti.state == State.NONE

    def test_requeue_over_pool_concurrency(self, create_task_instance, test_pool):
        ti = create_task_instance(
            dag_id='test_requeue_over_pool_concurrency',
            task_id='test_requeue_over_pool_concurrency_op',
            max_active_tis_per_dag=0,
            max_active_runs=1,
            max_active_tasks=2,
        )
        with create_session() as session:
            test_pool.slots = 0
            session.flush()
            ti.run()
            assert ti.state == State.NONE

    @pytest.mark.usefixtures('test_pool')
    def test_not_requeue_non_requeueable_task_instance(self, dag_maker):
        # Use BaseSensorOperator because sensor got
        # one additional DEP in BaseSensorOperator().deps
        with dag_maker(dag_id='test_not_requeue_non_requeueable_task_instance'):
            task = BaseSensorOperator(
                task_id='test_not_requeue_non_requeueable_task_instance_op',
                pool='test_pool',
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        ti.state = State.QUEUED
        with create_session() as session:
            session.add(ti)
            session.commit()

        all_deps = RUNNING_DEPS | task.deps
        all_non_requeueable_deps = all_deps - REQUEUEABLE_DEPS
        patch_dict = {}
        for dep in all_non_requeueable_deps:
            class_name = dep.__class__.__name__
            dep_patch = patch(f'{dep.__module__}.{class_name}.{dep._get_dep_statuses.__name__}')
            method_patch = dep_patch.start()
            method_patch.return_value = iter([TIDepStatus('mock_' + class_name, True, 'mock')])
            patch_dict[class_name] = (dep_patch, method_patch)

        for class_name, (dep_patch, method_patch) in patch_dict.items():
            method_patch.return_value = iter([TIDepStatus('mock_' + class_name, False, 'mock')])
            ti.run()
            assert ti.state == State.QUEUED
            dep_patch.return_value = TIDepStatus('mock_' + class_name, True, 'mock')

        for (dep_patch, method_patch) in patch_dict.values():
            dep_patch.stop()

    def test_mark_non_runnable_task_as_success(self, create_task_instance):
        """
        test that running task with mark_success param update task state
        as SUCCESS without running task despite it fails dependency checks.
        """
        non_runnable_state = (set(State.task_states) - RUNNABLE_STATES - set(State.SUCCESS)).pop()
        ti = create_task_instance(
            dag_id='test_mark_non_runnable_task_as_success',
            task_id='test_mark_non_runnable_task_as_success_op',
            state=non_runnable_state,
        )
        ti.run(mark_success=True)
        assert ti.state == State.SUCCESS

    @pytest.mark.usefixtures('test_pool')
    def test_run_pooling_task(self, create_task_instance):
        """
        test that running a task in an existing pool update task state as SUCCESS.
        """
        ti = create_task_instance(
            dag_id='test_run_pooling_task',
            task_id='test_run_pooling_task_op',
            pool='test_pool',
        )

        ti.run()

        assert ti.state == State.SUCCESS

    @pytest.mark.usefixtures('test_pool')
    def test_pool_slots_property(self):
        """
        test that try to create a task with pool_slots less than 1
        """

        with pytest.raises(ValueError, match="pool slots .* cannot be less than 1"):
            dag = models.DAG(dag_id='test_run_pooling_task')
            EmptyOperator(
                task_id='test_run_pooling_task_op',
                dag=dag,
                pool='test_pool',
                pool_slots=0,
            )

    @provide_session
    def test_ti_updates_with_task(self, create_task_instance, session=None):
        """
        test that updating the executor_config propagates to the TaskInstance DB
        """
        ti = create_task_instance(
            dag_id='test_run_pooling_task',
            task_id='test_run_pooling_task_op',
            executor_config={'foo': 'bar'},
        )
        dag = ti.task.dag

        ti.run(session=session)
        tis = dag.get_task_instances()
        assert {'foo': 'bar'} == tis[0].executor_config
        task2 = EmptyOperator(
            task_id='test_run_pooling_task_op2',
            executor_config={'bar': 'baz'},
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            dag=dag,
        )

        ti2 = TI(task=task2, run_id=ti.run_id)
        session.add(ti2)
        session.flush()

        ti2.run(session=session)
        # Ensure it's reloaded
        ti2.executor_config = None
        ti2.refresh_from_db(session)
        assert {'bar': 'baz'} == ti2.executor_config
        session.rollback()

    def test_run_pooling_task_with_mark_success(self, create_task_instance):
        """
        test that running task in an existing pool with mark_success param
        update task state as SUCCESS without running task
        despite it fails dependency checks.
        """
        ti = create_task_instance(
            dag_id='test_run_pooling_task_with_mark_success',
            task_id='test_run_pooling_task_with_mark_success_op',
        )

        ti.run(mark_success=True)
        assert ti.state == State.SUCCESS

    def test_run_pooling_task_with_skip(self, dag_maker):
        """
        test that running task which returns AirflowSkipOperator will end
        up in a SKIPPED state.
        """

        def raise_skip_exception():
            raise AirflowSkipException

        with dag_maker(dag_id='test_run_pooling_task_with_skip'):
            task = PythonOperator(
                task_id='test_run_pooling_task_with_skip',
                python_callable=raise_skip_exception,
            )

        dr = dag_maker.create_dagrun(execution_date=timezone.utcnow())
        ti = dr.task_instances[0]
        ti.task = task
        ti.run()
        assert State.SKIPPED == ti.state

    def test_task_sigterm_works_with_retries(self, dag_maker):
        """
        Test that ensures that tasks are retried when they receive sigterm
        """

        def task_function(ti):
            os.kill(ti.pid, signal.SIGTERM)

        with dag_maker('test_mark_failure_2'):
            task = PythonOperator(
                task_id='test_on_failure',
                python_callable=task_function,
                retries=1,
                retry_delay=datetime.timedelta(seconds=2),
            )

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = task
        with pytest.raises(AirflowException):
            ti.run()
        ti.refresh_from_db()
        assert ti.state == State.UP_FOR_RETRY

    @pytest.mark.parametrize("state", [State.SUCCESS, State.FAILED, State.SKIPPED])
    def test_task_sigterm_doesnt_change_state_of_finished_tasks(self, state, dag_maker):
        session = settings.Session()

        def task_function(ti):
            ti.state = state
            session.merge(ti)
            session.commit()
            raise AirflowException()

        with dag_maker('test_mark_failure_2'):
            task = PythonOperator(
                task_id='test_on_failure',
                python_callable=task_function,
            )

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = task

        ti.run()
        ti.refresh_from_db()
        ti.state == state

    @pytest.mark.parametrize(
        "state, exception, retries",
        [
            (State.FAILED, AirflowException, 0),
            (State.SKIPPED, AirflowSkipException, 0),
            (State.SUCCESS, None, 0),
            (State.UP_FOR_RESCHEDULE, AirflowRescheduleException(timezone.utcnow()), 0),
            (State.UP_FOR_RETRY, AirflowException, 1),
        ],
    )
    def test_task_wipes_next_fields(self, session, dag_maker, state, exception, retries):
        """
        Test that ensures that tasks wipe their next_method and next_kwargs
        when the TI enters one of the configured states.
        """

        def _raise_if_exception():
            if exception:
                raise exception

        with dag_maker("test_deferred_method_clear"):
            task = PythonOperator(
                task_id="test_deferred_method_clear_task",
                python_callable=_raise_if_exception,
                retries=retries,
                retry_delay=datetime.timedelta(seconds=2),
            )

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.next_method = "execute"
        ti.next_kwargs = {}
        session.merge(ti)
        session.commit()

        ti.task = task
        if state in [State.FAILED, State.UP_FOR_RETRY]:
            with pytest.raises(exception):
                ti.run()
        else:
            ti.run()
        ti.refresh_from_db()

        assert ti.next_method is None
        assert ti.next_kwargs is None
        assert ti.state == state

    @freeze_time('2021-09-19 04:56:35', as_kwarg='frozen_time')
    def test_retry_delay(self, dag_maker, frozen_time=None):
        """
        Test that retry delays are respected
        """
        with dag_maker(dag_id='test_retry_handling'):
            task = BashOperator(
                task_id='test_retry_handling_op',
                bash_command='exit 1',
                retries=1,
                retry_delay=datetime.timedelta(seconds=3),
            )

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task

        assert ti.try_number == 1
        # first run -- up for retry
        run_with_error(ti)
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 2

        # second run -- still up for retry because retry_delay hasn't expired
        frozen_time.tick(delta=datetime.timedelta(seconds=3))
        run_with_error(ti)
        assert ti.state == State.UP_FOR_RETRY

        # third run -- failed
        frozen_time.tick(delta=datetime.datetime.resolution)
        run_with_error(ti)
        assert ti.state == State.FAILED

    def test_retry_handling(self, dag_maker):
        """
        Test that task retries are handled properly
        """
        expected_rendered_ti_fields = {'env': None, 'bash_command': 'echo test_retry_handling; exit 1'}

        with dag_maker(dag_id='test_retry_handling') as dag:
            task = BashOperator(
                task_id='test_retry_handling_op',
                bash_command='echo {{dag.dag_id}}; exit 1',
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
            )

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        assert ti.try_number == 1

        # first run -- up for retry
        run_with_error(ti)
        assert ti.state == State.UP_FOR_RETRY
        assert ti._try_number == 1
        assert ti.try_number == 2

        # second run -- fail
        run_with_error(ti)
        assert ti.state == State.FAILED
        assert ti._try_number == 2
        assert ti.try_number == 3

        # Clear the TI state since you can't run a task with a FAILED state without
        # clearing it first
        dag.clear()

        # third run -- up for retry
        run_with_error(ti)
        assert ti.state == State.UP_FOR_RETRY
        assert ti._try_number == 3
        assert ti.try_number == 4

        # fourth run -- fail
        run_with_error(ti)
        ti.refresh_from_db()
        assert ti.state == State.FAILED
        assert ti._try_number == 4
        assert ti.try_number == 5
        assert RenderedTaskInstanceFields.get_templated_fields(ti) == expected_rendered_ti_fields

    def test_next_retry_datetime(self, dag_maker):
        delay = datetime.timedelta(seconds=30)
        max_delay = datetime.timedelta(minutes=60)

        with dag_maker(dag_id='fail_dag'):
            task = BashOperator(
                task_id='task_with_exp_backoff_and_max_delay',
                bash_command='exit 1',
                retries=3,
                retry_delay=delay,
                retry_exponential_backoff=True,
                max_retry_delay=max_delay,
            )
        ti = dag_maker.create_dagrun().task_instances[0]
        ti.task = task
        ti.end_date = pendulum.instance(timezone.utcnow())

        date = ti.next_retry_datetime()
        # between 30 * 2^0.5 and 30 * 2^1 (15 and 30)
        period = ti.end_date.add(seconds=30) - ti.end_date.add(seconds=15)
        assert date in period

        ti.try_number = 3
        date = ti.next_retry_datetime()
        # between 30 * 2^2 and 30 * 2^3 (120 and 240)
        period = ti.end_date.add(seconds=240) - ti.end_date.add(seconds=120)
        assert date in period

        ti.try_number = 5
        date = ti.next_retry_datetime()
        # between 30 * 2^4 and 30 * 2^5 (480 and 960)
        period = ti.end_date.add(seconds=960) - ti.end_date.add(seconds=480)
        assert date in period

        ti.try_number = 9
        date = ti.next_retry_datetime()
        assert date == ti.end_date + max_delay

        ti.try_number = 50
        date = ti.next_retry_datetime()
        assert date == ti.end_date + max_delay

    @pytest.mark.parametrize("seconds", [0, 0.5, 1])
    def test_next_retry_datetime_short_or_zero_intervals(self, dag_maker, seconds):
        delay = datetime.timedelta(seconds=seconds)
        max_delay = datetime.timedelta(minutes=60)

        with dag_maker(dag_id='fail_dag'):
            task = BashOperator(
                task_id='task_with_exp_backoff_and_short_or_zero_time_interval',
                bash_command='exit 1',
                retries=3,
                retry_delay=delay,
                retry_exponential_backoff=True,
                max_retry_delay=max_delay,
            )
        ti = dag_maker.create_dagrun().task_instances[0]
        ti.task = task
        ti.end_date = pendulum.instance(timezone.utcnow())

        date = ti.next_retry_datetime()
        assert date == ti.end_date + datetime.timedelta(seconds=1)

    def test_reschedule_handling(self, dag_maker):
        """
        Test that task reschedules are handled properly
        """
        # Return values of the python sensor callable, modified during tests
        done = False
        fail = False

        def func():
            if fail:
                raise AirflowException()
            return done

        with dag_maker(dag_id='test_reschedule_handling') as dag:
            task = PythonSensor(
                task_id='test_reschedule_handling_sensor',
                poke_interval=0,
                mode='reschedule',
                python_callable=func,
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
            )

        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        assert ti._try_number == 0
        assert ti.try_number == 1

        def run_ti_and_assert(
            run_date,
            expected_start_date,
            expected_end_date,
            expected_duration,
            expected_state,
            expected_try_number,
            expected_task_reschedule_count,
        ):
            with freeze_time(run_date):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            assert ti.state == expected_state
            assert ti._try_number == expected_try_number
            assert ti.try_number == expected_try_number + 1
            assert ti.start_date == expected_start_date
            assert ti.end_date == expected_end_date
            assert ti.duration == expected_duration
            trs = TaskReschedule.find_for_task_instance(ti)
            assert len(trs) == expected_task_reschedule_count

        date1 = timezone.utcnow()
        date2 = date1 + datetime.timedelta(minutes=1)
        date3 = date2 + datetime.timedelta(minutes=1)
        date4 = date3 + datetime.timedelta(minutes=1)

        # Run with multiple reschedules.
        # During reschedule the try number remains the same, but each reschedule is recorded.
        # The start date is expected to remain the initial date, hence the duration increases.
        # When finished the try number is incremented and there is no reschedule expected
        # for this try.

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        done, fail = False, False
        run_ti_and_assert(date2, date1, date2, 60, State.UP_FOR_RESCHEDULE, 0, 2)

        done, fail = False, False
        run_ti_and_assert(date3, date1, date3, 120, State.UP_FOR_RESCHEDULE, 0, 3)

        done, fail = True, False
        run_ti_and_assert(date4, date1, date4, 180, State.SUCCESS, 1, 0)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        assert ti.state == State.NONE
        assert ti._try_number == 1

        # Run again after clearing with reschedules and a retry.
        # The retry increments the try number, and for that try no reschedule is expected.
        # After the retry the start date is reset, hence the duration is also reset.

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 1, 1)

        done, fail = False, True
        run_ti_and_assert(date2, date1, date2, 60, State.UP_FOR_RETRY, 2, 0)

        done, fail = False, False
        run_ti_and_assert(date3, date3, date3, 0, State.UP_FOR_RESCHEDULE, 2, 1)

        done, fail = True, False
        run_ti_and_assert(date4, date3, date4, 60, State.SUCCESS, 3, 0)

    def test_mapped_reschedule_handling(self, dag_maker):
        """
        Test that mapped task reschedules are handled properly
        """
        # Return values of the python sensor callable, modified during tests
        done = False
        fail = False

        def func():
            if fail:
                raise AirflowException()
            return done

        with dag_maker(dag_id='test_reschedule_handling') as dag:

            task = PythonSensor.partial(
                task_id='test_reschedule_handling_sensor',
                mode='reschedule',
                python_callable=func,
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
            ).expand(poke_interval=[0])

        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]

        ti.task = task
        assert ti._try_number == 0
        assert ti.try_number == 1

        def run_ti_and_assert(
            run_date,
            expected_start_date,
            expected_end_date,
            expected_duration,
            expected_state,
            expected_try_number,
            expected_task_reschedule_count,
        ):
            ti.refresh_from_task(task)
            with freeze_time(run_date):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            assert ti.state == expected_state
            assert ti._try_number == expected_try_number
            assert ti.try_number == expected_try_number + 1
            assert ti.start_date == expected_start_date
            assert ti.end_date == expected_end_date
            assert ti.duration == expected_duration
            trs = TaskReschedule.find_for_task_instance(ti)
            assert len(trs) == expected_task_reschedule_count

        date1 = timezone.utcnow()
        date2 = date1 + datetime.timedelta(minutes=1)
        date3 = date2 + datetime.timedelta(minutes=1)
        date4 = date3 + datetime.timedelta(minutes=1)

        # Run with multiple reschedules.
        # During reschedule the try number remains the same, but each reschedule is recorded.
        # The start date is expected to remain the initial date, hence the duration increases.
        # When finished the try number is incremented and there is no reschedule expected
        # for this try.

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        done, fail = False, False
        run_ti_and_assert(date2, date1, date2, 60, State.UP_FOR_RESCHEDULE, 0, 2)

        done, fail = False, False
        run_ti_and_assert(date3, date1, date3, 120, State.UP_FOR_RESCHEDULE, 0, 3)

        done, fail = True, False
        run_ti_and_assert(date4, date1, date4, 180, State.SUCCESS, 1, 0)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        assert ti.state == State.NONE
        assert ti._try_number == 1

        # Run again after clearing with reschedules and a retry.
        # The retry increments the try number, and for that try no reschedule is expected.
        # After the retry the start date is reset, hence the duration is also reset.

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 1, 1)

        done, fail = False, True
        run_ti_and_assert(date2, date1, date2, 60, State.UP_FOR_RETRY, 2, 0)

        done, fail = False, False
        run_ti_and_assert(date3, date3, date3, 0, State.UP_FOR_RESCHEDULE, 2, 1)

        done, fail = True, False
        run_ti_and_assert(date4, date3, date4, 60, State.SUCCESS, 3, 0)

    @pytest.mark.usefixtures('test_pool')
    def test_mapped_task_reschedule_handling_clear_reschedules(self, dag_maker):
        """
        Test that mapped task reschedules clearing are handled properly
        """
        # Return values of the python sensor callable, modified during tests
        done = False
        fail = False

        def func():
            if fail:
                raise AirflowException()
            return done

        with dag_maker(dag_id='test_reschedule_handling') as dag:
            task = PythonSensor.partial(
                task_id='test_reschedule_handling_sensor',
                mode='reschedule',
                python_callable=func,
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
                pool='test_pool',
            ).expand(poke_interval=[0])
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        assert ti._try_number == 0
        assert ti.try_number == 1

        def run_ti_and_assert(
            run_date,
            expected_start_date,
            expected_end_date,
            expected_duration,
            expected_state,
            expected_try_number,
            expected_task_reschedule_count,
        ):
            ti.refresh_from_task(task)
            with freeze_time(run_date):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            assert ti.state == expected_state
            assert ti._try_number == expected_try_number
            assert ti.try_number == expected_try_number + 1
            assert ti.start_date == expected_start_date
            assert ti.end_date == expected_end_date
            assert ti.duration == expected_duration
            trs = TaskReschedule.find_for_task_instance(ti)
            assert len(trs) == expected_task_reschedule_count

        date1 = timezone.utcnow()

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        assert ti.state == State.NONE
        assert ti._try_number == 0
        # Check that reschedules for ti have also been cleared.
        trs = TaskReschedule.find_for_task_instance(ti)
        assert not trs

    @pytest.mark.usefixtures('test_pool')
    def test_reschedule_handling_clear_reschedules(self, dag_maker):
        """
        Test that task reschedules clearing are handled properly
        """
        # Return values of the python sensor callable, modified during tests
        done = False
        fail = False

        def func():
            if fail:
                raise AirflowException()
            return done

        with dag_maker(dag_id='test_reschedule_handling') as dag:
            task = PythonSensor(
                task_id='test_reschedule_handling_sensor',
                poke_interval=0,
                mode='reschedule',
                python_callable=func,
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
                pool='test_pool',
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        assert ti._try_number == 0
        assert ti.try_number == 1

        def run_ti_and_assert(
            run_date,
            expected_start_date,
            expected_end_date,
            expected_duration,
            expected_state,
            expected_try_number,
            expected_task_reschedule_count,
        ):
            with freeze_time(run_date):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            assert ti.state == expected_state
            assert ti._try_number == expected_try_number
            assert ti.try_number == expected_try_number + 1
            assert ti.start_date == expected_start_date
            assert ti.end_date == expected_end_date
            assert ti.duration == expected_duration
            trs = TaskReschedule.find_for_task_instance(ti)
            assert len(trs) == expected_task_reschedule_count

        date1 = timezone.utcnow()

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        assert ti.state == State.NONE
        assert ti._try_number == 0
        # Check that reschedules for ti have also been cleared.
        trs = TaskReschedule.find_for_task_instance(ti)
        assert not trs

    def test_depends_on_past(self, dag_maker):
        with dag_maker(dag_id='test_depends_on_past'):
            task = EmptyOperator(
                task_id='test_dop_task',
                depends_on_past=True,
            )
        dag_maker.create_dagrun(
            state=State.FAILED,
            run_type=DagRunType.SCHEDULED,
        )

        run_date = task.start_date + datetime.timedelta(days=5)

        dr = dag_maker.create_dagrun(
            execution_date=run_date,
            run_type=DagRunType.SCHEDULED,
        )

        ti = dr.task_instances[0]
        ti.task = task

        # depends_on_past prevents the run
        task.run(start_date=run_date, end_date=run_date, ignore_first_depends_on_past=False)
        ti.refresh_from_db()
        assert ti.state is None

        # ignore first depends_on_past to allow the run
        task.run(start_date=run_date, end_date=run_date, ignore_first_depends_on_past=True)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    # Parameterized tests to check for the correct firing
    # of the trigger_rule under various circumstances
    # Numeric fields are in order:
    #   successes, skipped, failed, upstream_failed, done
    @pytest.mark.parametrize(
        "trigger_rule,successes,skipped,failed,upstream_failed,done,"
        "flag_upstream_failed,expect_state,expect_completed",
        [
            #
            # Tests for all_success
            #
            ['all_success', 5, 0, 0, 0, 0, True, None, True],
            ['all_success', 2, 0, 0, 0, 0, True, None, False],
            ['all_success', 2, 0, 1, 0, 0, True, State.UPSTREAM_FAILED, False],
            ['all_success', 2, 1, 0, 0, 0, True, State.SKIPPED, False],
            #
            # Tests for one_success
            #
            ['one_success', 5, 0, 0, 0, 5, True, None, True],
            ['one_success', 2, 0, 0, 0, 2, True, None, True],
            ['one_success', 2, 0, 1, 0, 3, True, None, True],
            ['one_success', 2, 1, 0, 0, 3, True, None, True],
            ['one_success', 0, 5, 0, 0, 5, True, State.SKIPPED, False],
            ['one_success', 0, 4, 1, 0, 5, True, State.UPSTREAM_FAILED, False],
            ['one_success', 0, 3, 1, 1, 5, True, State.UPSTREAM_FAILED, False],
            ['one_success', 0, 4, 0, 1, 5, True, State.UPSTREAM_FAILED, False],
            ['one_success', 0, 0, 5, 0, 5, True, State.UPSTREAM_FAILED, False],
            ['one_success', 0, 0, 4, 1, 5, True, State.UPSTREAM_FAILED, False],
            ['one_success', 0, 0, 0, 5, 5, True, State.UPSTREAM_FAILED, False],
            #
            # Tests for all_failed
            #
            ['all_failed', 5, 0, 0, 0, 5, True, State.SKIPPED, False],
            ['all_failed', 0, 0, 5, 0, 5, True, None, True],
            ['all_failed', 2, 0, 0, 0, 2, True, State.SKIPPED, False],
            ['all_failed', 2, 0, 1, 0, 3, True, State.SKIPPED, False],
            ['all_failed', 2, 1, 0, 0, 3, True, State.SKIPPED, False],
            #
            # Tests for one_failed
            #
            ['one_failed', 5, 0, 0, 0, 0, True, None, False],
            ['one_failed', 2, 0, 0, 0, 0, True, None, False],
            ['one_failed', 2, 0, 1, 0, 0, True, None, True],
            ['one_failed', 2, 1, 0, 0, 3, True, None, False],
            ['one_failed', 2, 3, 0, 0, 5, True, State.SKIPPED, False],
            #
            # Tests for done
            #
            ['all_done', 5, 0, 0, 0, 5, True, None, True],
            ['all_done', 2, 0, 0, 0, 2, True, None, False],
            ['all_done', 2, 0, 1, 0, 3, True, None, False],
            ['all_done', 2, 1, 0, 0, 3, True, None, False],
        ],
    )
    def test_check_task_dependencies(
        self,
        trigger_rule: str,
        successes: int,
        skipped: int,
        failed: int,
        upstream_failed: int,
        done: int,
        flag_upstream_failed: bool,
        expect_state: State,
        expect_completed: bool,
        dag_maker,
    ):
        with dag_maker() as dag:
            downstream = EmptyOperator(task_id="downstream", trigger_rule=trigger_rule)
            for i in range(5):
                task = EmptyOperator(task_id=f'runme_{i}', dag=dag)
                task.set_downstream(downstream)
            assert task.start_date is not None
            run_date = task.start_date + datetime.timedelta(days=5)

        ti = dag_maker.create_dagrun(execution_date=run_date).get_task_instance(downstream.task_id)
        ti.task = downstream
        dep_results = TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=successes,
            skipped=skipped,
            failed=failed,
            upstream_failed=upstream_failed,
            done=done,
            dep_context=DepContext(),
            flag_upstream_failed=flag_upstream_failed,
        )
        completed = all(dep.passed for dep in dep_results)

        assert completed == expect_completed
        assert ti.state == expect_state

    def test_respects_prev_dagrun_dep(self, create_task_instance):
        ti = create_task_instance()
        failing_status = [TIDepStatus('test fail status name', False, 'test fail reason')]
        passing_status = [TIDepStatus('test pass status name', True, 'test passing reason')]
        with patch(
            'airflow.ti_deps.deps.prev_dagrun_dep.PrevDagrunDep.get_dep_statuses', return_value=failing_status
        ):
            assert not ti.are_dependencies_met()
        with patch(
            'airflow.ti_deps.deps.prev_dagrun_dep.PrevDagrunDep.get_dep_statuses', return_value=passing_status
        ):
            assert ti.are_dependencies_met()

    @pytest.mark.parametrize(
        "downstream_ti_state, expected_are_dependents_done",
        [
            (State.SUCCESS, True),
            (State.SKIPPED, True),
            (State.RUNNING, False),
            (State.FAILED, False),
            (State.NONE, False),
        ],
    )
    @provide_session
    def test_are_dependents_done(
        self, downstream_ti_state, expected_are_dependents_done, create_task_instance, session=None
    ):
        ti = create_task_instance(session=session)
        dag = ti.task.dag
        downstream_task = EmptyOperator(task_id='downstream_task', dag=dag)
        ti.task >> downstream_task

        downstream_ti = TI(downstream_task, run_id=ti.run_id)

        downstream_ti.set_state(downstream_ti_state, session)
        session.flush()
        assert ti.are_dependents_done(session) == expected_are_dependents_done

    def test_xcom_pull(self, dag_maker):
        """Test xcom_pull, using different filtering methods."""
        with dag_maker(dag_id="test_xcom") as dag:
            task_1 = EmptyOperator(task_id="test_xcom_1")
            task_2 = EmptyOperator(task_id="test_xcom_2")

        dagrun = dag_maker.create_dagrun(start_date=timezone.datetime(2016, 6, 1, 0, 0, 0))
        ti1 = dagrun.get_task_instance(task_1.task_id)

        # Push a value
        ti1.xcom_push(key='foo', value='bar')

        # Push another value with the same key (but by a different task)
        XCom.set(
            key='foo',
            value='baz',
            task_id=task_2.task_id,
            dag_id=dag.dag_id,
            execution_date=dagrun.execution_date,
        )

        # Pull with no arguments
        result = ti1.xcom_pull()
        assert result is None
        # Pull the value pushed most recently by any task.
        result = ti1.xcom_pull(key='foo')
        assert result in 'baz'
        # Pull the value pushed by the first task
        result = ti1.xcom_pull(task_ids='test_xcom_1', key='foo')
        assert result == 'bar'
        # Pull the value pushed by the second task
        result = ti1.xcom_pull(task_ids='test_xcom_2', key='foo')
        assert result == 'baz'
        # Pull the values pushed by both tasks & Verify Order of task_ids pass & values returned
        result = ti1.xcom_pull(task_ids=['test_xcom_1', 'test_xcom_2'], key='foo')
        assert result == ['bar', 'baz']

    def test_xcom_pull_mapped(self, dag_maker, session):
        with dag_maker(dag_id="test_xcom", session=session):
            # Use the private _expand() method to avoid the empty kwargs check.
            # We don't care about how the operator runs here, only its presence.
            task_1 = EmptyOperator.partial(task_id="task_1")._expand(EXPAND_INPUT_EMPTY, strict=False)
            EmptyOperator(task_id="task_2")

        dagrun = dag_maker.create_dagrun(start_date=timezone.datetime(2016, 6, 1, 0, 0, 0))

        ti_1_0 = dagrun.get_task_instance("task_1", session=session)
        ti_1_0.map_index = 0
        ti_1_1 = session.merge(TI(task_1, run_id=dagrun.run_id, map_index=1, state=ti_1_0.state))
        session.flush()

        ti_1_0.xcom_push(key=XCOM_RETURN_KEY, value="a", session=session)
        ti_1_1.xcom_push(key=XCOM_RETURN_KEY, value="b", session=session)

        ti_2 = dagrun.get_task_instance("task_2", session=session)

        assert set(ti_2.xcom_pull(["task_1"], session=session)) == {"a", "b"}  # Ordering not guaranteed.
        assert ti_2.xcom_pull(["task_1"], map_indexes=0, session=session) == ["a"]

        assert ti_2.xcom_pull(map_indexes=[0, 1], session=session) == ["a", "b"]
        assert ti_2.xcom_pull("task_1", map_indexes=[1, 0], session=session) == ["b", "a"]
        assert ti_2.xcom_pull(["task_1"], map_indexes=[0, 1], session=session) == ["a", "b"]

        assert ti_2.xcom_pull("task_1", map_indexes=1, session=session) == "b"
        assert list(ti_2.xcom_pull("task_1", session=session)) == ["a", "b"]

    def test_xcom_pull_after_success(self, create_task_instance):
        """
        tests xcom set/clear relative to a task in a 'success' rerun scenario
        """
        key = 'xcom_key'
        value = 'xcom_value'

        ti = create_task_instance(
            dag_id='test_xcom',
            schedule='@monthly',
            task_id='test_xcom',
            pool='test_xcom',
        )

        ti.run(mark_success=True)
        ti.xcom_push(key=key, value=value)
        assert ti.xcom_pull(task_ids='test_xcom', key=key) == value
        ti.run()
        # The second run and assert is to handle AIRFLOW-131 (don't clear on
        # prior success)
        assert ti.xcom_pull(task_ids='test_xcom', key=key) == value

        # Test AIRFLOW-703: Xcom shouldn't be cleared if the task doesn't
        # execute, even if dependencies are ignored
        ti.run(ignore_all_deps=True, mark_success=True)
        assert ti.xcom_pull(task_ids='test_xcom', key=key) == value
        # Xcom IS finally cleared once task has executed
        ti.run(ignore_all_deps=True)
        assert ti.xcom_pull(task_ids='test_xcom', key=key) is None

    def test_xcom_pull_after_deferral(self, create_task_instance, session):
        """
        tests xcom will not clear before a task runs its next method after deferral.
        """

        key = 'xcom_key'
        value = 'xcom_value'

        ti = create_task_instance(
            dag_id='test_xcom',
            schedule='@monthly',
            task_id='test_xcom',
            pool='test_xcom',
        )

        ti.run(mark_success=True)
        ti.xcom_push(key=key, value=value)

        ti.next_method = "execute"
        session.merge(ti)
        session.commit()

        ti.run(ignore_all_deps=True)
        assert ti.xcom_pull(task_ids='test_xcom', key=key) == value

    def test_xcom_pull_different_execution_date(self, create_task_instance):
        """
        tests xcom fetch behavior with different execution dates, using
        both xcom_pull with "include_prior_dates" and without
        """
        key = 'xcom_key'
        value = 'xcom_value'

        ti = create_task_instance(
            dag_id='test_xcom',
            schedule='@monthly',
            task_id='test_xcom',
            pool='test_xcom',
        )
        exec_date = ti.dag_run.execution_date

        ti.run(mark_success=True)
        ti.xcom_push(key=key, value=value)
        assert ti.xcom_pull(task_ids='test_xcom', key=key) == value
        ti.run()
        exec_date += datetime.timedelta(days=1)
        dr = ti.task.dag.create_dagrun(run_id="test2", execution_date=exec_date, state=None)
        ti = TI(task=ti.task, run_id=dr.run_id)
        ti.run()
        # We have set a new execution date (and did not pass in
        # 'include_prior_dates'which means this task should now have a cleared
        # xcom value
        assert ti.xcom_pull(task_ids='test_xcom', key=key) is None
        # We *should* get a value using 'include_prior_dates'
        assert ti.xcom_pull(task_ids='test_xcom', key=key, include_prior_dates=True) == value

    def test_xcom_push_flag(self, dag_maker):
        """
        Tests the option for Operators to push XComs
        """
        value = 'hello'
        task_id = 'test_no_xcom_push'

        with dag_maker(dag_id='test_xcom'):
            # nothing saved to XCom
            task = PythonOperator(
                task_id=task_id,
                python_callable=lambda: value,
                do_xcom_push=False,
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        ti.run()
        assert ti.xcom_pull(task_ids=task_id, key=models.XCOM_RETURN_KEY) is None

    def test_post_execute_hook(self, dag_maker):
        """
        Test that post_execute hook is called with the Operator's result.
        The result ('error') will cause an error to be raised and trapped.
        """

        class TestError(Exception):
            pass

        class TestOperator(PythonOperator):
            def post_execute(self, context, result=None):
                if result == 'error':
                    raise TestError('expected error.')

        with dag_maker(dag_id='test_post_execute_dag'):
            task = TestOperator(
                task_id='test_operator',
                python_callable=lambda: 'error',
            )
        ti = dag_maker.create_dagrun(execution_date=DEFAULT_DATE).task_instances[0]
        ti.task = task
        with pytest.raises(TestError):
            ti.run()

    def test_check_and_change_state_before_execution(self, create_task_instance):
        ti = create_task_instance(dag_id='test_check_and_change_state_before_execution')
        SerializedDagModel.write_dag(ti.task.dag)

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id)

        assert ti_from_deserialized_task._try_number == 0
        assert ti_from_deserialized_task.check_and_change_state_before_execution()
        # State should be running, and try_number column should be incremented
        assert ti_from_deserialized_task.state == State.RUNNING
        assert ti_from_deserialized_task._try_number == 1

    def test_check_and_change_state_before_execution_dep_not_met(self, create_task_instance):
        ti = create_task_instance(dag_id='test_check_and_change_state_before_execution')
        task2 = EmptyOperator(task_id='task2', dag=ti.task.dag, start_date=DEFAULT_DATE)
        ti.task >> task2
        SerializedDagModel.write_dag(ti.task.dag)

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti2 = TI(task=serialized_dag.get_task(task2.task_id), run_id=ti.run_id)
        assert not ti2.check_and_change_state_before_execution()

    def test_check_and_change_state_before_execution_dep_not_met_already_running(self, create_task_instance):
        """return False if the task instance state is running"""
        ti = create_task_instance(dag_id='test_check_and_change_state_before_execution')
        with create_session() as _:
            ti.state = State.RUNNING

        SerializedDagModel.write_dag(ti.task.dag)

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id)

        assert not ti_from_deserialized_task.check_and_change_state_before_execution()
        assert ti_from_deserialized_task.state == State.RUNNING

    def test_check_and_change_state_before_execution_dep_not_met_not_runnable_state(
        self, create_task_instance
    ):
        """return False if the task instance state is failed"""
        ti = create_task_instance(dag_id='test_check_and_change_state_before_execution')
        with create_session() as _:
            ti.state = State.FAILED

        SerializedDagModel.write_dag(ti.task.dag)

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id)

        assert not ti_from_deserialized_task.check_and_change_state_before_execution()
        assert ti_from_deserialized_task.state == State.FAILED

    def test_try_number(self, create_task_instance):
        """
        Test the try_number accessor behaves in various running states
        """
        ti = create_task_instance(dag_id='test_check_and_change_state_before_execution')
        assert 1 == ti.try_number
        ti.try_number = 2
        ti.state = State.RUNNING
        assert 2 == ti.try_number
        ti.state = State.SUCCESS
        assert 3 == ti.try_number

    def test_get_num_running_task_instances(self, create_task_instance):
        session = settings.Session()

        ti1 = create_task_instance(
            dag_id='test_get_num_running_task_instances', task_id='task1', session=session
        )

        dr = ti1.task.dag.create_dagrun(
            execution_date=DEFAULT_DATE + datetime.timedelta(days=1),
            state=None,
            run_id='2',
            session=session,
        )
        assert ti1 in session
        ti2 = dr.task_instances[0]
        ti2.task = ti1.task

        ti3 = create_task_instance(
            dag_id='test_get_num_running_task_instances_dummy', task_id='task2', session=session
        )
        assert ti3 in session
        assert ti1 in session

        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.RUNNING
        assert ti3 in session
        session.commit()

        assert 1 == ti1.get_num_running_task_instances(session=session)
        assert 1 == ti2.get_num_running_task_instances(session=session)
        assert 1 == ti3.get_num_running_task_instances(session=session)

    def test_log_url(self, create_task_instance):
        ti = create_task_instance(dag_id='dag', task_id='op', execution_date=timezone.datetime(2018, 1, 1))

        expected_url = (
            'http://localhost:8080/log?'
            'execution_date=2018-01-01T00%3A00%3A00%2B00%3A00'
            '&task_id=op'
            '&dag_id=dag'
            '&map_index=-1'
        )
        assert ti.log_url == expected_url

    def test_mark_success_url(self, create_task_instance):
        now = pendulum.now('Europe/Brussels')
        ti = create_task_instance(dag_id='dag', task_id='op', execution_date=now)
        query = urllib.parse.parse_qs(
            urllib.parse.urlparse(ti.mark_success_url).query, keep_blank_values=True, strict_parsing=True
        )
        assert query['dag_id'][0] == 'dag'
        assert query['task_id'][0] == 'op'
        assert query['dag_run_id'][0] == 'test'
        assert ti.execution_date == now

    def test_overwrite_params_with_dag_run_conf(self, create_task_instance):
        ti = create_task_instance()
        dag_run = ti.dag_run
        dag_run.conf = {"override": True}
        params = {"override": False}

        ti.overwrite_params_with_dag_run_conf(params, dag_run)

        assert params["override"] is True

    def test_overwrite_params_with_dag_run_none(self, create_task_instance):
        ti = create_task_instance()
        params = {"override": False}

        ti.overwrite_params_with_dag_run_conf(params, None)

        assert params["override"] is False

    def test_overwrite_params_with_dag_run_conf_none(self, create_task_instance):
        ti = create_task_instance()
        params = {"override": False}
        dag_run = ti.dag_run

        ti.overwrite_params_with_dag_run_conf(params, dag_run)

        assert params["override"] is False

    @pytest.mark.parametrize("use_native_obj", [True, False])
    @patch('airflow.models.taskinstance.send_email')
    def test_email_alert(self, mock_send_email, dag_maker, use_native_obj):
        with dag_maker(dag_id='test_failure_email', render_template_as_native_obj=use_native_obj):
            task = BashOperator(task_id='test_email_alert', bash_command='exit 1', email='to')
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task

        try:
            ti.run()
        except AirflowException:
            pass

        (email, title, body), _ = mock_send_email.call_args
        assert email == 'to'
        assert 'test_email_alert' in title
        assert 'test_email_alert' in body
        assert 'Try 1' in body

    @conf_vars(
        {
            ('email', 'subject_template'): '/subject/path',
            ('email', 'html_content_template'): '/html_content/path',
        }
    )
    @patch('airflow.models.taskinstance.send_email')
    def test_email_alert_with_config(self, mock_send_email, dag_maker):
        with dag_maker(dag_id='test_failure_email'):
            task = BashOperator(
                task_id='test_email_alert_with_config',
                bash_command='exit 1',
                email='to',
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task

        opener = mock_open(read_data='template: {{ti.task_id}}')
        with patch('airflow.models.taskinstance.open', opener, create=True):
            try:
                ti.run()
            except AirflowException:
                pass

        (email, title, body), _ = mock_send_email.call_args
        assert email == 'to'
        assert 'template: test_email_alert_with_config' == title
        assert 'template: test_email_alert_with_config' == body

    @patch('airflow.models.taskinstance.send_email')
    def test_email_alert_with_filenotfound_config(self, mock_send_email, dag_maker):
        with dag_maker(dag_id='test_failure_email'):
            task = BashOperator(
                task_id='test_email_alert_with_config',
                bash_command='exit 1',
                email='to',
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task

        # Run test when the template file is not found
        opener = mock_open(read_data='template: {{ti.task_id}}')
        opener.side_effect = FileNotFoundError
        with patch('airflow.models.taskinstance.open', opener, create=True):
            try:
                ti.run()
            except AirflowException:
                pass

        (email_error, title_error, body_error), _ = mock_send_email.call_args

        # Rerun task without any error and no template file
        try:
            ti.run()
        except AirflowException:
            pass

        (email_default, title_default, body_default), _ = mock_send_email.call_args

        assert email_error == email_default == 'to'
        assert title_default == title_error
        assert body_default == body_error

    @pytest.mark.parametrize("task_id", ["test_email_alert", "test_email_alert__1"])
    @patch('airflow.models.taskinstance.send_email')
    def test_failure_mapped_taskflow(self, mock_send_email, dag_maker, session, task_id):
        with dag_maker(session=session) as dag:

            @dag.task(email='to')
            def test_email_alert(x):
                raise RuntimeError("Fail please")

            test_email_alert.expand(x=["a", "b"])  # This is 'test_email_alert'.
            test_email_alert.expand(x=[1, 2, 3])  # This is 'test_email_alert__1'.

        dr: DagRun = dag_maker.create_dagrun(execution_date=timezone.utcnow())

        ti = dr.get_task_instance(task_id, map_index=0, session=session)
        assert ti is not None

        # The task will fail and trigger email reporting.
        with pytest.raises(RuntimeError, match=r"^Fail please$"):
            ti.run(session=session)

        (email, title, body), _ = mock_send_email.call_args
        assert email == "to"
        assert title == f"Airflow alert: <TaskInstance: test_dag.{task_id} test map_index=0 [failed]>"
        assert body.startswith("Try 1")
        assert "test_email_alert" in body

        tf = (
            session.query(TaskFail)
            .filter_by(dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id, map_index=ti.map_index)
            .one_or_none()
        )
        assert tf, "TaskFail was recorded"

    def test_set_duration(self):
        task = EmptyOperator(task_id='op', email='test@test.test')
        ti = TI(task=task)
        ti.start_date = datetime.datetime(2018, 10, 1, 1)
        ti.end_date = datetime.datetime(2018, 10, 1, 2)
        ti.set_duration()
        assert ti.duration == 3600

    def test_set_duration_empty_dates(self):
        task = EmptyOperator(task_id='op', email='test@test.test')
        ti = TI(task=task)
        ti.set_duration()
        assert ti.duration is None

    def test_success_callback_no_race_condition(self, create_task_instance):
        callback_wrapper = CallbackWrapper()
        ti = create_task_instance(
            on_success_callback=callback_wrapper.success_handler,
            end_date=timezone.utcnow() + datetime.timedelta(days=10),
            execution_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        session = settings.Session()
        session.merge(ti)
        session.commit()

        callback_wrapper.wrap_task_instance(ti)
        ti._run_raw_task()
        assert callback_wrapper.callback_ran
        assert callback_wrapper.task_state_in_callback == State.SUCCESS
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    def test_outlet_datasets(self, create_task_instance):
        """
        Verify that when we have an outlet dataset on a task, and the task
        completes successfully, a DatasetDagRunQueue is logged.
        """
        from airflow.example_dags import example_datasets
        from airflow.example_dags.example_datasets import dag1

        session = settings.Session()
        dagbag = DagBag(dag_folder=example_datasets.__file__)
        dagbag.collect_dags(only_if_updated=False, safe_mode=False)
        dagbag.sync_to_db(session=session)
        run_id = str(uuid4())
        dr = DagRun(dag1.dag_id, run_id=run_id, run_type='anything')
        session.merge(dr)
        task = dag1.get_task('producing_task_1')
        task.bash_command = 'echo 1'  # make it go faster
        ti = TaskInstance(task, run_id=run_id)
        session.merge(ti)
        session.commit()
        ti._run_raw_task()
        ti.refresh_from_db()
        assert ti.state == TaskInstanceState.SUCCESS

        # check that no other dataset events recorded
        event = (
            session.query(DatasetEvent)
            .join(DatasetEvent.dataset)
            .filter(DatasetEvent.source_task_instance == ti)
            .one()
        )
        assert event
        assert event.dataset

        # check that one queue record created for each dag that depends on dataset 1
        assert session.query(DatasetDagRunQueue.target_dag_id).filter_by(
            dataset_id=event.dataset.id
        ).order_by(DatasetDagRunQueue.target_dag_id).all() == [
            ('dataset_consumes_1',),
            ('dataset_consumes_1_and_2',),
            ('dataset_consumes_1_never_scheduled',),
        ]

        # check that one event record created for dataset1 and this TI
        assert session.query(DatasetModel.uri).join(DatasetEvent.dataset).filter(
            DatasetEvent.source_task_instance == ti
        ).one() == ('s3://dag1/output_1.txt',)

        # check that the dataset event has an earlier timestamp than the DDRQ's
        ddrq_timestamps = (
            session.query(DatasetDagRunQueue.created_at).filter_by(dataset_id=event.dataset.id).all()
        )
        assert all([event.timestamp < ddrq_timestamp for (ddrq_timestamp,) in ddrq_timestamps])

    def test_outlet_datasets_failed(self, create_task_instance):
        """
        Verify that when we have an outlet dataset on a task, and the task
        failed, a DatasetDagRunQueue is not logged, and a DatasetEvent is
        not generated
        """
        from tests.dags import test_datasets
        from tests.dags.test_datasets import dag_with_fail_task

        session = settings.Session()
        dagbag = DagBag(dag_folder=test_datasets.__file__)
        dagbag.collect_dags(only_if_updated=False, safe_mode=False)
        dagbag.sync_to_db(session=session)
        run_id = str(uuid4())
        dr = DagRun(dag_with_fail_task.dag_id, run_id=run_id, run_type='anything')
        session.merge(dr)
        task = dag_with_fail_task.get_task('fail_task')
        ti = TaskInstance(task, run_id=run_id)
        session.merge(ti)
        session.commit()
        with pytest.raises(AirflowFailException):
            ti._run_raw_task()
        ti.refresh_from_db()
        assert ti.state == TaskInstanceState.FAILED

        # check that no dagruns were queued
        assert session.query(DatasetDagRunQueue).count() == 0

        # check that no dataset events were generated
        assert session.query(DatasetEvent).count() == 0

    def test_outlet_datasets_skipped(self, create_task_instance):
        """
        Verify that when we have an outlet dataset on a task, and the task
        is skipped, a DatasetDagRunQueue is not logged, and a DatasetEvent is
        not generated
        """
        from tests.dags import test_datasets
        from tests.dags.test_datasets import dag_with_skip_task

        session = settings.Session()
        dagbag = DagBag(dag_folder=test_datasets.__file__)
        dagbag.collect_dags(only_if_updated=False, safe_mode=False)
        dagbag.sync_to_db(session=session)
        run_id = str(uuid4())
        dr = DagRun(dag_with_skip_task.dag_id, run_id=run_id, run_type='anything')
        session.merge(dr)
        task = dag_with_skip_task.get_task('skip_task')
        ti = TaskInstance(task, run_id=run_id)
        session.merge(ti)
        session.commit()
        ti._run_raw_task()
        ti.refresh_from_db()
        assert ti.state == TaskInstanceState.SKIPPED

        # check that no dagruns were queued
        assert session.query(DatasetDagRunQueue).count() == 0

        # check that no dataset events were generated
        assert session.query(DatasetEvent).count() == 0

    @staticmethod
    def _test_previous_dates_setup(
        schedule_interval: str | datetime.timedelta | None,
        catchup: bool,
        scenario: list[TaskInstanceState],
        dag_maker,
    ) -> list:
        dag_id = 'test_previous_dates'
        with dag_maker(dag_id=dag_id, schedule=schedule_interval, catchup=catchup):
            task = EmptyOperator(task_id='task')

        def get_test_ti(execution_date: pendulum.DateTime, state: str) -> TI:
            dr = dag_maker.create_dagrun(
                run_id=f'test__{execution_date.isoformat()}',
                run_type=DagRunType.SCHEDULED,
                state=state,
                execution_date=execution_date,
                start_date=pendulum.now('UTC'),
            )
            ti = dr.task_instances[0]
            ti.task = task
            ti.set_state(state=State.SUCCESS, session=dag_maker.session)
            return ti

        date = cast(pendulum.DateTime, pendulum.parse('2019-01-01T00:00:00+00:00'))

        ret = []

        for idx, state in enumerate(scenario):
            new_date = date.add(days=idx)
            ti = get_test_ti(new_date, state)
            ret.append(ti)

        return ret

    _prev_dates_param_list = [
        pytest.param('0 0 * * * ', True, id='cron/catchup'),
        pytest.param('0 0 * * *', False, id='cron/no-catchup'),
        pytest.param(None, True, id='no-sched/catchup'),
        pytest.param(None, False, id='no-sched/no-catchup'),
        pytest.param(datetime.timedelta(days=1), True, id='timedelta/catchup'),
        pytest.param(datetime.timedelta(days=1), False, id='timedelta/no-catchup'),
    ]

    @pytest.mark.parametrize("schedule_interval, catchup", _prev_dates_param_list)
    def test_previous_ti(self, schedule_interval, catchup, dag_maker) -> None:

        scenario = [State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario, dag_maker)

        assert ti_list[0].get_previous_ti() is None

        assert ti_list[2].get_previous_ti().run_id == ti_list[1].run_id

        assert ti_list[2].get_previous_ti().run_id != ti_list[0].run_id

    @pytest.mark.parametrize("schedule_interval, catchup", _prev_dates_param_list)
    def test_previous_ti_success(self, schedule_interval, catchup, dag_maker) -> None:

        scenario = [State.FAILED, State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario, dag_maker)

        assert ti_list[0].get_previous_ti(state=State.SUCCESS) is None
        assert ti_list[1].get_previous_ti(state=State.SUCCESS) is None

        assert ti_list[3].get_previous_ti(state=State.SUCCESS).run_id == ti_list[1].run_id

        assert ti_list[3].get_previous_ti(state=State.SUCCESS).run_id != ti_list[2].run_id

    @pytest.mark.parametrize("schedule_interval, catchup", _prev_dates_param_list)
    def test_previous_execution_date_success(self, schedule_interval, catchup, dag_maker) -> None:

        scenario = [State.FAILED, State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario, dag_maker)
        # vivify
        for ti in ti_list:
            ti.execution_date

        assert ti_list[0].get_previous_execution_date(state=State.SUCCESS) is None
        assert ti_list[1].get_previous_execution_date(state=State.SUCCESS) is None
        assert ti_list[3].get_previous_execution_date(state=State.SUCCESS) == ti_list[1].execution_date
        assert ti_list[3].get_previous_execution_date(state=State.SUCCESS) != ti_list[2].execution_date

    @pytest.mark.parametrize("schedule_interval, catchup", _prev_dates_param_list)
    def test_previous_start_date_success(self, schedule_interval, catchup, dag_maker) -> None:

        scenario = [State.FAILED, State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario, dag_maker)

        assert ti_list[0].get_previous_start_date(state=State.SUCCESS) is None
        assert ti_list[1].get_previous_start_date(state=State.SUCCESS) is None
        assert ti_list[3].get_previous_start_date(state=State.SUCCESS) == ti_list[1].start_date
        assert ti_list[3].get_previous_start_date(state=State.SUCCESS) != ti_list[2].start_date

    def test_get_previous_start_date_none(self, dag_maker):
        """
        Test that get_previous_start_date() can handle TaskInstance with no start_date.
        """
        with dag_maker("test_get_previous_start_date_none", schedule=None) as dag:
            task = EmptyOperator(task_id="op")

        day_1 = DEFAULT_DATE
        day_2 = DEFAULT_DATE + datetime.timedelta(days=1)

        # Create a DagRun for day_1 and day_2. Calling ti_2.get_previous_start_date()
        # should return the start_date of ti_1 (which is None because ti_1 was not run).
        # It should not raise an error.
        dagrun_1 = dag_maker.create_dagrun(
            execution_date=day_1,
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
        )

        dagrun_2 = dag.create_dagrun(
            execution_date=day_2,
            state=State.RUNNING,
            run_type=DagRunType.MANUAL,
        )

        ti_1 = dagrun_1.get_task_instance(task.task_id)
        ti_2 = dagrun_2.get_task_instance(task.task_id)
        ti_1.task = task
        ti_2.task = task

        assert ti_2.get_previous_start_date() == ti_1.start_date
        assert ti_1.start_date is None

    def test_context_triggering_dataset_events_none(self, session, create_task_instance):
        ti = create_task_instance()
        template_context = ti.get_template_context()

        assert ti in session
        session.expunge_all()

        assert template_context["triggering_dataset_events"] == {}

    def test_context_triggering_dataset_events(self, create_dummy_dag, session):
        ds1 = DatasetModel(id=1, uri="one")
        ds2 = DatasetModel(id=2, uri="two")
        session.add_all([ds1, ds2])
        session.commit()

        # it's easier to fake a manual run here
        dag, task1 = create_dummy_dag(
            dag_id="test_triggering_dataset_events",
            schedule=None,
            start_date=DEFAULT_DATE,
            task_id="test_context",
            with_dagrun_type=DagRunType.MANUAL,
            session=session,
        )
        dr = dag.create_dagrun(
            run_id="test2",
            run_type=DagRunType.DATASET_TRIGGERED,
            execution_date=timezone.utcnow(),
            state=None,
            session=session,
        )
        ds1_event = DatasetEvent(dataset_id=1)
        ds2_event_1 = DatasetEvent(dataset_id=2)
        ds2_event_2 = DatasetEvent(dataset_id=2)
        dr.consumed_dataset_events.append(ds1_event)
        dr.consumed_dataset_events.append(ds2_event_1)
        dr.consumed_dataset_events.append(ds2_event_2)
        session.commit()

        ti = dr.get_task_instance(task1.task_id, session=session)
        ti.refresh_from_task(task1)

        # Check we run this in the same context as the actual task at runtime!
        assert ti in session
        session.expunge(ti)
        session.expunge(dr)

        template_context = ti.get_template_context()

        assert template_context["triggering_dataset_events"] == {
            "one": [ds1_event],
            "two": [ds2_event_1, ds2_event_2],
        }

    def test_pendulum_template_dates(self, create_task_instance):
        ti = create_task_instance(
            dag_id='test_pendulum_template_dates',
            task_id='test_pendulum_template_dates_task',
            schedule='0 12 * * *',
        )

        template_context = ti.get_template_context()

        assert isinstance(template_context["data_interval_start"], pendulum.DateTime)
        assert isinstance(template_context["data_interval_end"], pendulum.DateTime)

    def test_template_render(self, create_task_instance):
        ti = create_task_instance(
            dag_id="test_template_render",
            task_id="test_template_render_task",
            schedule="0 12 * * *",
        )
        template_context = ti.get_template_context()
        result = ti.task.render_template("Task: {{ dag.dag_id }} -> {{ task.task_id }}", template_context)
        assert result == "Task: test_template_render -> test_template_render_task"

    def test_template_render_deprecated(self, create_task_instance):
        ti = create_task_instance(
            dag_id="test_template_render",
            task_id="test_template_render_task",
            schedule="0 12 * * *",
        )
        template_context = ti.get_template_context()
        with pytest.deprecated_call():
            result = ti.task.render_template("Execution date: {{ execution_date }}", template_context)
        assert result.startswith("Execution date: ")

    @pytest.mark.parametrize(
        "content, expected_output",
        [
            ('{{ conn.get("a_connection").host }}', 'hostvalue'),
            ('{{ conn.get("a_connection", "unused_fallback").host }}', 'hostvalue'),
            ('{{ conn.get("missing_connection", {"host": "fallback_host"}).host }}', 'fallback_host'),
            ('{{ conn.a_connection.host }}', 'hostvalue'),
            ('{{ conn.a_connection.login }}', 'loginvalue'),
            ('{{ conn.a_connection.password }}', 'passwordvalue'),
            ('{{ conn.a_connection.extra_dejson["extra__asana__workspace"] }}', 'extra1'),
            ('{{ conn.a_connection.extra_dejson.extra__asana__workspace }}', 'extra1'),
        ],
    )
    def test_template_with_connection(self, content, expected_output, create_task_instance):
        """
        Test the availability of variables in templates
        """
        with create_session() as session:
            clear_db_connections(add_default_connections_back=False)
            merge_conn(
                Connection(
                    conn_id="a_connection",
                    conn_type="a_type",
                    description="a_conn_description",
                    host="hostvalue",
                    login="loginvalue",
                    password="passwordvalue",
                    schema="schemavalues",
                    extra={
                        "extra__asana__workspace": "extra1",
                    },
                ),
                session,
            )

        ti = create_task_instance()

        context = ti.get_template_context()
        result = ti.task.render_template(content, context)
        assert result == expected_output

    @pytest.mark.parametrize(
        "content, expected_output",
        [
            ('{{ var.value.a_variable }}', 'a test value'),
            ('{{ var.value.get("a_variable") }}', 'a test value'),
            ('{{ var.value.get("a_variable", "unused_fallback") }}', 'a test value'),
            ('{{ var.value.get("missing_variable", "fallback") }}', 'fallback'),
        ],
    )
    def test_template_with_variable(self, content, expected_output, create_task_instance):
        """
        Test the availability of variables in templates
        """
        Variable.set('a_variable', 'a test value')

        ti = create_task_instance()
        context = ti.get_template_context()
        result = ti.task.render_template(content, context)
        assert result == expected_output

    def test_template_with_variable_missing(self, create_task_instance):
        """
        Test the availability of variables in templates
        """
        ti = create_task_instance()
        context = ti.get_template_context()
        with pytest.raises(KeyError):
            ti.task.render_template('{{ var.value.get("missing_variable") }}', context)

    @pytest.mark.parametrize(
        "content, expected_output",
        [
            ('{{ var.value.a_variable }}', '{\n  "a": {\n    "test": "value"\n  }\n}'),
            ('{{ var.json.a_variable["a"]["test"] }}', 'value'),
            ('{{ var.json.get("a_variable")["a"]["test"] }}', 'value'),
            ('{{ var.json.get("a_variable", {"a": {"test": "unused_fallback"}})["a"]["test"] }}', 'value'),
            ('{{ var.json.get("missing_variable", {"a": {"test": "fallback"}})["a"]["test"] }}', 'fallback'),
        ],
    )
    def test_template_with_json_variable(self, content, expected_output, create_task_instance):
        """
        Test the availability of variables in templates
        """
        Variable.set('a_variable', {'a': {'test': 'value'}}, serialize_json=True)

        ti = create_task_instance()
        context = ti.get_template_context()
        result = ti.task.render_template(content, context)
        assert result == expected_output

    def test_template_with_json_variable_missing(self, create_task_instance):
        ti = create_task_instance()
        context = ti.get_template_context()
        with pytest.raises(KeyError):
            ti.task.render_template('{{ var.json.get("missing_variable") }}', context)

    @pytest.mark.parametrize(
        ("field", "expected"),
        [
            ("next_ds", "2016-01-01"),
            ("next_ds_nodash", "20160101"),
            ("prev_ds", "2015-12-31"),
            ("prev_ds_nodash", "20151231"),
            ("yesterday_ds", "2015-12-31"),
            ("yesterday_ds_nodash", "20151231"),
            ("tomorrow_ds", "2016-01-02"),
            ("tomorrow_ds_nodash", "20160102"),
        ],
    )
    def test_deprecated_context(self, field, expected, create_task_instance):
        ti = create_task_instance(execution_date=DEFAULT_DATE)
        context = ti.get_template_context()
        with pytest.deprecated_call() as recorder:
            assert context[field] == expected
        message_beginning = (
            f"Accessing {field!r} from the template is deprecated and "
            f"will be removed in a future version."
        )

        recorded_message = [str(m.message) for m in recorder]
        assert len(recorded_message) == 1
        assert recorded_message[0].startswith(message_beginning)

    def test_template_with_custom_timetable_deprecated_context(self, create_task_instance):
        ti = create_task_instance(
            start_date=DEFAULT_DATE,
            timetable=AfterWorkdayTimetable(),
            run_type=DagRunType.SCHEDULED,
            execution_date=timezone.datetime(2021, 9, 6),
            data_interval=(timezone.datetime(2021, 9, 6), timezone.datetime(2021, 9, 7)),
        )
        context = ti.get_template_context()
        with pytest.deprecated_call():
            assert context["execution_date"] == pendulum.DateTime(2021, 9, 6, tzinfo=timezone.TIMEZONE)
        with pytest.deprecated_call():
            assert context["next_ds"] == "2021-09-07"
        with pytest.deprecated_call():
            assert context["next_ds_nodash"] == "20210907"
        with pytest.deprecated_call():
            assert context["next_execution_date"] == pendulum.DateTime(2021, 9, 7, tzinfo=timezone.TIMEZONE)
        with pytest.deprecated_call():
            assert context["prev_ds"] is None, "Does not make sense for custom timetable"
        with pytest.deprecated_call():
            assert context["prev_ds_nodash"] is None, "Does not make sense for custom timetable"
        with pytest.deprecated_call():
            assert context["prev_execution_date"] is None, "Does not make sense for custom timetable"

    def test_execute_callback(self, create_task_instance):
        called = False

        def on_execute_callable(context):
            nonlocal called
            called = True
            assert context['dag_run'].dag_id == 'test_dagrun_execute_callback'

        ti = create_task_instance(
            dag_id='test_execute_callback',
            on_execute_callback=on_execute_callable,
            state=State.RUNNING,
        )

        session = settings.Session()

        session.merge(ti)
        session.commit()

        ti._run_raw_task()
        assert called
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    @pytest.mark.parametrize(
        "finished_state, callback_type",
        [
            (State.SUCCESS, "on_success"),
            (State.UP_FOR_RETRY, "on_retry"),
            (State.FAILED, "on_failure"),
        ],
    )
    def test_finished_callbacks_handle_and_log_exception(
        self, finished_state, callback_type, create_task_instance
    ):
        called = completed = False

        def on_finish_callable(context):
            nonlocal called, completed
            called = True
            raise KeyError
            completed = True

        ti = create_task_instance(
            end_date=timezone.utcnow() + datetime.timedelta(days=10),
            on_success_callback=on_finish_callable,
            on_retry_callback=on_finish_callable,
            on_failure_callback=on_finish_callable,
            state=finished_state,
        )
        ti._log = mock.Mock()
        ti._run_finished_callback(on_finish_callable, {}, callback_type)

        assert called
        assert not completed
        expected_message = f"Error when executing {callback_type} callback"
        ti.log.exception.assert_called_once_with(expected_message)

    @provide_session
    def test_handle_failure(self, create_dummy_dag, session=None):
        start_date = timezone.datetime(2016, 6, 1)
        clear_db_runs()

        mock_on_failure_1 = mock.MagicMock()
        mock_on_retry_1 = mock.MagicMock()
        dag, task1 = create_dummy_dag(
            dag_id="test_handle_failure",
            schedule=None,
            start_date=start_date,
            task_id="test_handle_failure_on_failure",
            with_dagrun_type=DagRunType.MANUAL,
            on_failure_callback=mock_on_failure_1,
            on_retry_callback=mock_on_retry_1,
            session=session,
        )
        dr = dag.create_dagrun(
            run_id="test2",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.utcnow(),
            state=None,
            session=session,
        )

        ti1 = dr.get_task_instance(task1.task_id, session=session)
        ti1.task = task1
        ti1.state = State.FAILED
        ti1.handle_failure("test failure handling")

        context_arg_1 = mock_on_failure_1.call_args[0][0]
        assert context_arg_1 and "task_instance" in context_arg_1
        mock_on_retry_1.assert_not_called()

        mock_on_failure_2 = mock.MagicMock()
        mock_on_retry_2 = mock.MagicMock()
        task2 = EmptyOperator(
            task_id="test_handle_failure_on_retry",
            on_failure_callback=mock_on_failure_2,
            on_retry_callback=mock_on_retry_2,
            retries=1,
            dag=dag,
        )
        ti2 = TI(task=task2, run_id=dr.run_id)
        ti2.state = State.FAILED
        session.add(ti2)
        session.flush()
        ti2.handle_failure("test retry handling")

        mock_on_failure_2.assert_not_called()

        context_arg_2 = mock_on_retry_2.call_args[0][0]
        assert context_arg_2 and "task_instance" in context_arg_2

        # test the scenario where normally we would retry but have been asked to fail
        mock_on_failure_3 = mock.MagicMock()
        mock_on_retry_3 = mock.MagicMock()
        task3 = EmptyOperator(
            task_id="test_handle_failure_on_force_fail",
            on_failure_callback=mock_on_failure_3,
            on_retry_callback=mock_on_retry_3,
            retries=1,
            dag=dag,
        )
        ti3 = TI(task=task3, run_id=dr.run_id)
        session.add(ti3)
        session.flush()
        ti3.state = State.FAILED
        ti3.handle_failure("test force_fail handling", force_fail=True)

        context_arg_3 = mock_on_failure_3.call_args[0][0]
        assert context_arg_3 and "task_instance" in context_arg_3
        mock_on_retry_3.assert_not_called()

    def test_handle_failure_updates_queued_task_try_number(self, dag_maker):
        session = settings.Session()
        with dag_maker():
            task = EmptyOperator(task_id="mytask", retries=1)
        dr = dag_maker.create_dagrun()
        ti = TI(task=task, run_id=dr.run_id)
        ti.state = State.QUEUED
        session.merge(ti)
        session.flush()
        assert ti.state == State.QUEUED
        assert ti.try_number == 1
        ti.handle_failure("test queued ti", test_mode=True)
        assert ti.state == State.UP_FOR_RETRY
        # Assert that 'ti._try_number' is bumped from 0 to 1. This is the last/current try
        assert ti._try_number == 1
        # Check 'ti.try_number' is bumped to 2. This is try_number for next run
        assert ti.try_number == 2

    @patch.object(Stats, 'incr')
    def test_handle_failure_no_task(self, Stats_incr, dag_maker):
        """
        When a zombie is detected for a DAG with a parse error, we need to be able to run handle_failure
        _without_ ti.task being set
        """
        session = settings.Session()
        with dag_maker():
            task = EmptyOperator(task_id="mytask", retries=1)
        dr = dag_maker.create_dagrun()
        ti = TI(task=task, run_id=dr.run_id)
        ti = session.merge(ti)
        ti.task = None
        ti.state = State.QUEUED
        session.flush()

        assert ti.task is None, "Check critical pre-condition"

        assert ti.state == State.QUEUED
        assert ti.try_number == 1

        ti.handle_failure("test queued ti", test_mode=False)
        assert ti.state == State.UP_FOR_RETRY
        # Assert that 'ti._try_number' is bumped from 0 to 1. This is the last/current try
        assert ti._try_number == 1
        # Check 'ti.try_number' is bumped to 2. This is try_number for next run
        assert ti.try_number == 2

        Stats_incr.assert_any_call('ti_failures')
        Stats_incr.assert_any_call('operator_failures_EmptyOperator')

    def test_handle_failure_task_undefined(self, create_task_instance):
        """
        When the loaded taskinstance does not use refresh_from_task, the task may be undefined.
        For example:
            the DAG file has been deleted before executing _execute_task_callbacks
        """
        ti = create_task_instance()
        del ti.task
        ti.handle_failure("test ti.task undefined")

    def test_does_not_retry_on_airflow_fail_exception(self, dag_maker):
        def fail():
            raise AirflowFailException("hopeless")

        with dag_maker(dag_id='test_does_not_retry_on_airflow_fail_exception'):
            task = PythonOperator(
                task_id='test_raise_airflow_fail_exception',
                python_callable=fail,
                retries=1,
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        try:
            ti.run()
        except AirflowFailException:
            pass  # expected
        assert State.FAILED == ti.state

    def test_retries_on_other_exceptions(self, dag_maker):
        def fail():
            raise AirflowException("maybe this will pass?")

        with dag_maker(dag_id='test_retries_on_other_exceptions'):
            task = PythonOperator(
                task_id='test_raise_other_exception',
                python_callable=fail,
                retries=1,
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        try:
            ti.run()
        except AirflowException:
            pass  # expected
        assert State.UP_FOR_RETRY == ti.state

    def test_stacktrace_on_failure_starts_with_task_execute_method(self, dag_maker):
        def fail():
            raise AirflowException("maybe this will pass?")

        with dag_maker(dag_id='test_retries_on_other_exceptions'):
            task = PythonOperator(
                task_id='test_raise_other_exception',
                python_callable=fail,
                retries=1,
            )
        ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        with patch.object(TI, "log") as log, pytest.raises(AirflowException):
            ti.run()
        assert len(log.error.mock_calls) == 1
        assert log.error.call_args[0] == ("Task failed with exception",)
        exc_info = log.error.call_args[1]["exc_info"]
        filename = exc_info[2].tb_frame.f_code.co_filename
        formatted_exc = format_exception(*exc_info)
        assert sys.modules[PythonOperator.__module__].__file__ == filename, "".join(formatted_exc)

    def _env_var_check_callback(self):
        assert 'test_echo_env_variables' == os.environ['AIRFLOW_CTX_DAG_ID']
        assert 'hive_in_python_op' == os.environ['AIRFLOW_CTX_TASK_ID']
        assert DEFAULT_DATE.isoformat() == os.environ['AIRFLOW_CTX_EXECUTION_DATE']
        assert DagRun.generate_run_id(DagRunType.MANUAL, DEFAULT_DATE) == os.environ['AIRFLOW_CTX_DAG_RUN_ID']

    def test_echo_env_variables(self, dag_maker):
        with dag_maker(
            'test_echo_env_variables',
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=10),
        ):
            op = PythonOperator(task_id='hive_in_python_op', python_callable=self._env_var_check_callback)
        dr = dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            external_trigger=False,
        )
        ti = TI(task=op, run_id=dr.run_id)
        ti.state = State.RUNNING
        session = settings.Session()
        session.merge(ti)
        session.commit()
        ti._run_raw_task()
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    @patch.object(Stats, 'incr')
    def test_task_stats(self, stats_mock, create_task_instance):
        ti = create_task_instance(
            dag_id='test_task_start_end_stats',
            end_date=timezone.utcnow() + datetime.timedelta(days=10),
            state=State.RUNNING,
        )
        stats_mock.reset_mock()

        session = settings.Session()
        session.merge(ti)
        session.commit()
        ti._run_raw_task()
        ti.refresh_from_db()
        stats_mock.assert_called_with(f'ti.finish.{ti.dag_id}.{ti.task_id}.{ti.state}')
        for state in State.task_states:
            assert call(f'ti.finish.{ti.dag_id}.{ti.task_id}.{state}', count=0) in stats_mock.mock_calls
        assert call(f'ti.start.{ti.dag_id}.{ti.task_id}') in stats_mock.mock_calls
        assert stats_mock.call_count == len(State.task_states) + 4

    def test_command_as_list(self, create_task_instance):
        ti = create_task_instance()
        ti.task.dag.fileloc = os.path.join(TEST_DAGS_FOLDER, 'x.py')
        assert ti.command_as_list() == [
            'airflow',
            'tasks',
            'run',
            ti.dag_id,
            ti.task_id,
            ti.run_id,
            '--subdir',
            'DAGS_FOLDER/x.py',
        ]

    def test_generate_command_default_param(self):
        dag_id = 'test_generate_command_default_param'
        task_id = 'task'
        assert_command = ['airflow', 'tasks', 'run', dag_id, task_id, 'run_1']
        generate_command = TI.generate_command(dag_id=dag_id, task_id=task_id, run_id='run_1')
        assert assert_command == generate_command

    def test_generate_command_specific_param(self):
        dag_id = 'test_generate_command_specific_param'
        task_id = 'task'
        assert_command = [
            'airflow',
            'tasks',
            'run',
            dag_id,
            task_id,
            'run_1',
            '--mark-success',
            '--map-index',
            '0',
        ]
        generate_command = TI.generate_command(
            dag_id=dag_id, task_id=task_id, run_id='run_1', mark_success=True, map_index=0
        )
        assert assert_command == generate_command

    @provide_session
    def test_get_rendered_template_fields(self, dag_maker, session=None):

        with dag_maker('test-dag', session=session) as dag:
            task = BashOperator(task_id='op1', bash_command="{{ task.task_id }}")
        dag.fileloc = TEST_DAGS_FOLDER / 'test_get_k8s_pod_yaml.py'
        ti = dag_maker.create_dagrun().task_instances[0]
        ti.task = task

        session.add(RenderedTaskInstanceFields(ti))
        session.flush()

        # Create new TI for the same Task
        new_task = BashOperator(task_id='op12', bash_command="{{ task.task_id }}", dag=dag)

        new_ti = TI(task=new_task, run_id=ti.run_id)
        new_ti.get_rendered_template_fields(session=session)

        assert "op1" == ti.task.bash_command

        # CleanUp
        with create_session() as session:
            session.query(RenderedTaskInstanceFields).delete()

    @mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
    @mock.patch("airflow.settings.pod_mutation_hook")
    def test_render_k8s_pod_yaml(self, pod_mutation_hook, create_task_instance):
        ti = create_task_instance(
            dag_id='test_render_k8s_pod_yaml',
            run_id='test_run_id',
            task_id='op1',
            execution_date=DEFAULT_DATE,
        )

        expected_pod_spec = {
            'metadata': {
                'annotations': {
                    'dag_id': 'test_render_k8s_pod_yaml',
                    'run_id': 'test_run_id',
                    'task_id': 'op1',
                    'try_number': '1',
                },
                'labels': {
                    'airflow-worker': '0',
                    'airflow_version': version,
                    'dag_id': 'test_render_k8s_pod_yaml',
                    'run_id': 'test_run_id',
                    'kubernetes_executor': 'True',
                    'task_id': 'op1',
                    'try_number': '1',
                },
                'name': mock.ANY,
                'namespace': 'default',
            },
            'spec': {
                'containers': [
                    {
                        'args': [
                            'airflow',
                            'tasks',
                            'run',
                            'test_render_k8s_pod_yaml',
                            'op1',
                            'test_run_id',
                            '--subdir',
                            __file__,
                        ],
                        'name': 'base',
                        'env': [{'name': 'AIRFLOW_IS_K8S_EXECUTOR_POD', 'value': 'True'}],
                    }
                ]
            },
        }

        assert ti.render_k8s_pod_yaml() == expected_pod_spec
        pod_mutation_hook.assert_called_once_with(mock.ANY)

    @mock.patch.dict(os.environ, {"AIRFLOW_IS_K8S_EXECUTOR_POD": "True"})
    @mock.patch.object(RenderedTaskInstanceFields, 'get_k8s_pod_yaml')
    def test_get_rendered_k8s_spec(self, rtif_get_k8s_pod_yaml, create_task_instance):
        # Create new TI for the same Task
        ti = create_task_instance()

        patcher = mock.patch.object(ti, 'render_k8s_pod_yaml', autospec=True)

        fake_spec = {"ermagawds": "pods"}

        session = mock.Mock()

        with patcher as render_k8s_pod_yaml:
            rtif_get_k8s_pod_yaml.return_value = fake_spec
            assert ti.get_rendered_k8s_spec(session) == fake_spec

            rtif_get_k8s_pod_yaml.assert_called_once_with(ti, session=session)
            render_k8s_pod_yaml.assert_not_called()

            # Now test that when we _dont_ find it in the DB, it calls render_k8s_pod_yaml
            rtif_get_k8s_pod_yaml.return_value = None
            render_k8s_pod_yaml.return_value = fake_spec

            assert ti.get_rendered_k8s_spec(session) == fake_spec

            render_k8s_pod_yaml.assert_called_once()

    def test_set_state_up_for_retry(self, create_task_instance):
        ti = create_task_instance(state=State.RUNNING)

        start_date = timezone.utcnow()
        ti.start_date = start_date

        ti.set_state(State.UP_FOR_RETRY)
        assert ti.state == State.UP_FOR_RETRY
        assert ti.start_date == start_date, "Start date should have been left alone"
        assert ti.start_date < ti.end_date
        assert ti.duration > 0

    def test_refresh_from_db(self, create_task_instance):
        run_date = timezone.utcnow()

        expected_values = {
            "task_id": "test_refresh_from_db_task",
            "dag_id": "test_refresh_from_db_dag",
            "run_id": "test",
            "map_index": -1,
            "start_date": run_date + datetime.timedelta(days=1),
            "end_date": run_date + datetime.timedelta(days=1, seconds=1, milliseconds=234),
            "duration": 1.234,
            "state": State.SUCCESS,
            "_try_number": 1,
            "max_tries": 1,
            "hostname": "some_unique_hostname",
            "unixname": "some_unique_unixname",
            "job_id": 1234,
            "pool": "some_fake_pool_id",
            "pool_slots": 25,
            "queue": "some_queue_id",
            "priority_weight": 123,
            "operator": "some_custom_operator",
            "queued_dttm": run_date + datetime.timedelta(hours=1),
            "queued_by_job_id": 321,
            "pid": 123,
            "executor_config": {"Some": {"extra": "information"}},
            "external_executor_id": "some_executor_id",
            "trigger_timeout": None,
            "trigger_id": None,
            "next_kwargs": None,
            "next_method": None,
            "updated_at": None,
        }
        # Make sure we aren't missing any new value in our expected_values list.
        expected_keys = {f"task_instance.{key.lstrip('_')}" for key in expected_values}
        assert {str(c) for c in TI.__table__.columns} == expected_keys, (
            "Please add all non-foreign values of TaskInstance to this list. "
            "This prevents refresh_from_db() from missing a field."
        )

        ti = create_task_instance(task_id=expected_values['task_id'], dag_id=expected_values['dag_id'])
        for key, expected_value in expected_values.items():
            setattr(ti, key, expected_value)
        with create_session() as session:
            session.merge(ti)
            session.commit()

        mock_task = mock.MagicMock()
        mock_task.task_id = expected_values["task_id"]
        mock_task.dag_id = expected_values["dag_id"]

        ti = TI(task=mock_task, run_id="test")
        ti.refresh_from_db()
        for key, expected_value in expected_values.items():
            assert hasattr(ti, key), f"Key {key} is missing in the TaskInstance."
            assert (
                getattr(ti, key) == expected_value
            ), f"Key: {key} had different values. Make sure it loads it in the refresh refresh_from_db()"

    def test_operator_field_with_serialization(self, create_task_instance):

        ti = create_task_instance()
        assert ti.task.task_type == 'EmptyOperator'
        assert ti.task.operator_name == 'EmptyOperator'

        # Verify that ti.operator field renders correctly "without" Serialization
        assert ti.operator == "EmptyOperator"

        serialized_op = SerializedBaseOperator.serialize_operator(ti.task)
        deserialized_op = SerializedBaseOperator.deserialize_operator(serialized_op)
        assert deserialized_op.task_type == 'EmptyOperator'
        # Verify that ti.operator field renders correctly "with" Serialization
        ser_ti = TI(task=deserialized_op, run_id=None)
        assert ser_ti.operator == "EmptyOperator"
        assert ser_ti.task.operator_name == 'EmptyOperator'


@pytest.mark.parametrize("pool_override", [None, "test_pool2"])
def test_refresh_from_task(pool_override):
    task = EmptyOperator(
        task_id="empty",
        queue="test_queue",
        pool="test_pool1",
        pool_slots=3,
        priority_weight=10,
        run_as_user="test",
        retries=30,
        executor_config={"KubernetesExecutor": {"image": "myCustomDockerImage"}},
    )
    ti = TI(task, run_id=None)
    ti.refresh_from_task(task, pool_override=pool_override)

    assert ti.queue == task.queue

    if pool_override:
        assert ti.pool == pool_override
    else:
        assert ti.pool == task.pool

    assert ti.pool_slots == task.pool_slots
    assert ti.priority_weight == task.priority_weight_total
    assert ti.run_as_user == task.run_as_user
    assert ti.max_tries == task.retries
    assert ti.executor_config == task.executor_config
    assert ti.operator == EmptyOperator.__name__

    # Test that refresh_from_task does not reset ti.max_tries
    expected_max_tries = task.retries + 10
    ti.max_tries = expected_max_tries
    ti.refresh_from_task(task)
    assert ti.max_tries == expected_max_tries


class TestRunRawTaskQueriesCount:
    """
    These tests are designed to detect changes in the number of queries executed
    when calling _run_raw_task
    """

    @staticmethod
    def _clean():
        db.clear_db_runs()
        db.clear_db_pools()
        db.clear_db_dags()
        db.clear_db_sla_miss()
        db.clear_db_import_errors()
        db.clear_db_datasets()

    def setup_method(self) -> None:
        self._clean()

    def teardown_method(self) -> None:
        self._clean()


@pytest.mark.parametrize("mode", ["poke", "reschedule"])
@pytest.mark.parametrize("retries", [0, 1])
def test_sensor_timeout(mode, retries, dag_maker):
    """
    Test that AirflowSensorTimeout does not cause sensor to retry.
    """

    def timeout():
        raise AirflowSensorTimeout

    mock_on_failure = mock.MagicMock()
    with dag_maker(dag_id=f'test_sensor_timeout_{mode}_{retries}'):
        PythonSensor(
            task_id='test_raise_sensor_timeout',
            python_callable=timeout,
            on_failure_callback=mock_on_failure,
            retries=retries,
            mode=mode,
        )
    ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]

    with pytest.raises(AirflowSensorTimeout):
        ti.run()

    assert mock_on_failure.called
    assert ti.state == State.FAILED


@pytest.mark.parametrize("mode", ["poke", "reschedule"])
@pytest.mark.parametrize("retries", [0, 1])
def test_mapped_sensor_timeout(mode, retries, dag_maker):
    """
    Test that AirflowSensorTimeout does not cause mapped sensor to retry.
    """

    def timeout():
        raise AirflowSensorTimeout

    mock_on_failure = mock.MagicMock()
    with dag_maker(dag_id=f'test_sensor_timeout_{mode}_{retries}'):
        PythonSensor.partial(
            task_id='test_raise_sensor_timeout',
            python_callable=timeout,
            on_failure_callback=mock_on_failure,
            retries=retries,
        ).expand(mode=[mode])
    ti = dag_maker.create_dagrun(execution_date=timezone.utcnow()).task_instances[0]

    with pytest.raises(AirflowSensorTimeout):
        ti.run()

    assert mock_on_failure.called
    assert ti.state == State.FAILED


@pytest.mark.parametrize("mode", ["poke", "reschedule"])
@pytest.mark.parametrize("retries", [0, 1])
def test_mapped_sensor_works(mode, retries, dag_maker):
    """
    Test that mapped sensors reaches success state.
    """

    def timeout(ti):
        return 1

    with dag_maker(dag_id=f'test_sensor_timeout_{mode}_{retries}'):
        PythonSensor.partial(
            task_id='test_raise_sensor_timeout',
            python_callable=timeout,
            retries=retries,
        ).expand(mode=[mode])
    ti = dag_maker.create_dagrun().task_instances[0]

    ti.run()
    assert ti.state == State.SUCCESS


class TestTaskInstanceRecordTaskMapXComPush:
    """Test TI.xcom_push() correctly records return values for task-mapping."""

    def setup_class(self):
        """Ensure we start fresh."""
        with create_session() as session:
            session.query(TaskMap).delete()

    @pytest.mark.parametrize("xcom_value", [[1, 2, 3], {"a": 1, "b": 2}, "abc"])
    def test_not_recorded_if_leaf(self, dag_maker, xcom_value):
        """Return value should not be recorded if there are no downstreams."""
        with dag_maker(dag_id="test_not_recorded_for_unused") as dag:

            @dag.task()
            def push_something():
                return xcom_value

            push_something()

        ti = next(ti for ti in dag_maker.create_dagrun().task_instances if ti.task_id == "push_something")
        ti.run()

        assert dag_maker.session.query(TaskMap).count() == 0

    @pytest.mark.parametrize("xcom_value", [[1, 2, 3], {"a": 1, "b": 2}, "abc"])
    def test_not_recorded_if_not_used(self, dag_maker, xcom_value):
        """Return value should not be recorded if no downstreams are mapped."""
        with dag_maker(dag_id="test_not_recorded_for_unused") as dag:

            @dag.task()
            def push_something():
                return xcom_value

            @dag.task()
            def completely_different():
                pass

            push_something() >> completely_different()

        ti = next(ti for ti in dag_maker.create_dagrun().task_instances if ti.task_id == "push_something")
        ti.run()

        assert dag_maker.session.query(TaskMap).count() == 0

    @pytest.mark.parametrize("xcom_value", [[1, 2, 3], {"a": 1, "b": 2}, "abc"])
    def test_not_recorded_if_irrelevant(self, dag_maker, xcom_value):
        """Return value should only be recorded if a mapped downstream uses the it."""
        with dag_maker(dag_id="test_not_recorded_for_unused") as dag:

            @dag.task()
            def push_1():
                return xcom_value

            @dag.task()
            def push_2():
                return [-1, -2]

            @dag.task()
            def show(arg1, arg2):
                print(arg1, arg2)

            show.partial(arg1=push_1()).expand(arg2=push_2())

        tis = {ti.task_id: ti for ti in dag_maker.create_dagrun().task_instances}

        tis["push_1"].run()
        assert dag_maker.session.query(TaskMap).count() == 0

        tis["push_2"].run()
        assert dag_maker.session.query(TaskMap).count() == 1

    @pytest.mark.parametrize(
        "return_value, exception_type, error_message",
        [
            ("abc", UnmappableXComTypePushed, "unmappable return type 'str'"),
            (None, XComForMappingNotPushed, "did not push XCom for task mapping"),
        ],
    )
    def test_expand_error_if_unmappable_type(self, dag_maker, return_value, exception_type, error_message):
        """If an unmappable return value is used for expand(), fail the task that pushed the XCom."""
        with dag_maker(dag_id="test_expand_error_if_unmappable_type") as dag:

            @dag.task()
            def push_something():
                return return_value

            @dag.task()
            def pull_something(value):
                print(value)

            pull_something.expand(value=push_something())

        ti = next(ti for ti in dag_maker.create_dagrun().task_instances if ti.task_id == "push_something")
        with pytest.raises(exception_type) as ctx:
            ti.run()

        assert dag_maker.session.query(TaskMap).count() == 0
        assert ti.state == TaskInstanceState.FAILED
        assert str(ctx.value) == error_message

    @pytest.mark.parametrize(
        "return_value, exception_type, error_message",
        [
            (123, UnmappableXComTypePushed, "unmappable return type 'int'"),
            (None, XComForMappingNotPushed, "did not push XCom for task mapping"),
        ],
    )
    def test_expand_kwargs_error_if_unmappable_type(
        self,
        dag_maker,
        return_value,
        exception_type,
        error_message,
    ):
        """If an unmappable return value is used for expand_kwargs(), fail the task that pushed the XCom."""
        with dag_maker(dag_id="test_expand_kwargs_error_if_unmappable_type") as dag:

            @dag.task()
            def push():
                return return_value

            MockOperator.partial(task_id="pull").expand_kwargs(push())

        ti = next(ti for ti in dag_maker.create_dagrun().task_instances if ti.task_id == "push")
        with pytest.raises(exception_type) as ctx:
            ti.run()

        assert dag_maker.session.query(TaskMap).count() == 0
        assert ti.state == TaskInstanceState.FAILED
        assert str(ctx.value) == error_message

    @pytest.mark.parametrize(
        "create_upstream",
        [
            # The task returns an invalid expand_kwargs() input (a list[int] instead of list[dict]).
            pytest.param(lambda: task(task_id="push")(lambda: [0])(), id="normal"),
            # This task returns a list[dict] (correct), but we use map() to transform it to list[int] (wrong).
            pytest.param(lambda: task(task_id="push")(lambda: [{"v": ""}])().map(lambda _: 0), id="mapped"),
        ],
    )
    def test_expand_kwargs_error_if_received_invalid(self, dag_maker, session, create_upstream):
        with dag_maker(dag_id="test_expand_kwargs_error_if_received_invalid", session=session):
            push_task = create_upstream()

            @task()
            def pull(v):
                print(v)

            pull.expand_kwargs(push_task)

        dr = dag_maker.create_dagrun()

        # Run "push".
        decision = dr.task_instance_scheduling_decisions(session=session)
        assert decision.schedulable_tis
        for ti in decision.schedulable_tis:
            ti.run()

        # Run "pull".
        decision = dr.task_instance_scheduling_decisions(session=session)
        assert decision.schedulable_tis
        for ti in decision.schedulable_tis:
            with pytest.raises(ValueError) as ctx:
                ti.run()
            assert str(ctx.value) == "expand_kwargs() expects a list[dict], not list[int]"

    @pytest.mark.parametrize(
        "downstream, error_message",
        [
            ("taskflow", "mapping already partial argument: arg2"),
            ("classic", "unmappable or already specified argument: arg2"),
        ],
        ids=["taskflow", "classic"],
    )
    @pytest.mark.parametrize("strict", [True, False], ids=["strict", "override"])
    def test_expand_kwargs_override_partial(self, dag_maker, session, downstream, error_message, strict):
        class ClassicOperator(MockOperator):
            def execute(self, context):
                return (self.arg1, self.arg2)

        with dag_maker(dag_id="test_expand_kwargs_override_partial", session=session) as dag:

            @dag.task()
            def push():
                return [{"arg1": "a"}, {"arg1": "b", "arg2": "c"}]

            push_task = push()

            ClassicOperator.partial(task_id="classic", arg2="d").expand_kwargs(push_task, strict=strict)

            @dag.task(task_id="taskflow")
            def pull(arg1, arg2):
                return (arg1, arg2)

            pull.partial(arg2="d").expand_kwargs(push_task, strict=strict)

        dr = dag_maker.create_dagrun()
        next(ti for ti in dr.task_instances if ti.task_id == "push").run()

        decision = dr.task_instance_scheduling_decisions(session=session)
        tis = {(ti.task_id, ti.map_index, ti.state): ti for ti in decision.schedulable_tis}
        assert sorted(tis) == [
            ("classic", 0, None),
            ("classic", 1, None),
            ("taskflow", 0, None),
            ("taskflow", 1, None),
        ]

        ti = tis[((downstream, 0, None))]
        ti.run()
        ti.xcom_pull(task_ids=downstream, map_indexes=0, session=session) == ["a", "d"]

        ti = tis[((downstream, 1, None))]
        if strict:
            with pytest.raises(TypeError) as ctx:
                ti.run()
            assert str(ctx.value) == error_message
        else:
            ti.run()
            ti.xcom_pull(task_ids=downstream, map_indexes=1, session=session) == ["b", "c"]

    def test_error_if_upstream_does_not_push(self, dag_maker):
        """Fail the upstream task if it fails to push the XCom used for task mapping."""
        with dag_maker(dag_id="test_not_recorded_for_unused") as dag:

            @dag.task(do_xcom_push=False)
            def push_something():
                return [1, 2]

            @dag.task()
            def pull_something(value):
                print(value)

            pull_something.expand(value=push_something())

        ti = next(ti for ti in dag_maker.create_dagrun().task_instances if ti.task_id == "push_something")
        with pytest.raises(XComForMappingNotPushed) as ctx:
            ti.run()

        assert dag_maker.session.query(TaskMap).count() == 0
        assert ti.state == TaskInstanceState.FAILED
        assert str(ctx.value) == "did not push XCom for task mapping"

    @conf_vars({("core", "max_map_length"): "1"})
    def test_error_if_unmappable_length(self, dag_maker):
        """If an unmappable return value is used to map, fail the task that pushed the XCom."""
        with dag_maker(dag_id="test_not_recorded_for_unused") as dag:

            @dag.task()
            def push_something():
                return [1, 2]

            @dag.task()
            def pull_something(value):
                print(value)

            pull_something.expand(value=push_something())

        ti = next(ti for ti in dag_maker.create_dagrun().task_instances if ti.task_id == "push_something")
        with pytest.raises(UnmappableXComLengthPushed) as ctx:
            ti.run()

        assert dag_maker.session.query(TaskMap).count() == 0
        assert ti.state == TaskInstanceState.FAILED
        assert str(ctx.value) == "unmappable return value length: 2 > 1"

    @pytest.mark.parametrize(
        "xcom_value, expected_length, expected_keys",
        [
            ([1, 2, 3], 3, None),
            ({"a": 1, "b": 2}, 2, ["a", "b"]),
        ],
    )
    def test_written_task_map(self, dag_maker, xcom_value, expected_length, expected_keys):
        """Return value should be recorded in TaskMap if it's used by a downstream to map."""
        with dag_maker(dag_id="test_written_task_map") as dag:

            @dag.task()
            def push_something():
                return xcom_value

            @dag.task()
            def pull_something(value):
                print(value)

            pull_something.expand(value=push_something())

        dag_run = dag_maker.create_dagrun()
        ti = next(ti for ti in dag_run.task_instances if ti.task_id == "push_something")
        ti.run()

        task_map = dag_maker.session.query(TaskMap).one()
        assert task_map.dag_id == "test_written_task_map"
        assert task_map.task_id == "push_something"
        assert task_map.run_id == dag_run.run_id
        assert task_map.map_index == -1
        assert task_map.length == expected_length
        assert task_map.keys == expected_keys


class TestMappedTaskInstanceReceiveValue:
    @pytest.mark.parametrize(
        "literal, expected_outputs",
        [
            pytest.param([1, 2, 3], [1, 2, 3], id="list"),
            pytest.param({"a": 1, "b": 2}, [("a", 1), ("b", 2)], id="dict"),
        ],
    )
    def test_map_literal(self, literal, expected_outputs, dag_maker, session):
        outputs = []

        with dag_maker(dag_id="literal", session=session) as dag:

            @dag.task
            def show(value):
                outputs.append(value)

            show.expand(value=literal)

        dag_run = dag_maker.create_dagrun()
        show_task = dag.get_task("show")
        mapped_tis = (
            session.query(TI)
            .filter_by(task_id='show', dag_id=dag_run.dag_id, run_id=dag_run.run_id)
            .order_by(TI.map_index)
            .all()
        )
        assert len(mapped_tis) == len(literal)

        for ti in sorted(mapped_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(show_task)
            ti.run()
        assert outputs == expected_outputs

    @pytest.mark.parametrize(
        "upstream_return, expected_outputs",
        [
            pytest.param([1, 2, 3], [1, 2, 3], id="list"),
            pytest.param({"a": 1, "b": 2}, [("a", 1), ("b", 2)], id="dict"),
        ],
    )
    def test_map_xcom(self, upstream_return, expected_outputs, dag_maker, session):
        outputs = []

        with dag_maker(dag_id="xcom", session=session) as dag:

            @dag.task
            def emit():
                return upstream_return

            @dag.task
            def show(value):
                outputs.append(value)

            show.expand(value=emit())

        dag_run = dag_maker.create_dagrun()
        emit_ti = dag_run.get_task_instance("emit", session=session)
        emit_ti.refresh_from_task(dag.get_task("emit"))
        emit_ti.run()

        show_task = dag.get_task("show")
        mapped_tis, max_map_index = show_task.expand_mapped_task(dag_run.run_id, session=session)
        assert max_map_index + 1 == len(mapped_tis) == len(upstream_return)

        for ti in sorted(mapped_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(show_task)
            ti.run()
        assert outputs == expected_outputs

    def test_map_product(self, dag_maker, session):
        outputs = []

        with dag_maker(dag_id="product", session=session) as dag:

            @dag.task
            def emit_numbers():
                return [1, 2]

            @dag.task
            def emit_letters():
                return {"a": "x", "b": "y", "c": "z"}

            @dag.task
            def show(number, letter):
                outputs.append((number, letter))

            show.expand(number=emit_numbers(), letter=emit_letters())

        dag_run = dag_maker.create_dagrun()
        for task_id in ["emit_numbers", "emit_letters"]:
            ti = dag_run.get_task_instance(task_id, session=session)
            ti.refresh_from_task(dag.get_task(task_id))
            ti.run()

        show_task = dag.get_task("show")
        mapped_tis, max_map_index = show_task.expand_mapped_task(dag_run.run_id, session=session)
        assert max_map_index + 1 == len(mapped_tis) == 6

        for ti in sorted(mapped_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(show_task)
            ti.run()
        assert outputs == [
            (1, ("a", "x")),
            (1, ("b", "y")),
            (1, ("c", "z")),
            (2, ("a", "x")),
            (2, ("b", "y")),
            (2, ("c", "z")),
        ]

    def test_map_product_same(self, dag_maker, session):
        """Test a mapped task can refer to the same source multiple times."""
        outputs = []

        with dag_maker(dag_id="product_same", session=session) as dag:

            @dag.task
            def emit_numbers():
                return [1, 2]

            @dag.task
            def show(a, b):
                outputs.append((a, b))

            emit_task = emit_numbers()
            show.expand(a=emit_task, b=emit_task)

        dag_run = dag_maker.create_dagrun()
        ti = dag_run.get_task_instance("emit_numbers", session=session)
        ti.refresh_from_task(dag.get_task("emit_numbers"))
        ti.run()

        show_task = dag.get_task("show")
        assert show_task.parse_time_mapped_ti_count is None
        mapped_tis, max_map_index = show_task.expand_mapped_task(dag_run.run_id, session=session)
        assert max_map_index + 1 == len(mapped_tis) == 4

        for ti in sorted(mapped_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(show_task)
            ti.run()
        assert outputs == [(1, 1), (1, 2), (2, 1), (2, 2)]

    def test_map_literal_cross_product(self, dag_maker, session):
        """Test a mapped task with literal cross product args expand properly."""
        outputs = []

        with dag_maker(dag_id="product_same_types", session=session) as dag:

            @dag.task
            def show(a, b):
                outputs.append((a, b))

            show.expand(a=[2, 4, 8], b=[5, 10])

        dag_run = dag_maker.create_dagrun()

        show_task = dag.get_task("show")
        assert show_task.parse_time_mapped_ti_count == 6
        mapped_tis, max_map_index = show_task.expand_mapped_task(dag_run.run_id, session=session)
        assert len(mapped_tis) == 0  # Expanded at parse!
        assert max_map_index == 5

        tis = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag.dag_id,
                TaskInstance.task_id == 'show',
                TaskInstance.run_id == dag_run.run_id,
            )
            .order_by(TaskInstance.map_index)
            .all()
        )
        for ti in tis:
            ti.refresh_from_task(show_task)
            ti.run()
        assert outputs == [(2, 5), (2, 10), (4, 5), (4, 10), (8, 5), (8, 10)]

    def test_map_in_group(self, tmp_path: pathlib.Path, dag_maker, session):
        out = tmp_path.joinpath("out")
        out.touch()

        with dag_maker(dag_id="in_group", session=session) as dag:

            @dag.task
            def envs():
                return [{"VAR1": "FOO"}, {"VAR1": "BAR"}]

            @dag.task
            def cmds():
                return [f'echo "hello $VAR1" >> {out}', f'echo "goodbye $VAR1" >> {out}']

            with TaskGroup(group_id="dynamic"):
                BashOperator.partial(task_id="bash", do_xcom_push=False).expand(
                    env=envs(),
                    bash_command=cmds(),
                )

        dag_run: DagRun = dag_maker.create_dagrun()
        original_tis = {ti.task_id: ti for ti in dag_run.get_task_instances(session=session)}

        for task_id in ["dynamic.envs", "dynamic.cmds"]:
            ti = original_tis[task_id]
            ti.refresh_from_task(dag.get_task(task_id))
            ti.run()

        bash_task = dag.get_task("dynamic.bash")
        mapped_bash_tis, max_map_index = bash_task.expand_mapped_task(dag_run.run_id, session=session)
        assert max_map_index == 3  # 2 * 2 mapped tasks.
        for ti in sorted(mapped_bash_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(bash_task)
            ti.run()

        with out.open() as f:
            out_lines = [line.strip() for line in f]
        assert out_lines == ["hello FOO", "goodbye FOO", "hello BAR", "goodbye BAR"]


@mock.patch("airflow.models.taskinstance.XCom.deserialize_value", side_effect=XCom.deserialize_value)
def test_ti_xcom_pull_on_mapped_operator_return_lazy_iterable(mock_deserialize_value, dag_maker, session):
    """Ensure we access XCom lazily when pulling from a mapped operator."""
    with dag_maker(dag_id="test_xcom", session=session):
        # Use the private _expand() method to avoid the empty kwargs check.
        # We don't care about how the operator runs here, only its presence.
        task_1 = EmptyOperator.partial(task_id="task_1")._expand(EXPAND_INPUT_EMPTY, strict=False)
        EmptyOperator(task_id="task_2")

    dagrun = dag_maker.create_dagrun()

    ti_1_0 = dagrun.get_task_instance("task_1", session=session)
    ti_1_0.map_index = 0
    ti_1_1 = session.merge(TaskInstance(task_1, run_id=dagrun.run_id, map_index=1, state=ti_1_0.state))
    session.flush()

    ti_1_0.xcom_push(key=XCOM_RETURN_KEY, value="a", session=session)
    ti_1_1.xcom_push(key=XCOM_RETURN_KEY, value="b", session=session)

    ti_2 = dagrun.get_task_instance("task_2", session=session)

    # Simply pulling the joined XCom value should not deserialize.
    joined = ti_2.xcom_pull("task_1", session=session)
    assert mock_deserialize_value.call_count == 0

    assert repr(joined) == "_LazyXComAccess(dag_id='test_xcom', run_id='test', task_id='task_1')"

    # Only when we go through the iterable does deserialization happen.
    it = iter(joined)
    assert next(it) == "a"
    assert mock_deserialize_value.call_count == 1
    assert next(it) == "b"
    assert mock_deserialize_value.call_count == 2
    with pytest.raises(StopIteration):
        next(it)


def test_ti_mapped_depends_on_mapped_xcom_arg(dag_maker, session):
    with dag_maker(session=session) as dag:

        @dag.task
        def add_one(x):
            return x + 1

        two_three_four = add_one.expand(x=[1, 2, 3])
        add_one.expand(x=two_three_four)

    dagrun = dag_maker.create_dagrun()
    for map_index in range(3):
        ti = dagrun.get_task_instance("add_one", map_index=map_index, session=session)
        ti.refresh_from_task(dag.get_task("add_one"))
        ti.run()

    task_345 = dag.get_task("add_one__1")
    for ti in task_345.expand_mapped_task(dagrun.run_id, session=session)[0]:
        ti.refresh_from_task(task_345)
        ti.run()

    query = XCom.get_many(run_id=dagrun.run_id, task_ids=["add_one__1"], session=session)
    assert [x.value for x in query.order_by(None).order_by(XCom.map_index)] == [3, 4, 5]


def test_mapped_upstream_return_none_should_skip(dag_maker, session):
    results = set()

    with dag_maker(dag_id="test_mapped_upstream_return_none_should_skip", session=session) as dag:

        @dag.task()
        def transform(value):
            if value == "b":  # Now downstream doesn't map against this!
                return None
            return value

        @dag.task()
        def pull(value):
            results.add(value)

        original = ["a", "b", "c"]
        transformed = transform.expand(value=original)  # ["a", None, "c"]
        pull.expand(value=transformed)  # ["a", "c"]

    dr = dag_maker.create_dagrun()

    decision = dr.task_instance_scheduling_decisions(session=session)
    tis = {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}
    assert sorted(tis) == [("transform", 0), ("transform", 1), ("transform", 2)]
    for ti in tis.values():
        ti.run()

    decision = dr.task_instance_scheduling_decisions(session=session)
    tis = {(ti.task_id, ti.map_index): ti for ti in decision.schedulable_tis}
    assert sorted(tis) == [("pull", 0), ("pull", 1)]
    for ti in tis.values():
        ti.run()

    assert results == {"a", "c"}


def test_expand_non_templated_field(dag_maker, session):
    """Test expand on non-templated fields sets upstream deps properly."""

    class SimpleBashOperator(BashOperator):
        template_fields = ()

    with dag_maker(dag_id="product_same_types", session=session) as dag:

        @dag.task
        def get_extra_env():
            return [{"foo": "bar"}, {"foo": "biz"}]

        SimpleBashOperator.partial(task_id="echo", bash_command="echo $FOO").expand(env=get_extra_env())

    dag_maker.create_dagrun()

    echo_task = dag.get_task("echo")
    assert "get_extra_env" in echo_task.upstream_task_ids
