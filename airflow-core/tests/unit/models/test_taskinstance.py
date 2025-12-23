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

import contextlib
import datetime
import operator
import os
import pathlib
from typing import TYPE_CHECKING, cast
from unittest import mock
from unittest.mock import patch

import pendulum
import pytest
import time_machine
import uuid6
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.exceptions import (
    AirflowException,
    AirflowSkipException,
)
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    PartitionedAssetKeyLog,
)
from airflow.models.connection import Connection
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.hitl_history import HITLDetailHistory
from airflow.models.pool import Pool
from airflow.models.renderedtifields import RenderedTaskInstanceFields
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import (
    TaskInstance,
    TaskInstance as TI,
    TaskInstanceNote,
    find_relevant_relatives,
)
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskmap import TaskMap
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.variable import Variable
from airflow.models.xcom import XComModel
from airflow.observability.stats import Stats
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import (
    HITLBranchOperator,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, BaseOperator, BaseSensorOperator, Metadata, task, task_group
from airflow.sdk.api.datamodels._generated import AssetEventResponse, AssetResponse
from airflow.sdk.definitions.asset import Asset, AssetAlias
from airflow.sdk.definitions.param import process_params
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.sdk.execution_time.comms import AssetEventsResult
from airflow.serialization.definitions.assets import SerializedAsset
from airflow.serialization.encoders import ensure_serialized_asset
from airflow.serialization.serialized_objects import SerializedBaseOperator, SerializedDAG
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.ti_deps.dependencies_states import RUNNABLE_STATES
from airflow.ti_deps.deps.base_ti_dep import TIDepStatus
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep, _UpstreamTIStates
from airflow.timetables.simple import IdentityMapper, PartitionedAssetTimetable
from airflow.utils.db import merge_conn
from airflow.utils.session import create_session, provide_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.db import clear_db_connections, clear_db_runs
from tests_common.test_utils.mock_operators import MockOperator
from unit.models import DEFAULT_DATE

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import DagMaker

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag, pytest.mark.want_activate_assets]


@pytest.fixture
def test_pool():
    with create_session() as session:
        test_pool = Pool(pool="test_pool", slots=1, include_deferred=False)
        session.add(test_pool)
        session.flush()
        yield test_pool
        session.rollback()


@pytest.fixture
def task_reschedules_for_ti():
    def wrapper(ti):
        with create_session() as session:
            return session.scalars(TaskReschedule.stmt_for_task_instance(ti=ti, descending=False)).all()

    return wrapper


class CallbackWrapper:
    task_id: str | None = None
    dag_id: str | None = None
    logical_date: datetime.datetime | None = None
    task_state_in_callback: str | None = None
    callback_ran = False

    def wrap_task_instance(self, ti):
        self.task_id = ti.task_id
        self.dag_id = ti.dag_id
        self.logical_date = ti.logical_date
        self.task_state_in_callback = ""
        self.callback_ran = False

    def success_handler(self, context):
        self.callback_ran = True
        self.task_state_in_callback = context["ti"].state


@pytest.fixture(autouse=True)
def clean_db():
    db.clear_db_dags()
    db.clear_db_pools()
    db.clear_db_runs()
    db.clear_rendered_ti_fields()
    db.clear_db_task_reschedule()
    db.clear_db_assets()
    db.clear_db_xcom()
    db.clear_db_pakl()
    db.clear_db_apdr()
    db.clear_db_assets()


class TestTaskInstance:
    def setup_method(self):
        # We don't want to store any code for (test) dags created in this file
        with patch.object(settings, "STORE_DAG_CODE", False):
            yield

    @pytest.mark.need_serialized_dag(False)
    def test_set_task_dates(self, dag_maker):
        """
        Test that tasks properly take start/end dates from DAGs
        """
        with dag_maker("dag", end_date=DEFAULT_DATE + datetime.timedelta(days=10)) as dag:
            pass

        op1 = EmptyOperator(task_id="op_1")
        assert op1.start_date is None
        assert op1.end_date is None

        # dag should assign its dates to op1 because op1 has no dates
        dag.add_task(op1)
        dag_maker.create_dagrun()
        assert op1.start_date == dag.start_date
        assert op1.end_date == dag.end_date

        op2 = EmptyOperator(
            task_id="op_2",
            start_date=DEFAULT_DATE - datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=11),
        )

        # dag should assign its dates to op2 because they are more restrictive
        dag.add_task(op2)
        assert op2.start_date == dag.start_date
        assert op2.end_date == dag.end_date

        op3 = EmptyOperator(
            task_id="op_3",
            start_date=DEFAULT_DATE + datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=9),
        )
        # op3 should keep its dates because they are more restrictive
        dag.add_task(op3)
        assert op3.start_date == DEFAULT_DATE + datetime.timedelta(days=1)
        assert op3.end_date == DEFAULT_DATE + datetime.timedelta(days=9)

    @pytest.mark.need_serialized_dag(False)
    def test_set_dag(self, dag_maker):
        """
        Test assigning Operators to Dags, including deferred assignment
        """
        with dag_maker("dag") as dag:
            pass
        with dag_maker("dag2") as dag2:
            pass
        op = EmptyOperator(task_id="op_1")

        # no dag assigned
        assert not op.has_dag()
        with pytest.raises(RuntimeError):
            getattr(op, "dag")

        # no improper assignment
        with pytest.raises(TypeError):
            op.dag = 1

        op.dag = dag

        # no reassignment
        with pytest.raises(ValueError, match="can not be changed"):
            op.dag = dag2

        # but assigning the same dag is ok
        op.dag = dag

        assert op.dag is dag
        assert op in dag.tasks

    @pytest.mark.need_serialized_dag(False)
    def test_infer_dag(self, create_dummy_dag):
        op1 = EmptyOperator(task_id="test_op_1")
        op2 = EmptyOperator(task_id="test_op_2")

        dag, op3 = create_dummy_dag(task_id="test_op_3")

        _, op4 = create_dummy_dag("dag2", task_id="test_op_4")

        # double check dags
        assert [i.has_dag() for i in [op1, op2, op3, op4]] == [False, False, True, True]

        # can't combine operators with no dags
        with pytest.raises(
            ValueError, match="Tried to create relationships between tasks that don't have Dags yet"
        ):
            op1.set_downstream(op2)

        # op2 should infer dag from op1
        op1.dag = dag
        op1.set_downstream(op2)
        assert op2.dag is dag

        # can't assign across multiple DAGs
        with pytest.raises(RuntimeError):
            op1.set_downstream(op4)
        with pytest.raises(RuntimeError):
            op1.set_downstream([op3, op4])

    def test_bitshift_compose_operators(self, dag_maker):
        with dag_maker("dag"):
            op1 = EmptyOperator(task_id="test_op_1")
            op2 = EmptyOperator(task_id="test_op_2")
            op3 = EmptyOperator(task_id="test_op_3")

            op1 >> op2 << op3
        dag_maker.create_dagrun()
        # op2 should be downstream of both
        assert op2 in op1.downstream_list
        assert op2 in op3.downstream_list

    def test_init_on_load(self, create_task_instance):
        ti = create_task_instance()
        # ensure log is correctly created for ORM ti
        assert ti.log.name == "airflow.task"
        assert not ti.test_mode

    @patch.object(SerializedDAG, "get_concurrency_reached")
    def test_requeue_over_dag_concurrency(self, mock_concurrency_reached, create_task_instance):
        mock_concurrency_reached.return_value = True

        ti = create_task_instance(
            dag_id="test_requeue_over_dag_concurrency",
            task_id="test_requeue_over_dag_concurrency_op",
            max_active_runs=1,
            max_active_tasks=2,
            dagrun_state=State.QUEUED,
        )
        ti.run()
        assert ti.state == State.NONE

    def test_requeue_over_max_active_tis_per_dag(self, create_task_instance):
        ti = create_task_instance(
            dag_id="test_requeue_over_max_active_tis_per_dag",
            task_id="test_requeue_over_max_active_tis_per_dag_op",
            max_active_tis_per_dag=0,
            max_active_runs=1,
            max_active_tasks=2,
            dagrun_state=State.QUEUED,
            serialized=True,
        )

        ti.run()
        assert ti.state == State.NONE

    def test_requeue_over_max_active_tis_per_dagrun(self, create_task_instance):
        ti = create_task_instance(
            dag_id="test_requeue_over_max_active_tis_per_dagrun",
            task_id="test_requeue_over_max_active_tis_per_dagrun_op",
            max_active_tis_per_dagrun=0,
            max_active_runs=1,
            max_active_tasks=2,
            dagrun_state=State.QUEUED,
            serialized=True,
        )

        ti.run()
        assert ti.state == State.NONE

    def test_requeue_over_pool_concurrency(self, create_task_instance, test_pool):
        ti = create_task_instance(
            dag_id="test_requeue_over_pool_concurrency",
            task_id="test_requeue_over_pool_concurrency_op",
            max_active_tis_per_dag=0,
            max_active_runs=1,
            max_active_tasks=2,
            serialized=True,
        )
        with create_session() as session:
            test_pool.slots = 0
            session.flush()
            ti.run()
            assert ti.state == State.NONE

    @pytest.mark.usefixtures("test_pool")
    def test_not_requeue_non_requeueable_task_instance(self, dag_maker):
        # Use BaseSensorOperator because sensor got
        # one additional DEP in BaseSensorOperator().deps
        with dag_maker(dag_id="test_not_requeue_non_requeueable_task_instance"):
            task = BaseSensorOperator(
                task_id="test_not_requeue_non_requeueable_task_instance_op",
                pool="test_pool",
            )
        ti = dag_maker.create_dagrun(logical_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        ti.state = State.QUEUED
        with create_session() as session:
            session.add(ti)
            session.commit()

        all_deps = RUNNING_DEPS | {ReadyToRescheduleDep()}
        all_non_requeueable_deps = all_deps - REQUEUEABLE_DEPS
        patch_dict = {}
        for dep in all_non_requeueable_deps:
            class_name = dep.__class__.__name__
            dep_patch = patch(f"{dep.__module__}.{class_name}.{dep._get_dep_statuses.__name__}")
            method_patch = dep_patch.start()
            method_patch.return_value = iter([TIDepStatus("mock_" + class_name, True, "mock")])
            patch_dict[class_name] = (dep_patch, method_patch)

        for class_name, (dep_patch, method_patch) in patch_dict.items():
            method_patch.return_value = iter([TIDepStatus("mock_" + class_name, False, "mock")])
            ti.run()
            assert ti.state == State.QUEUED
            dep_patch.return_value = TIDepStatus("mock_" + class_name, True, "mock")

        for dep_patch, _ in patch_dict.values():
            dep_patch.stop()

    def test_mark_non_runnable_task_as_success(self, create_task_instance):
        """
        test that running task with mark_success param update task state
        as SUCCESS without running task despite it fails dependency checks.
        """
        non_runnable_state = (set(State.task_states) - RUNNABLE_STATES - set(State.SUCCESS)).pop()
        ti = create_task_instance(
            dag_id="test_mark_non_runnable_task_as_success",
            task_id="test_mark_non_runnable_task_as_success_op",
            state=non_runnable_state,
        )
        ti.run(mark_success=True)
        assert ti.state == State.SUCCESS

    @pytest.mark.usefixtures("test_pool")
    def test_run_pooling_task(self, create_task_instance):
        """
        test that running a task in an existing pool update task state as SUCCESS.
        """
        ti = create_task_instance(
            dag_id="test_run_pooling_task",
            task_id="test_run_pooling_task_op",
            pool="test_pool",
            serialized=True,
        )

        ti.run()

        assert ti.state == State.SUCCESS

    @pytest.mark.usefixtures("test_pool")
    def test_pool_slots_property(self):
        """
        test that try to create a task with pool_slots less than 1
        """
        dag = DAG(dag_id="test_run_pooling_task", schedule=None)
        with pytest.raises(ValueError, match="pool slots .* cannot be less than 1"):
            EmptyOperator(
                task_id="test_run_pooling_task_op",
                dag=dag,
                pool="test_pool",
                pool_slots=0,
            )

    @provide_session
    def test_ti_updates_with_task(self, create_task_instance, session):
        """
        test that updating the executor_config propagates to the TaskInstance DB
        """
        ti = create_task_instance(
            dag_id="test_run_pooling_task",
            task_id="test_run_pooling_task_op",
            executor_config={"foo": "bar"},
        )
        dag = ti.task.dag

        ti.run(session=session)
        executor_configs = session.scalars(
            select(TaskInstance.executor_config).where(TaskInstance.dag_id == ti.dag_id)
        ).all()
        assert executor_configs == [{"foo": "bar"}]

        task2 = EmptyOperator(
            task_id="test_run_pooling_task_op2",
            executor_config={"bar": "baz"},
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            dag=dag,
        )
        ti2 = TI(task=task2, run_id=ti.run_id, dag_version_id=ti.dag_version_id)
        session.add(ti2)
        session.flush()

        ti2.run(session=session)
        # Ensure it's reloaded
        ti2.executor_config = None
        ti2.refresh_from_db(session)
        assert ti2.executor_config == {"bar": "baz"}
        session.rollback()

    def test_run_pooling_task_with_mark_success(self, create_task_instance):
        """
        test that running task in an existing pool with mark_success param
        update task state as SUCCESS without running task
        despite it fails dependency checks.
        """
        ti = create_task_instance(
            dag_id="test_run_pooling_task_with_mark_success",
            task_id="test_run_pooling_task_with_mark_success_op",
            serialized=True,
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

        with dag_maker(dag_id="test_run_pooling_task_with_skip", serialized=True):
            task = PythonOperator(
                task_id="test_run_pooling_task_with_skip",
                python_callable=raise_skip_exception,
            )

        dr = dag_maker.create_dagrun(logical_date=timezone.utcnow())
        ti = dr.task_instances[0]
        ti.task = task
        ti.run()
        assert ti.state == State.SKIPPED

    def test_retry_delay(self, dag_maker, time_machine):
        """
        Test that retry delays are respected
        """
        time_machine.move_to("2021-09-19 04:56:35", tick=False)
        with dag_maker(dag_id="test_retry_handling"):
            task = BashOperator(
                task_id="test_retry_handling_op",
                bash_command="exit 1",
                retries=1,
                retry_delay=datetime.timedelta(seconds=3),
            )

        def run_with_error(ti):
            orig_task, ti.task = ti.task, task
            with contextlib.suppress(AirflowException):
                ti.run()
            ti.task = orig_task

        ti = dag_maker.create_dagrun(logical_date=timezone.utcnow()).task_instances[0]
        with create_session() as session:
            session.get(TaskInstance, ti.id).try_number += 1

        # first run -- up for retry
        run_with_error(ti)
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 1

        with create_session() as session:
            session.get(TaskInstance, ti.id).try_number += 1

        # second run -- still up for retry because retry_delay hasn't expired
        time_machine.coordinates.shift(3)
        run_with_error(ti)
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 2

        with create_session() as session:
            session.get(TaskInstance, ti.id).try_number += 1

        # third run -- failed
        time_machine.coordinates.shift(datetime.datetime.resolution)
        run_with_error(ti)
        assert ti.state == State.FAILED
        assert ti.try_number == 3

    def test_retry_handling(self, dag_maker, session):
        """
        Test that task retries are handled properly
        """
        expected_rendered_ti_fields = {
            "env": None,
            "bash_command": "echo test_retry_handling; exit 1",
            "cwd": None,
        }

        with dag_maker(dag_id="test_retry_handling", serialized=True) as dag:
            task = BashOperator(
                task_id="test_retry_handling_op",
                bash_command="echo {{dag.dag_id}}; exit 1",
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
            )

        def run_with_error(ti):
            orig_task, ti.task = ti.task, task
            with contextlib.suppress(AirflowException):
                ti.run()
            ti.task = orig_task

        ti = dag_maker.create_dagrun(logical_date=timezone.utcnow()).task_instances[0]
        assert ti.try_number == 0
        session.get(TaskInstance, ti.id).try_number += 1
        session.commit()

        # first run -- up for retry
        run_with_error(ti)
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 1

        session.get(TaskInstance, ti.id).try_number += 1
        session.commit()

        # second run -- fail
        run_with_error(ti)
        assert ti.state == State.FAILED
        assert ti.try_number == 2

        # Clear the TI state since you can't run a task with a FAILED state without
        # clearing it first
        dag.clear()

        ti.refresh_from_db(session)
        ti.try_number += 1
        session.add(ti)
        session.commit()

        # third run -- up for retry
        run_with_error(ti)
        ti.refresh_from_db()
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 3

        session.get(TaskInstance, ti.id).try_number += 1
        session.commit()

        # fourth run -- fail
        run_with_error(ti)
        ti.refresh_from_db()
        assert ti.state == State.FAILED
        assert ti.try_number == 4
        assert RenderedTaskInstanceFields.get_templated_fields(ti) == expected_rendered_ti_fields

    def test_next_retry_datetime(self, dag_maker):
        delay = datetime.timedelta(seconds=30)
        max_delay = datetime.timedelta(minutes=60)

        with dag_maker(dag_id="fail_dag"):
            task = BashOperator(
                task_id="task_with_exp_backoff_and_max_delay",
                bash_command="exit 1",
                retries=3,
                retry_delay=delay,
                retry_exponential_backoff=2.0,
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

    def test_next_retry_datetime_returns_max_for_overflow(self, dag_maker):
        delay = datetime.timedelta(seconds=30)
        max_delay = datetime.timedelta(minutes=60)

        with dag_maker(dag_id="fail_dag"):
            task = BashOperator(
                task_id="task_with_exp_backoff_and_max_delay",
                bash_command="exit 1",
                retries=3,
                retry_delay=delay,
                retry_exponential_backoff=2.0,
                max_retry_delay=max_delay,
            )
        ti = dag_maker.create_dagrun().task_instances[0]
        ti.task = task
        ti.end_date = pendulum.instance(timezone.utcnow())

        ti.try_number = 5000
        date = ti.next_retry_datetime()
        assert date == ti.end_date + max_delay

        ti.try_number = 50000
        date = ti.next_retry_datetime()
        assert date == ti.end_date + max_delay

    @pytest.mark.parametrize("seconds", [0, 0.5, 1])
    def test_next_retry_datetime_short_or_zero_intervals(self, dag_maker, seconds):
        delay = datetime.timedelta(seconds=seconds)
        max_delay = datetime.timedelta(minutes=60)

        with dag_maker(dag_id="fail_dag"):
            task = BashOperator(
                task_id="task_with_exp_backoff_and_short_or_zero_time_interval",
                bash_command="exit 1",
                retries=3,
                retry_delay=delay,
                retry_exponential_backoff=2.0,
                max_retry_delay=max_delay,
            )
        ti = dag_maker.create_dagrun().task_instances[0]
        ti.task = task
        ti.end_date = pendulum.instance(timezone.utcnow())

        date = ti.next_retry_datetime()
        assert date == ti.end_date + datetime.timedelta(seconds=1)

    def test_next_retry_datetime_with_custom_multiplier(self, dag_maker):
        delay = datetime.timedelta(minutes=4)

        with dag_maker(dag_id="fail_dag"):
            task = BashOperator(
                task_id="task_with_custom_multiplier",
                bash_command="exit 1",
                retries=3,
                retry_delay=delay,
                retry_exponential_backoff=5.0,
            )
        ti = dag_maker.create_dagrun().task_instances[0]
        ti.task = task
        ti.end_date = pendulum.instance(timezone.utcnow())

        ti.try_number = 1
        date = ti.next_retry_datetime()
        period = ti.end_date.add(seconds=1200) - ti.end_date.add(seconds=240)
        assert date in period

        ti.try_number = 2
        date = ti.next_retry_datetime()
        period = ti.end_date.add(seconds=6000) - ti.end_date.add(seconds=1200)
        assert date in period

    @pytest.mark.usefixtures("test_pool")
    def test_mapped_task_reschedule_handling_clear_reschedules(self, dag_maker, task_reschedules_for_ti):
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

        with dag_maker(dag_id="test_reschedule_handling", serialized=True) as dag:
            task = PythonSensor.partial(
                task_id="test_reschedule_handling_sensor",
                mode="reschedule",
                python_callable=func,
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
                pool="test_pool",
            ).expand(poke_interval=[0])
        ti = dag_maker.create_dagrun(logical_date=timezone.utcnow()).task_instances[0]
        ti.task = task

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
            with time_machine.travel(run_date, tick=False):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            assert ti.state == expected_state
            assert ti.try_number == expected_try_number
            assert ti.start_date == expected_start_date
            assert ti.end_date == expected_end_date
            assert ti.duration == expected_duration
            assert len(task_reschedules_for_ti(ti)) == expected_task_reschedule_count

        date1 = timezone.utcnow()

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        assert ti.state == State.NONE
        assert ti.try_number == 0
        # Check that reschedules for ti have also been cleared.
        assert not task_reschedules_for_ti(ti)

    @pytest.mark.usefixtures("test_pool")
    def test_reschedule_handling_clear_reschedules(self, dag_maker, task_reschedules_for_ti):
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

        with dag_maker(dag_id="test_reschedule_handling", serialized=True) as dag:
            task = PythonSensor(
                task_id="test_reschedule_handling_sensor",
                poke_interval=0,
                mode="reschedule",
                python_callable=func,
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
                pool="test_pool",
            )
        ti = dag_maker.create_dagrun(logical_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        assert ti.try_number == 0

        def run_ti_and_assert(
            run_date,
            expected_start_date,
            expected_end_date,
            expected_duration,
            expected_state,
            expected_try_number,
            expected_task_reschedule_count,
        ):
            with time_machine.travel(run_date, tick=False):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            assert ti.state == expected_state
            assert ti.try_number == expected_try_number
            assert ti.start_date == expected_start_date
            assert ti.end_date == expected_end_date
            assert ti.duration == expected_duration
            assert len(task_reschedules_for_ti(ti)) == expected_task_reschedule_count

        date1 = timezone.utcnow()

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        assert ti.state == State.NONE
        assert ti.try_number == 0
        # Check that reschedules for ti have also been cleared.
        assert not task_reschedules_for_ti(ti)

    def test_depends_on_past_catchup_true(self, dag_maker):
        class CustomOp(BaseOperator):
            def execute(self, context): ...

        with dag_maker(dag_id="test_depends_on_past", serialized=True, catchup=True):
            task = CustomOp(
                task_id="test_dop_task",
                depends_on_past=True,
            )
        dag_maker.create_dagrun(
            state=State.FAILED,
            run_type=DagRunType.SCHEDULED,
        )

        run_date = task.start_date + datetime.timedelta(days=5)

        dr = dag_maker.create_dagrun(
            logical_date=run_date,
            run_type=DagRunType.SCHEDULED,
        )

        ti = dr.task_instances[0]
        ti.task = task

        # depends_on_past prevents the run
        dag_maker.run_ti(task.task_id, dr, ignore_depends_on_past=False)
        ti.refresh_from_db()
        assert ti.state is None

        # ignore first depends_on_past to allow the run
        dag_maker.run_ti(task.task_id, dr, ignore_depends_on_past=True)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    def test_depends_on_past_catchup_false(self, dag_maker):
        class CustomOp(BaseOperator):
            def execute(self, context): ...

        with dag_maker(dag_id="test_depends_on_past_catchup_false", serialized=True, catchup=False):
            task = CustomOp(
                task_id="test_dop_task",
                depends_on_past=True,
            )

        dag_maker.create_dagrun(
            state=State.FAILED,
            run_type=DagRunType.SCHEDULED,
        )

        run_date = task.start_date + datetime.timedelta(days=5)

        dr = dag_maker.create_dagrun(
            logical_date=run_date,
            run_type=DagRunType.SCHEDULED,
        )

        ti = dr.task_instances[0]
        ti.task = task

        # With catchup=False, depends_on_past behavior is different:
        # The task ignores historical dependencies since catchup=False means
        # "only consider runs from now forward"
        dag_maker.run_ti(task.task_id, dr, ignore_depends_on_past=False)
        ti.refresh_from_db()

        # The task runs successfully even with depends_on_past=True because
        # catchup=False changes how historical dependencies are considered
        assert ti.state == State.SUCCESS

        # ignore_first_depends_on_past should still allow the run with catchup=False
        dag_maker.run_ti(task.task_id, dr, ignore_depends_on_past=True)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    # Parameterized tests to check for the correct firing
    # of the trigger_rule under various circumstances
    # Numeric fields are in order:
    #   successes, skipped, failed, upstream_failed, removed, done
    @pytest.mark.parametrize(
        (
            "trigger_rule",
            "upstream_setups",
            "upstream_states",
            "flag_upstream_failed",
            "expect_state",
            "expect_passed",
        ),
        [
            #
            # Tests for all_success
            #
            ["all_success", 0, _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, None, True],
            ["all_success", 0, _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, None, False],
            ["all_success", 0, _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["all_success", 0, _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, State.SKIPPED, False],
            #
            # Tests for one_success
            #
            ["one_success", 0, _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, None, True],
            ["one_success", 0, _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, None, True],
            ["one_success", 0, _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, None, True],
            ["one_success", 0, _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, None, True],
            ["one_success", 0, _UpstreamTIStates(0, 5, 0, 0, 0, 5, 0, 0), True, State.SKIPPED, False],
            ["one_success", 0, _UpstreamTIStates(0, 4, 1, 0, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", 0, _UpstreamTIStates(0, 3, 1, 1, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", 0, _UpstreamTIStates(0, 4, 0, 1, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", 0, _UpstreamTIStates(0, 0, 5, 0, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", 0, _UpstreamTIStates(0, 0, 4, 1, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", 0, _UpstreamTIStates(0, 0, 0, 5, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            #
            # Tests for all_failed
            #
            ["all_failed", 0, _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, State.SKIPPED, False],
            ["all_failed", 0, _UpstreamTIStates(0, 0, 5, 0, 0, 5, 0, 0), True, None, True],
            ["all_failed", 0, _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, State.SKIPPED, False],
            ["all_failed", 0, _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, State.SKIPPED, False],
            ["all_failed", 0, _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, State.SKIPPED, False],
            #
            # Tests for one_failed
            #
            ["one_failed", 0, _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, State.SKIPPED, False],
            ["one_failed", 0, _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, None, False],
            ["one_failed", 0, _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, None, True],
            ["one_failed", 0, _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, None, False],
            ["one_failed", 0, _UpstreamTIStates(2, 3, 0, 0, 0, 5, 0, 0), True, State.SKIPPED, False],
            #
            # Tests for done
            #
            ["all_done", 0, _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, None, True],
            ["all_done", 0, _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, None, False],
            ["all_done", 0, _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, None, False],
            ["all_done", 0, _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, None, False],
            #
            # Tests for all_done_setup_success: no upstream setups -> same as all_done
            #
            ["all_done_setup_success", 0, _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, None, True],
            ["all_done_setup_success", 0, _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, None, False],
            ["all_done_setup_success", 0, _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, None, False],
            ["all_done_setup_success", 0, _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, None, False],
            #
            # Tests for all_done_setup_success: with upstream setups -> different from all_done
            #
            # params:
            #   trigger_rule
            #   upstream_setups
            #   upstream_states
            #   flag_upstream_failed
            #   expect_state
            #   expect_passed
            # states: success, skipped, failed, upstream_failed, removed, done, success_setup, skipped_setup
            # all setups succeeded - one
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(6, 0, 0, 0, 0, 6, 1, 0),
                True,
                None,
                True,
                id="all setups succeeded - one",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(7, 0, 0, 0, 0, 7, 2, 0),
                True,
                None,
                True,
                id="all setups succeeded - two",
            ),
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(5, 0, 1, 0, 0, 6, 0, 0),
                True,
                State.UPSTREAM_FAILED,
                False,
                id="setups failed - one",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(5, 0, 2, 0, 0, 7, 0, 0),
                True,
                State.UPSTREAM_FAILED,
                False,
                id="setups failed - two",
            ),
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(5, 1, 0, 0, 0, 6, 0, 1),
                True,
                State.SKIPPED,
                False,
                id="setups skipped - one",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(5, 2, 0, 0, 0, 7, 0, 2),
                True,
                State.SKIPPED,
                False,
                id="setups skipped - two",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(5, 1, 1, 0, 0, 7, 0, 1),
                True,
                State.UPSTREAM_FAILED,
                False,
                id="one setup failed one setup skipped",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(6, 0, 1, 0, 0, 7, 1, 0),
                True,
                (True, None),  # is_teardown=True, expect_state=None
                True,
                id="is teardown one setup failed one setup success",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(6, 0, 1, 0, 0, 7, 1, 0),
                True,
                (False, "upstream_failed"),  # is_teardown=False, expect_state="upstream_failed"
                True,
                id="not teardown one setup failed one setup success",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(6, 1, 0, 0, 0, 7, 1, 1),
                True,
                (True, None),  # is_teardown=True, expect_state=None
                True,
                id="is teardown one setup success one setup skipped",
            ),
            pytest.param(
                "all_done_setup_success",
                2,
                _UpstreamTIStates(6, 1, 0, 0, 0, 7, 1, 1),
                True,
                (False, "skipped"),  # is_teardown=False, expect_state="skipped"
                True,
                id="not teardown one setup success one setup skipped",
            ),
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(3, 0, 0, 0, 0, 3, 1, 0),
                True,
                None,
                False,
                id="not all done",
            ),
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(3, 0, 1, 0, 0, 4, 1, 0),
                True,
                (True, None),  # is_teardown=True, expect_state=None
                False,
                id="is teardown not all done one failed",
            ),
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(3, 0, 1, 0, 0, 4, 1, 0),
                True,
                (False, "upstream_failed"),  # is_teardown=False, expect_state="upstream_failed"
                False,
                id="not teardown not all done one failed",
            ),
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(3, 1, 0, 0, 0, 4, 1, 0),
                True,
                (True, None),  # is_teardown=True, expect_state=None
                False,
                id="not all done one skipped",
            ),
            pytest.param(
                "all_done_setup_success",
                1,
                _UpstreamTIStates(3, 1, 0, 0, 0, 4, 1, 0),
                True,
                (False, "skipped"),  # is_teardown=False, expect_state="skipped'
                False,
                id="not all done one skipped",
            ),
        ],
    )
    def test_check_task_dependencies(
        self,
        monkeypatch,
        dag_maker,
        trigger_rule: str,
        upstream_setups: int,
        upstream_states: _UpstreamTIStates,
        flag_upstream_failed: bool,
        expect_state: State,
        expect_passed: bool,
    ):
        # this allows us to change the expected state depending on whether the
        # task is a teardown
        set_teardown = False
        if isinstance(expect_state, tuple):
            set_teardown, expect_state = expect_state
            assert isinstance(set_teardown, bool)

        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)

        # sanity checks
        s = upstream_states
        assert s.skipped >= s.skipped_setup
        assert s.success >= s.success_setup
        assert s.done == s.failed + s.success + s.removed + s.upstream_failed + s.skipped

        with dag_maker():
            downstream = EmptyOperator(task_id="downstream", trigger_rule=trigger_rule)
            if set_teardown:
                downstream.as_teardown()
            for i in range(5):
                task = EmptyOperator(task_id=f"work_{i}")
                task.set_downstream(downstream)
            for i in range(upstream_setups):
                task = EmptyOperator(task_id=f"setup_{i}").as_setup()
                task.set_downstream(downstream)
            assert task.start_date is not None
            run_date = task.start_date + datetime.timedelta(days=5)

        dr = dag_maker.create_dagrun(logical_date=run_date)
        dag_maker.session.commit()
        ti = dr.get_task_instance(downstream.task_id)
        ti.task = downstream

        dep_results = TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
            session=dag_maker.session,
        )
        completed = all(dep.passed for dep in dep_results)

        ti = dr.get_task_instance(downstream.task_id)

        assert completed == expect_passed
        assert ti.state == expect_state

    # Parameterized tests to check for the correct firing
    # of the trigger_rule under various circumstances of mapped task
    # Numeric fields are in order:
    #   successes, skipped, failed, upstream_failed, done,remove
    # Does not work for database isolation mode because there is local test monkeypatching of upstream_failed
    # That never gets propagated to internal_api
    @pytest.mark.parametrize(
        ("trigger_rule", "upstream_states", "flag_upstream_failed", "expect_state", "expect_completed"),
        [
            #
            # Tests for all_success
            #
            ["all_success", _UpstreamTIStates(5, 0, 0, 0, 0, 0, 0, 0), True, None, True],
            ["all_success", _UpstreamTIStates(2, 0, 0, 0, 0, 0, 0, 0), True, None, False],
            ["all_success", _UpstreamTIStates(2, 0, 1, 0, 0, 0, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["all_success", _UpstreamTIStates(2, 1, 0, 0, 0, 0, 0, 0), True, State.SKIPPED, False],
            # ti.map_index >= success
            ["all_success", _UpstreamTIStates(3, 0, 0, 0, 2, 0, 0, 0), True, State.REMOVED, True],
            #
            # Tests for one_success
            #
            ["one_success", _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, None, True],
            ["one_success", _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, None, True],
            ["one_success", _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, None, True],
            ["one_success", _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, None, True],
            ["one_success", _UpstreamTIStates(0, 5, 0, 0, 0, 5, 0, 0), True, State.SKIPPED, False],
            ["one_success", _UpstreamTIStates(0, 4, 1, 0, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", _UpstreamTIStates(0, 3, 1, 1, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", _UpstreamTIStates(0, 4, 0, 1, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", _UpstreamTIStates(0, 0, 5, 0, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", _UpstreamTIStates(0, 0, 4, 1, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            ["one_success", _UpstreamTIStates(0, 0, 0, 5, 0, 5, 0, 0), True, State.UPSTREAM_FAILED, False],
            #
            # Tests for all_failed
            #
            ["all_failed", _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, State.SKIPPED, False],
            ["all_failed", _UpstreamTIStates(0, 0, 5, 0, 0, 5, 0, 0), True, None, True],
            ["all_failed", _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, State.SKIPPED, False],
            ["all_failed", _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, State.SKIPPED, False],
            ["all_failed", _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, State.SKIPPED, False],
            [
                "all_failed",
                _UpstreamTIStates(2, 1, 0, 0, 1, 4, 0, 0),
                True,
                State.SKIPPED,
                False,
            ],  # One removed
            #
            # Tests for one_failed
            #
            ["one_failed", _UpstreamTIStates(5, 0, 0, 0, 0, 0, 0, 0), True, None, False],
            ["one_failed", _UpstreamTIStates(2, 0, 0, 0, 0, 0, 0, 0), True, None, False],
            ["one_failed", _UpstreamTIStates(2, 0, 1, 0, 0, 0, 0, 0), True, None, True],
            ["one_failed", _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, None, False],
            ["one_failed", _UpstreamTIStates(2, 3, 0, 0, 0, 5, 0, 0), True, State.SKIPPED, False],
            [
                "one_failed",
                _UpstreamTIStates(2, 2, 0, 0, 1, 5, 0, 0),
                True,
                State.SKIPPED,
                False,
            ],  # One removed
            #
            # Tests for done
            #
            ["all_done", _UpstreamTIStates(5, 0, 0, 0, 0, 5, 0, 0), True, None, True],
            ["all_done", _UpstreamTIStates(2, 0, 0, 0, 0, 2, 0, 0), True, None, False],
            ["all_done", _UpstreamTIStates(2, 0, 1, 0, 0, 3, 0, 0), True, None, False],
            ["all_done", _UpstreamTIStates(2, 1, 0, 0, 0, 3, 0, 0), True, None, False],
        ],
    )
    def test_check_task_dependencies_for_mapped(
        self,
        monkeypatch,
        dag_maker,
        session,
        trigger_rule: str,
        upstream_states: _UpstreamTIStates,
        flag_upstream_failed: bool,
        expect_state: State,
        expect_completed: bool,
    ):
        from airflow.sdk import task

        @task
        def do_something(i):
            return 1

        @task(trigger_rule=trigger_rule)
        def do_something_else(i):
            return 1

        with dag_maker(dag_id="test_dag", session=session):
            nums = do_something.expand(i=[i + 1 for i in range(5)])
            do_something_else.expand(i=nums)

        dr = dag_maker.create_dagrun()
        dag_maker.session.commit()
        monkeypatch.setattr(_UpstreamTIStates, "calculate", lambda *_: upstream_states)
        ti = dr.get_task_instance("do_something_else", session=session)
        ti.map_index = 0
        base_task = ti.task

        for map_index in range(1, 5):
            ti = TaskInstance(
                base_task, run_id=dr.run_id, map_index=map_index, dag_version_id=ti.dag_version_id
            )
            session.add(ti)
            ti.dag_run = dr
        session.flush()
        session.commit()
        downstream = ti.task
        ti = dr.get_task_instance(task_id="do_something_else", map_index=3, session=session)
        ti.task = downstream
        dep_results = TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            dep_context=DepContext(flag_upstream_failed=flag_upstream_failed),
            session=session,
        )
        completed = all(dep.passed for dep in dep_results)
        ti = dr.get_task_instance(task_id="do_something_else", map_index=3, session=session)

        assert completed == expect_completed
        assert ti.state == expect_state

    def test_respects_prev_dagrun_dep(self, dag_maker, session):
        with dag_maker("test_respects_prev_dagrun_dep", serialized=True) as dag:
            EmptyOperator(task_id="t")

        dr = dag_maker.create_dagrun(session=session)
        ti = dr.get_task_instance(task_id="t", session=session)

        # Operate on serialized task
        ti.task = dag.task_dict[ti.task_id]
        failing_status = [TIDepStatus("test fail status name", False, "test fail reason")]
        passing_status = [TIDepStatus("test pass status name", True, "test passing reason")]
        with patch(
            "airflow.ti_deps.deps.prev_dagrun_dep.PrevDagrunDep.get_dep_statuses", return_value=failing_status
        ):
            assert not ti.are_dependencies_met()
        with patch(
            "airflow.ti_deps.deps.prev_dagrun_dep.PrevDagrunDep.get_dep_statuses", return_value=passing_status
        ):
            assert ti.are_dependencies_met()

    @pytest.mark.parametrize(
        ("downstream_ti_state", "expected_are_dependents_done"),
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
        self, downstream_ti_state, expected_are_dependents_done, create_task_instance, session
    ):
        ti = create_task_instance(session=session)
        dag = ti.task.dag
        downstream_task = EmptyOperator(task_id="downstream_task", dag=dag)
        ti.task >> downstream_task

        downstream_ti = TI(downstream_task, run_id=ti.run_id, dag_version_id=ti.dag_version_id)

        downstream_ti.set_state(downstream_ti_state, session)
        session.flush()
        assert ti.are_dependents_done(session) == expected_are_dependents_done

    def test_xcom_push_flag(self, dag_maker):
        """
        Tests the option for Operators to push XComs
        """
        value = "hello"
        task_id = "test_no_xcom_push"

        with dag_maker(dag_id="test_xcom", serialized=True):
            # nothing saved to XCom
            task = PythonOperator(
                task_id=task_id,
                python_callable=lambda: value,
                do_xcom_push=False,
            )
        ti = dag_maker.create_dagrun(logical_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        ti.run()
        assert ti.xcom_pull(task_ids=task_id) is None

    def test_check_and_change_state_before_execution(self, create_task_instance, testing_dag_bundle):
        expected_external_executor_id = "banana"
        ti = create_task_instance(
            dag_id="test_check_and_change_state_before_execution",
            external_executor_id=expected_external_executor_id,
        )

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(
            task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id, dag_version_id=ti.dag_version_id
        )

        assert ti_from_deserialized_task.try_number == 0
        assert ti_from_deserialized_task.check_and_change_state_before_execution()
        # State should be running, and try_number column should be incremented
        assert ti_from_deserialized_task.external_executor_id == expected_external_executor_id
        assert ti_from_deserialized_task.state == State.RUNNING
        assert ti_from_deserialized_task.try_number == 0

    def test_check_and_change_state_before_execution_provided_id_overrides(
        self, create_task_instance, testing_dag_bundle
    ):
        expected_external_executor_id = "banana"
        ti = create_task_instance(
            dag_id="test_check_and_change_state_before_execution",
            external_executor_id="apple",
        )
        assert ti.external_executor_id == "apple"

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(
            task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id, dag_version_id=ti.dag_version_id
        )

        assert ti_from_deserialized_task.try_number == 0
        assert ti_from_deserialized_task.check_and_change_state_before_execution(
            external_executor_id=expected_external_executor_id
        )
        # State should be running, and try_number column should be incremented
        assert ti_from_deserialized_task.external_executor_id == expected_external_executor_id
        assert ti_from_deserialized_task.state == State.RUNNING
        assert ti_from_deserialized_task.try_number == 0

    def test_check_and_change_state_before_execution_with_exec_id(self, create_task_instance):
        expected_external_executor_id = "minions"
        ti = create_task_instance(dag_id="test_check_and_change_state_before_execution")
        assert ti.external_executor_id is None

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(
            task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id, dag_version_id=ti.dag_version_id
        )

        assert ti_from_deserialized_task.try_number == 0
        assert ti_from_deserialized_task.check_and_change_state_before_execution(
            external_executor_id=expected_external_executor_id
        )
        # State should be running, and try_number column should be unchanged
        assert ti_from_deserialized_task.external_executor_id == expected_external_executor_id
        assert ti_from_deserialized_task.state == State.RUNNING
        assert ti_from_deserialized_task.try_number == 0

    def test_check_and_change_state_before_execution_dep_not_met(self, dag_maker):
        with dag_maker(dag_id="test_check_and_change_state_before_execution") as dag:
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2", start_date=DEFAULT_DATE)
            task1 >> task2
        dr = dag_maker.create_dagrun()
        ti2 = dr.get_task_instance("task2")
        ti2.refresh_from_task(dag.get_task("task2"))  # Need scheduler task for the check.
        assert not ti2.check_and_change_state_before_execution()

    def test_check_and_change_state_before_execution_dep_not_met_already_running(
        self, create_task_instance, testing_dag_bundle
    ):
        """Return False if the task instance state is running"""
        ti = create_task_instance(dag_id="test_check_and_change_state_before_execution")
        with create_session() as _:
            ti.state = State.RUNNING

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(
            task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id, dag_version_id=ti.dag_version_id
        )

        assert not ti_from_deserialized_task.check_and_change_state_before_execution()
        assert ti_from_deserialized_task.state == State.RUNNING
        assert ti_from_deserialized_task.external_executor_id is None

    def test_check_and_change_state_before_execution_dep_not_met_not_runnable_state(
        self, create_task_instance, testing_dag_bundle
    ):
        """Return False if the task instance state is failed"""
        ti = create_task_instance(dag_id="test_check_and_change_state_before_execution")
        with create_session() as _:
            ti.state = State.FAILED

        serialized_dag = SerializedDagModel.get(ti.task.dag.dag_id).dag
        ti_from_deserialized_task = TI(
            task=serialized_dag.get_task(ti.task_id), run_id=ti.run_id, dag_version_id=ti.dag_version_id
        )

        assert not ti_from_deserialized_task.check_and_change_state_before_execution()
        assert ti_from_deserialized_task.state == State.FAILED

    def test_try_number(self, create_task_instance):
        """
        Test the try_number accessor behaves in various running states
        """
        ti = create_task_instance(dag_id="test_try_number")
        # TI starts at 0.  It's only incremented by the scheduler.
        assert ti.try_number == 0
        ti.try_number = 2
        assert ti.try_number == 2
        ti.state = State.RUNNING
        assert ti.try_number == 2  # unaffected by state
        ti.state = State.SUCCESS
        assert ti.try_number == 2  # unaffected by state

    def test_get_num_running_task_instances(self, dag_maker, create_task_instance):
        session = settings.Session()

        ti1 = create_task_instance(
            dag_id="test_get_num_running_task_instances", task_id="task1", session=session
        )

        logical_date = DEFAULT_DATE + datetime.timedelta(days=1)
        dr = dag_maker.create_dagrun(
            logical_date=logical_date,
            run_type=DagRunType.MANUAL,
            state=None,
            run_id="2",
            session=session,
            data_interval=(logical_date, logical_date),
            run_after=logical_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        assert ti1 in session
        ti2 = dr.task_instances[0]
        ti2.task = ti1.task

        ti3 = create_task_instance(
            dag_id="test_get_num_running_task_instances_dummy", task_id="task2", session=session
        )
        assert ti3 in session
        assert ti1 in session

        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.RUNNING
        assert ti3 in session
        session.commit()

        assert ti1.get_num_running_task_instances(session=session) == 1
        assert ti2.get_num_running_task_instances(session=session) == 1
        assert ti3.get_num_running_task_instances(session=session) == 1

    def test_get_num_running_task_instances_per_dagrun(self, create_task_instance, dag_maker):
        session = settings.Session()

        with dag_maker(dag_id="test_dag"):
            MockOperator.partial(task_id="task_1").expand_kwargs([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
            MockOperator.partial(task_id="task_2").expand_kwargs([{"a": 1, "b": 2}])
            MockOperator.partial(task_id="task_3").expand_kwargs([{"a": 1, "b": 2}])

        dr1 = dag_maker.create_dagrun(
            logical_date=timezone.utcnow(), state=DagRunState.RUNNING, run_id="run_id_1", session=session
        )
        tis1 = {(ti.task_id, ti.map_index): ti for ti in dr1.task_instances}
        print(f"tis1: {tis1}")

        dr2 = dag_maker.create_dagrun(
            logical_date=timezone.utcnow(), state=DagRunState.RUNNING, run_id="run_id_2", session=session
        )
        tis2 = {(ti.task_id, ti.map_index): ti for ti in dr2.task_instances}

        assert tis1[("task_1", 0)] in session
        assert tis1[("task_1", 1)] in session
        assert tis1[("task_2", 0)] in session
        assert tis1[("task_3", 0)] in session
        assert tis2[("task_1", 0)] in session
        assert tis2[("task_1", 1)] in session
        assert tis2[("task_2", 0)] in session
        assert tis2[("task_3", 0)] in session

        tis1[("task_1", 0)].state = State.RUNNING
        tis1[("task_1", 1)].state = State.QUEUED
        tis1[("task_2", 0)].state = State.RUNNING
        tis1[("task_3", 0)].state = State.RUNNING
        tis2[("task_1", 0)].state = State.RUNNING
        tis2[("task_1", 1)].state = State.QUEUED
        tis2[("task_2", 0)].state = State.RUNNING
        tis2[("task_3", 0)].state = State.RUNNING

        session.commit()

        assert tis1[("task_1", 0)].get_num_running_task_instances(session=session, same_dagrun=True) == 1
        assert tis1[("task_1", 1)].get_num_running_task_instances(session=session, same_dagrun=True) == 1
        assert tis1[("task_2", 0)].get_num_running_task_instances(session=session) == 2
        assert tis1[("task_3", 0)].get_num_running_task_instances(session=session, same_dagrun=True) == 1

        assert tis2[("task_1", 0)].get_num_running_task_instances(session=session, same_dagrun=True) == 1
        assert tis2[("task_1", 1)].get_num_running_task_instances(session=session, same_dagrun=True) == 1
        assert tis2[("task_2", 0)].get_num_running_task_instances(session=session) == 2
        assert tis2[("task_3", 0)].get_num_running_task_instances(session=session, same_dagrun=True) == 1

    def test_log_url(self, create_task_instance):
        ti = create_task_instance(dag_id="my_dag", task_id="op", logical_date=timezone.datetime(2018, 1, 1))

        expected_url = "http://localhost:8080/dags/my_dag/runs/test/tasks/op"
        assert ti.log_url == expected_url

        ti.map_index = 1
        ti.try_number = 2
        session = settings.Session()
        session.merge(ti)
        session.commit()

        expected_url = "http://localhost:8080/dags/my_dag/runs/test/tasks/op/mapped/1?try_number=2"
        assert ti.log_url == expected_url

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"inlets": [Asset(uri="file://some.txt")]},
            {"outlets": [Asset(uri="file://some.txt")]},
            {"on_success_callback": lambda *args, **kwargs: None},
            {"on_execute_callback": lambda *args, **kwargs: None},
        ],
    )
    def test_is_schedulable_task_empty_operator_evaluates_true(self, kwargs, create_task_instance):
        ti = create_task_instance(
            dag_id="my_dag", task_id="op", logical_date=timezone.datetime(2018, 1, 1), **kwargs
        )
        assert ti.is_schedulable

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"on_failure_callback": lambda *args, **kwargs: None},
            {"on_skipped_callback": lambda *args, **kwargs: None},
            {"on_retry_callback": lambda *args, **kwargs: None},
        ],
    )
    def test_is_schedulable_task_empty_operator_evaluates_false(self, kwargs, create_task_instance):
        ti = create_task_instance(
            dag_id="my_dag", task_id="op", logical_date=timezone.datetime(2018, 1, 1), **kwargs
        )
        assert not ti.is_schedulable

    def test_is_schedulable_task_non_empty_operator(self):
        dag = DAG(dag_id="test_dag")

        regular_task = BashOperator(task_id="regular", bash_command="echo test", dag=dag)
        mapped_task = BashOperator.partial(task_id="mapped", dag=dag).expand(bash_command=["echo 1"])

        regular_ti = TaskInstance(task=regular_task, dag_version_id=mock.MagicMock())
        mapped_ti = TaskInstance(task=mapped_task, dag_version_id=mock.MagicMock())

        assert regular_ti.is_schedulable
        assert mapped_ti.is_schedulable

    def test_mark_success_url(self, create_task_instance):
        now = pendulum.now("Europe/Brussels")
        ti = create_task_instance(dag_id="dag", task_id="op", logical_date=now)

        expected_url = "http://localhost:8080/dags/dag/runs/test/tasks/op"
        assert ti.mark_success_url == expected_url

    def test_overwrite_params_with_dag_run_conf(self, create_task_instance):
        ti = create_task_instance()
        dag_run = ti.dag_run
        dag_run.conf = {"override": True}
        ti.task.params = {"override": False}

        params = process_params(ti.task.dag, ti.task, dag_run.conf, suppress_exception=False)
        assert params["override"] is True

    def test_overwrite_params_with_dag_run_none(self, create_task_instance):
        ti = create_task_instance()
        ti.task.params = {"override": False}

        params = process_params(ti.task.dag, ti.task, None, suppress_exception=False)
        assert params["override"] is False

    def test_overwrite_params_with_dag_run_conf_none(self, create_task_instance):
        ti = create_task_instance()
        dag_run = ti.dag_run
        ti.task.params = {"override": False}

        params = process_params(ti.task.dag, ti.task, dag_run.conf, suppress_exception=False)
        assert params["override"] is False

    def test_set_duration(self):
        task = EmptyOperator(task_id="op", email="test@test.test")
        ti = TI(task=task, dag_version_id=mock.MagicMock())
        ti.start_date = datetime.datetime(2018, 10, 1, 1)
        ti.end_date = datetime.datetime(2018, 10, 1, 2)
        ti.set_duration()
        assert ti.duration == 3600

    def test_set_duration_empty_dates(self):
        task = EmptyOperator(task_id="op", email="test@test.test")
        ti = TI(task=task, dag_version_id=mock.MagicMock())
        ti.set_duration()
        assert ti.duration is None

    def test_outlet_asset_extra(self, dag_maker: DagMaker, session: Session):
        from airflow.sdk.definitions.asset import Asset

        with dag_maker(schedule=None, serialized=True, session=session):

            @task(outlets=Asset("test_outlet_asset_extra_1"))
            def write1(*, outlet_events=None):
                if TYPE_CHECKING:
                    assert isinstance(outlet_events, dict)
                outlet_events[Asset("test_outlet_asset_extra_1")].extra = {
                    "foo": "bar",
                    "this": {"is": "nested", "value": 1},
                }

            write1()

            def _write2_post_execute(context, _):
                context["outlet_events"][Asset("test_outlet_asset_extra_2")].extra = {"x": 1}

            BashOperator(
                task_id="write2",
                bash_command=":",
                outlets=Asset("test_outlet_asset_extra_2"),
                post_execute=_write2_post_execute,
            )

        dr: DagRun = dag_maker.create_dagrun()
        for ti in dr.get_task_instances(session=session):
            ti.run(session=session)

        events = dict((tuple(row)) for row in session.execute(select(AssetEvent.source_task_id, AssetEvent)))
        assert set(events) == {"write1", "write2"}

        assert events["write1"].source_dag_id == dr.dag_id
        assert events["write1"].source_run_id == dr.run_id
        assert events["write1"].source_task_id == "write1"
        assert events["write1"].asset.uri == "test_outlet_asset_extra_1"
        assert events["write1"].extra == {"foo": "bar", "this": {"is": "nested", "value": 1}}

        assert events["write2"].source_dag_id == dr.dag_id
        assert events["write2"].source_run_id == dr.run_id
        assert events["write2"].source_task_id == "write2"
        assert events["write2"].asset.uri == "test_outlet_asset_extra_2"
        assert events["write2"].extra == {"x": 1}

    def test_outlet_asset_extra_ignore_different(self, dag_maker, session):
        from airflow.sdk.definitions.asset import Asset

        with dag_maker(schedule=None, serialized=True, session=session):

            @task(outlets=Asset("test_outlet_asset_extra"))
            def write(*, outlet_events):
                outlet_events[Asset("test_outlet_asset_extra")].extra = {"one": 1}
                outlet_events[Asset("different_uri")].extra = {"foo": "bar"}  # Will be silently dropped.

            write()

        dr: DagRun = dag_maker.create_dagrun()
        dr.get_task_instance("write").run(session=session)

        event = session.scalars(select(AssetEvent)).one()
        assert event.source_dag_id == dr.dag_id
        assert event.source_run_id == dr.run_id
        assert event.source_task_id == "write"
        assert event.extra == {"one": 1}

    def test_outlet_asset_alias(self, dag_maker, session):
        from airflow.sdk.definitions.asset import Asset

        asset_uri = "test_outlet_asset_alias_test_case_ds"
        alias_name_1 = "test_outlet_asset_alias_test_case_asset_alias_1"

        asm = AssetModel(id=1, uri=asset_uri)
        session.add_all([asm, AssetActive.for_asset(asm)])
        session.commit()

        with dag_maker(dag_id="producer_dag", schedule=None, serialized=True, session=session):

            @task(outlets=AssetAlias(alias_name_1))
            def producer(*, outlet_events):
                outlet_events[AssetAlias(alias_name_1)].add(Asset(asset_uri))

            producer()

        dr: DagRun = dag_maker.create_dagrun()

        for ti in dr.get_task_instances(session=session):
            ti.run(session=session)

        producer_events = session.execute(
            select(AssetEvent).where(AssetEvent.source_task_id == "producer")
        ).fetchall()

        assert len(producer_events) == 1

        producer_event = producer_events[0][0]
        assert producer_event.source_task_id == "producer"
        assert producer_event.source_dag_id == "producer_dag"
        assert producer_event.source_run_id == "test"
        assert producer_event.source_map_index == -1
        assert producer_event.asset.uri == asset_uri
        assert len(producer_event.source_aliases) == 1
        assert producer_event.extra == {}
        assert producer_event.source_aliases[0].name == alias_name_1

        asset_obj = session.scalar(select(AssetModel).where(AssetModel.uri == asset_uri))
        assert len(asset_obj.aliases) == 1
        assert asset_obj.aliases[0].name == alias_name_1

        asset_alias_obj = session.scalar(select(AssetAliasModel).where(AssetAliasModel.name == alias_name_1))
        assert len(asset_alias_obj.assets) == 1
        assert asset_alias_obj.assets[0].uri == asset_uri

    def test_outlet_multiple_asset_alias(self, dag_maker, session):
        from airflow.sdk.definitions.asset import Asset

        asset_uri = "test_outlet_maa_ds"
        asset_alias_name_1 = "test_outlet_maa_asset_alias_1"
        asset_alias_name_2 = "test_outlet_maa_asset_alias_2"
        asset_alias_name_3 = "test_outlet_maa_asset_alias_3"

        asm = AssetModel(id=1, uri=asset_uri)
        session.add_all([asm, AssetActive.for_asset(asm)])
        session.commit()

        with dag_maker(dag_id="producer_dag", schedule=None, serialized=True, session=session):

            @task(
                outlets=[
                    AssetAlias(asset_alias_name_1),
                    AssetAlias(asset_alias_name_2),
                    AssetAlias(asset_alias_name_3),
                ]
            )
            def producer(*, outlet_events):
                outlet_events[AssetAlias(asset_alias_name_1)].add(Asset(asset_uri))
                outlet_events[AssetAlias(asset_alias_name_2)].add(Asset(asset_uri))
                outlet_events[AssetAlias(asset_alias_name_3)].add(Asset(asset_uri), extra={"k": "v"})

            producer()

        dr: DagRun = dag_maker.create_dagrun()

        for ti in dr.get_task_instances(session=session):
            ti.run(session=session)

        producer_events = session.execute(
            select(AssetEvent).where(AssetEvent.source_task_id == "producer")
        ).fetchall()

        assert len(producer_events) == 2
        for row in producer_events:
            producer_event = row[0]
            assert producer_event.source_task_id == "producer"
            assert producer_event.source_dag_id == "producer_dag"
            assert producer_event.source_run_id == "test"
            assert producer_event.source_map_index == -1
            assert producer_event.asset.uri == asset_uri

            if not producer_event.extra:
                assert producer_event.extra == {}
                assert len(producer_event.source_aliases) == 2
                assert {alias.name for alias in producer_event.source_aliases} == {
                    asset_alias_name_1,
                    asset_alias_name_2,
                }
            else:
                assert producer_event.extra == {"k": "v"}
                assert len(producer_event.source_aliases) == 1
                assert producer_event.source_aliases[0].name == asset_alias_name_3

        asset_obj = session.scalar(select(AssetModel).where(AssetModel.uri == asset_uri))
        assert len(asset_obj.aliases) == 3
        assert {alias.name for alias in asset_obj.aliases} == {
            asset_alias_name_1,
            asset_alias_name_2,
            asset_alias_name_3,
        }

        asset_alias_objs = session.scalars(select(AssetAliasModel)).all()
        assert len(asset_alias_objs) == 3
        for asset_alias_obj in asset_alias_objs:
            assert len(asset_alias_obj.assets) == 1
            assert asset_alias_obj.assets[0].uri == asset_uri

    def test_outlet_asset_alias_through_metadata(self, dag_maker, session):
        asset_uri = "test_outlet_asset_alias_through_metadata_ds"
        asset_alias_name = "test_outlet_asset_alias_through_metadata_asset_alias"

        asm = AssetModel(id=1, uri="test_outlet_asset_alias_through_metadata_ds")
        session.add_all([asm, AssetActive.for_asset(asm)])
        session.commit()

        with dag_maker(dag_id="producer_dag", schedule=None, serialized=True, session=session):

            @task(outlets=AssetAlias(asset_alias_name))
            def producer(*, outlet_events):
                yield Metadata(Asset(asset_uri), extra={"key": "value"}, alias=AssetAlias(asset_alias_name))

            producer()

        dr: DagRun = dag_maker.create_dagrun()

        for ti in dr.get_task_instances(session=session):
            ti.run(session=session)

        producer_event = session.scalar(select(AssetEvent).where(AssetEvent.source_task_id == "producer"))

        assert producer_event.source_task_id == "producer"
        assert producer_event.source_dag_id == "producer_dag"
        assert producer_event.source_run_id == "test"
        assert producer_event.source_map_index == -1
        assert producer_event.asset.uri == asset_uri
        assert producer_event.extra == {"key": "value"}
        assert len(producer_event.source_aliases) == 1
        assert producer_event.source_aliases[0].name == asset_alias_name

        asset_obj = session.scalar(select(AssetModel).where(AssetModel.uri == asset_uri))
        assert len(asset_obj.aliases) == 1
        assert asset_obj.aliases[0].name == asset_alias_name

        asset_alias_obj = session.scalar(select(AssetAliasModel))
        assert len(asset_alias_obj.assets) == 1
        assert asset_alias_obj.assets[0].uri == asset_uri

    @pytest.mark.need_serialized_dag
    def test_outlet_asset_alias_asset_not_exists(self, dag_maker, session):
        asset_alias_name = "test_outlet_asset_alias_asset_not_exists_asset_alias"
        asset_uri = "does_not_exist"

        asset_model_chheck_stmt = select(AssetModel)
        asset_event_check_stmt = select(AssetEvent)

        assert session.scalar(asset_model_chheck_stmt) is None
        assert session.scalar(asset_event_check_stmt) is None

        with dag_maker(dag_id="producer_dag", schedule=None, session=session):

            @task(outlets=AssetAlias(asset_alias_name))
            def producer(*, outlet_events):
                outlet_events[AssetAlias(asset_alias_name)].add(Asset(asset_uri), extra={"key": "value"})

            producer()

        (ti,) = dag_maker.create_dagrun().get_task_instances(session=session)
        ti.run(session=session)

        asset_model = session.scalars(asset_model_chheck_stmt).one()
        assert asset_model.uri == asset_uri
        assert asset_model.active is None, "dynamically created asset should be inactive"
        assert session.scalars(asset_event_check_stmt).one().uri == asset_uri

    def test_outlet_asset_alias_asset_inactive(self, dag_maker, session):
        asset1 = SerializedAsset("asset1", "asset1", "", {}, [])
        asset2 = SerializedAsset("asset2", "asset2", "", {}, [])
        asm1 = AssetModel.from_serialized(asset1)
        asm2 = AssetModel.from_serialized(asset2)
        session.add_all([asm1, asm2, AssetActive.for_asset(asm1)])
        session.flush()

        asset_alias_name = "alias_with_inactive_asset"

        with dag_maker(dag_id="producer_dag", schedule=None, session=session):

            @task(outlets=AssetAlias(asset_alias_name))
            def producer_without_inactive(*, outlet_events):
                outlet_events[AssetAlias(asset_alias_name)].add(asset1, extra={"key": "value1"})

            @task(outlets=AssetAlias(asset_alias_name))
            def producer_with_inactive(*, outlet_events):
                outlet_events[AssetAlias(asset_alias_name)].add(asset2, extra={"key": "value2"})

            producer_without_inactive() >> producer_with_inactive()

        tis = {ti.task_id: ti for ti in dag_maker.create_dagrun().task_instances}
        tis["producer_without_inactive"].run(session=session)
        tis["producer_with_inactive"].run(session=session)

        producer_events = {
            e.source_task_id: e
            for e in session.scalars(select(AssetEvent).where(AssetEvent.source_dag_id == "producer_dag"))
        }
        assert set(producer_events) == {"producer_without_inactive", "producer_with_inactive"}

        assert producer_events["producer_without_inactive"].source_task_id == "producer_without_inactive"
        assert producer_events["producer_without_inactive"].source_dag_id == "producer_dag"
        assert producer_events["producer_without_inactive"].source_run_id == "test"
        assert producer_events["producer_without_inactive"].source_map_index == -1
        assert producer_events["producer_without_inactive"].asset.uri == "asset1"
        assert producer_events["producer_without_inactive"].extra == {"key": "value1"}
        assert len(producer_events["producer_without_inactive"].source_aliases) == 1
        assert producer_events["producer_without_inactive"].source_aliases[0].name == asset_alias_name

        assert producer_events["producer_with_inactive"].source_task_id == "producer_with_inactive"
        assert producer_events["producer_with_inactive"].source_dag_id == "producer_dag"
        assert producer_events["producer_with_inactive"].source_run_id == "test"
        assert producer_events["producer_with_inactive"].source_map_index == -1
        assert producer_events["producer_with_inactive"].asset.uri == "asset2"
        assert producer_events["producer_with_inactive"].extra == {"key": "value2"}
        assert len(producer_events["producer_with_inactive"].source_aliases) == 1
        assert producer_events["producer_with_inactive"].source_aliases[0].name == asset_alias_name

        asset_obj_1 = session.scalar(select(AssetModel).where(AssetModel.name == "asset1"))
        assert len(asset_obj_1.aliases) == 1
        assert asset_obj_1.aliases[0].name == asset_alias_name
        assert asset_obj_1.active is not None, "should stay active"

        asset_obj_2 = session.scalar(select(AssetModel).where(AssetModel.name == "asset2"))
        assert len(asset_obj_2.aliases) == 1
        assert asset_obj_2.aliases[0].name == asset_alias_name
        assert asset_obj_2.active is None, "should stay inactive"

        asset_alias_obj = session.scalar(select(AssetAliasModel))
        assert sorted(a.name for a in asset_alias_obj.assets) == ["asset1", "asset2"]

    def test_inlet_asset_extra(self, dag_maker, session, mock_supervisor_comms):
        from airflow.sdk.definitions.asset import Asset

        mock_supervisor_comms.send.return_value = AssetEventsResult(
            asset_events=[
                AssetEventResponse(
                    id=1,
                    created_dagruns=[],
                    timestamp=timezone.utcnow(),
                    extra={"from": f"write{i}"},
                    asset=AssetResponse(
                        name="test_inlet_asset_extra", uri="test_inlet_asset_extra", group="asset"
                    ),
                )
                for i in (1, 2, 3)
            ]
        )

        read_task_evaluated = False

        with dag_maker(schedule=None, serialized=True, session=session):

            @task(outlets=Asset("test_inlet_asset_extra"))
            def write(*, ti, outlet_events):
                outlet_events[Asset("test_inlet_asset_extra")].extra = {"from": ti.task_id}
                with pytest.raises(TypeError):
                    outlet_events["test_inlet_asset_extra"]

            @task(inlets=Asset("test_inlet_asset_extra"))
            def read(*, inlet_events):
                second_event = inlet_events[Asset("test_inlet_asset_extra")][1]
                assert second_event.asset.uri == "test_inlet_asset_extra"
                assert second_event.extra == {"from": "write2"}

                last_event = inlet_events[Asset("test_inlet_asset_extra")][-1]
                assert last_event.asset.uri == "test_inlet_asset_extra"
                assert last_event.extra == {"from": "write3"}

                with pytest.raises(KeyError):
                    inlet_events[Asset("does_not_exist")]
                with pytest.raises(IndexError):
                    inlet_events[Asset("test_inlet_asset_extra")][5]

                # TODO: Support slices.

                nonlocal read_task_evaluated
                read_task_evaluated = True

            [
                write.override(task_id="write1")(),
                write.override(task_id="write2")(),
                write.override(task_id="write3")(),
            ] >> read()

        dr: DagRun = dag_maker.create_dagrun()

        # Run "write1", "write2", and "write3" (in this order).
        decision = dr.task_instance_scheduling_decisions(session=session)
        for ti in sorted(decision.schedulable_tis, key=operator.attrgetter("task_id")):
            # TODO: TaskSDK #45549
            ti.task = dag_maker.dag.get_task(ti.task_id)
            ti.run(session=session)

        # Run "read".
        decision = dr.task_instance_scheduling_decisions(session=session)
        for ti in decision.schedulable_tis:
            # TODO: TaskSDK #45549
            ti.task = dag_maker.dag.get_task(ti.task_id)
            ti.run(session=session)

        # Should be done.
        assert not dr.task_instance_scheduling_decisions(session=session).schedulable_tis
        assert read_task_evaluated

    def test_inlet_unresolved_asset_alias(self, dag_maker, session, mock_supervisor_comms):
        asset_alias_name = "test_inlet_asset_extra_asset_alias"
        mock_supervisor_comms.send.return_value = AssetEventsResult(asset_events=[])

        asset_alias_model = AssetAliasModel(name=asset_alias_name)
        session.add(asset_alias_model)
        session.commit()

        with dag_maker(schedule=None, session=session):

            @task(inlets=AssetAlias(asset_alias_name))
            def read(*, inlet_events):
                with pytest.raises(IndexError):
                    inlet_events[AssetAlias(asset_alias_name)][0]

            read()

        dr: DagRun = dag_maker.create_dagrun()
        for ti in dr.get_task_instances(session=session):
            # TODO: TaskSDK #45549
            ti.task = dag_maker.dag.get_task(ti.task_id)
            ti.run(session=session)

        # Should be done.
        assert not dr.task_instance_scheduling_decisions(session=session).schedulable_tis

    def test_changing_of_asset_when_adrq_is_already_populated(self, dag_maker):
        """
        Test that when a task that produces asset has ran, that changing the consumer
        dag asset will not cause primary key blank-out
        """
        from airflow.sdk.definitions.asset import Asset

        with dag_maker(schedule=None, serialized=True) as dag1:

            @task(outlets=Asset("test/1"))
            def test_task1():
                print(1)

            test_task1()

        dr1 = dag_maker.create_dagrun()
        test_task1 = dag1.get_task("test_task1")

        with dag_maker(dag_id="testdag", schedule=[Asset("test/1")], serialized=True):

            @task
            def test_task2():
                print(1)

            test_task2()

        ti = dr1.get_task_instance(task_id="test_task1")
        ti.run()
        # Change the asset.
        with dag_maker(dag_id="testdag", schedule=[Asset("test2/1")], serialized=True):

            @task
            def test_task2():
                print(1)

            test_task2()

    @staticmethod
    def _test_previous_dates_setup(
        schedule: str | datetime.timedelta | None,
        catchup: bool,
        scenario: list[TaskInstanceState],
        dag_maker,
    ) -> list:
        dag_id = "test_previous_dates"
        with dag_maker(dag_id=dag_id, schedule=schedule, catchup=catchup, serialized=True):
            task = EmptyOperator(task_id="task")

        def get_test_ti(logical_date: pendulum.DateTime, state: str) -> TI:
            dr = dag_maker.create_dagrun(
                run_id=f"test__{logical_date.isoformat()}",
                run_type=DagRunType.SCHEDULED,
                state=state,
                logical_date=logical_date,
                start_date=pendulum.now("UTC"),
            )
            ti = dr.task_instances[0]
            ti.task = task
            ti.set_state(state=State.SUCCESS, session=dag_maker.session)
            return ti

        date = cast("pendulum.DateTime", pendulum.parse("2019-01-01T00:00:00+00:00"))

        ret = []

        for idx, state in enumerate(scenario):
            new_date = date.add(days=idx)
            ti = get_test_ti(new_date, state)
            ret.append(ti)

        return ret

    _prev_dates_param_list = [
        pytest.param("0 0 * * * ", True, id="cron/catchup"),
        pytest.param("0 0 * * *", False, id="cron/no-catchup"),
        pytest.param(None, True, id="no-sched/catchup"),
        pytest.param(None, False, id="no-sched/no-catchup"),
        pytest.param(datetime.timedelta(days=1), True, id="timedelta/catchup"),
        pytest.param(datetime.timedelta(days=1), False, id="timedelta/no-catchup"),
    ]

    @pytest.mark.parametrize(("schedule", "catchup"), _prev_dates_param_list)
    def test_previous_ti(self, schedule, catchup, dag_maker) -> None:
        scenario = [State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule, catchup, scenario, dag_maker)

        assert ti_list[0].get_previous_ti() is None

        assert ti_list[2].get_previous_ti().run_id == ti_list[1].run_id

        assert ti_list[2].get_previous_ti().run_id != ti_list[0].run_id

    @pytest.mark.parametrize(("schedule", "catchup"), _prev_dates_param_list)
    def test_previous_ti_success(self, schedule, catchup, dag_maker) -> None:
        scenario = [State.FAILED, State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule, catchup, scenario, dag_maker)

        assert ti_list[0].get_previous_ti(state=State.SUCCESS) is None
        assert ti_list[1].get_previous_ti(state=State.SUCCESS) is None

        assert ti_list[3].get_previous_ti(state=State.SUCCESS).run_id == ti_list[1].run_id

        assert ti_list[3].get_previous_ti(state=State.SUCCESS).run_id != ti_list[2].run_id

    def test_context_triggering_asset_events_none(self, session, create_task_instance):
        ti = create_task_instance()
        template_context = ti.get_template_context()

        assert ti in session
        session.expunge_all()

        assert template_context["triggering_asset_events"] == {}

    def test_context_triggering_asset_events(self, create_dummy_dag, session):
        ds1 = AssetModel(id=1, uri="one")
        ds2 = AssetModel(id=2, uri="two")
        session.add_all([ds1, ds2])
        session.commit()

        logical_date = timezone.utcnow()
        # it's easier to fake a manual run here
        dag, task1 = create_dummy_dag(
            dag_id="test_triggering_asset_events",
            schedule=None,
            start_date=DEFAULT_DATE,
            task_id="test_context",
            with_dagrun_type=DagRunType.MANUAL,
            session=session,
        )
        dr = dag.create_dagrun(
            run_id="test2",
            run_type=DagRunType.ASSET_TRIGGERED,
            logical_date=logical_date,
            state=DagRunState.RUNNING,
            session=session,
            data_interval=(logical_date, logical_date),
            run_after=logical_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        ds1_event = AssetEvent(asset_id=1)
        ds2_event_1 = AssetEvent(asset_id=2)
        ds2_event_2 = AssetEvent(asset_id=2)
        dr.consumed_asset_events.append(ds1_event)
        dr.consumed_asset_events.append(ds2_event_1)
        dr.consumed_asset_events.append(ds2_event_2)
        session.commit()

        ti = dr.get_task_instance(task1.task_id, session=session)
        ti.refresh_from_task(task1)

        # Check we run this in the same context as the actual task at runtime!
        assert ti in session
        session.expunge(ti)
        session.expunge(dr)

        template_context = ti.get_template_context()

        assert template_context["triggering_asset_events"] == {
            "one": [ds1_event],
            "two": [ds2_event_1, ds2_event_2],
        }

    def test_pendulum_template_dates(self, create_task_instance):
        ti = create_task_instance(
            dag_id="test_pendulum_template_dates",
            task_id="test_pendulum_template_dates_task",
            schedule="0 12 * * *",
            serialized=True,
        )

        template_context = ti.get_template_context()

        assert isinstance(template_context["data_interval_start"], pendulum.DateTime)
        assert isinstance(template_context["data_interval_end"], pendulum.DateTime)

    def test_template_render(self, create_task_instance, session):
        ti = create_task_instance(
            dag_id="test_template_render",
            task_id="test_template_render_task",
            schedule="0 12 * * *",
        )
        session.add(ti)
        session.commit()
        template_context = ti.get_template_context()
        result = ti.task.render_template("Task: {{ dag.dag_id }} -> {{ task.task_id }}", template_context)
        assert result == "Task: test_template_render -> test_template_render_task"

    @pytest.mark.parametrize(
        ("content", "expected_output"),
        [
            ('{{ conn.get("a_connection").host }}', "hostvalue"),
            ('{{ conn.get("a_connection", "unused_fallback").host }}', "hostvalue"),
            ('{{ conn.get("missing_connection", {"host": "fallback_host"}).host }}', "fallback_host"),
            ("{{ conn.a_connection.host }}", "hostvalue"),
            ("{{ conn.a_connection.login }}", "loginvalue"),
            ("{{ conn.a_connection.password }}", "passwordvalue"),
            ('{{ conn.a_connection.extra_dejson["extra__asana__workspace"] }}', "extra1"),
            ("{{ conn.a_connection.extra_dejson.extra__asana__workspace }}", "extra1"),
        ],
    )
    def test_template_with_connection(self, content, expected_output, create_task_instance, session):
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
        session.add(ti)
        session.commit()

        context = ti.get_template_context()
        result = ti.task.render_template(content, context)
        assert result == expected_output

    @pytest.mark.parametrize(
        ("content", "expected_output"),
        [
            ("{{ var.value.a_variable }}", "a test value"),
            ('{{ var.value.get("a_variable") }}', "a test value"),
            ('{{ var.value.get("a_variable", "unused_fallback") }}', "a test value"),
            ('{{ var.value.get("missing_variable", "fallback") }}', "fallback"),
        ],
    )
    def test_template_with_variable(self, content, expected_output, create_task_instance, session):
        """
        Test the availability of variables in templates
        """
        Variable.set("a_variable", "a test value")

        ti = create_task_instance()
        session.add(ti)
        session.commit()
        context = ti.get_template_context()
        result = ti.task.render_template(content, context)
        assert result == expected_output

    def test_template_with_variable_missing(self, create_task_instance, session):
        """
        Test the availability of variables in templates
        """
        ti = create_task_instance()
        session.add(ti)
        session.commit()
        context = ti.get_template_context()
        with pytest.raises(KeyError):
            ti.task.render_template('{{ var.value.get("missing_variable") }}', context)

    @pytest.mark.parametrize(
        ("content", "expected_output"),
        [
            ("{{ var.value.a_variable }}", '{\n  "a": {\n    "test": "value"\n  }\n}'),
            ('{{ var.json.a_variable["a"]["test"] }}', "value"),
            ('{{ var.json.get("a_variable")["a"]["test"] }}', "value"),
            ('{{ var.json.get("a_variable", {"a": {"test": "unused_fallback"}})["a"]["test"] }}', "value"),
            ('{{ var.json.get("missing_variable", {"a": {"test": "fallback"}})["a"]["test"] }}', "fallback"),
        ],
    )
    def test_template_with_json_variable(self, content, expected_output, create_task_instance, session):
        """
        Test the availability of variables in templates
        """
        Variable.set("a_variable", {"a": {"test": "value"}}, serialize_json=True)

        ti = create_task_instance()
        session.add(ti)
        session.commit()
        context = ti.get_template_context()
        result = ti.task.render_template(content, context)
        assert result == expected_output

    def test_template_with_json_variable_missing(self, create_task_instance, session):
        ti = create_task_instance()
        session.add(ti)
        session.commit()
        context = ti.get_template_context()
        with pytest.raises(KeyError):
            ti.task.render_template('{{ var.json.get("missing_variable") }}', context)

    @provide_session
    def test_handle_failure_calls_listener(self, dag_maker, session):
        class CustomOp(BaseOperator):
            def execute(self, context): ...

        start_date = timezone.datetime(2016, 6, 1)
        clear_db_runs()

        from airflow.listeners.listener import get_listener_manager

        listener_callback_on_error = mock.MagicMock()
        get_listener_manager().pm.hook.on_task_instance_failed = listener_callback_on_error

        with dag_maker(dag_id="test_handle_failure", start_date=start_date, schedule=None) as dag:
            task1 = CustomOp(
                task_id="test_handle_failure_on_failure",
            )

        dag_maker.create_dagrun(session=session, run_type=DagRunType.MANUAL, start_date=start_date)

        logical_date = timezone.utcnow()
        dr = dag.create_dagrun(
            run_id="test2",
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
            state=DagRunState.RUNNING,
            start_date=logical_date - datetime.timedelta(hours=1),
            session=session,
            data_interval=(logical_date, logical_date),
            run_after=logical_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        dr.set_state(DagRunState.FAILED)
        ti1 = dr.get_task_instance(task1.task_id, session=session)
        ti1.task = task1

        ti1.state = State.FAILED
        error_message = "test failure handling"
        ti1.handle_failure(error_message)

        # check that the listener callback was called, and that it can access the error
        listener_callback_on_error.assert_called_once()
        callback_args = listener_callback_on_error.call_args.kwargs
        assert "error" in callback_args
        assert callback_args["error"] == error_message

    def test_handle_failure_updates_queued_task_updates_state(self, dag_maker):
        session = settings.Session()
        with dag_maker():
            task = EmptyOperator(task_id="mytask", retries=1)
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id)
        ti.state = State.QUEUED
        session.merge(ti)
        session.flush()
        assert ti.state == State.QUEUED
        ti.handle_failure("test queued ti", test_mode=True)
        assert ti.state == State.UP_FOR_RETRY

    @patch.object(Stats, "incr")
    def test_handle_failure_no_task(self, Stats_incr, dag_maker):
        """
        When a task instance heartbeat timeout is detected for a DAG with a parse error,
        we need to be able to run handle_failure _without_ ti.task being set
        """
        session = settings.Session()
        with dag_maker():
            task = EmptyOperator(task_id="mytask", retries=1)
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session=session)
        ti.try_number += 1
        ti = session.merge(ti)
        ti.task = None
        ti.state = State.QUEUED
        session.flush()
        expected_stats_tags = {"dag_id": ti.dag_id, "task_id": ti.task_id}

        assert ti.task is None, "Check critical pre-condition"

        assert ti.state == State.QUEUED
        assert ti.try_number == 1

        ti.handle_failure("test queued ti", test_mode=False)
        assert ti.state == State.UP_FOR_RETRY
        # try_number remains at 1
        assert ti.try_number == 1

        Stats_incr.assert_any_call("ti_failures", tags=expected_stats_tags)
        Stats_incr.assert_any_call("operator_failures_EmptyOperator", tags=expected_stats_tags)
        Stats_incr.assert_any_call(
            "operator_failures", tags={**expected_stats_tags, "operator": "EmptyOperator"}
        )

    def test_handle_failure_task_undefined(self, create_task_instance):
        """
        When the loaded taskinstance does not use refresh_from_task, the task may be undefined.
        For example:
            the DAG file has been deleted before executing _execute_task_callbacks
        """
        ti = create_task_instance()
        del ti.task
        ti.handle_failure("test ti.task undefined")

    def test_handle_failure_fail_fast(self, dag_maker, session):
        start_date = timezone.datetime(2016, 6, 1)

        class CustomOp(BaseOperator):
            def execute(self, context): ...

        reg_states = [State.RUNNING, State.FAILED, State.QUEUED, State.SCHEDULED, State.DEFERRED]

        with dag_maker(
            dag_id="test_handle_failure_fail_fast",
            start_date=start_date,
            schedule=None,
            fail_fast=True,
        ):
            CustomOp(task_id="task1", trigger_rule="all_success")
            for i, _ in enumerate(reg_states):
                CustomOp(task_id=f"reg_Task{i}")
            CustomOp(task_id="fail_Task")

        logical_date = timezone.utcnow()
        dr = dag_maker.create_dagrun(
            run_id="test_ff",
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
            state=DagRunState.RUNNING,
            session=session,
            data_interval=(logical_date, logical_date),
            run_after=logical_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        dr.set_state(DagRunState.SUCCESS)

        tis = {ti.task_id: ti for ti in dr.task_instances}
        tis["task1"].state = State.SUCCESS
        for i, state in enumerate(reg_states):
            tis[f"reg_Task{i}"].state = state
        tis["fail_Task"].state = State.FAILED
        session.flush()

        tis["fail_Task"].handle_failure("test retry handling")
        assert {task_id: ti.state for task_id, ti in tis.items()} == {
            "task1": State.SUCCESS,
            "fail_Task": State.FAILED,
            "reg_Task0": State.FAILED,
            "reg_Task1": State.FAILED,
            "reg_Task2": State.SKIPPED,
            "reg_Task3": State.SKIPPED,
            "reg_Task4": State.SKIPPED,
        }

    def test_retries_on_other_exceptions(self, dag_maker):
        def fail():
            raise AirflowException("maybe this will pass?")

        with dag_maker(dag_id="test_retries_on_other_exceptions"):
            task = PythonOperator(
                task_id="test_raise_other_exception",
                python_callable=fail,
                retries=1,
            )
        ti = dag_maker.create_dagrun(logical_date=timezone.utcnow()).task_instances[0]
        ti.task = task
        with contextlib.suppress(AirflowException):
            ti.run()
        assert ti.state == State.UP_FOR_RETRY

    def _env_var_check_callback(self):
        assert os.environ["AIRFLOW_CTX_DAG_ID"] == "test_echo_env_variables"
        assert os.environ["AIRFLOW_CTX_TASK_ID"] == "hive_in_python_op"
        assert DEFAULT_DATE.isoformat() == os.environ["AIRFLOW_CTX_LOGICAL_DATE"]
        assert (
            DagRun.generate_run_id(
                run_type=DagRunType.MANUAL, logical_date=DEFAULT_DATE, run_after=DEFAULT_DATE
            )
            == os.environ["AIRFLOW_CTX_DAG_RUN_ID"]
        )

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
        hybrid_props = ["rendered_map_index", "task_display_name"]
        expected_values = {
            "task_id": "test_refresh_from_db_task",
            "dag_id": "test_refresh_from_db_dag",
            "run_id": "test",
            "map_index": -1,
            "start_date": run_date + datetime.timedelta(days=1),
            "end_date": run_date + datetime.timedelta(days=1, seconds=1, milliseconds=234),
            "duration": 1.234,
            "state": State.SUCCESS,
            "try_number": 1,
            "max_tries": 1,
            "hostname": "some_unique_hostname",
            "id": str(uuid6.uuid7()),
            "unixname": "some_unique_unixname",
            "pool": "some_fake_pool_id",
            "pool_slots": 25,
            "queue": "some_queue_id",
            "priority_weight": 123,
            "operator": "some_custom_operator",
            "custom_operator_name": "some_custom_operator",
            "queued_dttm": run_date + datetime.timedelta(hours=1),
            "scheduled_dttm": run_date + datetime.timedelta(hours=1),
            "rendered_map_index": None,
            "queued_by_job_id": 321,
            "pid": 123,
            "last_heartbeat_at": run_date + datetime.timedelta(hours=1, seconds=4),
            "executor": "some_executor",
            "executor_config": {"Some": {"extra": "information"}},
            "external_executor_id": "some_executor_id",
            "trigger_timeout": None,
            "trigger_id": None,
            "next_kwargs": None,
            "next_method": None,
            "updated_at": None,
            "task_display_name": "Test Refresh from DB Task",
            "dag_version_id": mock.ANY,
            "context_carrier": {},
            "span_status": SpanStatus.ENDED,
        }
        # Make sure we aren't missing any new value in our expected_values list.
        expected_keys = {f"task_instance.{key}" for key in expected_values}
        assert {str(c) for c in TI.__table__.columns} == expected_keys, (
            "Please add all non-foreign values of TaskInstance to this list. "
            "This prevents refresh_from_db() from missing a field."
        )

        ti = create_task_instance(
            task_id=expected_values["task_id"],
            task_display_name=expected_values["task_display_name"],
            dag_id=expected_values["dag_id"],
        )
        for key, expected_value in expected_values.items():
            if key not in hybrid_props:
                setattr(ti, key, expected_value)
        with create_session() as session:
            session.merge(ti)
            session.commit()

        mock_task = mock.MagicMock()
        mock_task.task_id = expected_values["task_id"]
        mock_task.dag_id = expected_values["dag_id"]

        ti = TI(task=mock_task, run_id="test", dag_version_id=ti.dag_version_id)
        ti.refresh_from_db()
        for key, expected_value in expected_values.items():
            assert hasattr(ti, key), f"Key {key} is missing in the TaskInstance."
            if key not in hybrid_props:
                assert getattr(ti, key) == expected_value, (
                    f"Key: {key} had different values. Make sure it loads it in the refresh refresh_from_db()"
                )

    def test_operator_field_with_serialization(self, create_task_instance):
        ti = create_task_instance()
        assert ti.task.task_type == "EmptyOperator"
        assert ti.task.operator_name == "EmptyOperator"

        # Verify that ti.operator field renders correctly "without" Serialization
        assert ti.operator == "EmptyOperator"

        serialized_op = SerializedBaseOperator.serialize_operator(ti.task)
        deserialized_op = SerializedBaseOperator.deserialize_operator(serialized_op)
        assert deserialized_op.task_type == "EmptyOperator"
        # Verify that ti.operator field renders correctly "with" Serialization
        ser_ti = TI(task=deserialized_op, run_id=None, dag_version_id=ti.dag_version_id)
        assert ser_ti.operator == "EmptyOperator"
        assert ser_ti.task.operator_name == "EmptyOperator"

    def test_clear_db_references(self, session, create_task_instance):
        tables = [RenderedTaskInstanceFields, XComModel]
        ti = create_task_instance()
        ti.note = "sample note"

        session.merge(ti)
        session.commit()
        for table in [RenderedTaskInstanceFields]:
            session.add(table(ti))
        XComModel.set(key="key", value="value", task_id=ti.task_id, dag_id=ti.dag_id, run_id=ti.run_id)
        session.commit()
        for table in tables:
            assert session.query(table).count() == 1

        ti_note = session.query(TaskInstanceNote).filter_by(ti_id=ti.id).one()
        assert ti_note.content == "sample note"

        ti.clear_db_references(session)
        for table in tables:
            assert session.query(table).count() == 0

        assert session.query(TaskInstanceNote).filter_by(ti_id=ti.id).one_or_none() is None

    def test_skipped_task_call_on_skipped_callback(self, dag_maker):
        def raise_skip_exception():
            raise AirflowSkipException

        on_skipped_callback_function = mock.Mock(
            __call__=mock.MagicMock(),
            __name__="on_skipped_callback_function",
        )

        on_success_callback_function = mock.Mock(
            __call__=mock.MagicMock(),
            __name__="on_success_callback_function",
        )

        with dag_maker(dag_id="test_skipped_task", serialized=True):
            task = PythonOperator(
                task_id="test_skipped_task",
                python_callable=raise_skip_exception,
                on_skipped_callback=on_skipped_callback_function,
                on_success_callback=on_success_callback_function,
            )
        dr = dag_maker.create_dagrun(logical_date=timezone.utcnow())
        ti = dr.task_instances[0]
        ti.task = task
        ti.run()
        assert ti.state == State.SKIPPED
        on_skipped_callback_function.assert_called_once()
        on_success_callback_function.assert_not_called()

    def test_task_instance_history_is_created_when_ti_goes_for_retry(self, dag_maker, session):
        with dag_maker(serialized=True):
            task = BashOperator(
                task_id="test_history_tab",
                bash_command="ech",
                retries=1,
                retry_delay=datetime.timedelta(seconds=2),
            )

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = task
        try_id = ti.id
        with pytest.raises(AirflowException):
            ti.run()
        ti = session.query(TaskInstance).one()
        # the ti.id should be different from the previous one
        assert ti.id != try_id
        assert ti.state == State.UP_FOR_RETRY
        assert session.query(TaskInstance).count() == 1
        tih = session.query(TaskInstanceHistory).all()
        assert len(tih) == 1
        # the new try_id should be different from what's recorded in tih
        assert str(tih[0].task_instance_id) == try_id

    def test_task_instance_history_with_hitl_history_is_created_when_ti_goes_for_retry(
        self, dag_maker: DagMaker, session: Session
    ):
        with dag_maker(serialized=True):
            task = HITLBranchOperator(
                task_id="hitl_test",
                subject="This is subject",
                body="This is body",
                options=["1", "2", "3", "4", "5"],
                params={"input": 1},
                retries=1,
                retry_delay=datetime.timedelta(seconds=2),
                notifiers=[None],
            )

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.task = task
        try_id = ti.id
        with pytest.raises(TypeError):
            ti.run()
        ti = session.query(TaskInstance).one()
        # the ti.id should be different from the previous one
        assert ti.id != try_id
        assert ti.state == State.UP_FOR_RETRY
        assert session.query(TaskInstance).count() == 1
        tih = session.query(TaskInstanceHistory).all()
        assert len(tih) == 1
        # the new try_id should be different from what's recorded in tih
        assert str(tih[0].task_instance_id) == try_id
        hitl_histories = session.query(HITLDetailHistory).all()
        assert len(hitl_histories) == 1
        assert str(hitl_histories[0].task_instance.id) == try_id


@pytest.mark.parametrize("pool_override", [None, "test_pool2"])
@pytest.mark.parametrize("queue_by_policy", [None, "forced_queue"])
def test_refresh_from_task(pool_override, queue_by_policy, monkeypatch):
    default_queue = "test_queue"
    expected_queue = queue_by_policy or default_queue
    if queue_by_policy:
        # Apply a dummy cluster policy to check if it is always applied
        def mock_policy(task_instance: TaskInstance):
            task_instance.queue = queue_by_policy

        monkeypatch.setattr("airflow.models.taskinstance.task_instance_mutation_hook", mock_policy)

    task = EmptyOperator(
        task_id="empty",
        queue=default_queue,
        pool="test_pool1",
        pool_slots=3,
        priority_weight=10,
        run_as_user="test",
        retries=30,
        executor_config={"KubernetesExecutor": {"image": "myCustomDockerImage"}},
    )
    ti = TI(task, run_id=None, dag_version_id=mock.MagicMock())
    ti.refresh_from_task(task, pool_override=pool_override)

    assert ti.queue == expected_queue

    if pool_override:
        assert ti.pool == pool_override
    else:
        assert ti.pool == task.pool

    assert ti.pool_slots == task.pool_slots
    assert ti.priority_weight == task.weight_rule.get_weight(ti)
    assert ti.run_as_user == task.run_as_user
    assert ti.max_tries == task.retries
    assert ti.executor_config == task.executor_config
    assert ti.operator == EmptyOperator.__name__

    # Test that refresh_from_task does not reset ti.max_tries
    expected_max_tries = task.retries + 10
    ti.max_tries = expected_max_tries
    ti.refresh_from_task(task)
    assert ti.max_tries == expected_max_tries


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

    @pytest.mark.parametrize("xcom_1", [[1, 2, 3], {"a": 1, "b": 2}, "abc"])
    @pytest.mark.parametrize("xcom_4", [[1, 2, 3], {"a": 1, "b": 2}])
    def test_not_recorded_if_irrelevant(self, dag_maker, xcom_1, xcom_4):
        """Return value should only be recorded if a mapped downstream uses the it."""
        with dag_maker(dag_id="test_not_recorded_for_unused") as dag:

            @dag.task()
            def push_1():
                return xcom_1

            @dag.task()
            def push_2():
                return [-1, -2]

            @dag.task()
            def push_3():
                return ["x", "y"]

            @dag.task()
            def push_4():
                return xcom_4

            @dag.task()
            def show(arg1, arg2):
                print(arg1, arg2)

            @task_group()
            def tg(arg):
                show(arg1=task_3, arg2=arg)

            task_3 = push_3()
            show.partial(arg1=push_1()).expand(arg2=push_2())
            tg.expand(arg=push_4())

        tis = {ti.task_id: ti for ti in dag_maker.create_dagrun().task_instances}

        tis["push_1"].run()
        assert dag_maker.session.query(TaskMap).count() == 0

        tis["push_2"].run()
        assert dag_maker.session.query(TaskMap).count() == 1

        tis["push_3"].run()
        assert dag_maker.session.query(TaskMap).count() == 1

        tis["push_4"].run()
        assert dag_maker.session.query(TaskMap).count() == 2


class TestMappedTaskInstanceReceiveValue:
    @pytest.mark.parametrize(
        ("literal", "expected_outputs"),
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

            show_task = show.expand(value=literal).operator

        dag_run = dag_maker.create_dagrun()
        mapped_tis = (
            session.query(TI)
            .filter_by(task_id="show", dag_id=dag_run.dag_id, run_id=dag_run.run_id)
            .order_by(TI.map_index)
            .all()
        )
        assert len(mapped_tis) == len(literal)

        for ti in sorted(mapped_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(show_task)
            ti.run()
        assert outputs == expected_outputs

    @pytest.mark.parametrize(
        ("upstream_return", "expected_outputs"),
        [
            pytest.param([1, 2, 3], [1, 2, 3], id="list"),
            pytest.param({"a": 1, "b": 2}, [("a", 1), ("b", 2)], id="dict"),
        ],
    )
    def test_map_xcom(self, upstream_return, expected_outputs, dag_maker, session):
        outputs = []

        with dag_maker(dag_id="xcom", session=session, serialized=True) as dag:

            @dag.task
            def emit():
                return upstream_return

            @dag.task
            def show(value):
                outputs.append(value)

            show.expand(value=emit())

        dag_run = dag_maker.create_dagrun()
        emit_ti = dag_run.get_task_instance("emit", session=session)
        emit_ti.refresh_from_task(dag_maker.dag.get_task("emit"))
        dag_maker.run_ti(emit_ti.task_id, dag_run=dag_run, session=session)

        show_task = dag.get_task("show")
        mapped_tis, max_map_index = TaskMap.expand_mapped_task(
            dag.task_dict[show_task.task_id], dag_run.run_id, session=session
        )
        assert max_map_index + 1 == len(mapped_tis) == len(upstream_return)

        for ti in sorted(mapped_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(show_task)
            dag_maker.run_ti(ti.task_id, dag_run=dag_run, map_index=ti.map_index, session=session)
        assert outputs == expected_outputs

    def test_map_literal_cross_product(self, dag_maker, session):
        """Test a mapped task with literal cross product args expand properly."""
        outputs = []

        with dag_maker(dag_id="product_same_types", session=session, serialized=True) as dag:

            @dag.task
            def show(a, b):
                outputs.append((a, b))

            show.expand(a=[2, 4, 8], b=[5, 10])

        dag_run = dag_maker.create_dagrun()

        show_task = dag.get_task("show")
        assert show_task.get_parse_time_mapped_ti_count() == 6
        mapped_tis, max_map_index = TaskMap.expand_mapped_task(show_task, dag_run.run_id, session=session)
        assert len(mapped_tis) == 0  # Expanded at parse!
        assert max_map_index == 5

        tis = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag.dag_id,
                TaskInstance.task_id == "show",
                TaskInstance.run_id == dag_run.run_id,
            )
            .order_by(TaskInstance.map_index)
            .all()
        )
        for ti in tis:
            ti.refresh_from_task(show_task)
            dag_maker.run_ti(ti.task_id, map_index=ti.map_index, dag_run=dag_run, session=session)
        assert outputs == [(2, 5), (2, 10), (4, 5), (4, 10), (8, 5), (8, 10)]

    def test_map_in_group(self, tmp_path: pathlib.Path, dag_maker, session):
        out = tmp_path.joinpath("out")
        out.touch()

        with dag_maker(dag_id="in_group", session=session, serialized=True) as dag:

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
            dag_maker.run_ti(ti.task_id, map_index=ti.map_index, dag_run=dag_run, session=session)

        bash_task = dag.get_task("dynamic.bash")
        mapped_bash_tis, max_map_index = TaskMap.expand_mapped_task(
            bash_task, dag_run.run_id, session=session
        )
        assert max_map_index == 3  # 2 * 2 mapped tasks.
        for ti in sorted(mapped_bash_tis, key=operator.attrgetter("map_index")):
            ti.refresh_from_task(bash_task)
            dag_maker.run_ti(ti.task_id, map_index=ti.map_index, dag_run=dag_run, session=session)

        with out.open() as f:
            out_lines = [line.strip() for line in f]
        assert out_lines == ["hello FOO", "goodbye FOO", "hello BAR", "goodbye BAR"]


def _get_lazy_xcom_access_expected_sql_lines() -> list[str]:
    backend = os.environ.get("BACKEND")
    if backend == "mysql":
        return [
            "SELECT xcom.value",
            "FROM xcom",
            "WHERE xcom.dag_id = 'test_dag' AND xcom.run_id = 'test' "
            "AND xcom.task_id = 't' AND xcom.map_index = -1 AND xcom.`key` = 'xxx'",
        ]
    if backend == "postgres":
        return [
            "SELECT xcom.value",
            "FROM xcom",
            "WHERE xcom.dag_id = 'test_dag' AND xcom.run_id = 'test' "
            "AND xcom.task_id = 't' AND xcom.map_index = -1 AND xcom.key = 'xxx'",
        ]
    if backend == "sqlite":
        return [
            "SELECT xcom.value",
            "FROM xcom",
            "WHERE xcom.dag_id = 'test_dag' AND xcom.run_id = 'test' "
            "AND xcom.task_id = 't' AND xcom.map_index = -1 AND xcom.\"key\" = 'xxx'",
        ]
    raise RuntimeError(f"unknown backend {backend!r}")


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


def test_taskinstance_with_note(create_task_instance, session):
    ti: TaskInstance = create_task_instance(session=session)
    ti.note = "ti with note"

    session.add(ti)
    session.commit()

    ti_note: TaskInstanceNote = session.query(TaskInstanceNote).filter_by(ti_id=ti.id).one()
    assert ti_note.content == "ti with note"

    session.delete(ti)
    session.commit()

    assert session.query(TaskInstance).filter_by(id=ti.id).one_or_none() is None
    assert session.query(TaskInstanceNote).filter_by(ti_id=ti.id).one_or_none() is None


def test__refresh_from_db_should_not_increment_try_number(dag_maker, session):
    with dag_maker():
        BashOperator(task_id="hello", bash_command="hi")
    dag_maker.create_dagrun(state="success")
    ti = session.scalar(select(TaskInstance))
    session.get(TaskInstance, ti.id).try_number += 1
    session.commit()
    assert ti.task_id == "hello"  # just to confirm...
    assert ti.try_number == 1  # starts out as 1
    ti.refresh_from_db()
    assert ti.try_number == 1  # stays 1


def test_delete_dagversion_restricted_when_taskinstance_exists(dag_maker, session):
    """
    Ensure that deleting a DagVersion with existing TaskInstance references is restricted (ON DELETE RESTRICT).
    """

    with dag_maker(dag_id="test_dag_restrict", session=session) as dag:
        EmptyOperator(task_id="task1")
    dag_maker.create_dagrun(session=session)

    version = session.scalar(select(DagVersion).where(DagVersion.dag_id == dag.dag_id))
    assert version is not None

    ti = session.scalars(select(TaskInstance).where(TaskInstance.dag_version_id == version.id)).first()
    assert ti is not None

    session.delete(version)
    with pytest.raises(IntegrityError):
        session.commit()


@pytest.mark.parametrize(
    ("normal_tasks", "mapped_tasks", "expected"),
    [
        # 4 is just a regular task so it depends on all its upstreams.
        pytest.param(["4"], [], {"1", "2", "3"}, id="nonmapped"),
        # 3 is a mapped; it depends on all tis of the mapped upstream 2.
        pytest.param(["3"], [], {"1", "2"}, id="mapped-whole"),
        # Every ti of a mapped task depends on all tis of the mapped upstream.
        pytest.param([], [("3", 1)], {"1", "2"}, id="mapped-one"),
        # Same as the (non-group) unmapped case, d depends on all upstreams.
        pytest.param(["d"], [], {"a", "b", "c"}, id="group-nonmapped"),
        # This specifies c tis in ALL mapped task groups, so all b tis are needed.
        pytest.param(["c"], [], {"a", "b"}, id="group-mapped-whole"),
        # This only specifies one c ti, so only one b ti from the same mapped instance is returned.
        pytest.param([], [("c", 1)], {"a", ("b", 1)}, id="group-mapped-one"),
    ],
)
def test_find_relevant_relatives(dag_maker, session, normal_tasks, mapped_tasks, expected):
    # 1 -> 2[] -> 3[] -> 4
    #
    # a -> " b --> c " -> d
    #      "== g[] =="
    with dag_maker(session=session) as dag:
        t1 = EmptyOperator(task_id="1")
        t2 = MockOperator.partial(task_id="2").expand(arg1=["x", "y"])
        t3 = MockOperator.partial(task_id="3").expand(arg1=["x", "y"])
        t4 = EmptyOperator(task_id="4")
        t1 >> t2 >> t3 >> t4

        ta = EmptyOperator(task_id="a")

        @task_group(prefix_group_id=False)
        def g(v):
            tb = MockOperator(task_id="b", arg1=v)
            tc = MockOperator(task_id="c", arg1=v)
            tb >> tc

        td = EmptyOperator(task_id="d")
        ta >> g.expand(v=["x", "y", "z"]) >> td

    dr = dag_maker.create_dagrun(state="success")

    result = find_relevant_relatives(
        normal_tasks=normal_tasks,
        mapped_tasks=mapped_tasks,
        direction="upstream",
        dag=dag,
        run_id=dr.run_id,
        session=session,
    )
    assert result == expected


def test_find_relevant_relatives_with_non_mapped_task_as_tuple(dag_maker, session):
    """Test that specifying a non-mapped task as a tuple doesn't raise NotMapped exception."""
    # t1 -> t2 (non-mapped) -> t3
    with dag_maker(session=session) as dag:
        t1 = EmptyOperator(task_id="t1")
        t2 = EmptyOperator(task_id="t2")
        t3 = EmptyOperator(task_id="t3")
        t1 >> t2 >> t3

    dr = dag_maker.create_dagrun(state="success")

    # Specifying t2 as a tuple (t2, 0) even though it's not mapped should not raise NotMapped
    # It should treat t2 as a normal task and return its upstream t1
    result = find_relevant_relatives(
        normal_tasks=[],
        mapped_tasks=[("t2", 0)],
        direction="upstream",
        dag=dag,
        run_id=dr.run_id,
        session=session,
    )
    # Should return t1 as the upstream of t2
    assert result == {"t1"}


def test_when_dag_run_has_partition_then_asset_does(dag_maker, session):
    asset = Asset(name="hello")
    with dag_maker(dag_id="asset_event_tester", schedule=None) as dag:
        EmptyOperator(task_id="hi", outlets=[asset])
    dr = dag_maker.create_dagrun(partition_key="abc123", session=session)
    assert dr.partition_key == "abc123"
    [ti] = dr.get_task_instances(session=session)

    TaskInstance.register_asset_changes_in_db(
        ti=ti,
        task_outlets=[ensure_serialized_asset(asset).asprofile()],
        outlet_events=[],
        session=session,
    )
    actual_event = session.scalar(select(AssetEvent).where(AssetEvent.source_dag_id == dag.dag_id))
    assert actual_event.partition_key == "abc123"
    assert not session.scalar(select(PartitionedAssetKeyLog))
    assert not session.scalar(select(AssetPartitionDagRun))


def test_when_dag_run_has_partition_and_downstreams_listening_then_tables_populated(
    dag_maker,
    session,
):
    asset = Asset(name="hello")
    with dag_maker(dag_id="asset_event_tester", schedule=None, session=session) as dag:
        EmptyOperator(task_id="hi", outlets=[asset])
    dag1_id = dag.dag_id
    dr = dag_maker.create_dagrun(partition_key="abc123", session=session)
    assert dr.partition_key == "abc123"
    [ti] = dr.get_task_instances(session=session)
    session.commit()

    with dag_maker(
        dag_id="asset_event_listener",
        schedule=PartitionedAssetTimetable(assets=asset, partition_mapper=IdentityMapper()),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    TaskInstance.register_asset_changes_in_db(
        ti=ti,
        task_outlets=[ensure_serialized_asset(asset).asprofile()],
        outlet_events=[],
        session=session,
    )
    session.commit()
    event = session.scalar(select(AssetEvent).where(AssetEvent.source_dag_id == dag1_id))
    assert event.partition_key == "abc123"
    pakl = session.scalar(select(PartitionedAssetKeyLog))
    apdr = session.scalar(select(AssetPartitionDagRun))
    assert pakl.asset_event_id == event.id
    assert pakl.asset_partition_dag_run_id == apdr.id
    assert pakl.source_partition_key == "abc123"
    assert pakl.target_dag_id == "asset_event_listener"
