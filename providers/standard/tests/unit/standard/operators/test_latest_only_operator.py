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

import pytest
import time_machine
from sqlalchemy import select

from airflow import settings
from airflow.models import DagRun, TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]
from airflow.timetables.base import DataInterval
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_runs, clear_db_xcom
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_1, AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG
    from airflow.timetables.trigger import DeltaTriggerTimetable
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)


def get_task_instances(task_id):
    session = settings.Session()
    logical_date = DagRun.logical_date if AIRFLOW_V_3_0_PLUS else DagRun.execution_date
    return session.scalars(
        select(TaskInstance)
        .join(TaskInstance.dag_run)
        .where(TaskInstance.task_id == task_id)
        .order_by(logical_date)
    ).all()


class TestLatestOnlyOperator:
    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_xcom()

    def setup_class(self):
        self.clean_db()

    def setup_method(self):
        self.freezer = time_machine.travel(FROZEN_NOW, tick=False)
        self.freezer.start()

    def teardown_method(self):
        self.freezer.stop()
        self.clean_db()

    def test_run(self, dag_maker):
        with dag_maker(
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, schedule=INTERVAL, serialized=True
        ):
            LatestOnlyOperator(task_id="latest")
        dr = dag_maker.create_dagrun()
        dag_maker.run_ti("latest", dr)

    def test_skipping_non_latest(self, dag_maker):
        with dag_maker(
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, schedule=INTERVAL, serialized=True
        ):
            latest_task = LatestOnlyOperator(task_id="latest")
            downstream_task = EmptyOperator(task_id="downstream")
            downstream_task2 = EmptyOperator(task_id="downstream_2")
            downstream_task3 = EmptyOperator(task_id="downstream_3", trigger_rule=TriggerRule.NONE_FAILED)

            downstream_task.set_upstream(latest_task)
            downstream_task2.set_upstream(downstream_task)
            downstream_task3.set_upstream(downstream_task)

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr0 = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            start_date=timezone.utcnow(),
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        dr1 = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            start_date=timezone.utcnow(),
            logical_date=timezone.datetime(2016, 1, 1, 12),
            state=State.RUNNING,
            data_interval=DataInterval(timezone.datetime(2016, 1, 1, 12), timezone.datetime(2016, 1, 1, 12)),
            **triggered_by_kwargs,
        )

        dr2 = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            start_date=timezone.utcnow(),
            logical_date=END_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(END_DATE + INTERVAL, END_DATE + INTERVAL),
            **triggered_by_kwargs,
        )

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            # AIP-72
            # Running the "latest" task for each of the DAG runs to test the skipping of downstream tasks
            # via the DownstreamTasksSkipped exception call.
            # The DownstreamTasksSkipped exception is raised when the task completes successfully so
            # we can simulate the downstream tasks being skipped by setting their state to SKIPPED
            # and the task that raised the exception to SUCCESS.
            latest_ti0 = dr0.get_task_instance(task_id="latest")

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                latest_ti0.run()

            assert exc_info.value.tasks == [("downstream", -1)]
            # TODO: Set state is needed until #45549 is completed.
            latest_ti0.set_state(State.SUCCESS)
            dr0.get_task_instance(task_id="downstream").set_state(State.SKIPPED)

            # ---
            latest_ti1 = dr1.get_task_instance(task_id="latest")
            latest_ti1.task = latest_task

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                latest_ti1.run()

            assert exc_info.value.tasks == [("downstream", -1)]
            # TODO: Set state is needed until #45549 is completed.
            latest_ti1.set_state(State.SUCCESS)
            dr1.get_task_instance(task_id="downstream").set_state(State.SKIPPED)

            # The last DAG run should run all tasks and latest task should be successful
            # and not raise DownstreamTasksSkipped exception
            latest_ti2 = dr2.get_task_instance(task_id="latest")
            latest_ti2.task = latest_task
            latest_ti2.run()
        else:
            for dr in [dr0, dr1, dr2]:
                dag_maker.run_ti("latest", dr)

        if AIRFLOW_V_3_0_PLUS:
            date_getter = operator.attrgetter("logical_date")
        else:
            date_getter = operator.attrgetter("execution_date")

        latest_instances = get_task_instances("latest")
        exec_date_to_latest_state = {date_getter(ti): ti.state for ti in latest_instances}
        assert exec_date_to_latest_state == {
            timezone.datetime(2016, 1, 1): "success",
            timezone.datetime(2016, 1, 1, 12): "success",
            timezone.datetime(2016, 1, 2): "success",
        }

        # Verify the state of the other downstream tasks
        for dr in [dr0, dr1, dr2]:
            dag_maker.run_ti("downstream", dr)
            dag_maker.run_ti("downstream_2", dr)
            dag_maker.run_ti("downstream_3", dr)

        downstream_instances = get_task_instances("downstream")
        exec_date_to_downstream_state = {date_getter(ti): ti.state for ti in downstream_instances}
        assert exec_date_to_downstream_state == {
            timezone.datetime(2016, 1, 1): "skipped",
            timezone.datetime(2016, 1, 1, 12): "skipped",
            timezone.datetime(2016, 1, 2): "success",
        }

        downstream_instances = get_task_instances("downstream_2")
        exec_date_to_downstream_state = {date_getter(ti): ti.state for ti in downstream_instances}
        assert exec_date_to_downstream_state == {
            timezone.datetime(2016, 1, 1): None,
            timezone.datetime(2016, 1, 1, 12): None,
            timezone.datetime(2016, 1, 2): "success",
        }

        downstream_instances = get_task_instances("downstream_3")
        exec_date_to_downstream_state = {date_getter(ti): ti.state for ti in downstream_instances}
        assert exec_date_to_downstream_state == {
            timezone.datetime(2016, 1, 1): "success",
            timezone.datetime(2016, 1, 1, 12): "success",
            timezone.datetime(2016, 1, 2): "success",
        }

    def test_not_skipping_manual(self, dag_maker):
        with dag_maker(
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE}, schedule=INTERVAL, serialized=True
        ):
            latest_task = LatestOnlyOperator(task_id="latest")
            downstream_task = EmptyOperator(task_id="downstream")
            downstream_task2 = EmptyOperator(task_id="downstream_2")

            downstream_task.set_upstream(latest_task)
            downstream_task2.set_upstream(downstream_task)

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        logical_date = timezone.datetime(2016, 1, 1, 12)
        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            logical_date=logical_date,
            state=State.RUNNING,
            data_interval=DataInterval(logical_date, logical_date),
            **triggered_by_kwargs,
        )

        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            logical_date=END_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(END_DATE, END_DATE),
            **triggered_by_kwargs,
        )

        # Get all created dag runs and run tasks for each
        all_drs = dag_maker.session.scalars(select(DagRun).where(DagRun.dag_id == dag_maker.dag.dag_id)).all()
        for dr in all_drs:
            dag_maker.run_ti("latest", dr)
            dag_maker.run_ti("downstream", dr)
            dag_maker.run_ti("downstream_2", dr)

        latest_instances = get_task_instances("latest")
        if AIRFLOW_V_3_0_PLUS:
            exec_date_to_latest_state = {ti.logical_date: ti.state for ti in latest_instances}
        else:
            exec_date_to_latest_state = {ti.execution_date: ti.state for ti in latest_instances}
        assert exec_date_to_latest_state == {
            timezone.datetime(2016, 1, 1): "success",
            timezone.datetime(2016, 1, 1, 12): "success",
            timezone.datetime(2016, 1, 2): "success",
        }

        downstream_instances = get_task_instances("downstream")
        if AIRFLOW_V_3_0_PLUS:
            exec_date_to_downstream_state = {ti.logical_date: ti.state for ti in downstream_instances}
        else:
            exec_date_to_downstream_state = {ti.execution_date: ti.state for ti in downstream_instances}
        assert exec_date_to_downstream_state == {
            timezone.datetime(2016, 1, 1): "success",
            timezone.datetime(2016, 1, 1, 12): "success",
            timezone.datetime(2016, 1, 2): "success",
        }

        downstream_instances = get_task_instances("downstream_2")
        if AIRFLOW_V_3_0_PLUS:
            exec_date_to_downstream_state = {ti.logical_date: ti.state for ti in downstream_instances}
        else:
            exec_date_to_downstream_state = {ti.execution_date: ti.state for ti in downstream_instances}
        assert exec_date_to_downstream_state == {
            timezone.datetime(2016, 1, 1): "success",
            timezone.datetime(2016, 1, 1, 12): "success",
            timezone.datetime(2016, 1, 2): "success",
        }

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Only applicable to Airflow 3.0+")
    def test_zero_length_interval_treated_as_latest(self, run_task):
        """Test that when the data_interval_start and data_interval_end are the same, the task is treated as latest."""
        with DAG(
            "test_dag",
            schedule=DeltaTriggerTimetable(datetime.timedelta(hours=1)),
            start_date=DEFAULT_DATE,
            catchup=False,
        ):
            latest_task = LatestOnlyOperator(task_id="latest")
            downstream_task = EmptyOperator(task_id="downstream")
            latest_task >> downstream_task

        run_task(latest_task, run_type=DagRunType.SCHEDULED)

        assert run_task.dagrun.data_interval_start == run_task.dagrun.data_interval_end

        # The task will raise DownstreamTasksSkipped exception if it is not the latest run
        assert run_task.state == State.SUCCESS

    def test_regular_latest_only_run(self, dag_maker):
        """Test latest_only running in normal mode."""
        with dag_maker(
            "test_dag",
            start_date=DEFAULT_DATE,
            schedule="* * * * *",
            catchup=False,
        ):
            latest_task = LatestOnlyOperator(task_id="latest")
            downstream_task = EmptyOperator(task_id="downstream")
            latest_task >> downstream_task

        dr = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
        )

        dag_maker.run_ti("latest", dr)
