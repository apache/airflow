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
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy import select

from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

pytestmark = pytest.mark.db_test

if AIRFLOW_V_3_0_PLUS:
    from airflow.models.dag_version import DagVersion
    from airflow.providers.common.compat.sdk import DownstreamTasksSkipped
    from airflow.providers.standard.utils.skipmixin import SkipMixin
    from airflow.sdk import task, task_group
else:
    from airflow.decorators import task, task_group  # type: ignore[attr-defined,no-redef]
    from airflow.models.skipmixin import SkipMixin

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
DEFAULT_DAG_RUN_ID = "test1"


class TestSkipMixin:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()

    def setup_method(self):
        self.clean_db()

    def teardown_method(self):
        self.clean_db()

    def test_skip(self, dag_maker, session, time_machine):
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        time_machine.move_to(now, tick=False)
        with dag_maker("dag"):
            tasks = [EmptyOperator(task_id="task")]

        if AIRFLOW_V_3_0_PLUS:
            dag_run = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                logical_date=now,
                state=State.FAILED,
            )

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                SkipMixin().skip(ti=dag_run.get_task_instance("task"), tasks=tasks)

            assert exc_info.value.tasks == ["task"]
        else:
            from airflow import settings

            dag_run = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                execution_date=now,
                state=State.FAILED,
            )
            session = settings.Session()
            SkipMixin().skip(dag_run=dag_run, execution_date=now, tasks=tasks)

            session.scalar(
                select(TI).where(
                    TI.dag_id == "dag",
                    TI.task_id == "task",
                    TI.state == State.SKIPPED,
                    TI.start_date == now,
                    TI.end_date == now,
                )
            )

    def test_skip_none_tasks(self):
        if AIRFLOW_V_3_0_PLUS:
            assert SkipMixin().skip(ti=Mock(), tasks=[]) is None
        else:
            session = Mock()
            assert SkipMixin().skip(dag_run=None, execution_date=None, tasks=[]) is None
            assert not session.query.called
            assert not session.commit.called

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 2 had a different implementation")
    def test_skip__only_mapped_operators_passed(self):
        from airflow.sdk.definitions.mappedoperator import MappedOperator

        ti = Mock(map_index=2)
        assert SkipMixin().skip(ti=ti, tasks=[MagicMock(spec=MappedOperator)]) is None

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 2 had a different implementation")
    def test_skip__only_none_mapped_operators_passed(self):
        from airflow.sdk.definitions.mappedoperator import MappedOperator

        ti = Mock(map_index=-1)
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            SkipMixin().skip(ti=ti, tasks=[MagicMock(spec=MappedOperator, task_id="task")])
        assert exc_info.value.tasks == ["task"]

    @pytest.mark.parametrize(
        ("branch_task_ids", "expected_states"),
        [
            (None, {"task2": State.SKIPPED, "task3": State.SKIPPED}),
            ([], {"task2": State.SKIPPED, "task3": State.SKIPPED}),
        ],
        ids=["None", "empty-list"],
    )
    def test_skip_all_except__branch_task_ids_none(
        self, dag_maker, branch_task_ids, expected_states, session
    ):
        with dag_maker(
            "dag_test_skip_all_except",
            serialized=True,
        ) as dag:
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")

            task1 >> [task2, task3]
        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)
        if AIRFLOW_V_3_0_PLUS:
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID, dag_version_id=dag_version.id)
        else:
            ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID)

        if AIRFLOW_V_3_0_PLUS:
            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)

            assert set(exc_info.value.tasks) == {("task2", -1), ("task3", -1)}
        else:
            ti2 = TI(task2, run_id=DEFAULT_DAG_RUN_ID)
            ti3 = TI(task3, run_id=DEFAULT_DAG_RUN_ID)

            SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)

            session.expire_all()

            def get_state(ti):
                ti.refresh_from_db()
                return ti.state

            executed_states = {"task2": get_state(ti2), "task3": get_state(ti3)}

            assert executed_states == expected_states

    @pytest.mark.parametrize(
        ("branch_task_ids", "expected_states"),
        [
            (["task2"], {"task2": State.NONE, "task3": State.SKIPPED}),
            (("task2",), {"task2": State.NONE, "task3": State.SKIPPED}),
            ("task2", {"task2": State.NONE, "task3": State.SKIPPED}),
        ],
        ids=["list-of-task-ids", "tuple-of-task-ids", "str-task-id"],
    )
    def test_skip_all_except__skip_task3(self, dag_maker, branch_task_ids, expected_states, session):
        with dag_maker(
            "dag_test_skip_all_except",
            serialized=True,
        ):
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")

            task1 >> [task2, task3]
        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)
        if AIRFLOW_V_3_0_PLUS:
            dag_version = DagVersion.get_latest_version(task1.dag_id)
            ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID, dag_version_id=dag_version.id)
        else:
            ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID)

        if AIRFLOW_V_3_0_PLUS:
            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)

            assert set(exc_info.value.tasks) == {("task3", -1)}
        else:
            ti2 = TI(task2, run_id=DEFAULT_DAG_RUN_ID)
            ti3 = TI(task3, run_id=DEFAULT_DAG_RUN_ID)

            SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)

            session.expire_all()

            def get_state(ti):
                ti.refresh_from_db()
                return ti.state

            executed_states = {"task2": get_state(ti2), "task3": get_state(ti3)}

            assert executed_states == expected_states

    @pytest.mark.skipif(
        AIRFLOW_V_3_0_PLUS, reason="In Airflow 3, `NotPreviouslySkippedDep` is used for this case"
    )
    @pytest.mark.need_serialized_dag
    def test_mapped_tasks_skip_all_except(self, dag_maker):
        with dag_maker("dag_test_skip_all_except") as dag:

            @task
            def branch_op(k): ...

            @task_group
            def task_group_op(k):
                branch_a = EmptyOperator(task_id="branch_a")
                branch_b = EmptyOperator(task_id="branch_b")
                branch_op(k) >> [branch_a, branch_b]

            task_group_op.expand(k=[0, 1])

        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)
        if AIRFLOW_V_3_0_PLUS:
            dag_version = DagVersion.get_latest_version(dag.dag_id)
            branch_op_ti_0 = TI(
                dag.get_task("task_group_op.branch_op"),
                run_id=DEFAULT_DAG_RUN_ID,
                map_index=0,
                dag_version_id=dag_version.id,
            )
            branch_op_ti_1 = TI(
                dag.get_task("task_group_op.branch_op"),
                run_id=DEFAULT_DAG_RUN_ID,
                map_index=1,
                dag_version_id=dag_version.id,
            )
            branch_a_ti_0 = TI(
                dag.get_task("task_group_op.branch_a"),
                run_id=DEFAULT_DAG_RUN_ID,
                map_index=0,
                dag_version_id=dag_version.id,
            )
            branch_a_ti_1 = TI(
                dag.get_task("task_group_op.branch_a"),
                run_id=DEFAULT_DAG_RUN_ID,
                map_index=1,
                dag_version_id=dag_version.id,
            )
            branch_b_ti_0 = TI(
                dag.get_task("task_group_op.branch_b"),
                run_id=DEFAULT_DAG_RUN_ID,
                map_index=0,
                dag_version_id=dag_version.id,
            )
            branch_b_ti_1 = TI(
                dag.get_task("task_group_op.branch_b"),
                run_id=DEFAULT_DAG_RUN_ID,
                map_index=1,
                dag_version_id=dag_version.id,
            )
        else:
            branch_op_ti_0 = TI(
                dag.get_task("task_group_op.branch_op"), run_id=DEFAULT_DAG_RUN_ID, map_index=0
            )
            branch_op_ti_1 = TI(
                dag.get_task("task_group_op.branch_op"), run_id=DEFAULT_DAG_RUN_ID, map_index=1
            )
            branch_a_ti_0 = TI(dag.get_task("task_group_op.branch_a"), run_id=DEFAULT_DAG_RUN_ID, map_index=0)
            branch_a_ti_1 = TI(dag.get_task("task_group_op.branch_a"), run_id=DEFAULT_DAG_RUN_ID, map_index=1)
            branch_b_ti_0 = TI(dag.get_task("task_group_op.branch_b"), run_id=DEFAULT_DAG_RUN_ID, map_index=0)
            branch_b_ti_1 = TI(dag.get_task("task_group_op.branch_b"), run_id=DEFAULT_DAG_RUN_ID, map_index=1)

        SkipMixin().skip_all_except(ti=branch_op_ti_0, branch_task_ids="task_group_op.branch_a")
        SkipMixin().skip_all_except(ti=branch_op_ti_1, branch_task_ids="task_group_op.branch_b")

        def get_state(ti):
            ti.refresh_from_db()
            return ti.state

        assert get_state(branch_a_ti_0) == State.NONE
        assert get_state(branch_b_ti_0) == State.SKIPPED
        assert get_state(branch_a_ti_1) == State.SKIPPED
        assert get_state(branch_b_ti_1) == State.NONE

    def test_raise_exception_on_not_accepted_branch_task_ids_type(self, dag_maker):
        with dag_maker("dag_test_skip_all_except_wrong_type"):
            task = EmptyOperator(task_id="task")
        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)
        if AIRFLOW_V_3_0_PLUS:
            dag_version = DagVersion.get_latest_version(task.dag_id)
            ti1 = TI(task, run_id=DEFAULT_DAG_RUN_ID, dag_version_id=dag_version.id)
        else:
            ti1 = TI(task, run_id=DEFAULT_DAG_RUN_ID)
        error_message = (
            r"'branch_task_ids' must be either None, a task ID, or an Iterable of IDs, but got 'int'\."
        )
        with pytest.raises(AirflowException, match=error_message):
            SkipMixin().skip_all_except(ti=ti1, branch_task_ids=42)

    def test_raise_exception_on_not_accepted_iterable_branch_task_ids_type(self, dag_maker):
        with dag_maker("dag_test_skip_all_except_wrong_type"):
            task = EmptyOperator(task_id="task")
        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)
        if AIRFLOW_V_3_0_PLUS:
            dag_version = DagVersion.get_latest_version(task.dag_id)
            ti1 = TI(task, run_id=DEFAULT_DAG_RUN_ID, dag_version_id=dag_version.id)
        else:
            ti1 = TI(task, run_id=DEFAULT_DAG_RUN_ID)

        if AIRFLOW_V_3_0_PLUS:
            # Improved error message for Airflow 3.0+
            error_message = (
                r"Unable to branch to the specified tasks\. "
                r"The branching function returned invalid 'branch_task_ids': \{\(42, 'int'\)\}\. "
                r"Please check that your function returns an Iterable of valid task IDs that exist in your DAG\."
            )
        else:
            # Old error message for Airflow 2.x
            error_message = (
                r"'branch_task_ids' expected all task IDs are strings\. "
                r"Invalid tasks found: \{\(42, 'int'\)\}\."
            )

        with pytest.raises(AirflowException, match=error_message):
            SkipMixin().skip_all_except(ti=ti1, branch_task_ids=["task", 42])

    @pytest.mark.parametrize(
        "branch_task_ids",
        [
            pytest.param("task4", id="invalid-single-task"),
            pytest.param(["task2", "task4"], id="invalid-any-task-in-list"),
            pytest.param(["task5", "task4"], id="invalid-all-task-in-list"),
        ],
    )
    def test_raise_exception_on_not_valid_branch_task_ids(self, dag_maker, branch_task_ids):
        with dag_maker("dag_test_skip_all_except_wrong_type", serialized=True):
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")

            task1 >> [task2, task3]
        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)
        if AIRFLOW_V_3_0_PLUS:
            dag_version = DagVersion.get_latest_version(task1.dag_id)
            ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID, dag_version_id=dag_version.id)
        else:
            ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID)

        error_message = r"'branch_task_ids' must contain only valid task_ids. Invalid tasks found: .*"
        with pytest.raises(AirflowException, match=error_message):
            SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Issue only exists in Airflow 3.x")
    def test_ensure_tasks_includes_sensors_airflow_3x(self, dag_maker):
        """Test that sensors (inheriting from airflow.sdk.BaseOperator) are properly handled by _ensure_tasks."""
        from airflow.providers.standard.utils.skipmixin import _ensure_tasks
        from airflow.sdk import BaseOperator as SDKBaseOperator
        from airflow.sdk.bases.sensor import BaseSensorOperator

        class DummySensor(BaseSensorOperator):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.timeout = 0
                self.poke_interval = 0

            def poke(self, context):
                return True

        with dag_maker("dag_test_sensor_skipping") as dag:
            regular_task = EmptyOperator(task_id="regular_task")
            sensor_task = DummySensor(task_id="sensor_task")
            downstream_task = EmptyOperator(task_id="downstream_task")

            regular_task >> [sensor_task, downstream_task]

        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)

        downstream_nodes = dag.get_task("regular_task").downstream_list
        task_list = _ensure_tasks(downstream_nodes)

        # Verify both the regular operator and sensor are included
        task_ids = [t.task_id for t in task_list]
        assert "sensor_task" in task_ids, "Sensor should be included in task list"
        assert "downstream_task" in task_ids, "Regular task should be included in task list"
        assert len(task_list) == 2, "Both tasks should be included"

        # Also verify that the sensor is actually an instance of the correct BaseOperator
        sensor_in_list = next((t for t in task_list if t.task_id == "sensor_task"), None)
        assert sensor_in_list is not None, "Sensor task should be found in list"
        assert isinstance(sensor_in_list, SDKBaseOperator), "Sensor should be instance of SDK BaseOperator"

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Integration test for Airflow 3.x sensor skipping")
    def test_skip_sensor_in_branching_scenario(self, dag_maker):
        """Integration test: verify sensors are properly skipped by branching operators in Airflow 3.x."""
        from airflow.sdk.bases.sensor import BaseSensorOperator

        # Create a dummy sensor for testing
        class DummySensor(BaseSensorOperator):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.timeout = 0
                self.poke_interval = 0

            def poke(self, context):
                return True

        with dag_maker("dag_test_branch_sensor_skipping"):
            branch_task = EmptyOperator(task_id="branch_task")
            regular_task = EmptyOperator(task_id="regular_task")
            sensor_task = DummySensor(task_id="sensor_task")
            branch_task >> [regular_task, sensor_task]

        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)

        dag_version = DagVersion.get_latest_version(branch_task.dag_id)
        ti_branch = TI(branch_task, run_id=DEFAULT_DAG_RUN_ID, dag_version_id=dag_version.id)

        # Test skipping the sensor (follow regular_task branch)
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            SkipMixin().skip_all_except(ti=ti_branch, branch_task_ids="regular_task")

        # Verify that the sensor task is properly marked for skipping
        skipped_tasks = set(exc_info.value.tasks)
        assert ("sensor_task", -1) in skipped_tasks, "Sensor task should be marked for skipping"

        # Test skipping the regular task (follow sensor_task branch)
        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            SkipMixin().skip_all_except(ti=ti_branch, branch_task_ids="sensor_task")

        # Verify that the regular task is properly marked for skipping
        skipped_tasks = set(exc_info.value.tasks)
        assert ("regular_task", -1) in skipped_tasks, "Regular task should be marked for skipping"
