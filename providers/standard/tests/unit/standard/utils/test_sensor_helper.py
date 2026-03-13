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
from typing import TYPE_CHECKING
from unittest import mock

import pendulum
import pytest

from airflow.models import DagBag, TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.utils.sensor_helper import (
    _count_stmt,
    _get_count,
    _get_external_task_group_task_ids,
)
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils import db
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

try:
    from airflow.sdk.definitions.taskgroup import TaskGroup
except ImportError:  # Fallback for Airflow < 3.1
    from airflow.utils.task_group import TaskGroup  # type: ignore[no-redef]
try:
    from airflow.sdk import timezone
except ImportError:  # Fallback for Airflow < 3.1
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.serialization.definitions.dag import SerializedDAG

TI = TaskInstance


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for AF2")
@pytest.mark.db_test
class TestSensorHelper:
    DAG_ID = "test_dag"
    TASK_ID = "test_task"
    TASK_ID_LIST = ["task_0", "task_1"]
    TASK_GROUP_ID = "test_task_group"
    TASK_GROUP = {TASK_GROUP_ID: ["subtask_0", "subtask_1"]}
    DTTM_FILTER = [
        datetime.datetime(2025, 1, 1),
        datetime.datetime(2025, 1, 2),
        datetime.datetime(2025, 1, 3),
    ]
    ALLOWED_STATES = [TaskInstanceState.SUCCESS.value, TaskInstanceState.SKIPPED.value]

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session):
        self._clean_db()
        yield
        self._clean_db()

    @staticmethod
    def _clean_db():
        db.clear_db_dags()
        db.clear_db_jobs()
        db.clear_db_runs()

    @staticmethod
    def create_dag_run(
        dag: SerializedDAG,
        *,
        task_states: Mapping[str, TaskInstanceState] | None = None,
        execution_date: datetime.datetime | None = None,
        session: Session,
    ):
        now = timezone.utcnow()
        execution_date = pendulum.instance(execution_date or now)
        run_type = DagRunType.MANUAL
        data_interval = dag.timetable.infer_manual_data_interval(run_after=execution_date)
        dag_run = dag.create_dagrun(
            run_id=dag.timetable.generate_run_id(
                run_type=run_type,
                run_after=execution_date,
                data_interval=None,
                logical_date=execution_date,
            ),
            run_type=run_type,
            data_interval=data_interval,
            start_date=now,
            state=DagRunState.SUCCESS,  # not important
            external_trigger=False,
            execution_date=execution_date,  # type: ignore[call-arg]
        )

        if task_states is not None:
            for task_id, task_state in task_states.items():
                ti = dag_run.get_task_instance(task_id)
                if TYPE_CHECKING:
                    assert ti
                ti.set_state(task_state, session)
            session.flush()

    @pytest.mark.parametrize(
        "dttm_to_task_state",
        [
            {
                datetime.datetime(2025, 1, 1): {
                    "task_0": TaskInstanceState.SUCCESS.value,
                    "task_1": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.SUCCESS.value,
                },
                datetime.datetime(2025, 1, 2): {
                    "task_0": TaskInstanceState.SUCCESS.value,
                    "task_1": TaskInstanceState.SKIPPED.value,
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.SKIPPED.value,
                },
                datetime.datetime(2025, 1, 3): {
                    "task_0": TaskInstanceState.SKIPPED.value,
                    "task_1": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_0": TaskInstanceState.SKIPPED.value,
                    "test_task_group.subtask_1": TaskInstanceState.SUCCESS.value,
                },
            },
            {
                datetime.datetime(2025, 1, 1): {
                    "task_0": TaskInstanceState.SUCCESS.value,
                    "task_1": TaskInstanceState.FAILED.value,
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.SKIPPED.value,
                },
                datetime.datetime(2025, 1, 2): {
                    "task_0": TaskInstanceState.SKIPPED.value,
                    "task_1": TaskInstanceState.FAILED.value,
                    "test_task_group.subtask_0": TaskInstanceState.FAILED.value,
                    "test_task_group.subtask_1": TaskInstanceState.SUCCESS.value,
                },
                datetime.datetime(2025, 1, 3): {
                    "task_0": TaskInstanceState.FAILED.value,
                    "task_1": TaskInstanceState.FAILED.value,
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.SKIPPED.value,
                },
            },
        ],  # these can be any TaskInstanceState
    )
    @pytest.mark.need_serialized_dag
    def test_count_stmt(self, dttm_to_task_state, dag_maker, session):
        with dag_maker(dag_id=self.DAG_ID, session=session) as dag:
            for task_id in self.TASK_ID_LIST:
                EmptyOperator(task_id=task_id)

            with TaskGroup(group_id=self.TASK_GROUP_ID):
                for subtask_id in self.TASK_GROUP[self.TASK_GROUP_ID]:
                    EmptyOperator(task_id=subtask_id)

        for dttm in self.DTTM_FILTER:
            self.create_dag_run(
                dag=dag, task_states=dttm_to_task_state[dttm], execution_date=dttm, session=session
            )

        dttm_filter = [pendulum.instance(dttm) for dttm in self.DTTM_FILTER]
        count = session.scalar(
            _count_stmt(
                model=TI, states=self.ALLOWED_STATES, dttm_filter=dttm_filter, external_dag_id=self.DAG_ID
            )
        )

        allowed_task_instance_states = [
            state
            for task_states in dttm_to_task_state.values()
            for state in task_states.values()
            if state in self.ALLOWED_STATES
        ]

        allowed_state_count = len(allowed_task_instance_states)
        assert count == allowed_state_count

    @pytest.mark.need_serialized_dag
    def test_get_external_task_group_task_ids(self, dag_maker, session):
        with dag_maker(dag_id=self.DAG_ID) as dag:
            with TaskGroup(group_id=self.TASK_GROUP_ID):
                for subtask_id in self.TASK_GROUP[self.TASK_GROUP_ID]:
                    EmptyOperator(task_id=subtask_id)

        for dttm in self.DTTM_FILTER:
            self.create_dag_run(dag=dag, execution_date=dttm, session=session)

        with mock.patch.object(DagBag, "get_dag") as mock_get_dag:
            mock_get_dag.return_value = dag

            dttm_filter = [pendulum.instance(dttm) for dttm in self.DTTM_FILTER]
            external_task_group_task_ids = _get_external_task_group_task_ids(
                dttm_filter=dttm_filter,
                external_task_group_id=self.TASK_GROUP_ID,
                external_dag_id=self.DAG_ID,
                session=session,
            )

        assert len(external_task_group_task_ids) == 6
        assert external_task_group_task_ids.count(("test_task_group.subtask_0", -1)) == 3
        assert external_task_group_task_ids.count(("test_task_group.subtask_1", -1)) == 3

    @pytest.mark.parametrize(
        "state",
        [
            TaskInstanceState.SUCCESS.value,
            TaskInstanceState.SKIPPED.value,
            TaskInstanceState.FAILED.value,
        ],
    )
    @pytest.mark.need_serialized_dag
    def test_get_count_with_different_states(self, state, dag_maker, session):
        with dag_maker(dag_id=self.DAG_ID) as dag:
            EmptyOperator(task_id=self.TASK_ID)

        now = timezone.utcnow()
        self.create_dag_run(dag, task_states={self.TASK_ID: state}, execution_date=now, session=session)

        count = _get_count(
            dttm_filter=[now],
            external_task_ids=[self.TASK_ID],
            external_task_group_id=None,
            external_dag_id=self.DAG_ID,
            states=[state],
            session=session,
        )

        assert count == 1

    @pytest.mark.parametrize(
        "task_states",
        [
            {
                datetime.datetime(2025, 1, 1): TaskInstanceState.SUCCESS.value,
                datetime.datetime(2025, 1, 2): TaskInstanceState.SKIPPED.value,
                datetime.datetime(2025, 1, 3): TaskInstanceState.FAILED.value,
            },
            {
                datetime.datetime(2025, 1, 1): TaskInstanceState.FAILED.value,
                datetime.datetime(2025, 1, 2): TaskInstanceState.FAILED.value,
                datetime.datetime(2025, 1, 3): TaskInstanceState.SUCCESS.value,
            },
        ],  # these can be any TaskInstanceState
    )
    @pytest.mark.need_serialized_dag
    def test_get_count_with_one_task(self, task_states, dag_maker, session):
        with dag_maker(dag_id=self.DAG_ID) as dag:
            EmptyOperator(task_id=self.TASK_ID)

        for dttm in self.DTTM_FILTER:
            self.create_dag_run(
                dag=dag, task_states={self.TASK_ID: task_states[dttm]}, execution_date=dttm, session=session
            )

        dttm_filter = [pendulum.instance(dttm) for dttm in self.DTTM_FILTER]
        count = _get_count(
            dttm_filter=dttm_filter,
            external_task_ids=[self.TASK_ID],
            external_task_group_id=None,
            external_dag_id=self.DAG_ID,
            states=self.ALLOWED_STATES,
            session=session,
        )

        allowed_state_count = len([state for state in task_states.values() if state in self.ALLOWED_STATES])
        assert count == allowed_state_count

    @pytest.mark.parametrize(
        "dttm_to_task_state",
        [
            {
                datetime.datetime(2025, 1, 1): {
                    "task_0": TaskInstanceState.SUCCESS.value,
                    "task_1": TaskInstanceState.SUCCESS.value,
                },
                datetime.datetime(2025, 1, 2): {
                    "task_0": TaskInstanceState.SUCCESS.value,
                    "task_1": TaskInstanceState.SKIPPED.value,
                },
                datetime.datetime(2025, 1, 3): {
                    "task_0": TaskInstanceState.SKIPPED.value,
                    "task_1": TaskInstanceState.SUCCESS.value,
                },
            },
            {
                datetime.datetime(2025, 1, 1): {
                    "task_0": TaskInstanceState.SUCCESS.value,
                    "task_1": TaskInstanceState.FAILED.value,
                },
                datetime.datetime(2025, 1, 2): {
                    "task_0": TaskInstanceState.FAILED.value,
                    "task_1": TaskInstanceState.SKIPPED.value,
                },
                datetime.datetime(2025, 1, 3): {
                    "task_0": TaskInstanceState.SUCCESS.value,
                    "task_1": TaskInstanceState.SKIPPED.value,
                },
            },
        ],  # these can be any TaskInstanceState
    )
    @pytest.mark.need_serialized_dag
    def test_get_count_with_multiple_tasks(self, dttm_to_task_state, dag_maker, session):
        with dag_maker(dag_id=self.DAG_ID) as dag:
            for task_id in self.TASK_ID_LIST:
                EmptyOperator(task_id=task_id)

        for dttm in self.DTTM_FILTER:
            self.create_dag_run(
                dag=dag, task_states=dttm_to_task_state[dttm], execution_date=dttm, session=session
            )

        dttm_filter = [pendulum.instance(dttm) for dttm in self.DTTM_FILTER]
        count = _get_count(
            dttm_filter=dttm_filter,
            external_task_ids=self.TASK_ID_LIST,
            external_task_group_id=None,
            external_dag_id=self.DAG_ID,
            states=self.ALLOWED_STATES,
            session=session,
        )

        allowed_task_instance_states = [
            state
            for task_states in dttm_to_task_state.values()
            for state in task_states.values()
            if state in self.ALLOWED_STATES
        ]

        allowed_state_count = len(allowed_task_instance_states) / len(self.TASK_ID_LIST)
        assert count == allowed_state_count

    @pytest.mark.parametrize(
        "dttm_to_subtask_state",
        [
            {
                datetime.datetime(2025, 1, 1): {
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.SUCCESS.value,
                },
                datetime.datetime(2025, 1, 2): {
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.SKIPPED.value,
                },
                datetime.datetime(2025, 1, 3): {
                    "test_task_group.subtask_0": TaskInstanceState.SKIPPED.value,
                    "test_task_group.subtask_1": TaskInstanceState.SUCCESS.value,
                },
            },
            {
                datetime.datetime(2025, 1, 1): {
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.FAILED.value,
                },
                datetime.datetime(2025, 1, 2): {
                    "test_task_group.subtask_0": TaskInstanceState.FAILED.value,
                    "test_task_group.subtask_1": TaskInstanceState.SKIPPED.value,
                },
                datetime.datetime(2025, 1, 3): {
                    "test_task_group.subtask_0": TaskInstanceState.SUCCESS.value,
                    "test_task_group.subtask_1": TaskInstanceState.SKIPPED.value,
                },
            },
        ],  # these can be any TaskInstanceState
    )
    @pytest.mark.need_serialized_dag
    def test_get_count_with_task_group(self, dttm_to_subtask_state, dag_maker, session):
        with dag_maker(dag_id=self.DAG_ID, session=session) as dag:
            with TaskGroup(group_id=self.TASK_GROUP_ID):
                for subtask_id in self.TASK_GROUP[self.TASK_GROUP_ID]:
                    EmptyOperator(task_id=subtask_id)

        for dttm in self.DTTM_FILTER:
            self.create_dag_run(
                dag=dag, task_states=dttm_to_subtask_state[dttm], execution_date=dttm, session=session
            )

        with mock.patch.object(DagBag, "get_dag") as mock_get_dag:
            mock_get_dag.return_value = dag

            dttm_filter = [pendulum.instance(dttm) for dttm in self.DTTM_FILTER]
            count = _get_count(
                dttm_filter=dttm_filter,
                external_task_ids=None,
                external_task_group_id=self.TASK_GROUP_ID,
                external_dag_id=self.DAG_ID,
                states=self.ALLOWED_STATES,
                session=session,
            )

        allowed_task_instance_states = [
            state
            for task_states in dttm_to_subtask_state.values()
            for state in task_states.values()
            if state in self.ALLOWED_STATES
        ]

        allowed_state_count = len(allowed_task_instance_states) / len(self.TASK_ID_LIST)
        assert count == allowed_state_count


@pytest.mark.parametrize(
    ("run_id_task_state_map", "states", "expected_count"),
    [
        pytest.param(
            {
                "run_id_1": {"task_id_1": "success", "task_id_2": "success"},
            },
            ["success"],
            1,
            id="single_runid_with_success",
        ),
        pytest.param(
            {
                "run_id_1": {"task_id_1": "success", "task_id_2": "success"},
                "run_id_2": {"task_id_1": "success", "task_id_2": "success"},
            },
            ["success"],
            2,
            id="multiple_runid_with_success",
        ),
        pytest.param(
            {
                "run_id_1": {"task_id_1": "failed", "task_id_2": "failed"},
            },
            ["failed"],
            1,
            id="single_runid_with_failed",
        ),
        pytest.param(
            {
                "run_id_1": {"task_id_1": "success", "task_id_2": "failed"},
            },
            ["failed"],
            0,
            id="single_runid_with_no_success",
        ),
    ],
)
@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for AF3")
def test_get_count_by_matched_states(
    run_id_task_state_map,
    states,
    expected_count,
):
    from airflow.providers.standard.utils.sensor_helper import _get_count_by_matched_states

    count = _get_count_by_matched_states(run_id_task_state_map=run_id_task_state_map, states=states)
    assert count == expected_count
