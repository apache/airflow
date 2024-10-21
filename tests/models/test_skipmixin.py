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
from unittest.mock import Mock, patch

import pytest

from airflow import settings
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


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

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @patch("airflow.utils.timezone.utcnow")
    def test_skip(self, mock_now, dag_maker):
        session = settings.Session()
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        mock_now.return_value = now
        with dag_maker("dag"):
            tasks = [EmptyOperator(task_id="task")]
        dag_run = dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=now,
            state=State.FAILED,
        )
        SkipMixin().skip(dag_run=dag_run, tasks=tasks)

        session.query(TI).filter(
            TI.dag_id == "dag",
            TI.task_id == "task",
            TI.state == State.SKIPPED,
            TI.start_date == now,
            TI.end_date == now,
        ).one()

    def test_skip_none_tasks(self):
        session = Mock()
        SkipMixin().skip(dag_run=None, tasks=[])
        assert not session.query.called
        assert not session.commit.called

    @pytest.mark.parametrize(
        "branch_task_ids, expected_states",
        [
            (["task2"], {"task2": State.NONE, "task3": State.SKIPPED}),
            (("task2",), {"task2": State.NONE, "task3": State.SKIPPED}),
            ("task2", {"task2": State.NONE, "task3": State.SKIPPED}),
            (None, {"task2": State.SKIPPED, "task3": State.SKIPPED}),
            ([], {"task2": State.SKIPPED, "task3": State.SKIPPED}),
        ],
        ids=["list-of-task-ids", "tuple-of-task-ids", "str-task-id", "None", "empty-list"],
    )
    def test_skip_all_except(self, dag_maker, branch_task_ids, expected_states):
        with dag_maker(
            "dag_test_skip_all_except",
            serialized=True,
        ):
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")

            task1 >> [task2, task3]
        dag_maker.create_dagrun(run_id=DEFAULT_DAG_RUN_ID)

        ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID)
        ti2 = TI(task2, run_id=DEFAULT_DAG_RUN_ID)
        ti3 = TI(task3, run_id=DEFAULT_DAG_RUN_ID)

        SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)

        def get_state(ti):
            ti.refresh_from_db()
            return ti.state

        executed_states = {"task2": get_state(ti2), "task3": get_state(ti3)}

        assert executed_states == expected_states

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
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
        branch_op_ti_0 = TI(dag.get_task("task_group_op.branch_op"), run_id=DEFAULT_DAG_RUN_ID, map_index=0)
        branch_op_ti_1 = TI(dag.get_task("task_group_op.branch_op"), run_id=DEFAULT_DAG_RUN_ID, map_index=1)
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
        ti1 = TI(task, run_id=DEFAULT_DAG_RUN_ID)
        error_message = (
            r"'branch_task_ids' expected all task IDs are strings. "
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

        ti1 = TI(task1, run_id=DEFAULT_DAG_RUN_ID)

        error_message = r"'branch_task_ids' must contain only valid task_ids. Invalid tasks found: .*"
        with pytest.raises(AirflowException, match=error_message):
            SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)
