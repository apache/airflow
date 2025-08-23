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

import datetime as dt
from typing import TYPE_CHECKING

import pytest

from airflow._shared.timezones.timezone import datetime
from airflow.api_fastapi.core_api.datamodels.common import BulkBody
from airflow.api_fastapi.core_api.services.public.task_instances import BulkTaskInstanceService
from airflow.models import DagRun, TaskInstance
from airflow.models.dag_version import DagVersion
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import (
    clear_db_runs,
)

pytestmark = pytest.mark.db_test

DEFAULT = datetime(2020, 1, 1)
DEFAULT_DATETIME_STR_1 = "2020-01-01T00:00:00+00:00"
DEFAULT_DATETIME_STR_2 = "2020-01-02T00:00:00+00:00"

DEFAULT_DATETIME_1 = dt.datetime.fromisoformat(DEFAULT_DATETIME_STR_1)
DEFAULT_DATETIME_2 = dt.datetime.fromisoformat(DEFAULT_DATETIME_STR_2)


class TestTaskInstanceEndpoint:
    @staticmethod
    def clear_db():
        clear_db_runs()

    def create_task_instances(
        self,
        session,
        dagbag,
        dag_id: str = "example_python_operator",
        update_extras: bool = True,
        task_instances=None,
        dag_run_state=DagRunState.RUNNING,
        with_ti_history=False,
    ):
        """Method to create task instances using kwargs and default arguments"""
        default_time = DEFAULT
        ti_init = {
            "logical_date": default_time,
            "state": State.RUNNING,
        }
        ti_extras = {
            "start_date": default_time + dt.timedelta(days=1),
            "end_date": default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
        }

        dag = dagbag.get_latest_version_of_dag(dag_id, session=session)
        tasks = dag.tasks
        counter = len(tasks)
        if task_instances is not None:
            counter = min(len(task_instances), counter)

        run_id = "TEST_DAG_RUN_ID"
        logical_date = ti_init.pop("logical_date", default_time)
        dr = None
        dag_version = DagVersion.get_latest_version(dag.dag_id, session=session)
        tis = []
        for i in range(counter):
            if task_instances is None:
                pass
            elif update_extras:
                ti_extras.update(task_instances[i])
            else:
                ti_init.update(task_instances[i])

            if "logical_date" in ti_init:
                run_id = f"TEST_DAG_RUN_ID_{i}"
                logical_date = ti_init.pop("logical_date")
                dr = None

            if not dr:
                dr = DagRun(
                    run_id=run_id,
                    dag_id=dag_id,
                    logical_date=logical_date,
                    run_type=DagRunType.MANUAL,
                    state=dag_run_state,
                )
                session.add(dr)
                session.flush()
            if TYPE_CHECKING:
                assert dag_version
            ti = TaskInstance(task=tasks[i], **ti_init, dag_version_id=dag_version.id)
            session.add(ti)
            ti.dag_run = dr
            ti.note = "placeholder-note"

            for key, value in ti_extras.items():
                setattr(ti, key, value)
            tis.append(ti)

        session.flush()

        if with_ti_history:
            for ti in tis:
                ti.try_number = 1
                session.merge(ti)
                session.flush()
            dag.clear(session=session)
            for ti in tis:
                ti.try_number = 2
                ti.queue = "default_queue"
                session.merge(ti)
                session.flush()
        session.commit()
        return tis


class TestCategorizeTaskInstances(TestTaskInstanceEndpoint):
    """Tests for the categorize_task_instances method in BulkTaskInstanceService."""

    def setup_method(self):
        self.clear_db()

    def teardown_method(self):
        self.clear_db()

    class MockUser:
        def get_id(self) -> str:
            return "test_user"

        def get_name(self) -> str:
            return "test_user"

    @pytest.mark.parametrize(
        "task_keys, expected_matched_keys, expected_not_found_keys, expected_matched_count, expected_not_found_count",
        [
            pytest.param(
                {("print_the_context", -1), ("log_sql_query", -1)},
                {("print_the_context", -1), ("log_sql_query", -1)},
                set(),
                2,
                0,
                id="all_found",
            ),
            pytest.param(
                {("nonexistent_task", -1), ("nonexistent_task", 0)},
                set(),
                {("nonexistent_task", -1), ("nonexistent_task", 0)},
                0,
                2,
                id="none_found",
            ),
            pytest.param(
                {("print_the_context", -1), ("print_the_context", 0)},
                {("print_the_context", -1)},
                {("print_the_context", 0)},
                1,
                1,
                id="mixed_found_and_not_found",
            ),
            pytest.param(set(), set(), set(), 0, 0, id="empty_input"),
        ],
    )
    def test_categorize_task_instances(
        self,
        session,
        dagbag,
        task_keys,
        expected_matched_keys,
        expected_not_found_keys,
        expected_matched_count,
        expected_not_found_count,
    ):
        """Test categorize_task_instances with various scenarios."""
        self.create_task_instances(session, dagbag)
        dag_id = "example_python_operator"
        run_id = "TEST_DAG_RUN_ID"

        user = self.MockUser()
        request = BulkBody(actions=[])
        service = BulkTaskInstanceService(
            session=session, request=request, dag_id=dag_id, dag_run_id=run_id, dag_bag=dagbag, user=user
        )

        _, matched_task_keys, not_found_task_keys = service.categorize_task_instances(task_keys)

        assert len(matched_task_keys) == expected_matched_count
        assert len(not_found_task_keys) == expected_not_found_count
        assert matched_task_keys == expected_matched_keys
        assert not_found_task_keys == expected_not_found_keys
