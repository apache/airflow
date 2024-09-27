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
import urllib

import pytest

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import DagRun, TaskInstance
from airflow.security import permissions
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.providers.fab.auth_manager.api_endpoints.api_connexion_utils import (
    create_user,
    delete_roles,
    delete_user,
)
from tests.test_utils.db import clear_db_runs, clear_db_sla_miss, clear_rendered_ti_fields

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

DEFAULT_DATETIME_1 = datetime(2020, 1, 1)
DEFAULT_DATETIME_STR_1 = "2020-01-01T00:00:00+00:00"
DEFAULT_DATETIME_STR_2 = "2020-01-02T00:00:00+00:00"

QUOTED_DEFAULT_DATETIME_STR_1 = urllib.parse.quote(DEFAULT_DATETIME_STR_1)
QUOTED_DEFAULT_DATETIME_STR_2 = urllib.parse.quote(DEFAULT_DATETIME_STR_2)


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api
    create_user(
        app,
        username="test_dag_read_only",
        role_name="TestDagReadOnly",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )
    create_user(
        app,
        username="test_task_read_only",
        role_name="TestTaskReadOnly",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )
    create_user(
        app,
        username="test_read_only_one_dag",
        role_name="TestReadOnlyOneDag",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )
    # For some reason, "DAG:example_python_operator" is not synced when in the above list of perms,
    # so do it manually here:
    app.appbuilder.sm.bulk_sync_roles(
        [
            {
                "role": "TestReadOnlyOneDag",
                "perms": [(permissions.ACTION_CAN_READ, "DAG:example_python_operator")],
            }
        ]
    )

    yield app

    delete_user(app, username="test_dag_read_only")
    delete_user(app, username="test_task_read_only")
    delete_user(app, username="test_read_only_one_dag")
    delete_roles(app)


class TestTaskInstanceEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app, dagbag) -> None:
        self.default_time = DEFAULT_DATETIME_1
        self.ti_init = {
            "execution_date": self.default_time,
            "state": State.RUNNING,
        }
        self.ti_extras = {
            "start_date": self.default_time + dt.timedelta(days=1),
            "end_date": self.default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
            "job_id": 0,
        }
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_runs()
        clear_db_sla_miss()
        clear_rendered_ti_fields()
        self.dagbag = dagbag

    def create_task_instances(
        self,
        session,
        dag_id: str = "example_python_operator",
        update_extras: bool = True,
        task_instances=None,
        dag_run_state=State.RUNNING,
        with_ti_history=False,
    ):
        """Method to create task instances using kwargs and default arguments"""

        dag = self.dagbag.get_dag(dag_id)
        tasks = dag.tasks
        counter = len(tasks)
        if task_instances is not None:
            counter = min(len(task_instances), counter)

        run_id = "TEST_DAG_RUN_ID"
        execution_date = self.ti_init.pop("execution_date", self.default_time)
        dr = None

        tis = []
        for i in range(counter):
            if task_instances is None:
                pass
            elif update_extras:
                self.ti_extras.update(task_instances[i])
            else:
                self.ti_init.update(task_instances[i])

            if "execution_date" in self.ti_init:
                run_id = f"TEST_DAG_RUN_ID_{i}"
                execution_date = self.ti_init.pop("execution_date")
                dr = None

            if not dr:
                dr = DagRun(
                    run_id=run_id,
                    dag_id=dag_id,
                    execution_date=execution_date,
                    run_type=DagRunType.MANUAL,
                    state=dag_run_state,
                )
                session.add(dr)
            ti = TaskInstance(task=tasks[i], **self.ti_init)
            session.add(ti)
            ti.dag_run = dr
            ti.note = "placeholder-note"

            for key, value in self.ti_extras.items():
                setattr(ti, key, value)
            tis.append(ti)

        session.commit()
        if with_ti_history:
            for ti in tis:
                ti.try_number = 1
                session.merge(ti)
            session.commit()
            dag.clear()
            for ti in tis:
                ti.try_number = 2
                ti.queue = "default_queue"
                session.merge(ti)
            session.commit()
        return tis


class TestGetTaskInstance(TestTaskInstanceEndpoint):
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize("username", ["test_dag_read_only", "test_task_read_only"])
    @provide_session
    def test_should_respond_200(self, username, session):
        self.create_task_instances(session)
        # Update ti and set operator to None to
        # test that operator field is nullable.
        # This prevents issue when users upgrade to 2.0+
        # from 1.10.x
        # https://github.com/apache/airflow/issues/14421
        session.query(TaskInstance).update({TaskInstance.operator: None}, synchronize_session="fetch")
        session.commit()
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": username},
        )
        assert response.status_code == 200


class TestGetTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "task_instances, user, expected_ti",
        [
            pytest.param(
                {
                    "example_python_operator": 2,
                    "example_skip_dag": 1,
                },
                "test_read_only_one_dag",
                2,
            ),
            pytest.param(
                {
                    "example_python_operator": 1,
                    "example_skip_dag": 2,
                },
                "test_read_only_one_dag",
                1,
            ),
        ],
    )
    def test_return_TI_only_from_readable_dags(self, task_instances, user, expected_ti, session):
        for dag_id in task_instances:
            self.create_task_instances(
                session,
                task_instances=[
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=i)}
                    for i in range(task_instances[dag_id])
                ],
                dag_id=dag_id,
            )
        response = self.client.get(
            "/api/v1/dags/~/dagRuns/~/taskInstances", environ_overrides={"REMOTE_USER": user}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == expected_ti
        assert len(response.json["task_instances"]) == expected_ti


class TestGetTaskInstancesBatch(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "task_instances, update_extras, payload, expected_ti_count, username",
        [
            pytest.param(
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                {"pool": ["test_pool_1", "test_pool_2"]},
                2,
                "test_dag_read_only",
                id="test pool filter",
            ),
            pytest.param(
                [
                    {"state": State.RUNNING},
                    {"state": State.QUEUED},
                    {"state": State.SUCCESS},
                    {"state": State.NONE},
                ],
                False,
                {"state": ["running", "queued", "none"]},
                3,
                "test_task_read_only",
                id="test state filter",
            ),
            pytest.param(
                [
                    {"state": State.NONE},
                    {"state": State.NONE},
                    {"state": State.NONE},
                    {"state": State.NONE},
                ],
                False,
                {},
                4,
                "test_task_read_only",
                id="test dag with null states",
            ),
            pytest.param(
                [
                    {"end_date": DEFAULT_DATETIME_1},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                {
                    "end_date_gte": DEFAULT_DATETIME_STR_1,
                    "end_date_lte": DEFAULT_DATETIME_STR_2,
                },
                2,
                "test_task_read_only",
                id="test end date filter",
            ),
            pytest.param(
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                {
                    "start_date_gte": DEFAULT_DATETIME_STR_1,
                    "start_date_lte": DEFAULT_DATETIME_STR_2,
                },
                2,
                "test_dag_read_only",
                id="test start date filter",
            ),
        ],
    )
    @pytest.mark.filterwarnings("ignore::airflow.exceptions.RemovedInAirflow3Warning")
    def test_should_respond_200(
        self, task_instances, update_extras, payload, expected_ti_count, username, session
    ):
        self.create_task_instances(
            session,
            update_extras=update_extras,
            task_instances=task_instances,
        )
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": username},
            json=payload,
        )
        assert response.status_code == 200, response.json
        assert expected_ti_count == response.json["total_entries"]
        assert expected_ti_count == len(response.json["task_instances"])

    def test_returns_403_forbidden_when_user_has_access_to_only_some_dags(self, session):
        self.create_task_instances(session=session)
        self.create_task_instances(session=session, dag_id="example_skip_dag")
        payload = {"dag_ids": ["example_python_operator", "example_skip_dag"]}

        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test_read_only_one_dag"},
            json=payload,
        )
        assert response.status_code == 403
        assert response.json == {
            "detail": "User not allowed to access some of these DAGs: ['example_python_operator', 'example_skip_dag']",
            "status": 403,
            "title": "Forbidden",
            "type": EXCEPTIONS_LINK_MAP[403],
        }


class TestPostSetTaskInstanceState(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize("username", ["test_dag_read_only", "test_task_read_only"])
    def test_should_raise_403_forbidden(self, username):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": username},
            json={
                "dry_run": True,
                "task_id": "print_the_context",
                "execution_date": DEFAULT_DATETIME_1.isoformat(),
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 403


class TestPatchTaskInstance(TestTaskInstanceEndpoint):
    ENDPOINT_URL = (
        "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
    )

    @pytest.mark.parametrize("username", ["test_dag_read_only", "test_task_read_only"])
    def test_should_raise_403_forbidden(self, username):
        response = self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": username},
            json={
                "dry_run": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 403


class TestGetTaskInstanceTry(TestTaskInstanceEndpoint):
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize("username", ["test_dag_read_only", "test_task_read_only"])
    @provide_session
    def test_should_respond_200(self, username, session):
        self.create_task_instances(session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True)

        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1",
            environ_overrides={"REMOTE_USER": username},
        )
        assert response.status_code == 200
