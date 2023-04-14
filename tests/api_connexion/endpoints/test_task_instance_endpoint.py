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
from unittest import mock

import pendulum
import pytest
from sqlalchemy.orm import contains_eager

from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models import DagRun, SlaMiss, TaskInstance, Trigger
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.security import permissions
from airflow.utils.platform import getuser
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_roles, delete_user
from tests.test_utils.db import clear_db_runs, clear_db_sla_miss, clear_rendered_ti_fields

DEFAULT_DATETIME_1 = datetime(2020, 1, 1)
DEFAULT_DATETIME_STR_1 = "2020-01-01T00:00:00+00:00"
DEFAULT_DATETIME_STR_2 = "2020-01-02T00:00:00+00:00"

QUOTED_DEFAULT_DATETIME_STR_1 = urllib.parse.quote(DEFAULT_DATETIME_STR_1)
QUOTED_DEFAULT_DATETIME_STR_2 = urllib.parse.quote(DEFAULT_DATETIME_STR_2)


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )
    create_user(
        app,  # type: ignore
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
        app,  # type: ignore
        username="test_task_read_only",
        role_name="TestTaskReadOnly",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_dag_read_only")  # type: ignore
    delete_user(app, username="test_task_read_only")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore
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
            ti.dag_run = dr
            ti.note = "placeholder-note"

            for key, value in self.ti_extras.items():
                setattr(ti, key, value)
            session.add(ti)
            tis.append(ti)

        session.commit()
        return tis


class TestGetTaskInstance(TestTaskInstanceEndpoint):
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize("username", ["test", "test_dag_read_only", "test_task_read_only"])
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
        assert response.json == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": None,
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 11,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "trigger": None,
            "triggerer_job": None,
        }

    def test_should_respond_200_with_task_state_in_deferred(self, session):
        now = pendulum.now("UTC")
        ti = self.create_task_instances(
            session, task_instances=[{"state": State.DEFERRED}], update_extras=True
        )[0]
        ti.trigger = Trigger("none", {})
        ti.trigger.created_date = now
        ti.triggerer_job = Job()
        TriggererJobRunner(job=ti.triggerer_job)
        ti.triggerer_job.state = "running"
        session.commit()
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": "test"},
        )
        data = response.json

        # this logic in effect replicates mock.ANY for these values
        values_to_ignore = {
            "trigger": ["created_date", "id", "triggerer_id"],
            "triggerer_job": ["executor_class", "hostname", "id", "latest_heartbeat", "start_date"],
        }
        for k, v in values_to_ignore.items():
            for elem in v:
                del data[k][elem]

        assert response.status_code == 200
        assert data == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "_PythonDecoratedOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 11,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "deferred",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "trigger": {
                "classpath": "none",
                "kwargs": "{}",
            },
            "triggerer_job": {
                "dag_id": None,
                "end_date": None,
                "job_type": "TriggererJob",
                "state": "running",
                "unixname": getuser(),
            },
        }

    def test_should_respond_200_with_task_state_in_removed(self, session):
        self.create_task_instances(session, task_instances=[{"state": State.REMOVED}], update_extras=True)
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "_PythonDecoratedOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 11,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "removed",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "trigger": None,
            "triggerer_job": None,
        }

    def test_should_respond_200_task_instance_with_sla_and_rendered(self, session):
        tis = self.create_task_instances(session)
        session.query()
        sla_miss = SlaMiss(
            task_id="print_the_context",
            dag_id="example_python_operator",
            execution_date=self.default_time,
            timestamp=self.default_time,
        )
        session.add(sla_miss)
        rendered_fields = RTIF(tis[0], render_templates=False)
        session.add(rendered_fields)
        session.commit()
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200

        assert response.json == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "_PythonDecoratedOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 11,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": {
                "dag_id": "example_python_operator",
                "description": None,
                "email_sent": False,
                "execution_date": "2020-01-01T00:00:00+00:00",
                "notification_sent": False,
                "task_id": "print_the_context",
                "timestamp": "2020-01-01T00:00:00+00:00",
            },
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "trigger": None,
            "triggerer_job": None,
        }

    def test_should_respond_200_mapped_task_instance_with_rtif(self, session):
        """Verify we don't duplicate rows through join to RTIF"""
        tis = self.create_task_instances(session)
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(task=old_ti.task, run_id=old_ti.run_id, map_index=idx)
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.commit()

        # in each loop, we should get the right mapped TI back
        for map_index in (1, 2):
            response = self.client.get(
                "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                f"/print_the_context/{map_index}",
                environ_overrides={"REMOTE_USER": "test"},
            )
            assert response.status_code == 200

            assert response.json == {
                "dag_id": "example_python_operator",
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00+00:00",
                "execution_date": "2020-01-01T00:00:00+00:00",
                "executor_config": "{}",
                "hostname": "",
                "map_index": map_index,
                "max_tries": 0,
                "note": "placeholder-note",
                "operator": "_PythonDecoratedOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 11,
                "queue": "default_queue",
                "queued_when": None,
                "sla_miss": None,
                "start_date": "2020-01-02T00:00:00+00:00",
                "state": "running",
                "task_id": "print_the_context",
                "try_number": 0,
                "unixname": getuser(),
                "dag_run_id": "TEST_DAG_RUN_ID",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "trigger": None,
                "triggerer_job": None,
            }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(
            "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_unmapped_map_index_should_return_404(self, session):
        self.create_task_instances(session)
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/-1",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404

    def test_should_return_404_for_mapped_endpoint(self, session):
        self.create_task_instances(session)
        for index in ["0", "1", "2"]:
            response = self.client.get(
                "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/"
                f"taskInstances/print_the_context/{index}",
                environ_overrides={"REMOTE_USER": "test"},
            )
            assert response.status_code == 404

    def test_should_return_404_for_list_mapped_endpoint(self, session):
        self.create_task_instances(session)
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/"
            "taskInstances/print_the_context/listMapped",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404


class TestGetTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "task_instances, update_extras, url, expected_ti",
        [
            pytest.param(
                [
                    {"execution_date": DEFAULT_DATETIME_1},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                False,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/~/"
                    f"taskInstances?execution_date_lte={QUOTED_DEFAULT_DATETIME_STR_1}"
                ),
                1,
                id="test execution date filter",
            ),
            pytest.param(
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances"
                    f"?start_date_gte={QUOTED_DEFAULT_DATETIME_STR_1}&"
                    f"start_date_lte={QUOTED_DEFAULT_DATETIME_STR_2}"
                ),
                2,
                id="test start date filter",
            ),
            pytest.param(
                [
                    {"end_date": DEFAULT_DATETIME_1},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances?"
                    f"end_date_gte={QUOTED_DEFAULT_DATETIME_STR_1}&"
                    f"end_date_lte={QUOTED_DEFAULT_DATETIME_STR_2}"
                ),
                2,
                id="test end date filter",
            ),
            pytest.param(
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/"
                    "taskInstances?duration_gte=100&duration_lte=200"
                ),
                3,
                id="test duration filter",
            ),
            pytest.param(
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                "/api/v1/dags/~/dagRuns/~/taskInstances?duration_gte=100&duration_lte=200",
                3,
                id="test duration filter ~",
            ),
            pytest.param(
                [
                    {"state": State.RUNNING},
                    {"state": State.QUEUED},
                    {"state": State.SUCCESS},
                    {"state": State.NONE},
                ],
                False,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/"
                    "TEST_DAG_RUN_ID/taskInstances?state=running,queued,none"
                ),
                3,
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
                ("/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                4,
                id="test null states with no filter",
            ),
            pytest.param(
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/"
                    "TEST_DAG_RUN_ID/taskInstances?pool=test_pool_1,test_pool_2"
                ),
                2,
                id="test pool filter",
            ),
            pytest.param(
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                "/api/v1/dags/~/dagRuns/~/taskInstances?pool=test_pool_1,test_pool_2",
                2,
                id="test pool filter ~",
            ),
            pytest.param(
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                (
                    "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID"
                    "/taskInstances?queue=test_queue_1,test_queue_2"
                ),
                2,
                id="test queue filter",
            ),
            pytest.param(
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                "/api/v1/dags/~/dagRuns/~/taskInstances?queue=test_queue_1,test_queue_2",
                2,
                id="test queue filter ~",
            ),
        ],
    )
    def test_should_respond_200(self, task_instances, update_extras, url, expected_ti, session):

        self.create_task_instances(
            session,
            update_extras=update_extras,
            task_instances=task_instances,
        )
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == expected_ti
        assert len(response.json["task_instances"]) == expected_ti

    def test_should_respond_200_for_dag_id_filter(self, session):
        self.create_task_instances(session)
        self.create_task_instances(session, dag_id="example_skip_dag")
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        count = session.query(TaskInstance).filter(TaskInstance.dag_id == "example_python_operator").count()
        assert count == response.json["total_entries"]
        assert count == len(response.json["task_instances"])

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances",
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/dags/example_python_operator/dagRuns/~/taskInstances",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403


class TestGetTaskInstancesBatch(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "task_instances, update_extras, payload, expected_ti_count, username",
        [
            pytest.param(
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                {"queue": ["test_queue_1", "test_queue_2"]},
                2,
                "test",
                id="test queue filter",
            ),
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
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                {"duration_gte": 100, "duration_lte": 200},
                3,
                "test",
                id="test duration filter",
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
            pytest.param(
                [
                    {"execution_date": DEFAULT_DATETIME_1},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4)},
                    {"execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=5)},
                ],
                False,
                {
                    "execution_date_gte": DEFAULT_DATETIME_1.isoformat(),
                    "execution_date_lte": (DEFAULT_DATETIME_1 + dt.timedelta(days=2)).isoformat(),
                },
                3,
                "test",
                id="with execution date filter",
            ),
        ],
    )
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

    @pytest.mark.parametrize(
        "task_instances, payload, expected_ti_count",
        [
            pytest.param(
                [
                    {"task": "test_1"},
                    {"task": "test_2"},
                ],
                {"dag_ids": ["latest_only"]},
                2,
                id="task_instance properties",
            ),
        ],
    )
    @provide_session
    def test_should_respond_200_when_task_instance_properties_are_none(
        self, task_instances, payload, expected_ti_count, session
    ):
        self.ti_extras.update(
            {
                "start_date": None,
                "end_date": None,
                "state": None,
            }
        )
        self.create_task_instances(
            session,
            dag_id="latest_only",
            task_instances=task_instances,
        )
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200, response.json
        assert expected_ti_count == response.json["total_entries"]
        assert expected_ti_count == len(response.json["task_instances"])

    @pytest.mark.parametrize(
        "payload, expected_ti, total_ti",
        [
            pytest.param(
                {"dag_ids": ["example_python_operator", "example_skip_dag"]},
                19,
                19,
                id="with dag filter",
            ),
        ],
    )
    @provide_session
    def test_should_respond_200_dag_ids_filter(self, payload, expected_ti, total_ti, session):
        self.create_task_instances(session)
        self.create_task_instances(session, dag_id="example_skip_dag")
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200
        assert len(response.json["task_instances"]) == expected_ti
        assert response.json["total_entries"] == total_ti

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            json={"dag_ids": ["example_python_operator", "example_skip_dag"]},
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
            json={"dag_ids": ["example_python_operator", "example_skip_dag"]},
        )
        assert response.status_code == 403

    def test_should_raise_400_for_no_json(self):
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json["detail"] == "Request body must not be empty"

    @pytest.mark.parametrize(
        "payload, expected",
        [
            ({"end_date_lte": "2020-11-10T12:42:39.442973"}, "is not a 'date-time'"),
            ({"end_date_gte": "2020-11-10T12:42:39.442973"}, "is not a 'date-time'"),
            ({"start_date_lte": "2020-11-10T12:42:39.442973"}, "is not a 'date-time'"),
            ({"start_date_gte": "2020-11-10T12:42:39.442973"}, "is not a 'date-time'"),
            ({"execution_date_gte": "2020-11-10T12:42:39.442973"}, "is not a 'date-time'"),
            ({"execution_date_lte": "2020-11-10T12:42:39.442973"}, "is not a 'date-time'"),
        ],
    )
    @provide_session
    def test_should_raise_400_for_naive_and_bad_datetime(self, payload, expected, session):
        self.create_task_instances(session)
        response = self.client.post(
            "/api/v1/dags/~/dagRuns/~/taskInstances/list",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 400
        assert expected in response.json["detail"]


class TestPostClearTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "main_dag, task_instances, request_dag, payload, expected_ti",
        [
            pytest.param(
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": True,
                    "start_date": DEFAULT_DATETIME_STR_2,
                    "only_failed": True,
                },
                2,
                id="clear start date filter",
            ),
            pytest.param(
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": True,
                    "end_date": DEFAULT_DATETIME_STR_2,
                    "only_failed": True,
                },
                2,
                id="clear end date filter",
            ),
            pytest.param(
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.RUNNING,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {"dry_run": True, "only_running": True, "only_failed": False},
                2,
                id="clear only running",
            ),
            pytest.param(
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.RUNNING,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": True,
                    "only_failed": True,
                },
                2,
                id="clear only failed",
            ),
            pytest.param(
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": True,
                    "task_ids": ["print_the_context", "sleep_for_1"],
                },
                2,
                id="clear by task ids",
            ),
            pytest.param(
                "example_subdag_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4),
                        "state": State.FAILED,
                    },
                ],
                "example_subdag_operator.section-1",
                {"dry_run": True, "include_parentdag": True},
                4,
                id="include parent dag",
            ),
            pytest.param(
                "example_subdag_operator.section-1",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                        "state": State.FAILED,
                    },
                ],
                "example_subdag_operator",
                {
                    "dry_run": True,
                    "include_subdags": True,
                },
                4,
                id="include sub dag",
            ),
            pytest.param(
                "example_python_operator",
                [
                    {"execution_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.RUNNING,
                    },
                ],
                "example_python_operator",
                {
                    "only_failed": True,
                },
                2,
                id="dry_run default",
            ),
        ],
    )
    @provide_session
    def test_should_respond_200(self, main_dag, task_instances, request_dag, payload, expected_ti, session):
        self.create_task_instances(
            session,
            dag_id=main_dag,
            task_instances=task_instances,
            update_extras=False,
        )
        self.app.dag_bag.sync_to_db()
        response = self.client.post(
            f"/api/v1/dags/{request_dag}/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200
        assert len(response.json["task_instances"]) == expected_ti

    @mock.patch("airflow.api_connexion.endpoints.task_instance_endpoint.clear_task_instances")
    def test_clear_taskinstance_is_called_with_queued_dr_state(self, mock_clearti, session):
        """Test that if reset_dag_runs is True, then clear_task_instances is called with State.QUEUED"""
        self.create_task_instances(session)
        dag_id = "example_python_operator"
        payload = {"include_subdags": True, "reset_dag_runs": True, "dry_run": False}
        self.app.dag_bag.sync_to_db()
        response = self.client.post(
            f"/api/v1/dags/{dag_id}/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200
        mock_clearti.assert_called_once_with(
            [], session, dag=self.app.dag_bag.get_dag(dag_id), dag_run_state=State.QUEUED
        )

    def test_clear_taskinstance_is_called_with_invalid_task_ids(self, session):
        """Test that dagrun is running when invalid task_ids are passed to clearTaskInstances API."""
        dag_id = "example_python_operator"
        tis = self.create_task_instances(session)
        dagrun = tis[0].get_dagrun()
        assert dagrun.state == "running"

        payload = {"dry_run": False, "reset_dag_runs": True, "task_ids": [""]}
        self.app.dag_bag.sync_to_db()
        response = self.client.post(
            f"/api/v1/dags/{dag_id}/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 200

        dagrun.refresh_from_db()
        assert dagrun.state == "running"
        assert all(ti.state == "running" for ti in tis)

    def test_should_respond_200_with_reset_dag_run(self, session):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": True,
            "only_failed": False,
            "only_running": True,
            "include_subdags": True,
        }
        task_instances = [
            {"execution_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.RUNNING,
            },
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                "state": State.RUNNING,
            },
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                "state": State.RUNNING,
            },
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4),
                "state": State.RUNNING,
            },
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=5),
                "state": State.RUNNING,
            },
        ]

        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=task_instances,
            update_extras=False,
            dag_run_state=State.FAILED,
        )
        response = self.client.post(
            f"/api/v1/dags/{dag_id}/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )

        failed_dag_runs = session.query(DagRun).filter(DagRun.state == "failed").count()
        assert 200 == response.status_code
        expected_response = [
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_0",
                "execution_date": "2020-01-01T00:00:00+00:00",
                "task_id": "print_the_context",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "execution_date": "2020-01-02T00:00:00+00:00",
                "task_id": "log_sql_query",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_2",
                "execution_date": "2020-01-03T00:00:00+00:00",
                "task_id": "sleep_for_0",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_3",
                "execution_date": "2020-01-04T00:00:00+00:00",
                "task_id": "sleep_for_1",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_4",
                "execution_date": "2020-01-05T00:00:00+00:00",
                "task_id": "sleep_for_2",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_5",
                "execution_date": "2020-01-06T00:00:00+00:00",
                "task_id": "sleep_for_3",
            },
        ]
        for task_instance in expected_response:
            assert task_instance in response.json["task_instances"]
        assert 6 == len(response.json["task_instances"])
        assert 0 == failed_dag_runs, 0

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/clearTaskInstances",
            json={
                "dry_run": False,
                "reset_dag_runs": True,
                "only_failed": False,
                "only_running": True,
                "include_subdags": True,
            },
        )
        assert_401(response)

    @pytest.mark.parametrize("username", ["test_no_permissions", "test_dag_read_only", "test_task_read_only"])
    def test_should_raise_403_forbidden(self, username: str):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/clearTaskInstances",
            environ_overrides={"REMOTE_USER": username},
            json={
                "dry_run": False,
                "reset_dag_runs": True,
                "only_failed": False,
                "only_running": True,
                "include_subdags": True,
            },
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "payload, expected",
        [
            ({"end_date": "2020-11-10T12:42:39.442973"}, "Naive datetime is disallowed"),
            ({"end_date": "2020-11-10T12:4po"}, "{'end_date': ['Not a valid datetime.']}"),
            ({"start_date": "2020-11-10T12:42:39.442973"}, "Naive datetime is disallowed"),
            ({"start_date": "2020-11-10T12:4po"}, "{'start_date': ['Not a valid datetime.']}"),
        ],
    )
    @provide_session
    def test_should_raise_400_for_naive_and_bad_datetime(self, payload, expected, session):
        task_instances = [
            {"execution_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "execution_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.RUNNING,
            },
        ]
        self.create_task_instances(
            session,
            dag_id="example_python_operator",
            task_instances=task_instances,
            update_extras=False,
        )
        self.app.dag_bag.sync_to_db()
        response = self.client.post(
            "/api/v1/dags/example_python_operator/clearTaskInstances",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 400
        assert response.json["detail"] == expected


class TestPostSetTaskInstanceState(TestTaskInstanceEndpoint):
    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
    def test_should_assert_call_mocked_api(self, mock_set_task_instance_state, session):
        self.create_task_instances(session)
        mock_set_task_instance_state.return_value = (
            session.query(TaskInstance)
            .join(TaskInstance.dag_run)
            .options(contains_eager(TaskInstance.dag_run))
            .filter(TaskInstance.task_id == "print_the_context")
            .all()
        )
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": "test"},
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
        assert response.status_code == 200
        assert response.json == {
            "task_instances": [
                {
                    "dag_id": "example_python_operator",
                    "dag_run_id": "TEST_DAG_RUN_ID",
                    "execution_date": "2020-01-01T00:00:00+00:00",
                    "task_id": "print_the_context",
                }
            ]
        }

        mock_set_task_instance_state.assert_called_once_with(
            commit=False,
            downstream=True,
            run_id=None,
            execution_date=DEFAULT_DATETIME_1,
            future=True,
            past=True,
            state="failed",
            task_id="print_the_context",
            upstream=True,
            session=session,
        )

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
    def test_should_assert_call_mocked_api_when_run_id(self, mock_set_task_instance_state, session):
        self.create_task_instances(session)
        run_id = "TEST_DAG_RUN_ID"
        mock_set_task_instance_state.return_value = (
            session.query(TaskInstance)
            .join(TaskInstance.dag_run)
            .filter(TaskInstance.task_id == "print_the_context")
            .all()
        )
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": True,
                "task_id": "print_the_context",
                "dag_run_id": run_id,
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 200
        assert response.json == {
            "task_instances": [
                {
                    "dag_id": "example_python_operator",
                    "dag_run_id": "TEST_DAG_RUN_ID",
                    "execution_date": "2020-01-01T00:00:00+00:00",
                    "task_id": "print_the_context",
                }
            ]
        }

        mock_set_task_instance_state.assert_called_once_with(
            commit=False,
            downstream=True,
            run_id=run_id,
            execution_date=None,
            future=True,
            past=True,
            state="failed",
            task_id="print_the_context",
            upstream=True,
            session=session,
        )

    @pytest.mark.parametrize(
        "error, code, payload",
        [
            [
                "{'_schema': ['Exactly one of execution_date or dag_run_id must be provided']}",
                400,
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
            ],
            [
                "Task instance not found for task 'print_the_context' on execution_date "
                "2021-01-01 00:00:00+00:00",
                404,
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "execution_date": "2021-01-01T00:00:00+00:00",
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
            ],
            [
                "Task instance not found for task 'print_the_context' on DAG run with ID 'TEST_DAG_RUN_'",
                404,
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "dag_run_id": "TEST_DAG_RUN_",
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
            ],
            [
                "{'_schema': ['Exactly one of execution_date or dag_run_id must be provided']}",
                400,
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "dag_run_id": "TEST_DAG_RUN_",
                    "execution_date": "2020-01-01T00:00:00+00:00",
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
            ],
        ],
    )
    def test_should_handle_errors(self, error, code, payload, session):
        self.create_task_instances(session)
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == code
        assert response.json["detail"] == error

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
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
        assert_401(response)

    @pytest.mark.parametrize("username", ["test_no_permissions", "test_dag_read_only", "test_task_read_only"])
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

    def test_should_raise_404_not_found_dag(self):
        response = self.client.post(
            "/api/v1/dags/INVALID_DAG/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": "test"},
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
        assert response.status_code == 404

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
    def test_should_raise_not_found_if_execution_date_is_wrong(self, mock_set_task_instance_state, session):
        self.create_task_instances(session)
        date = DEFAULT_DATETIME_1 + dt.timedelta(days=1)
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": True,
                "task_id": "print_the_context",
                "execution_date": date.isoformat(),
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 404
        assert response.json["detail"] == (
            f"Task instance not found for task 'print_the_context' on execution_date {date}"
        )
        assert mock_set_task_instance_state.call_count == 0

    def test_should_raise_404_not_found_task(self):
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": True,
                "task_id": "INVALID_TASK",
                "execution_date": DEFAULT_DATETIME_1.isoformat(),
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 404

    @pytest.mark.parametrize(
        "payload, expected",
        [
            (
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "execution_date": "2020-11-10T12:42:39.442973",
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
                "Naive datetime is disallowed",
            ),
            (
                {
                    "dry_run": True,
                    "task_id": "print_the_context",
                    "execution_date": "2020-11-10T12:4opfo",
                    "include_upstream": True,
                    "include_downstream": True,
                    "include_future": True,
                    "include_past": True,
                    "new_state": "failed",
                },
                "{'execution_date': ['Not a valid datetime.']}",
            ),
        ],
    )
    @provide_session
    def test_should_raise_400_for_naive_and_bad_datetime(self, payload, expected, session):
        self.create_task_instances(session)
        response = self.client.post(
            "/api/v1/dags/example_python_operator/updateTaskInstancesState",
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 400
        assert response.json["detail"] == expected


class TestPatchTaskInstance(TestTaskInstanceEndpoint):
    ENDPOINT_URL = (
        "/api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
    )

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
    def test_should_call_mocked_api(self, mock_set_task_instance_state, session):
        self.create_task_instances(session)

        NEW_STATE = "failed"
        mock_set_task_instance_state.return_value = session.get(
            TaskInstance,
            {
                "task_id": "print_the_context",
                "dag_id": "example_python_operator",
                "run_id": "TEST_DAG_RUN_ID",
                "map_index": -1,
            },
        )
        response = self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": False,
                "new_state": NEW_STATE,
            },
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_id": "example_python_operator",
            "dag_run_id": "TEST_DAG_RUN_ID",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "task_id": "print_the_context",
        }

        mock_set_task_instance_state.assert_called_once_with(
            task_id="print_the_context",
            run_id="TEST_DAG_RUN_ID",
            map_indexes=[-1],
            state=NEW_STATE,
            commit=True,
            session=session,
        )

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
    def test_should_not_call_mocked_api_for_dry_run(self, mock_set_task_instance_state, session):
        self.create_task_instances(session)

        NEW_STATE = "failed"
        mock_set_task_instance_state.return_value = session.get(
            TaskInstance,
            {
                "task_id": "print_the_context",
                "dag_id": "example_python_operator",
                "run_id": "TEST_DAG_RUN_ID",
                "map_index": -1,
            },
        )
        response = self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": True,
                "new_state": NEW_STATE,
            },
        )
        assert response.status_code == 200
        print(response.status_code)
        assert response.json == {
            "dag_id": "example_python_operator",
            "dag_run_id": "TEST_DAG_RUN_ID",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "task_id": "print_the_context",
        }

        mock_set_task_instance_state.assert_not_called()

    def test_should_update_task_instance_state(self, session):
        self.create_task_instances(session)

        NEW_STATE = "failed"

        self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": False,
                "new_state": NEW_STATE,
            },
        )

        response2 = self.client.get(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json={},
        )
        assert response2.status_code == 200
        assert response2.json["state"] == NEW_STATE

    def test_should_update_mapped_task_instance_state(self, session):
        NEW_STATE = "failed"
        map_index = 1
        tis = self.create_task_instances(session)
        ti = TaskInstance(task=tis[0].task, run_id=tis[0].run_id, map_index=map_index)
        ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
        session.add(ti)
        session.commit()

        self.client.patch(
            f"{self.ENDPOINT_URL}/{map_index}",
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": False,
                "new_state": NEW_STATE,
            },
        )

        response2 = self.client.get(
            f"{self.ENDPOINT_URL}/{map_index}",
            environ_overrides={"REMOTE_USER": "test"},
            json={},
        )
        assert response2.status_code == 200
        assert response2.json["state"] == NEW_STATE

    @pytest.mark.parametrize(
        "error, code, payload",
        [
            [
                "Task instance not found for task 'print_the_context' on DAG run with ID 'TEST_DAG_RUN_ID'",
                404,
                {
                    "dry_run": True,
                    "new_state": "failed",
                },
            ]
        ],
    )
    def test_should_handle_errors(self, error, code, payload, session):
        response = self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == code
        assert response.json["detail"] == error

    def test_should_raises_401_unauthenticated(self):
        response = self.client.patch(
            self.ENDPOINT_URL,
            json={
                "dry_run": False,
                "new_state": "failed",
            },
        )
        assert_401(response)

    @pytest.mark.parametrize("username", ["test_no_permissions", "test_dag_read_only", "test_task_read_only"])
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

    def test_should_raise_404_not_found_dag(self):
        response = self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 404

    def test_should_raise_404_not_found_task(self):
        response = self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json={
                "dry_run": True,
                "new_state": "failed",
            },
        )
        assert response.status_code == 404

    @pytest.mark.parametrize(
        "payload, expected",
        [
            (
                {
                    "dry_run": True,
                    "new_state": "failede",
                },
                f"'failede' is not one of ['{State.SUCCESS}', '{State.FAILED}'] - 'new_state'",
            ),
            (
                {
                    "dry_run": True,
                    "new_state": "queued",
                },
                f"'queued' is not one of ['{State.SUCCESS}', '{State.FAILED}'] - 'new_state'",
            ),
        ],
    )
    @provide_session
    def test_should_raise_400_for_invalid_task_instance_state(self, payload, expected, session):
        self.create_task_instances(session)
        response = self.client.patch(
            self.ENDPOINT_URL,
            environ_overrides={"REMOTE_USER": "test"},
            json=payload,
        )
        assert response.status_code == 400
        assert response.json["detail"] == expected
        assert response.json["detail"] == expected


class TestSetTaskInstanceNote(TestTaskInstanceEndpoint):
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @provide_session
    def test_should_respond_200(self, session):
        tis = self.create_task_instances(session)
        new_note_value = "My super cool TaskInstance note."
        response = self.client.patch(
            "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/setNote",
            json={"note": new_note_value},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200, response.text
        assert response.json == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00+00:00",
            "execution_date": "2020-01-01T00:00:00+00:00",
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "note": new_note_value,
            "operator": "_PythonDecoratedOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 11,
            "queue": "default_queue",
            "queued_when": None,
            "sla_miss": None,
            "start_date": "2020-01-02T00:00:00+00:00",
            "state": "running",
            "task_id": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "trigger": None,
            "triggerer_job": None,
        }
        ti = tis[0]
        assert ti.task_instance_note.user_id is not None

    def test_should_respond_200_mapped_task_instance_with_rtif(self, session):
        """Verify we don't duplicate rows through join to RTIF"""
        tis = self.create_task_instances(session)
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(task=old_ti.task, run_id=old_ti.run_id, map_index=idx)
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.commit()

        # in each loop, we should get the right mapped TI back
        for map_index in (1, 2):
            new_note_value = f"My super cool TaskInstance note {map_index}"
            response = self.client.patch(
                "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
                f"print_the_context/{map_index}/setNote",
                json={"note": new_note_value},
                environ_overrides={"REMOTE_USER": "test"},
            )
            assert response.status_code == 200, response.text

            assert response.json == {
                "dag_id": "example_python_operator",
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00+00:00",
                "execution_date": "2020-01-01T00:00:00+00:00",
                "executor_config": "{}",
                "hostname": "",
                "map_index": map_index,
                "max_tries": 0,
                "note": new_note_value,
                "operator": "_PythonDecoratedOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 11,
                "queue": "default_queue",
                "queued_when": None,
                "sla_miss": None,
                "start_date": "2020-01-02T00:00:00+00:00",
                "state": "running",
                "task_id": "print_the_context",
                "try_number": 0,
                "unixname": getuser(),
                "dag_run_id": "TEST_DAG_RUN_ID",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "trigger": None,
                "triggerer_job": None,
            }

    def test_should_raises_401_unauthenticated(self):
        for map_index in ["", "/0"]:
            url = (
                "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
                f"print_the_context{map_index}/setNote"
            )
            response = self.client.patch(
                url,
                json={"note": "I am setting a note while being unauthenticated."},
            )
            assert_401(response)

    def test_should_raise_403_forbidden(self):
        for map_index in ["", "/0"]:
            response = self.client.patch(
                "api/v1/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
                f"print_the_context{map_index}/setNote",
                json={"note": "I am setting a note without the proper permissions."},
                environ_overrides={"REMOTE_USER": "test_no_permissions"},
            )
            assert response.status_code == 403

    def test_should_respond_404(self, session):
        self.create_task_instances(session)
        for map_index in ["", "/0"]:
            response = self.client.patch(
                f"api/v1/dags/INVALID_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
                f"{map_index}/setNote",
                json={"note": "I am setting a note on a DAG that doesn't exist."},
                environ_overrides={"REMOTE_USER": "test"},
            )
            assert response.status_code == 404
