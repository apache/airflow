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
import itertools
import os
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pendulum
import pytest
from sqlalchemy import select

from airflow._shared.timezones.timezone import datetime
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagRun, Log, TaskInstance
from airflow.models.dag_version import DagVersion
from airflow.models.hitl import HITLDetail
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskmap import TaskMap
from airflow.models.trigger import Trigger
from airflow.sdk import BaseOperator
from airflow.utils.platform import getuser
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.api_fastapi import _check_task_instance_note
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_runs,
    clear_rendered_ti_fields,
)
from tests_common.test_utils.logs import check_last_log
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.taskinstance import create_task_instance

if TYPE_CHECKING:
    from tests_common.pytest_plugin import CreateTaskInstance

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

    def setup_method(self):
        self.clear_db()

    def teardown_method(self):
        self.clear_db()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, dagbag) -> None:
        self.default_time = DEFAULT
        self.ti_init = {
            "logical_date": self.default_time,
            "state": State.RUNNING,
        }
        self.ti_extras = {
            "start_date": self.default_time + dt.timedelta(days=1),
            "end_date": self.default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
        }
        clear_db_runs()
        clear_rendered_ti_fields()
        self.dagbag = dagbag

    def create_task_instances(
        self,
        session,
        dag_id: str = "example_python_operator",
        update_extras: bool = True,
        task_instances=None,
        dag_run_state=DagRunState.RUNNING,
        with_ti_history=False,
    ):
        """Method to create task instances using kwargs and default arguments"""
        dag = self.dagbag.get_latest_version_of_dag(dag_id, session=session)
        tasks = dag.tasks
        counter = len(tasks)
        if task_instances is not None:
            counter = min(len(task_instances), counter)

        run_id = "TEST_DAG_RUN_ID"
        logical_date = self.ti_init.pop("logical_date", self.default_time)
        dr = None
        dag_version = DagVersion.get_latest_version(dag.dag_id, session=session)
        tis = []
        for i in range(counter):
            map_indexes = (-1,)
            if task_instances:
                map_index = task_instances[i].get("map_index", -1)
                map_indexes = task_instances[i].pop("map_indexes", (map_index,))
                if update_extras:
                    self.ti_extras.update(task_instances[i])
                else:
                    self.ti_init.update(task_instances[i])

            if "logical_date" in self.ti_init:
                run_id = f"TEST_DAG_RUN_ID_{i}"
                logical_date = self.ti_init.pop("logical_date")
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

            for mi in map_indexes:
                kwargs = self.ti_init | {"map_index": mi}
                ti = TaskInstance(task=tasks[i], **kwargs, dag_version_id=dag_version.id)
                session.add(ti)
                ti.dag_run = dr
                ti.note = "placeholder-note"

                for key, value in self.ti_extras.items():
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


class TestGetTaskInstance(TestTaskInstanceEndpoint):
    def test_should_respond_200(self, test_client, session):
        self.create_task_instances(session)
        # Update ti and set operator to None to
        # test that operator field is nullable.
        # This prevents issue when users upgrade to 2.0+
        # from 1.10.x
        # https://github.com/apache/airflow/issues/14421
        session.query(TaskInstance).update({TaskInstance.operator: None}, synchronize_session="fetch")
        session.commit()
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": response_data["id"],
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": None,
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "running",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "rendered_map_index": None,
            "run_after": "2020-01-01T00:00:00Z",
            "trigger": None,
            "triggerer_job": None,
        }

    def test_should_respond_200_with_decorator(self, test_client, session):
        self.create_task_instances(session, "example_python_decorator")
        response = test_client.get(
            "/dags/example_python_decorator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["operator_name"] == "@task"
        assert response_json["operator"] == "_PythonDecoratedOperator"

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("run_id", "expected_version_number"),
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.api_fastapi.core_api.datamodels.dag_versions.hasattr")
    def test_should_respond_200_with_versions(
        self, mock_hasattr, test_client, run_id, expected_version_number
    ):
        mock_hasattr.return_value = False
        response = test_client.get(f"/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1")
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "id": response_data["id"],
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
            "dag_run_id": run_id,
            "dag_display_name": "dag_with_multiple_versions",
            "map_index": -1,
            "logical_date": mock.ANY,
            "start_date": None,
            "end_date": mock.ANY,
            "duration": None,
            "state": None,
            "try_number": 0,
            "max_tries": 0,
            "task_display_name": "task1",
            "hostname": "",
            "unixname": getuser(),
            "pool": "default_pool",
            "pool_slots": 1,
            "queue": "default",
            "priority_weight": 1,
            "operator": "EmptyOperator",
            "operator_name": "EmptyOperator",
            "queued_when": None,
            "scheduled_when": None,
            "pid": None,
            "executor": None,
            "executor_config": "{}",
            "note": None,
            "rendered_map_index": None,
            "rendered_fields": {},
            "run_after": mock.ANY,
            "trigger": None,
            "triggerer_job": None,
            "dag_version": {
                "id": response_data["dag_version"]["id"],
                "version_number": expected_version_number,
                "dag_id": "dag_with_multiple_versions",
                "dag_display_name": "dag_with_multiple_versions",
                "bundle_name": "dag_maker",
                "bundle_version": f"some_commit_hash{expected_version_number}",
                "bundle_url": f"http://test_host.github.com/tree/some_commit_hash{expected_version_number}/dags",
                "created_at": response_data["dag_version"]["created_at"],
            },
        }

    def test_should_respond_200_with_task_state_in_deferred(self, test_client, session):
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
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        response_data = response.json()

        # this logic in effect replicates mock.ANY for these values
        values_to_ignore = {
            "trigger": ["created_date", "id", "triggerer_id"],
            "triggerer_job": ["executor_class", "hostname", "id", "latest_heartbeat", "start_date"],
        }
        for k, v in values_to_ignore.items():
            for elem in v:
                del response_data[k][elem]

        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": response_data["id"],
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "deferred",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "run_after": "2020-01-01T00:00:00Z",
            "rendered_fields": {},
            "rendered_map_index": None,
            "trigger": {
                "classpath": "none",
                "kwargs": "{}",
                "queue": None,
            },
            "triggerer_job": {
                "dag_display_name": None,
                "dag_id": None,
                "end_date": None,
                "job_type": "TriggererJob",
                "state": "running",
                "unixname": getuser(),
            },
        }

    def test_should_respond_200_with_task_state_in_removed(self, test_client, session):
        self.create_task_instances(session, task_instances=[{"state": State.REMOVED}], update_extras=True)
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": response_data["id"],
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "removed",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "rendered_map_index": None,
            "run_after": "2020-01-01T00:00:00Z",
            "trigger": None,
            "triggerer_job": None,
        }

    def test_should_respond_200_task_instance_with_rendered(self, test_client, session):
        tis = self.create_task_instances(session)
        session.query()
        rendered_fields = RTIF(tis[0], render_templates=False)
        session.add(rendered_fields)
        session.commit()
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response.json() == {
            "dag_id": "example_python_operator",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": response_data["id"],
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "running",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "rendered_map_index": None,
            "run_after": "2020-01-01T00:00:00Z",
            "trigger": None,
            "triggerer_job": None,
        }

    def test_raises_404_for_nonexistent_task_instance(self, test_client):
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 404
        assert response.json() == {
            "detail": "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID` and task_id: `print_the_context` was not found"
        }

    def test_raises_404_for_mapped_task_instance_with_multiple_indexes(self, test_client, session):
        tis = self.create_task_instances(session)

        old_ti = tis[0]

        for index in range(3):
            ti = TaskInstance(
                task=old_ti.task, run_id=old_ti.run_id, map_index=index, dag_version_id=old_ti.dag_version_id
            )
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.delete(old_ti)
        session.commit()

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "Task instance is mapped, add the map_index value to the URL"}

    def test_raises_404_for_mapped_task_instance_with_one_index(self, test_client, session):
        tis = self.create_task_instances(session)

        old_ti = tis[0]

        ti = TaskInstance(
            task=old_ti.task, run_id=old_ti.run_id, map_index=2, dag_version_id=old_ti.dag_version_id
        )
        for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
            setattr(ti, attr, getattr(old_ti, attr))
        session.add(ti)
        session.delete(old_ti)
        session.commit()

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "Task instance is mapped, add the map_index value to the URL"}


class TestGetMappedTaskInstance(TestTaskInstanceEndpoint):
    def test_should_respond_200_mapped_task_instance_with_rtif(self, test_client, session):
        """Verify we don't duplicate rows through join to RTIF"""
        tis = self.create_task_instances(session)
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(
                task=old_ti.task, run_id=old_ti.run_id, map_index=idx, dag_version_id=old_ti.dag_version_id
            )
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.commit()

        # in each loop, we should get the right mapped TI back
        for map_index in (1, 2):
            response = test_client.get(
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                f"/print_the_context/{map_index}",
            )
            response_data = response.json()
            assert response.status_code == 200
            assert response_data == {
                "dag_id": "example_python_operator",
                "dag_version": {
                    "bundle_name": "dags-folder",
                    "bundle_url": None,
                    "bundle_version": None,
                    "created_at": response_data["dag_version"]["created_at"],
                    "dag_display_name": "example_python_operator",
                    "dag_id": "example_python_operator",
                    "id": response_data["dag_version"]["id"],
                    "version_number": 1,
                },
                "dag_display_name": "example_python_operator",
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00Z",
                "logical_date": "2020-01-01T00:00:00Z",
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "id": response_data["id"],
                "map_index": map_index,
                "max_tries": 0,
                "note": "placeholder-note",
                "operator": "PythonOperator",
                "operator_name": "PythonOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 9,
                "queue": "default_queue",
                "queued_when": None,
                "scheduled_when": None,
                "start_date": "2020-01-02T00:00:00Z",
                "state": "running",
                "task_id": "print_the_context",
                "task_display_name": "print_the_context",
                "try_number": 0,
                "unixname": getuser(),
                "dag_run_id": "TEST_DAG_RUN_ID",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "rendered_map_index": str(map_index),
                "run_after": "2020-01-01T00:00:00Z",
                "trigger": None,
                "triggerer_job": None,
            }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/1",
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/1",
        )
        assert response.status_code == 403

    def test_should_respond_404_wrong_map_index(self, test_client, session):
        self.create_task_instances(session)

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/10",
        )
        assert response.status_code == 404

        assert response.json() == {
            "detail": "The Mapped Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `print_the_context`, and map_index: `10` was not found"
        }


class TestGetMappedTaskInstances:
    @pytest.fixture(autouse=True)
    def setup_attrs(self) -> None:
        self.default_time = DEFAULT_DATETIME_1
        self.ti_init = {
            "logical_date": self.default_time,
            "state": State.RUNNING,
        }
        self.ti_extras = {
            "start_date": self.default_time + dt.timedelta(days=1),
            "end_date": self.default_time + dt.timedelta(days=2),
            "pid": 100,
            "duration": 10000,
            "pool": "default_pool",
            "queue": "default_queue",
        }
        clear_db_runs()
        clear_rendered_ti_fields()

    def create_dag_runs_with_mapped_tasks(self, dag_maker, session, dags=None):
        for dag_id, dag in (dags or {}).items():
            count = dag["success"] + dag["running"]
            with dag_maker(
                session=session, dag_id=dag_id, start_date=DEFAULT_DATETIME_1, serialized=True
            ) as sdag:
                task1 = BaseOperator(task_id="op1")
                mapped = MockOperator.partial(task_id="task_2", executor="default").expand(arg2=task1.output)

            dr = dag_maker.create_dagrun(
                run_id=f"run_{dag_id}",
                logical_date=DEFAULT_DATETIME_1,
                data_interval=(DEFAULT_DATETIME_1, DEFAULT_DATETIME_2),
            )
            dag_version = DagVersion.get_latest_version(dag_id)
            session.add(
                TaskMap(
                    dag_id=dr.dag_id,
                    task_id=task1.task_id,
                    run_id=dr.run_id,
                    map_index=-1,
                    length=count,
                    keys=None,
                )
            )

            if count:
                # Remove the map_index=-1 TI when we're creating other TIs
                session.query(TaskInstance).filter(
                    TaskInstance.dag_id == mapped.dag_id,
                    TaskInstance.task_id == mapped.task_id,
                    TaskInstance.run_id == dr.run_id,
                ).delete()

            for index, state in enumerate(
                itertools.chain(
                    itertools.repeat(TaskInstanceState.SUCCESS, dag["success"]),
                    itertools.repeat(TaskInstanceState.FAILED, dag["failed"]),
                    itertools.repeat(TaskInstanceState.RUNNING, dag["running"]),
                )
            ):
                ti = create_task_instance(
                    mapped, run_id=dr.run_id, map_index=index, state=state, dag_version_id=dag_version.id
                )
                setattr(ti, "start_date", DEFAULT_DATETIME_1)
                session.add(ti)

            DagBundlesManager().sync_bundles_to_db()
            dagbag = DagBag(os.devnull, include_examples=False)
            dagbag.dags = {dag_id: dag_maker.dag}
            sync_bag_to_db(dagbag, "dags-folder", None)
            session.flush()

            TaskMap.expand_mapped_task(sdag.task_dict[mapped.task_id], dr.run_id, session=session)

    @pytest.fixture
    def one_task_with_mapped_tis(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                "mapped_tis": {
                    "success": 3,
                    "failed": 0,
                    "running": 0,
                },
            },
        )

    @pytest.fixture
    def one_task_with_single_mapped_ti(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                "mapped_tis": {
                    "success": 1,
                    "failed": 0,
                    "running": 0,
                },
            },
        )

    @pytest.fixture
    def one_task_with_many_mapped_tis(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                "mapped_tis": {
                    "success": 5,
                    "failed": 20,
                    "running": 85,
                },
            },
        )

    @pytest.fixture
    def one_task_with_zero_mapped_tis(self, dag_maker, session):
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={
                "mapped_tis": {
                    "success": 0,
                    "failed": 0,
                    "running": 0,
                },
            },
        )

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
        )
        assert response.status_code == 403

    def test_should_respond_404(self, test_client):
        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "The Dag with ID: `mapped_tis` was not found"}

    def test_should_respond_200(self, one_task_with_many_mapped_tis, test_client):
        with assert_queries_count(4):
            response = test_client.get(
                "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            )

        assert response.status_code == 200
        assert response.json()["total_entries"] == 110
        assert len(response.json()["task_instances"]) == 50

    def test_offset_limit(self, test_client, one_task_with_many_mapped_tis):
        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"offset": 4, "limit": 10},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 110
        assert len(body["task_instances"]) == 10
        assert list(range(4, 14)) == [ti["map_index"] for ti in body["task_instances"]]

    @pytest.mark.parametrize(
        ("params", "expected_map_indexes"),
        [
            ({"order_by": "map_index", "limit": 100}, list(range(100))),
            ({"order_by": "-map_index", "limit": 100}, list(range(109, 9, -1))),
            (
                {"order_by": "state", "limit": 108},
                list(range(5, 25)) + list(range(25, 110)) + list(range(3)),
            ),
            (
                {"order_by": "-state", "limit": 100},
                list(range(5)[::-1]) + list(range(25, 110)[::-1]) + list(range(15, 25)[::-1]),
            ),
            ({"order_by": "logical_date", "limit": 100}, list(range(100))),
            ({"order_by": "-logical_date", "limit": 100}, list(range(109, 9, -1))),
            ({"order_by": "data_interval_start", "limit": 100}, list(range(100))),
            ({"order_by": "-data_interval_start", "limit": 100}, list(range(109, 9, -1))),
        ],
    )
    def test_mapped_instances_order(
        self, test_client, session, params, expected_map_indexes, one_task_with_many_mapped_tis
    ):
        with assert_queries_count(4):
            response = test_client.get(
                "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
                params=params,
            )

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 110
        assert len(body["task_instances"]) == params["limit"]
        assert expected_map_indexes == [ti["map_index"] for ti in body["task_instances"]]

    # Ordering of nulls values is DB specific.
    @pytest.mark.backend("sqlite")
    @pytest.mark.parametrize(
        ("params", "expected_map_indexes"),
        [
            ({"order_by": "rendered_map_index", "limit": 108}, list(range(1, 109))),  # Asc
            ({"order_by": "-rendered_map_index", "limit": 100}, [0] + list(range(11, 110)[::-1])),  # Desc
        ],
    )
    def test_rendered_map_index_order(
        self, test_client, session, params, expected_map_indexes, one_task_with_many_mapped_tis
    ):
        ti = (
            session.query(TaskInstance)
            .where(TaskInstance.task_id == "task_2", TaskInstance.map_index == 0)
            .first()
        )

        ti._rendered_map_index = "a"

        session.commit()

        with assert_queries_count(4):
            response = test_client.get(
                "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
                params=params,
            )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 110
        assert len(body["task_instances"]) == params["limit"]
        assert expected_map_indexes == [ti["map_index"] for ti in body["task_instances"]]

    def test_with_date(self, test_client, one_task_with_mapped_tis):
        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"start_date_gte": DEFAULT_DATETIME_1},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 3
        assert len(body["task_instances"]) == 3

        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"start_date_gte": DEFAULT_DATETIME_2},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0
        assert body["task_instances"] == []

    def test_with_logical_date(self, test_client, one_task_with_mapped_tis):
        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"logical_date_gte": DEFAULT_DATETIME_1},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 3
        assert len(body["task_instances"]) == 3

        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"logical_date_gte": DEFAULT_DATETIME_2},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0
        assert body["task_instances"] == []

    @pytest.mark.parametrize(
        ("query_params", "expected_total_entries", "expected_task_instance_count"),
        [
            ({"state": "success"}, 3, 3),
            ({"state": "running"}, 0, 0),
            ({"pool": "default_pool"}, 3, 3),
            ({"pool": "test_pool"}, 0, 0),
            ({"queue": "default"}, 3, 3),
            ({"queue": "test_queue"}, 0, 0),
            ({"executor": "default"}, 3, 3),
            ({"executor": "no_exec"}, 0, 0),
            ({"map_index": [0, 1]}, 2, 2),
            ({"map_index": [5]}, 0, 0),
        ],
    )
    def test_mapped_task_instances_filters(
        self,
        test_client,
        one_task_with_mapped_tis,
        query_params,
        expected_total_entries,
        expected_task_instance_count,
    ):
        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params=query_params,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert len(body["task_instances"]) == expected_task_instance_count

    def test_with_zero_mapped(self, test_client, one_task_with_zero_mapped_tis, session):
        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0
        assert body["task_instances"] == []

    def test_should_raise_404_not_found_for_nonexistent_task(
        self, one_task_with_zero_mapped_tis, test_client
    ):
        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/nonexistent_task/listMapped",
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "Task id nonexistent_task not found"


class TestGetTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        ("task_instances", "update_extras", "url", "params", "expected_ti", "expected_queries_number"),
        [
            pytest.param(
                [
                    {"logical_date": DEFAULT_DATETIME_1},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                False,
                "/dags/example_python_operator/dagRuns/~/taskInstances",
                {"logical_date_lte": DEFAULT_DATETIME_1},
                1,
                5,
                id="test logical date filter",
            ),
            pytest.param(
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                "/dags/example_python_operator/dagRuns/~/taskInstances",
                {"start_date_gte": DEFAULT_DATETIME_1, "start_date_lte": DEFAULT_DATETIME_STR_2},
                2,
                5,
                id="test start date filter",
            ),
            pytest.param(
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                "/dags/example_python_operator/dagRuns/~/taskInstances",
                {
                    "start_date_gt": (DEFAULT_DATETIME_1 - dt.timedelta(hours=1)).isoformat(),
                    "start_date_lt": DEFAULT_DATETIME_STR_2,
                },
                1,
                5,
                id="test start date gt and lt filter",
            ),
            pytest.param(
                [
                    {"end_date": DEFAULT_DATETIME_1},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                "/dags/example_python_operator/dagRuns/~/taskInstances?",
                {"end_date_gte": DEFAULT_DATETIME_1, "end_date_lte": DEFAULT_DATETIME_STR_2},
                2,
                5,
                id="test end date filter",
            ),
            pytest.param(
                [
                    {"end_date": DEFAULT_DATETIME_1},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"end_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                "/dags/example_python_operator/dagRuns/~/taskInstances?",
                {
                    "end_date_gt": DEFAULT_DATETIME_1,
                    "end_date_lt": (DEFAULT_DATETIME_2 + dt.timedelta(hours=1)).isoformat(),
                },
                1,
                5,
                id="test end date gt and lt filter",
            ),
            pytest.param(
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances",
                {"duration_gte": 100, "duration_lte": 200},
                3,
                7,
                id="test duration filter",
            ),
            pytest.param(
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"duration_gte": 100, "duration_lte": 200},
                3,
                3,
                id="test duration filter ~",
            ),
            pytest.param(
                [
                    {"duration": 100},
                    {"duration": 150},
                    {"duration": 200},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"duration_gt": 100, "duration_lt": 200},
                1,
                3,
                id="test duration gt and lt filter ~",
            ),
            pytest.param(
                [
                    {"state": State.RUNNING},
                    {"state": State.QUEUED},
                    {"state": State.SUCCESS},
                    {"state": State.NONE},
                ],
                False,
                ("/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"state": ["running", "queued", "none"]},
                3,
                7,
                id="test state filter",
            ),
            pytest.param(
                [
                    {"state": State.RUNNING},
                    {"state": State.QUEUED},
                    {"state": State.SUCCESS},
                    {"state": State.NONE},
                ],
                False,
                ("/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"state": ["no_status"]},
                1,
                7,
                id="test no_status state filter",
            ),
            pytest.param(
                [
                    {"state": State.NONE},
                    {"state": State.NONE},
                    {"state": State.NONE},
                    {"state": State.NONE},
                ],
                False,
                ("/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {},
                4,
                7,
                id="test null states with no filter",
            ),
            pytest.param(
                [{"start_date": None, "end_date": None}],
                True,
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances",
                {"start_date_gte": DEFAULT_DATETIME_STR_1},
                1,
                7,
                id="test start_date coalesce with null",
            ),
            pytest.param(
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                ("/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"pool": ["test_pool_1", "test_pool_2"]},
                2,
                7,
                id="test pool filter",
            ),
            pytest.param(
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"pool": ["test_pool_1", "test_pool_2"]},
                2,
                3,
                id="test pool filter ~",
            ),
            pytest.param(
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"pool_name_pattern": "test_pool"},
                3,
                3,
                id="test pool_name_pattern filter",
            ),
            pytest.param(
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances",
                {"queue": ["test_queue_1", "test_queue_2"]},
                2,
                7,
                id="test queue filter",
            ),
            pytest.param(
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"queue": ["test_queue_1", "test_queue_2"]},
                2,
                3,
                id="test queue filter ~",
            ),
            pytest.param(
                [
                    {"queue": "test_queue_1"},
                    {"queue": "test_queue_2"},
                    {"queue": "test_queue_3"},
                    {"queue": "other_queue_3"},
                    {"queue": "other_queue_3"},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"queue_name_pattern": "test"},
                3,
                3,
                id="test queue_name_pattern filter",
            ),
            pytest.param(
                [
                    {"executor": "test_exec_1"},
                    {"executor": "test_exec_2"},
                    {"executor": "test_exec_3"},
                ],
                True,
                ("/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"executor": ["test_exec_1", "test_exec_2"]},
                2,
                7,
                id="test_executor_filter",
            ),
            pytest.param(
                [
                    {"executor": "test_exec_1"},
                    {"executor": "test_exec_2"},
                    {"executor": "test_exec_3"},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"executor": ["test_exec_1", "test_exec_2"]},
                2,
                3,
                id="test executor filter ~",
            ),
            pytest.param(
                [
                    {"_task_display_property_value": "task_name_1"},
                    {"_task_display_property_value": "task_name_2"},
                    {"_task_display_property_value": "task_not_match_name_3"},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"task_display_name_pattern": "task_name"},
                2,
                3,
                id="test task_display_name_pattern filter",
            ),
            pytest.param(
                "task_group_test",
                True,
                ("/dags/example_task_group/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"task_group_id": "section_1"},
                3,
                7,
                id="test task_group filter with exact match",
            ),
            pytest.param(
                "task_group_test",
                True,
                ("/dags/example_task_group/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"task_group_id": "section_2"},
                4,  # section_2 has 4 tasks: task_1 + inner_section_2 (task_2, task_3, task_4)
                7,
                id="test task_group filter exact match on group_id",
            ),
            pytest.param(
                [
                    {"task_id": "task_match_id_1"},
                    {"task_id": "task_match_id_2"},
                    {"task_id": "task_match_id_3"},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"task_id": "task_match_id_2"},
                1,
                3,
                id="test task_id filter",
            ),
            pytest.param(
                [
                    {},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"version_number": [2]},
                2,
                3,
                id="test version number filter",
            ),
            pytest.param(
                [
                    {},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"version_number": [1, 2, 3]},
                7,  # apart from the TIs in the fixture, we also get one from
                # the create_task_instances method
                3,
                id="test multiple version numbers filter",
            ),
            pytest.param(
                [
                    {"try_number": 0},
                    {"try_number": 0},
                    {"try_number": 1},
                    {"try_number": 1},
                    {"try_number": 1},
                    {"try_number": 2},
                ],
                True,
                ("/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"try_number": [0, 1]},
                5,
                7,
                id="test_try_number_filter",
            ),
            pytest.param(
                [
                    {"operator": "FirstOperator"},
                    {"operator": "FirstOperator"},
                    {"operator": "SecondOperator"},
                    {"operator": "SecondOperator"},
                    {"operator": "SecondOperator"},
                    {"operator": "ThirdOperator"},
                    {"operator": "ThirdOperator"},
                    {"operator": "ThirdOperator"},
                    {"operator": "ThirdOperator"},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"operator": ["FirstOperator", "SecondOperator"]},
                5,
                3,
                id="test operator type filter filter",
            ),
            pytest.param(
                [
                    {"custom_operator_name": "CustomFirstOperator"},
                    {"custom_operator_name": "CustomSecondOperator"},
                    {"custom_operator_name": "SecondOperator"},
                    {"custom_operator_name": "ThirdOperator"},
                    {"custom_operator_name": "ThirdOperator"},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"operator_name_pattern": "Custom"},
                2,
                3,
                id="test operator_name_pattern filter",
            ),
            pytest.param(
                [
                    {"map_index": 0},
                    {"map_index": 1},
                    {"map_index": 2},
                    {"map_index": 3},
                    {"map_index": 4},
                    {"map_index": 5},
                    {"map_index": 6},
                    {"map_index": 7},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"map_index": [0, 1]},
                2,
                3,
                id="test map_index filter",
            ),
            pytest.param(
                [
                    {},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"run_id_pattern": "TEST_DAG_"},
                1,  # apart from the TIs in the fixture, we also get one from
                # the create_task_instances method
                3,
                id="test run_id_pattern filter",
            ),
            pytest.param(
                "dag_id_pattern_test",  # Special marker for multi-DAG test
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_pattern": "example_python_operator"},
                9,  # Based on test failure - example_python_operator creates 9 task instances
                3,
                id="test dag_id_pattern exact match",
            ),
            pytest.param(
                "dag_id_pattern_test",  # Special marker for multi-DAG test
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_pattern": "example_%"},
                17,  # Based on test failure - both DAGs together create 17 task instances
                3,
                id="test dag_id_pattern wildcard prefix",
            ),
            pytest.param(
                "dag_id_pattern_test",  # Special marker for multi-DAG test
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_pattern": "%skip%"},
                8,  # Based on test failure - example_skip_dag creates 8 task instances
                3,
                id="test dag_id_pattern wildcard contains",
            ),
            pytest.param(
                "dag_id_pattern_test",  # Special marker for multi-DAG test
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_pattern": "nonexistent"},
                0,
                3,
                id="test dag_id_pattern no match",
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_respond_200(
        self,
        test_client,
        task_instances,
        update_extras,
        url,
        params,
        expected_ti,
        expected_queries_number,
        session,
    ):
        # Special handling for dag_id_pattern tests that require multiple DAGs
        if task_instances == "dag_id_pattern_test":
            # Create task instances for multiple DAGs like the original test_dag_id_pattern_filter
            dag1_id = "example_python_operator"
            dag2_id = "example_skip_dag"
            self.create_task_instances(session, dag_id=dag1_id)
            self.create_task_instances(session, dag_id=dag2_id)
        elif task_instances == "task_group_test":
            # test with task group expansion
            self.create_task_instances(session, dag_id="example_task_group")
        else:
            self.create_task_instances(
                session,
                update_extras=update_extras,
                task_instances=task_instances,
            )
        with mock.patch("airflow.models.dag_version.DagBundlesManager") as dag_bundle_manager_mock:
            dag_bundle_manager_mock.return_value.view_url.return_value = "some_url"
            # Mock DagBundlesManager to avoid checking if dags-folder bundle is configured
            with assert_queries_count(expected_queries_number):
                response = test_client.get(url, params=params)
        if params == {"task_id_pattern": "task_match_id"}:
            import pprint

            pprint.pprint(response.json())
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_ti
        assert len(response.json()["task_instances"]) == expected_ti

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/example_python_operator/dagRuns/~/taskInstances",
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/example_python_operator/dagRuns/~/taskInstances",
        )
        assert response.status_code == 403

    def test_not_found(self, test_client):
        response = test_client.get("/dags/invalid/dagRuns/~/taskInstances")
        assert response.status_code == 404
        assert response.json() == {"detail": "The Dag with ID: `invalid` was not found"}

        response = test_client.get("/dags/~/dagRuns/invalid/taskInstances")
        assert response.status_code == 404
        assert response.json() == {"detail": "DagRun with run_id: `invalid` was not found"}

    def test_bad_state(self, test_client):
        response = test_client.get("/dags/~/dagRuns/~/taskInstances", params={"state": "invalid"})
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Invalid value for state. Valid values are {', '.join(TaskInstanceState)}"
        )

    def test_return_TI_only_from_readable_dags(self, test_client, session):
        task_instances = {
            "example_python_operator": 1,
            "example_skip_dag": 2,
        }
        for dag_id in task_instances:
            self.create_task_instances(
                session,
                task_instances=[
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=i)}
                    for i in range(task_instances[dag_id])
                ],
                dag_id=dag_id,
            )
        response = test_client.get("/dags/~/dagRuns/~/taskInstances")
        assert response.status_code == 200
        assert response.json()["total_entries"] == 3
        assert len(response.json()["task_instances"]) == 3

    def test_should_respond_200_for_dag_id_filter(self, test_client, session):
        self.create_task_instances(session)
        self.create_task_instances(session, dag_id="example_skip_dag")
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/~/taskInstances",
        )

        assert response.status_code == 200
        count = session.query(TaskInstance).filter(TaskInstance.dag_id == "example_python_operator").count()
        assert count == response.json()["total_entries"]
        assert count == len(response.json()["task_instances"])

    @pytest.mark.parametrize(
        ("order_by_field", "base_date"),
        [
            ("start_date", DEFAULT_DATETIME_1 + timedelta(days=20)),
            ("logical_date", DEFAULT_DATETIME_2),
            ("data_interval_start", DEFAULT_DATETIME_1 + timedelta(days=5)),
            ("data_interval_end", DEFAULT_DATETIME_2 + timedelta(days=8)),
        ],
    )
    def test_should_respond_200_for_order_by(self, order_by_field, base_date, test_client, session):
        dag_id = "example_python_operator"

        dag_runs = [
            DagRun(
                dag_id=dag_id,
                run_id=f"run_{i}",
                run_type=DagRunType.MANUAL,
                logical_date=base_date + dt.timedelta(days=i),
                data_interval=(
                    base_date + dt.timedelta(days=i),
                    base_date + dt.timedelta(days=i, hours=1),
                ),
            )
            for i in range(10)
        ]
        session.add_all(dag_runs)
        session.commit()

        self.create_task_instances(
            session,
            task_instances=[
                {"run_id": f"run_{i}", "start_date": base_date + dt.timedelta(minutes=(i + 1))}
                for i in range(10)
            ],
            dag_id=dag_id,
        )

        ti_count = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).count()

        # Ascending order
        response_asc = test_client.get("/dags/~/dagRuns/~/taskInstances", params={"order_by": order_by_field})
        assert response_asc.status_code == 200
        assert response_asc.json()["total_entries"] == ti_count
        assert len(response_asc.json()["task_instances"]) == ti_count

        # Descending order
        response_desc = test_client.get(
            "/dags/~/dagRuns/~/taskInstances", params={"order_by": f"-{order_by_field}"}
        )
        assert response_desc.status_code == 200
        assert response_desc.json()["total_entries"] == ti_count
        assert len(response_desc.json()["task_instances"]) == ti_count

        # Compare
        field_asc = [ti["id"] for ti in response_asc.json()["task_instances"]]
        assert len(field_asc) == ti_count
        field_desc = [ti["id"] for ti in response_desc.json()["task_instances"]]
        assert len(field_desc) == ti_count
        assert field_asc == list(reversed(field_desc))

    def test_should_respond_200_for_pagination(self, test_client, session):
        dag_id = "example_python_operator"
        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(10)
            ],
            dag_id=dag_id,
        )

        # First 5 items
        response_batch1 = test_client.get(
            "/dags/~/dagRuns/~/taskInstances", params={"limit": 5, "offset": 0, "dag_ids": [dag_id]}
        )
        assert response_batch1.status_code == 200, response_batch1.json()
        num_entries_batch1 = len(response_batch1.json()["task_instances"])
        assert num_entries_batch1 == 5
        assert len(response_batch1.json()["task_instances"]) == 5

        # 5 items after that
        response_batch2 = test_client.get(
            "/dags/~/dagRuns/~/taskInstances", params={"limit": 5, "offset": 5, "dag_ids": [dag_id]}
        )
        assert response_batch2.status_code == 200, response_batch2.json()
        num_entries_batch2 = len(response_batch2.json()["task_instances"])
        assert num_entries_batch2 > 0
        assert len(response_batch2.json()["task_instances"]) > 0

        # Match
        ti_count = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).count()
        assert response_batch1.json()["total_entries"] == response_batch2.json()["total_entries"] == ti_count
        assert (num_entries_batch1 + num_entries_batch2) == ti_count
        assert response_batch1 != response_batch2


class TestGetTaskDependencies(TestTaskInstanceEndpoint):
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_should_respond_empty_non_scheduled(self, test_client, session):
        self.create_task_instances(session)
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/dependencies",
        )
        assert response.status_code == 200, response.text
        assert response.json() == {"dependencies": []}

    @pytest.mark.parametrize(
        ("state", "dependencies"),
        [
            (
                State.SCHEDULED,
                {
                    "dependencies": [
                        {
                            "name": "Logical Date",
                            "reason": "The logical date is 2020-01-01T00:00:00+00:00 but this is "
                            "before the task's start date 2021-01-01T00:00:00+00:00.",
                        },
                        {
                            "name": "Logical Date",
                            "reason": "The logical date is 2020-01-01T00:00:00+00:00 but this is "
                            "before the task's DAG's start date 2021-01-01T00:00:00+00:00.",
                        },
                    ],
                },
            ),
            (
                State.NONE,
                {
                    "dependencies": [
                        {
                            "name": "Logical Date",
                            "reason": "The logical date is 2020-01-01T00:00:00+00:00 but this is before the task's start date 2021-01-01T00:00:00+00:00.",
                        },
                        {
                            "name": "Logical Date",
                            "reason": "The logical date is 2020-01-01T00:00:00+00:00 but this is before the task's DAG's start date 2021-01-01T00:00:00+00:00.",
                        },
                        {"name": "Task Instance State", "reason": "Task is in the 'None' state."},
                    ]
                },
            ),
        ],
    )
    def test_should_respond_dependencies(self, test_client, session, state, dependencies):
        self.create_task_instances(session, task_instances=[{"state": state}], update_extras=True)

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/dependencies",
        )
        assert response.status_code == 200, response.text
        assert response.json() == dependencies

    def test_should_respond_dependencies_mapped(self, test_client, session):
        tis = self.create_task_instances(
            session, task_instances=[{"state": State.SCHEDULED}], update_extras=True
        )
        old_ti = tis[0]

        ti = TaskInstance(
            task=old_ti.task,
            run_id=old_ti.run_id,
            map_index=0,
            state=old_ti.state,
            dag_version_id=old_ti.dag_version_id,
        )
        session.add(ti)
        session.commit()

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/0/dependencies",
        )
        assert response.status_code == 200, response.text

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/0/dependencies",
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/0/dependencies",
        )
        assert response.status_code == 403


class TestGetTaskInstancesBatch(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        ("task_instances", "update_extras", "payload", "expected_ti_count"),
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
                id="test queue filter",
            ),
            pytest.param(
                [
                    {"executor": "test_exec_1"},
                    {"executor": "test_exec_2"},
                    {"executor": "test_exec_3"},
                ],
                True,
                {"executor": ["test_exec_1", "test_exec_2"]},
                2,
                id="test executor filter",
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
                id="test duration filter",
            ),
            pytest.param(
                [
                    {"logical_date": DEFAULT_DATETIME_1},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=5)},
                ],
                False,
                {
                    "logical_date_gte": DEFAULT_DATETIME_1.isoformat(),
                    "logical_date_lte": (DEFAULT_DATETIME_1 + dt.timedelta(days=2)).isoformat(),
                },
                3,
                id="with logical date filter",
            ),
            pytest.param(
                [
                    {"logical_date": DEFAULT_DATETIME_1},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3)},
                ],
                False,
                {
                    "dag_run_ids": ["TEST_DAG_RUN_ID_0", "TEST_DAG_RUN_ID_1"],
                },
                2,
                id="test dag run id filter",
            ),
            pytest.param(
                [
                    {"logical_date": DEFAULT_DATETIME_1},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3)},
                ],
                False,
                {
                    "task_ids": ["print_the_context", "log_sql_query"],
                },
                2,
                id="test task id filter",
            ),
        ],
    )
    def test_should_respond_200(
        self, test_client, task_instances, update_extras, payload, expected_ti_count, session
    ):
        self.create_task_instances(
            session,
            update_extras=update_extras,
            task_instances=task_instances,
        )
        with assert_queries_count(4):
            response = test_client.post(
                "/dags/~/dagRuns/~/taskInstances/list",
                json=payload,
            )
        body = response.json()
        assert response.status_code == 200, body
        assert expected_ti_count == body["total_entries"]
        assert expected_ti_count == len(body["task_instances"])
        check_last_log(session, dag_id="~", event="get_task_instances_batch", logical_date=None)

    def test_should_respond_200_for_order_by(self, test_client, session):
        dag_id = "example_python_operator"
        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(10)
            ],
            dag_id=dag_id,
        )

        ti_count = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).count()

        # Ascending order
        response_asc = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json={"order_by": "start_date", "dag_ids": [dag_id]},
        )
        assert response_asc.status_code == 200, response_asc.json()
        assert response_asc.json()["total_entries"] == ti_count
        assert len(response_asc.json()["task_instances"]) == ti_count

        # Descending order
        response_desc = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json={"order_by": "-start_date", "dag_ids": [dag_id]},
        )
        assert response_desc.status_code == 200, response_desc.json()
        assert response_desc.json()["total_entries"] == ti_count
        assert len(response_desc.json()["task_instances"]) == ti_count

        # Compare
        start_dates_asc = [ti["start_date"] for ti in response_asc.json()["task_instances"]]
        assert len(start_dates_asc) == ti_count
        start_dates_desc = [ti["start_date"] for ti in response_desc.json()["task_instances"]]
        assert len(start_dates_desc) == ti_count
        assert start_dates_asc == list(reversed(start_dates_desc))

    @pytest.mark.parametrize(
        ("task_instances", "payload", "expected_ti_count"),
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
    def test_should_respond_200_when_task_instance_properties_are_none(
        self, test_client, task_instances, payload, expected_ti_count, session
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
        response = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json=payload,
        )
        body = response.json()
        assert response.status_code == 200, body
        assert expected_ti_count == body["total_entries"]
        assert expected_ti_count == len(body["task_instances"])

    @pytest.mark.parametrize(
        ("payload", "expected_ti", "total_ti"),
        [
            pytest.param(
                {"dag_ids": ["example_python_operator", "example_skip_dag"]},
                17,
                17,
                id="with dag filter",
            ),
        ],
    )
    def test_should_respond_200_dag_ids_filter(self, test_client, payload, expected_ti, total_ti, session):
        self.create_task_instances(session)
        self.create_task_instances(session, dag_id="example_skip_dag")
        response = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json=payload,
        )
        assert response.status_code == 200
        assert len(response.json()["task_instances"]) == expected_ti
        assert response.json()["total_entries"] == total_ti

    def test_should_raise_400_for_no_json(self, test_client):
        response = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
        )
        assert response.status_code == 422
        assert response.json()["detail"] == [
            {
                "input": None,
                "loc": ["body"],
                "msg": "Field required",
                "type": "missing",
            },
        ]

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json={},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json={},
        )
        assert response.status_code == 403

    def test_should_respond_422_for_non_wildcard_path_parameters(self, test_client):
        response = test_client.post(
            "/dags/non_wildcard/dagRuns/~/taskInstances/list",
        )
        assert response.status_code == 422
        assert "Input should be '~'" in str(response.json()["detail"])

        response = test_client.post(
            "/dags/~/dagRuns/non_wildcard/taskInstances/list",
        )
        assert response.status_code == 422
        assert "Input should be '~'" in str(response.json()["detail"])

    @pytest.mark.parametrize(
        ("payload", "expected"),
        [
            ({"end_date_lte": "2020-11-10T12:42:39.442973"}, "Input should have timezone info"),
            ({"end_date_gte": "2020-11-10T12:42:39.442973"}, "Input should have timezone info"),
            ({"start_date_lte": "2020-11-10T12:42:39.442973"}, "Input should have timezone info"),
            ({"start_date_gte": "2020-11-10T12:42:39.442973"}, "Input should have timezone info"),
            ({"logical_date_gte": "2020-11-10T12:42:39.442973"}, "Input should have timezone info"),
            ({"logical_date_lte": "2020-11-10T12:42:39.442973"}, "Input should have timezone info"),
        ],
    )
    def test_should_raise_400_for_naive_and_bad_datetime(self, test_client, payload, expected, session):
        self.create_task_instances(session)
        response = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json=payload,
        )
        assert response.status_code == 422
        assert expected in str(response.json()["detail"])

    def test_should_respond_200_for_pagination(self, test_client, session):
        dag_id = "example_python_operator"

        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(10)
            ],
            dag_id=dag_id,
        )

        # First 5 items
        response_batch1 = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json={"page_limit": 5, "page_offset": 0},
        )
        assert response_batch1.status_code == 200, response_batch1.json()
        num_entries_batch1 = len(response_batch1.json()["task_instances"])
        assert num_entries_batch1 == 5
        assert len(response_batch1.json()["task_instances"]) == 5

        # 5 items after that
        response_batch2 = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json={"page_limit": 5, "page_offset": 5},
        )
        assert response_batch2.status_code == 200, response_batch2.json()
        num_entries_batch2 = len(response_batch2.json()["task_instances"])
        assert num_entries_batch2 > 0
        assert len(response_batch2.json()["task_instances"]) > 0

        # Match
        ti_count = 9
        assert response_batch1.json()["total_entries"] == response_batch2.json()["total_entries"] == ti_count
        assert (num_entries_batch1 + num_entries_batch2) == ti_count
        assert response_batch1 != response_batch2

        # default limit and offset
        response_batch3 = test_client.post(
            "/dags/~/dagRuns/~/taskInstances/list",
            json={},
        )

        num_entries_batch3 = len(response_batch3.json()["task_instances"])
        assert num_entries_batch3 == ti_count
        assert len(response_batch3.json()["task_instances"]) == ti_count


class TestGetTaskInstanceTry(TestTaskInstanceEndpoint):
    def test_should_respond_200(self, test_client, session):
        self.create_task_instances(session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True)
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1"
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "success",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 1,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "hitl_detail": None,
        }

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_should_respond_200_with_different_try_numbers(self, test_client, try_number, session):
        self.create_task_instances(session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True)
        response = test_client.get(
            f"/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/{try_number}",
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0 if try_number == 1 else 1,
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "success" if try_number == 1 else None,
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": try_number,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "hitl_detail": None,
        }

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_should_respond_200_with_mapped_task_at_different_try_numbers(
        self, test_client, try_number, session
    ):
        tis = self.create_task_instances(session, task_instances=[{"state": State.FAILED}])
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(
                task=old_ti.task, run_id=old_ti.run_id, map_index=idx, dag_version_id=old_ti.dag_version_id
            )
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            ti.try_number = 1
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.commit()
        tis = session.query(TaskInstance).all()
        # Record the task instance history
        from airflow.models.taskinstance import clear_task_instances

        clear_task_instances(tis, session)
        # Simulate the try_number increasing to new values in TI
        for ti in tis:
            if ti.map_index > 0:
                ti.try_number += 1
                ti.queue = "default_queue"
                session.merge(ti)
        session.commit()
        tis = session.query(TaskInstance).all()
        # in each loop, we should get the right mapped TI back
        for map_index in (1, 2):
            # Get the info from TIHistory: try_number 1, try_number 2 is TI table(latest)
            # TODO: Add "REMOTE_USER": "test" as per legacy code after adding Authentication
            response = test_client.get(
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                f"/print_the_context/{map_index}/tries/{try_number}",
            )
            response_data = response.json()
            assert response.status_code == 200
            assert response_data == {
                "dag_id": "example_python_operator",
                "dag_display_name": "example_python_operator",
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00Z",
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "map_index": map_index,
                "max_tries": 0 if try_number == 1 else 1,
                "operator": "PythonOperator",
                "operator_name": "PythonOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 9,
                "queue": "default_queue",
                "queued_when": None,
                "scheduled_when": None,
                "start_date": "2020-01-02T00:00:00Z",
                "state": "failed" if try_number == 1 else None,
                "task_id": "print_the_context",
                "task_display_name": "print_the_context",
                "try_number": try_number,
                "unixname": getuser(),
                "dag_run_id": "TEST_DAG_RUN_ID",
                "dag_version": {
                    "bundle_name": "dags-folder",
                    "bundle_url": None,
                    "bundle_version": None,
                    "created_at": response_data["dag_version"]["created_at"],
                    "dag_display_name": "example_python_operator",
                    "dag_id": "example_python_operator",
                    "id": response_data["dag_version"]["id"],
                    "version_number": 1,
                },
                "hitl_detail": None,
            }

    def test_should_respond_200_with_task_state_in_deferred(self, test_client, session):
        now = pendulum.now("UTC")
        ti = self.create_task_instances(
            session,
            task_instances=[{"state": State.DEFERRED}],
            update_extras=True,
        )[0]
        ti.trigger = Trigger("none", {})
        ti.trigger.created_date = now
        ti.triggerer_job = Job()
        TriggererJobRunner(job=ti.triggerer_job)
        ti.triggerer_job.state = "running"
        ti.try_number = 1
        session.merge(ti)
        session.flush()
        # Record the TaskInstanceHistory
        TaskInstanceHistory.record_ti(ti, session=session)
        session.flush()
        # Change TaskInstance try_number to 2, ensuring api checks TIHistory
        ti = session.query(TaskInstance).one_or_none()
        ti.try_number = 2
        session.merge(ti)
        # Set duration and end_date in TaskInstanceHistory for easy testing
        tih = session.query(TaskInstanceHistory).all()[0]
        tih.duration = 10000
        tih.end_date = self.default_time + dt.timedelta(days=2)
        session.merge(tih)
        session.commit()
        # Get the task instance details from TIHistory:
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1",
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "failed",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 1,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "hitl_detail": None,
        }

    def test_should_respond_200_with_task_state_in_removed(self, test_client, session):
        self.create_task_instances(
            session, task_instances=[{"state": State.REMOVED}], update_extras=True, with_ti_history=True
        )
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1",
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_display_name": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 9,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "removed",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 1,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "dag_version": {
                "bundle_name": "dags-folder",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "hitl_detail": None,
        }

    def test_should_respond_200_with_hitl(
        self, test_client, create_task_instance: CreateTaskInstance, session
    ):
        ti = create_task_instance(dag_id="test_hitl_dag", task_id="sample_task_hitl")
        ti.try_number = 1
        session.add(ti)
        hitl_detail = HITLDetail(
            ti_id=ti.id,
            options=["Approve", "Reject"],
            subject="This is subject",
            body="this is body",
            defaults=["Approve"],
            multiple=False,
            params={"input_1": 1},
            assignees=None,
        )
        session.add(hitl_detail)
        session.commit()
        # Record the TaskInstanceHistory
        TaskInstanceHistory.record_ti(ti, session=session)
        session.flush()

        response = test_client.get(
            f"/dags/{ti.dag_id}/dagRuns/{ti.run_id}/taskInstances/{ti.task_id}/tries/1",
        )
        assert response.status_code == 200
        assert response.json() == {
            "dag_id": "test_hitl_dag",
            "dag_display_name": "test_hitl_dag",
            "duration": None,
            "end_date": mock.ANY,
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "EmptyOperator",
            "operator_name": "EmptyOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": None,
            "state": None,
            "task_id": "sample_task_hitl",
            "task_display_name": "sample_task_hitl",
            "try_number": 1,
            "unixname": getuser(),
            "dag_run_id": "test",
            "dag_version": mock.ANY,
            "hitl_detail": {
                "assigned_users": [],
                "body": "this is body",
                "chosen_options": None,
                "created_at": mock.ANY,
                "defaults": ["Approve"],
                "multiple": False,
                "options": ["Approve", "Reject"],
                "params": {"input_1": {"value": 1, "description": None, "schema": {}}},
                "params_input": {},
                "responded_at": None,
                "responded_by_user": None,
                "response_received": False,
                "subject": "This is subject",
            },
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1",
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1",
        )
        assert response.status_code == 403

    def test_raises_404_for_nonexistent_task_instance(self, test_client, session):
        self.create_task_instances(session)
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/nonexistent_task/tries/0"
        )
        assert response.status_code == 404

        assert response.json() == {
            "detail": "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `nonexistent_task`, try_number: `0` and map_index: `-1` was not found"
        }

    @pytest.mark.parametrize(
        ("run_id", "expected_version_number"),
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.api_fastapi.core_api.datamodels.dag_versions.hasattr")
    def test_should_respond_200_with_versions(
        self, mock_hasattr, test_client, run_id, expected_version_number, session
    ):
        mock_hasattr.return_value = False
        response = test_client.get(
            f"/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1/tries/0"
        )
        assert response.status_code == 200
        assert response.json() == {
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
            "dag_display_name": "dag_with_multiple_versions",
            "dag_run_id": run_id,
            "map_index": -1,
            "start_date": None,
            "end_date": mock.ANY,
            "duration": None,
            "state": None,
            "try_number": 0,
            "max_tries": 0,
            "task_display_name": "task1",
            "hostname": "",
            "unixname": getuser(),
            "pool": "default_pool",
            "pool_slots": 1,
            "queue": "default",
            "priority_weight": 1,
            "operator": "EmptyOperator",
            "operator_name": "EmptyOperator",
            "queued_when": None,
            "scheduled_when": None,
            "pid": None,
            "executor": None,
            "executor_config": "{}",
            "dag_version": {
                "id": mock.ANY,
                "version_number": expected_version_number,
                "dag_id": "dag_with_multiple_versions",
                "bundle_name": "dag_maker",
                "bundle_version": f"some_commit_hash{expected_version_number}",
                "bundle_url": f"http://test_host.github.com/tree/some_commit_hash{expected_version_number}/dags",
                "created_at": mock.ANY,
                "dag_display_name": "dag_with_multiple_versions",
            },
            "hitl_detail": None,
        }

    @pytest.mark.parametrize(
        ("run_id", "expected_version_number"),
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_respond_200_with_versions_using_url_template(
        self, test_client, run_id, expected_version_number, session
    ):
        response = test_client.get(
            f"/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1/tries/0"
        )
        assert response.status_code == 200
        assert response.json() == {
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
            "dag_display_name": "dag_with_multiple_versions",
            "dag_run_id": run_id,
            "map_index": -1,
            "start_date": None,
            "end_date": mock.ANY,
            "duration": None,
            "state": None,
            "try_number": 0,
            "max_tries": 0,
            "task_display_name": "task1",
            "hostname": "",
            "unixname": getuser(),
            "pool": "default_pool",
            "pool_slots": 1,
            "queue": "default",
            "priority_weight": 1,
            "operator": "EmptyOperator",
            "operator_name": "EmptyOperator",
            "queued_when": None,
            "scheduled_when": None,
            "pid": None,
            "executor": None,
            "executor_config": "{}",
            "dag_version": {
                "id": mock.ANY,
                "version_number": expected_version_number,
                "dag_id": "dag_with_multiple_versions",
                "bundle_name": "dag_maker",
                "bundle_version": f"some_commit_hash{expected_version_number}",
                "bundle_url": f"http://test_host.github.com/tree/some_commit_hash{expected_version_number}/dags",
                "created_at": mock.ANY,
                "dag_display_name": "dag_with_multiple_versions",
            },
            "hitl_detail": None,
        }

    def test_should_not_return_duplicate_runs(self, test_client, session):
        """
        Test that ensures the task instances query doesn't return duplicates due to the updated join/filter logic.
        """
        self.create_task_instances(session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True)
        self.create_task_instances(
            session,
            dag_id="example_bash_operator",
            task_instances=[{"state": State.SUCCESS}],
            with_ti_history=True,
        )

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )

        assert response.status_code == 200

        response = response.json()

        assert response["total_entries"] == 2


class TestPostClearTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        ("main_dag", "task_instances", "request_dag", "payload", "expected_ti"),
        [
            pytest.param(
                "example_python_operator",
                [
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
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
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
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
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.RUNNING,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
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
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
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
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
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
                "example_python_operator",
                [
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
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
            pytest.param(
                "example_python_operator",
                [
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": False,
                    "task_ids": [["print_the_context", -1], "sleep_for_1"],
                },
                2,
                id="clear unmapped tasks with and without map index",
            ),
            pytest.param(
                "example_task_mapping_second_order",
                [
                    {
                        "logical_date": DEFAULT_DATETIME_1,
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                ],
                "example_task_mapping_second_order",
                {
                    "dry_run": False,
                    "task_ids": [["times_2", 0], ["add_10", 1]],
                },
                2,
                id="clear multiple mapped tasks",
            ),
            pytest.param(
                "example_task_mapping_second_order",
                [
                    {
                        "logical_date": DEFAULT_DATETIME_1,
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                ],
                "example_task_mapping_second_order",
                {
                    "dry_run": False,
                    "task_ids": [["times_2", 0], ["add_10", 1]],
                    "include_upstream": True,
                },
                5,
                id="clear mapped tasks and upstream tasks",
            ),
            pytest.param(
                "example_task_mapping_second_order",
                [
                    {
                        "logical_date": DEFAULT_DATETIME_1,
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                ],
                "example_task_mapping_second_order",
                {
                    "dry_run": False,
                    "task_ids": [["times_2", 0], ["add_10", 1]],
                    "include_downstream": True,
                },
                4,
                id="clear mapped tasks and downstream tasks",
            ),
            pytest.param(
                "example_task_mapping_second_order",
                [
                    {
                        "logical_date": DEFAULT_DATETIME_1,
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                ],
                "example_task_mapping_second_order",
                {
                    "dry_run": False,
                    "task_ids": [["times_2", 0], "add_10"],
                },
                4,
                id="clear mapped tasks with and without map index",
            ),
            pytest.param(
                "example_task_group_mapping",
                [
                    {
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                    {
                        "state": State.FAILED,
                        "map_indexes": (0, 1, 2),
                    },
                ],
                "example_task_group_mapping",
                {
                    "task_ids": [["op.mul_2", 0]],
                    "dag_run_id": "TEST_DAG_RUN_ID",
                    "include_upstream": True,
                },
                2,
                id="clear tasks in mapped task group",
            ),
        ],
    )
    def test_should_respond_200(
        self,
        test_client,
        session,
        main_dag,
        task_instances,
        request_dag,
        payload,
        expected_ti,
    ):
        self.create_task_instances(
            session,
            dag_id=main_dag,
            task_instances=task_instances,
            update_extras=False,
        )
        response = test_client.post(
            f"/dags/{request_dag}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_ti

        if not payload.get("dry_run", True):
            check_last_log(session, dag_id=request_dag, event="post_clear_task_instances", logical_date=None)

    @pytest.mark.parametrize("flag", ["include_future", "include_past"])
    def test_manual_run_with_none_logical_date_returns_400(self, test_client, session, flag):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": True,
            "dag_run_id": "TEST_DAG_RUN_ID_0",
            "only_failed": True,
            flag: True,
        }
        task_instances = [{"logical_date": None, "state": State.FAILED}]
        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=task_instances,
            update_extras=False,
            dag_run_state=State.FAILED,
        )
        response = test_client.post(f"/dags/{dag_id}/clearTaskInstances", json=payload)
        assert response.status_code == 400
        assert (
            "Cannot use include_past or include_future with no logical_date(e.g. manually or asset-triggered)."
            in response.json()["detail"]
        )

    @pytest.mark.parametrize(
        ("flag", "expected"),
        [
            ("include_past", 2),  # T0 ~ T1
            ("include_future", 2),  # T1 ~ T2
        ],
    )
    def test_with_dag_run_id_and_past_future_converts_to_date_range(
        self, test_client, session, flag, expected
    ):
        dag_id = "example_python_operator"
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},  # T0
            {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1), "state": State.FAILED},  # T1
            {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2), "state": State.FAILED},  # T2
        ]
        self.create_task_instances(session, dag_id=dag_id, task_instances=task_instances, update_extras=False)
        payload = {
            "dry_run": True,
            "only_failed": True,
            "dag_run_id": "TEST_DAG_RUN_ID_1",
            flag: True,
        }
        resp = test_client.post(f"/dags/{dag_id}/clearTaskInstances", json=payload)
        assert resp.status_code == 200
        assert resp.json()["total_entries"] == expected  # include_past => T0,T1 / include_future => T1,T2

    def test_with_dag_run_id_and_both_past_and_future_means_full_range(self, test_client, session):
        dag_id = "example_python_operator"
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1 - dt.timedelta(days=1), "state": State.FAILED},  # T0
            {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},  # T1
            {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1), "state": State.FAILED},  # T2
            {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2), "state": State.FAILED},  # T3
            {"logical_date": None, "state": State.FAILED},  # T4
        ]
        self.create_task_instances(session, dag_id=dag_id, task_instances=task_instances, update_extras=False)
        payload = {
            "dry_run": True,
            "only_failed": False,
            "dag_run_id": "TEST_DAG_RUN_ID_1",  # T1
            "include_past": True,
            "include_future": True,
        }
        resp = test_client.post(f"/dags/{dag_id}/clearTaskInstances", json=payload)
        assert resp.status_code == 200
        assert resp.json()["total_entries"] == 5  # T0 ~ #T4

    def test_with_dag_run_id_only_uses_run_id_based_clearing(self, test_client, session):
        dag_id = "example_python_operator"
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1, "state": State.SUCCESS},  # T0
            {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1), "state": State.FAILED},  # T1
            {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2), "state": State.SUCCESS},  # T2
        ]
        self.create_task_instances(session, dag_id=dag_id, task_instances=task_instances, update_extras=False)
        payload = {
            "dry_run": True,
            "only_failed": True,
            "dag_run_id": "TEST_DAG_RUN_ID_1",
        }
        resp = test_client.post(f"/dags/{dag_id}/clearTaskInstances", json=payload)
        assert resp.status_code == 200
        assert resp.json()["total_entries"] == 1
        assert resp.json()["task_instances"][0]["logical_date"] == "2020-01-02T00:00:00Z"  # T1

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            "/dags/dag_id/clearTaskInstances",
            json={},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            "/dags/dag_id/clearTaskInstances",
            json={},
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("main_dag", "task_instances", "request_dag", "payload", "expected_ti"),
        [
            pytest.param(
                "example_python_operator",
                [
                    {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                        "state": State.FAILED,
                    },
                    {
                        "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                        "state": State.FAILED,
                    },
                ],
                "example_python_operator",
                {
                    "dry_run": False,
                    "task_ids": [["print_the_context", 1, 2]],
                },
                2,
                id="clear mapped task and unmapped tasks together",
            ),
        ],
    )
    def test_should_respond_422(
        self,
        test_client,
        session,
        main_dag,
        task_instances,
        request_dag,
        payload,
        expected_ti,
    ):
        self.create_task_instances(
            session,
            dag_id=main_dag,
            task_instances=task_instances,
            update_extras=False,
        )
        response = test_client.post(
            f"/dags/{request_dag}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 422

    @mock.patch("airflow.api_fastapi.core_api.routes.public.task_instances.clear_task_instances")
    def test_clear_taskinstance_is_called_with_queued_dr_state(self, mock_clearti, test_client, session):
        """Test that if reset_dag_runs is True, then clear_task_instances is called with State.QUEUED"""
        self.create_task_instances(session)
        dag_id = "example_python_operator"
        payload = {"reset_dag_runs": True, "dry_run": False}
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200

        # dag (3rd argument) is a different session object. Manually asserting that the dag_id
        # is the same.
        mock_clearti.assert_called_once_with(
            [], mock.ANY, DagRunState.QUEUED, prevent_running_task=False, run_on_latest_version=False
        )

    def test_clear_taskinstance_is_called_with_invalid_task_ids(self, test_client, session):
        """Test that dagrun is running when invalid task_ids are passed to clearTaskInstances API."""
        dag_id = "example_python_operator"
        tis = self.create_task_instances(session)
        dagrun = tis[0].get_dagrun()
        assert dagrun.state == "running"

        payload = {"dry_run": False, "reset_dag_runs": True, "task_ids": [""]}
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200

        dagrun.refresh_from_db()
        assert dagrun.state == "running"
        assert all(ti.state == "running" for ti in tis)

    def test_should_respond_200_with_reset_dag_run(self, test_client, session):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": True,
            "only_failed": False,
            "only_running": True,
        }
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=5),
                "state": State.RUNNING,
            },
        ]

        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=task_instances,
            update_extras=False,
            dag_run_state=DagRunState.FAILED,
        )
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )

        failed_dag_runs = session.query(DagRun).filter(DagRun.state == "failed").count()
        assert response.status_code == 200
        expected_response = [
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_0",
                "task_id": "print_the_context",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "task_id": "log_sql_query",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_2",
                "task_id": "sleep_for_0",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_3",
                "task_id": "sleep_for_1",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_4",
                "task_id": "sleep_for_2",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_5",
                "task_id": "sleep_for_3",
            },
        ]
        for task_instance in expected_response:
            assert task_instance in [
                {key: ti[key] for key in task_instance.keys()} for ti in response.json()["task_instances"]
            ]
        assert response.json()["total_entries"] == 6
        assert failed_dag_runs == 0

    @pytest.mark.parametrize(
        ("target_logical_date", "response_logical_date"),
        [
            pytest.param(DEFAULT_DATETIME_1, "2020-01-01T00:00:00Z", id="date"),
            pytest.param(None, None, id="null"),
        ],
    )
    def test_should_respond_200_with_dag_run_id(
        self,
        test_client,
        session,
        target_logical_date,
        response_logical_date,
    ):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": False,
            "only_failed": False,
            "only_running": True,
            "dag_run_id": "TEST_DAG_RUN_ID_0",
        }
        if target_logical_date:
            task_instances = [
                {"logical_date": target_logical_date + dt.timedelta(days=i), "state": State.RUNNING}
                for i in range(6)
            ]
        else:
            self.ti_extras["run_after"] = DEFAULT_DATETIME_1
            task_instances = [{"logical_date": target_logical_date, "state": State.RUNNING} for _ in range(6)]

        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=task_instances,
            update_extras=False,
            dag_run_state=State.FAILED,
        )
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        response_data = response.json()
        expected_response = [
            {
                "dag_id": "example_python_operator",
                "dag_display_name": "example_python_operator",
                "dag_version": {
                    "bundle_name": "dags-folder",
                    "bundle_url": None,
                    "bundle_version": None,
                    "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                    "dag_display_name": "example_python_operator",
                    "dag_id": "example_python_operator",
                    "id": response_data["task_instances"][0]["dag_version"]["id"],
                    "version_number": 1,
                },
                "dag_run_id": "TEST_DAG_RUN_ID_0",
                "task_id": "print_the_context",
                "duration": response_data["task_instances"][0]["duration"],
                "end_date": response_data["task_instances"][0]["end_date"],
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "id": response_data["task_instances"][0]["id"],
                "logical_date": response_logical_date,
                "map_index": -1,
                "max_tries": 0,
                "note": "placeholder-note",
                "operator": "PythonOperator",
                "operator_name": "PythonOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 9,
                "queue": "default_queue",
                "queued_when": None,
                "scheduled_when": None,
                "rendered_fields": {},
                "rendered_map_index": None,
                "run_after": "2020-01-01T00:00:00Z",
                "start_date": "2020-01-02T00:00:00Z",
                "state": "restarting",
                "task_display_name": "print_the_context",
                "trigger": None,
                "triggerer_job": None,
                "try_number": 0,
                "unixname": getuser(),
            },
        ]
        assert response.status_code == 200
        assert response_data["task_instances"] == expected_response
        assert response_data["total_entries"] == 1

    def test_should_respond_200_with_include_past(self, test_client, session):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": False,
            "only_failed": False,
            "include_past": True,
            "only_running": True,
        }
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4),
                "state": State.RUNNING,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=5),
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
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200
        expected_response = [
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_0",
                "task_id": "print_the_context",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "task_id": "log_sql_query",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_2",
                "task_id": "sleep_for_0",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_3",
                "task_id": "sleep_for_1",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_4",
                "task_id": "sleep_for_2",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_5",
                "task_id": "sleep_for_3",
            },
        ]
        for task_instance in expected_response:
            assert task_instance in [
                {key: ti[key] for key in task_instance.keys()} for ti in response.json()["task_instances"]
            ]
        assert response.json()["total_entries"] == 6

    def test_should_respond_200_with_include_future(self, test_client, session):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": False,
            "only_failed": False,
            "include_future": True,
            "only_running": False,
        }
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1, "state": State.SUCCESS},
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.SUCCESS,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2),
                "state": State.SUCCESS,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=3),
                "state": State.SUCCESS,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=4),
                "state": State.SUCCESS,
            },
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=5),
                "state": State.SUCCESS,
            },
        ]

        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=task_instances,
            update_extras=False,
            dag_run_state=State.FAILED,
        )
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )

        assert response.status_code == 200
        expected_response = [
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_0",
                "task_id": "print_the_context",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "task_id": "log_sql_query",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_2",
                "task_id": "sleep_for_0",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_3",
                "task_id": "sleep_for_1",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_4",
                "task_id": "sleep_for_2",
            },
            {
                "dag_id": "example_python_operator",
                "dag_run_id": "TEST_DAG_RUN_ID_5",
                "task_id": "sleep_for_3",
            },
        ]
        for task_instance in expected_response:
            assert task_instance in [
                {key: ti[key] for key in task_instance.keys()} for ti in response.json()["task_instances"]
            ]
        assert response.json()["total_entries"] == 6

    def test_should_respond_404_for_nonexistent_dagrun_id(self, test_client, session):
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": False,
            "only_failed": False,
            "only_running": True,
            "dag_run_id": "TEST_DAG_RUN_ID_100",
        }
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
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
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )

        assert response.status_code == 404
        assert f"Dag Run id TEST_DAG_RUN_ID_100 not found in dag {dag_id}" in response.text

    @pytest.mark.parametrize(
        ("payload", "expected"),
        [
            (
                {"end_date": "2020-11-10T12:42:39.442973"},
                {
                    "detail": [
                        {
                            "type": "timezone_aware",
                            "loc": ["body", "end_date"],
                            "msg": "Input should have timezone info",
                            "input": "2020-11-10T12:42:39.442973",
                        }
                    ]
                },
            ),
            (
                {"end_date": "2020-11-10T12:4po"},
                {
                    "detail": [
                        {
                            "type": "datetime_from_date_parsing",
                            "loc": ["body", "end_date"],
                            "msg": "Input should be a valid datetime or date, unexpected extra characters at the end of the input",
                            "input": "2020-11-10T12:4po",
                            "ctx": {"error": "unexpected extra characters at the end of the input"},
                        }
                    ]
                },
            ),
            (
                {"start_date": "2020-11-10T12:42:39.442973"},
                {
                    "detail": [
                        {
                            "type": "timezone_aware",
                            "loc": ["body", "start_date"],
                            "msg": "Input should have timezone info",
                            "input": "2020-11-10T12:42:39.442973",
                        }
                    ]
                },
            ),
            (
                {"start_date": "2020-11-10T12:4po"},
                {
                    "detail": [
                        {
                            "type": "datetime_from_date_parsing",
                            "loc": ["body", "start_date"],
                            "msg": "Input should be a valid datetime or date, unexpected extra characters at the end of the input",
                            "input": "2020-11-10T12:4po",
                            "ctx": {"error": "unexpected extra characters at the end of the input"},
                        }
                    ]
                },
            ),
        ],
    )
    def test_should_raise_400_for_naive_and_bad_datetime(self, test_client, session, payload, expected):
        task_instances = [
            {"logical_date": DEFAULT_DATETIME_1, "state": State.RUNNING},
            {
                "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                "state": State.RUNNING,
            },
        ]
        self.create_task_instances(
            session,
            dag_id="example_python_operator",
            task_instances=task_instances,
            update_extras=False,
        )
        response = test_client.post(
            "/dags/example_python_operator/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 422
        assert response.json() == expected

    def test_raises_404_for_non_existent_dag(self, test_client):
        response = test_client.post(
            "/dags/non-existent-dag/clearTaskInstances",
            json={
                "dry_run": False,
                "reset_dag_runs": True,
                "only_failed": False,
                "only_running": True,
            },
        )
        assert response.status_code == 404
        assert "The Dag with ID: `non-existent-dag` was not found" in response.text

    @pytest.mark.parametrize(
        ("dry_run", "audit_log_count"),
        [
            (True, 0),
            (False, 1),
        ],
    )
    def test_dry_run_audit_log(self, test_client, session, dry_run, audit_log_count):
        dag_id = "example_python_operator"
        dag_run_id = "TEST_DAG_RUN_ID"
        event = "post_clear_task_instances"

        payload = {"dry_run": dry_run, "dag_run_id": dag_run_id}
        self.create_task_instances(session, dag_id)

        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )

        logs = (
            session.query(Log)
            .filter(Log.dag_id == dag_id, Log.run_id == dag_run_id, Log.event == event)
            .count()
        )

        assert response.status_code == 200
        assert logs == audit_log_count


class TestGetTaskInstanceTries(TestTaskInstanceEndpoint):
    def test_should_respond_200(self, test_client, session):
        self.create_task_instances(
            session=session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True
        )
        with assert_queries_count(3):
            response = test_client.get(
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
            )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data["total_entries"] == 2  # The task instance and its history
        assert len(response_data["task_instances"]) == 2
        assert response_data == {
            "task_instances": [
                {
                    "dag_id": "example_python_operator",
                    "dag_display_name": "example_python_operator",
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 0,
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 9,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "success",
                    "task_id": "print_the_context",
                    "task_display_name": "print_the_context",
                    "try_number": 1,
                    "unixname": getuser(),
                    "dag_run_id": "TEST_DAG_RUN_ID",
                    "dag_version": {
                        "bundle_name": "dags-folder",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][0]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "hitl_detail": None,
                },
                {
                    "dag_id": "example_python_operator",
                    "dag_display_name": "example_python_operator",
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 1,
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 9,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": None,
                    "task_id": "print_the_context",
                    "task_display_name": "print_the_context",
                    "try_number": 2,
                    "unixname": getuser(),
                    "dag_run_id": "TEST_DAG_RUN_ID",
                    "dag_version": {
                        "bundle_name": "dags-folder",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][1]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][1]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "hitl_detail": None,
                },
            ],
            "total_entries": 2,
        }

    def test_should_respond_200_with_hitl(
        self, test_client, create_task_instance: CreateTaskInstance, session
    ):
        ti = create_task_instance(dag_id="test_hitl_dag", task_id="sample_task_hitl")
        ti.try_number = 1
        session.add(ti)
        hitl_detail = HITLDetail(
            ti_id=ti.id,
            options=["Approve", "Reject"],
            subject="This is subject",
            body="this is body",
            defaults=["Approve"],
            multiple=False,
            params={"input_1": 1},
            assignees=None,
        )
        session.add(hitl_detail)
        session.commit()
        # Record the TaskInstanceHistory
        TaskInstanceHistory.record_ti(ti, session=session)
        session.flush()

        with assert_queries_count(3):
            response = test_client.get(
                f"/dags/{ti.dag_id}/dagRuns/{ti.run_id}/taskInstances/{ti.task_id}/tries",
            )
        assert response.status_code == 200
        assert response.json() == {
            "task_instances": [
                {
                    "dag_id": "test_hitl_dag",
                    "dag_display_name": "test_hitl_dag",
                    "duration": None,
                    "end_date": mock.ANY,
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 0,
                    "operator": "EmptyOperator",
                    "operator_name": "EmptyOperator",
                    "pid": None,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 1,
                    "queue": "default",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": None,
                    "state": None,
                    "task_id": "sample_task_hitl",
                    "task_display_name": "sample_task_hitl",
                    "try_number": 1,
                    "unixname": getuser(),
                    "dag_run_id": "test",
                    "dag_version": mock.ANY,
                    "hitl_detail": {
                        "assigned_users": [],
                        "body": "this is body",
                        "chosen_options": None,
                        "created_at": mock.ANY,
                        "defaults": ["Approve"],
                        "multiple": False,
                        "options": ["Approve", "Reject"],
                        "params": {"input_1": {"value": 1, "description": None, "schema": {}}},
                        "params_input": {},
                        "responded_at": None,
                        "responded_by_user": None,
                        "response_received": False,
                        "subject": "This is subject",
                    },
                },
            ],
            "total_entries": 1,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )
        assert response.status_code == 403

    def test_ti_in_retry_state_not_returned(self, test_client, session):
        self.create_task_instances(
            session=session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True
        )
        ti = session.query(TaskInstance).one()
        ti.state = State.UP_FOR_RETRY
        session.merge(ti)
        session.commit()

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data["total_entries"] == 1
        assert len(response_data["task_instances"]) == 1
        assert response_data == {
            "task_instances": [
                {
                    "dag_id": "example_python_operator",
                    "dag_display_name": "example_python_operator",
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 0,
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 9,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "success",
                    "task_id": "print_the_context",
                    "task_display_name": "print_the_context",
                    "try_number": 1,
                    "unixname": getuser(),
                    "dag_run_id": "TEST_DAG_RUN_ID",
                    "dag_version": {
                        "bundle_name": "dags-folder",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][0]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "hitl_detail": None,
                },
            ],
            "total_entries": 1,
        }

    def test_mapped_task_should_respond_200(self, test_client, session):
        tis = self.create_task_instances(session, task_instances=[{"state": State.FAILED}])
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(
                task=old_ti.task, run_id=old_ti.run_id, map_index=idx, dag_version_id=old_ti.dag_version_id
            )
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue"]:
                setattr(ti, attr, getattr(old_ti, attr))
            ti.try_number = 1
            session.add(ti)
        session.commit()
        tis = session.query(TaskInstance).all()

        # Record the task instance history
        from airflow.models.taskinstance import clear_task_instances

        clear_task_instances(tis, session)
        # Simulate the try_number increasing to new values in TI
        for ti in tis:
            if ti.map_index > 0:
                ti.try_number += 1
                ti.queue = "default_queue"
                session.merge(ti)
        session.commit()

        # in each loop, we should get the right mapped TI back
        for map_index in (1, 2):
            # Get the info from TIHistory: try_number 1, try_number 2 is TI table(latest)
            with assert_queries_count(3):
                response = test_client.get(
                    "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                    f"/print_the_context/{map_index}/tries",
                )
            response_data = response.json()
            assert response.status_code == 200
            assert (
                response_data["total_entries"] == 2
            )  # the mapped task was cleared. So both the task instance and its history
            assert len(response_data["task_instances"]) == 2
            assert response_data == {
                "task_instances": [
                    {
                        "dag_id": "example_python_operator",
                        "dag_display_name": "example_python_operator",
                        "duration": 10000.0,
                        "end_date": "2020-01-03T00:00:00Z",
                        "executor": None,
                        "executor_config": "{}",
                        "hostname": "",
                        "map_index": map_index,
                        "max_tries": 0,
                        "operator": "PythonOperator",
                        "operator_name": "PythonOperator",
                        "pid": 100,
                        "pool": "default_pool",
                        "pool_slots": 1,
                        "priority_weight": 9,
                        "queue": "default_queue",
                        "queued_when": None,
                        "scheduled_when": None,
                        "start_date": "2020-01-02T00:00:00Z",
                        "state": "failed",
                        "task_id": "print_the_context",
                        "task_display_name": "print_the_context",
                        "try_number": 1,
                        "unixname": getuser(),
                        "dag_run_id": "TEST_DAG_RUN_ID",
                        "dag_version": {
                            "bundle_name": "dags-folder",
                            "bundle_url": None,
                            "bundle_version": None,
                            "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                            "dag_display_name": "example_python_operator",
                            "dag_id": "example_python_operator",
                            "id": response_data["task_instances"][0]["dag_version"]["id"],
                            "version_number": 1,
                        },
                        "hitl_detail": None,
                    },
                    {
                        "dag_id": "example_python_operator",
                        "dag_display_name": "example_python_operator",
                        "duration": 10000.0,
                        "end_date": "2020-01-03T00:00:00Z",
                        "executor": None,
                        "executor_config": "{}",
                        "hostname": "",
                        "map_index": map_index,
                        "max_tries": 1,
                        "operator": "PythonOperator",
                        "operator_name": "PythonOperator",
                        "pid": 100,
                        "pool": "default_pool",
                        "pool_slots": 1,
                        "priority_weight": 9,
                        "queue": "default_queue",
                        "queued_when": None,
                        "scheduled_when": None,
                        "start_date": "2020-01-02T00:00:00Z",
                        "state": None,
                        "task_id": "print_the_context",
                        "task_display_name": "print_the_context",
                        "try_number": 2,
                        "unixname": getuser(),
                        "dag_run_id": "TEST_DAG_RUN_ID",
                        "dag_version": {
                            "bundle_name": "dags-folder",
                            "bundle_url": None,
                            "bundle_version": None,
                            "created_at": response_data["task_instances"][1]["dag_version"]["created_at"],
                            "dag_display_name": "example_python_operator",
                            "dag_id": "example_python_operator",
                            "id": response_data["task_instances"][1]["dag_version"]["id"],
                            "version_number": 1,
                        },
                        "hitl_detail": None,
                    },
                ],
                "total_entries": 2,
            }

    def test_raises_404_for_nonexistent_task_instance(self, test_client, session):
        self.create_task_instances(session)
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/non_existent_task/tries"
        )
        assert response.status_code == 404

        assert response.json() == {
            "detail": "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `non_existent_task` and map_index: `-1` was not found"
        }

    @pytest.mark.parametrize(
        ("run_id", "expected_version_number"),
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.api_fastapi.core_api.datamodels.dag_versions.hasattr")
    def test_should_respond_200_with_versions(
        self, mock_hasattr, test_client, run_id, expected_version_number
    ):
        mock_hasattr.return_value = False
        response = test_client.get(
            f"/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1/tries"
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data["task_instances"][0] == {
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
            "dag_display_name": "dag_with_multiple_versions",
            "dag_run_id": run_id,
            "map_index": -1,
            "start_date": None,
            "end_date": mock.ANY,
            "duration": None,
            "state": mock.ANY,
            "try_number": 0,
            "max_tries": 0,
            "task_display_name": "task1",
            "hostname": "",
            "unixname": getuser(),
            "pool": "default_pool",
            "pool_slots": 1,
            "queue": "default",
            "priority_weight": 1,
            "operator": "EmptyOperator",
            "operator_name": "EmptyOperator",
            "queued_when": None,
            "scheduled_when": None,
            "pid": None,
            "executor": None,
            "executor_config": "{}",
            "dag_version": {
                "id": mock.ANY,
                "version_number": expected_version_number,
                "dag_id": "dag_with_multiple_versions",
                "bundle_name": "dag_maker",
                "bundle_version": f"some_commit_hash{expected_version_number}",
                "bundle_url": f"http://test_host.github.com/tree/some_commit_hash{expected_version_number}/dags",
                "created_at": mock.ANY,
                "dag_display_name": "dag_with_multiple_versions",
            },
            "hitl_detail": None,
        }

    @pytest.mark.parametrize(
        ("run_id", "expected_version_number"),
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_respond_200_with_versions_using_url_template(
        self, test_client, run_id, expected_version_number
    ):
        response = test_client.get(
            f"/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1/tries"
        )
        assert response.status_code == 200
        assert response.json()["task_instances"][0] == {
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
            "dag_display_name": "dag_with_multiple_versions",
            "dag_run_id": run_id,
            "map_index": -1,
            "start_date": None,
            "end_date": mock.ANY,
            "duration": None,
            "state": mock.ANY,
            "try_number": 0,
            "max_tries": 0,
            "task_display_name": "task1",
            "hostname": "",
            "unixname": getuser(),
            "pool": "default_pool",
            "pool_slots": 1,
            "queue": "default",
            "priority_weight": 1,
            "operator": "EmptyOperator",
            "operator_name": "EmptyOperator",
            "queued_when": None,
            "scheduled_when": None,
            "pid": None,
            "executor": None,
            "executor_config": "{}",
            "dag_version": {
                "id": mock.ANY,
                "version_number": expected_version_number,
                "dag_id": "dag_with_multiple_versions",
                "bundle_name": "dag_maker",
                "bundle_version": f"some_commit_hash{expected_version_number}",
                "bundle_url": f"http://test_host.github.com/tree/some_commit_hash{expected_version_number}/dags",
                "created_at": mock.ANY,
                "dag_display_name": "dag_with_multiple_versions",
            },
            "hitl_detail": None,
        }


class TestPatchTaskInstance(TestTaskInstanceEndpoint):
    ENDPOINT_URL = "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
    NEW_STATE = "failed"
    DAG_ID = "example_python_operator"
    DAG_DISPLAY_NAME = "example_python_operator"
    TASK_ID = "print_the_context"
    RUN_ID = "TEST_DAG_RUN_ID"

    @pytest.fixture(autouse=True)
    def clean_listener_manager(self):
        get_listener_manager().clear()
        yield
        get_listener_manager().clear()

    @pytest.mark.parametrize(
        ("state", "listener_state"),
        [
            ("success", [TaskInstanceState.SUCCESS]),
            ("failed", [TaskInstanceState.FAILED]),
            ("skipped", []),
        ],
    )
    def test_patch_task_instance_notifies_listeners(self, test_client, session, state, listener_state):
        from unit.listeners.class_listener import ClassBasedListener

        self.create_task_instances(session)

        listener = ClassBasedListener()
        get_listener_manager().add_listener(listener)
        test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": state,
            },
        )

        response2 = test_client.get(self.ENDPOINT_URL)
        assert response2.status_code == 200
        assert response2.json()["state"] == state
        assert listener.state == listener_state

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_instance_state")
    def test_should_call_mocked_api(self, mock_set_ti_state, test_client, session):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.map_index == -1,
            )
        ).all()

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "task_instances": [
                {
                    "dag_id": self.DAG_ID,
                    "dag_display_name": self.DAG_DISPLAY_NAME,
                    "dag_version": {
                        "bundle_name": "dags-folder",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][0]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "dag_run_id": self.RUN_ID,
                    "logical_date": "2020-01-01T00:00:00Z",
                    "task_id": self.TASK_ID,
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "id": response_data["task_instances"][0]["id"],
                    "map_index": -1,
                    "max_tries": 0,
                    "note": "placeholder-note",
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 9,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_display_name": self.TASK_ID,
                    "try_number": 0,
                    "unixname": getuser(),
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                }
            ],
            "total_entries": 1,
        }

        mock_set_ti_state.assert_called_once_with(
            commit=True,
            downstream=False,
            upstream=False,
            future=False,
            map_indexes=None,
            past=False,
            run_id=self.RUN_ID,
            session=mock.ANY,
            state=self.NEW_STATE,
            task_id=self.TASK_ID,
        )
        check_last_log(session, dag_id=self.DAG_ID, event="patch_task_instance", logical_date=None)

    def test_should_update_task_instance_state(self, test_client, session):
        self.create_task_instances(session)

        test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )

        response2 = test_client.get(self.ENDPOINT_URL)
        assert response2.status_code == 200
        assert response2.json()["state"] == self.NEW_STATE

    def test_should_update_mapped_task_instance_state(self, test_client, session):
        map_index = 1
        tis = self.create_task_instances(session)
        ti = TaskInstance(
            task=tis[0].task, run_id=tis[0].run_id, map_index=map_index, dag_version_id=tis[0].dag_version_id
        )
        ti_2 = TaskInstance(
            task=tis[0].task,
            run_id=tis[0].run_id,
            map_index=map_index + 1,
            dag_version_id=tis[0].dag_version_id,
        )
        ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
        ti_2.rendered_task_instance_fields = RTIF(ti_2, render_templates=False)
        session.add(ti)
        session.add(ti_2)
        session.commit()

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/{map_index}",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 200

        response2 = test_client.get(f"{self.ENDPOINT_URL}/{map_index}")
        assert response2.status_code == 200
        assert response2.json()["state"] == self.NEW_STATE

        response3 = test_client.get(f"{self.ENDPOINT_URL}/{map_index + 1}")
        assert response3.status_code == 200
        assert response3.json()["state"] != self.NEW_STATE
        assert response3.json()["state"] is None

    def test_should_update_mapped_task_instance_summary_state(self, test_client, session):
        tis = self.create_task_instances(session)

        for map_index in [1, 2, 3]:
            ti = TaskInstance(
                task=tis[0].task,
                run_id=tis[0].run_id,
                map_index=map_index,
                dag_version_id=tis[0].dag_version_id,
            )
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            session.add(ti)
        tis[0].map_index = 0
        session.commit()

        response = test_client.patch(
            f"{self.ENDPOINT_URL}",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 200

        response_data = response.json()
        assert response_data["total_entries"] == 4
        for map_index in range(4):
            assert response_data["task_instances"][map_index]["state"] == self.NEW_STATE

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("error", "code", "payload"),
        [
            [
                [
                    "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `print_the_context` and map_index: `None` was not found",
                ],
                404,
                {
                    "new_state": "failed",
                },
            ]
        ],
    )
    def test_should_handle_errors(self, error, code, payload, test_client, session):
        response = test_client.patch(
            self.ENDPOINT_URL,
            json=payload,
        )
        assert response.status_code == code
        assert response.json()["detail"] == error

    def test_should_200_for_unknown_fields(self, test_client, session):
        self.create_task_instances(session)
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 200

    def test_should_raise_404_for_non_existent_dag(self, test_client):
        response = test_client.patch(
            "/dags/non-existent-dag/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "The Dag with ID: `non-existent-dag` was not found"}

    def test_should_raise_404_for_non_existent_task_in_dag(self, test_client):
        response = test_client.patch(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/non_existent_task",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404
        assert response.json() == {
            "detail": "Task 'non_existent_task' not found in DAG 'example_python_operator'"
        }

    def test_should_raise_404_not_found_dag(self, test_client):
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404

    def test_should_raise_404_not_found_task(self, test_client):
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404

    @pytest.mark.parametrize(
        ("payload", "expected"),
        [
            (
                {
                    "new_state": "failede",
                },
                f"'failede' is not one of ['{State.SUCCESS}', '{State.FAILED}', '{State.SKIPPED}']",
            ),
            (
                {
                    "new_state": "queued",
                },
                f"'queued' is not one of ['{State.SUCCESS}', '{State.FAILED}', '{State.SKIPPED}']",
            ),
        ],
    )
    def test_should_raise_422_for_invalid_task_instance_state(self, payload, expected, test_client, session):
        self.create_task_instances(session)
        response = test_client.patch(
            self.ENDPOINT_URL,
            json=payload,
        )
        assert response.status_code == 422
        assert response.json() == {
            "detail": [
                {
                    "type": "value_error",
                    "loc": ["body", "new_state"],
                    "msg": f"Value error, {expected}",
                    "input": payload["new_state"],
                    "ctx": {"error": {}},
                }
            ]
        }

    @pytest.mark.parametrize(
        ("new_state", "expected_status_code", "expected_json", "set_ti_state_call_count"),
        [
            (
                "failed",
                200,
                {
                    "task_instances": [
                        {
                            "dag_id": "example_python_operator",
                            "dag_display_name": "example_python_operator",
                            "dag_version": {
                                "bundle_name": "dags-folder",
                                "bundle_url": None,
                                "bundle_version": None,
                                "created_at": mock.ANY,
                                "dag_display_name": "example_python_operator",
                                "dag_id": "example_python_operator",
                                "id": mock.ANY,
                                "version_number": 1,
                            },
                            "dag_run_id": "TEST_DAG_RUN_ID",
                            "logical_date": "2020-01-01T00:00:00Z",
                            "task_id": "print_the_context",
                            "duration": 10000.0,
                            "end_date": "2020-01-03T00:00:00Z",
                            "executor": None,
                            "executor_config": "{}",
                            "hostname": "",
                            "id": mock.ANY,
                            "map_index": -1,
                            "max_tries": 0,
                            "note": "placeholder-note",
                            "operator": "PythonOperator",
                            "operator_name": "PythonOperator",
                            "pid": 100,
                            "pool": "default_pool",
                            "pool_slots": 1,
                            "priority_weight": 9,
                            "queue": "default_queue",
                            "queued_when": None,
                            "scheduled_when": None,
                            "start_date": "2020-01-02T00:00:00Z",
                            "state": "running",
                            "task_display_name": "print_the_context",
                            "try_number": 0,
                            "unixname": getuser(),
                            "rendered_fields": {},
                            "rendered_map_index": None,
                            "run_after": "2020-01-01T00:00:00Z",
                            "trigger": None,
                            "triggerer_job": None,
                        }
                    ],
                    "total_entries": 1,
                },
                1,
            ),
            (
                None,
                422,
                {
                    "detail": [
                        {
                            "type": "value_error",
                            "loc": ["body", "new_state"],
                            "msg": "Value error, 'new_state' should not be empty",
                            "input": None,
                            "ctx": {"error": {}},
                        }
                    ]
                },
                0,
            ),
        ],
    )
    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_instance_state")
    def test_update_mask_should_call_mocked_api(
        self,
        mock_set_ti_state,
        test_client,
        session,
        new_state,
        expected_status_code,
        expected_json,
        set_ti_state_call_count,
    ):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.map_index == -1,
            )
        ).all()

        response = test_client.patch(
            self.ENDPOINT_URL,
            params={"update_mask": "new_state"},
            json={
                "new_state": new_state,
            },
        )
        response_data = response.json()
        if expected_status_code == 200:
            expected_json["task_instances"][0]["dag_version"]["created_at"] = response_data["task_instances"][
                0
            ]["dag_version"]["created_at"]
            expected_json["task_instances"][0]["dag_version"]["id"] = response_data["task_instances"][0][
                "dag_version"
            ]["id"]
            expected_json["task_instances"][0]["id"] = response_data["task_instances"][0]["id"]
        assert response.status_code == expected_status_code
        assert response_data == expected_json
        assert mock_set_ti_state.call_count == set_ti_state_call_count

    @pytest.mark.parametrize(
        ("new_note_value", "ti_note_data"),
        [
            (
                "My super cool TaskInstance note.",
                {"content": "My super cool TaskInstance note.", "user_id": "test"},
            ),
            (
                None,
                {"content": None, "user_id": "test"},
            ),
        ],
    )
    def test_update_mask_set_note_should_respond_200(
        self, test_client, session, new_note_value, ti_note_data
    ):
        self.create_task_instances(session)
        response = test_client.patch(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            params={"update_mask": "note"},
            json={"note": new_note_value},
        )
        assert response.status_code == 200, response.text
        response_data = response.json()
        assert response_data == {
            "task_instances": [
                {
                    "dag_id": self.DAG_ID,
                    "dag_display_name": self.DAG_DISPLAY_NAME,
                    "dag_version": {
                        "bundle_name": "dags-folder",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][0]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "logical_date": "2020-01-01T00:00:00Z",
                    "id": response_data["task_instances"][0]["id"],
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 0,
                    "note": new_note_value,
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 9,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_id": self.TASK_ID,
                    "task_display_name": self.TASK_ID,
                    "try_number": 0,
                    "unixname": getuser(),
                    "dag_run_id": self.RUN_ID,
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                }
            ],
            "total_entries": 1,
        }
        _check_task_instance_note(session, response_data["task_instances"][0]["id"], ti_note_data)

    def test_set_note_should_respond_200(self, test_client, session):
        self.create_task_instances(session)
        new_note_value = "My super cool TaskInstance note."
        response = test_client.patch(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={"note": new_note_value},
        )
        assert response.status_code == 200, response.text
        response_data = response.json()
        assert response_data == {
            "task_instances": [
                {
                    "dag_id": self.DAG_ID,
                    "dag_display_name": self.DAG_DISPLAY_NAME,
                    "dag_version": {
                        "bundle_name": "dags-folder",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][0]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "logical_date": "2020-01-01T00:00:00Z",
                    "id": response_data["task_instances"][0]["id"],
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 0,
                    "note": new_note_value,
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 9,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_id": self.TASK_ID,
                    "task_display_name": self.TASK_ID,
                    "try_number": 0,
                    "unixname": getuser(),
                    "dag_run_id": self.RUN_ID,
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                }
            ],
            "total_entries": 1,
        }

        _check_task_instance_note(
            session, response_data["task_instances"][0]["id"], {"content": new_note_value, "user_id": "test"}
        )

    def test_set_note_should_respond_200_mapped_task_with_rtif(self, test_client, session):
        """Verify we don't duplicate rows through join to RTIF"""
        tis = self.create_task_instances(session)
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(
                task=old_ti.task, run_id=old_ti.run_id, map_index=idx, dag_version_id=old_ti.dag_version_id
            )
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.commit()

        # in each loop, we should get the right mapped TI back
        for map_index in (1, 2):
            new_note_value = f"My super cool TaskInstance note {map_index}"
            response = test_client.patch(
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
                f"print_the_context/{map_index}",
                json={"note": new_note_value},
            )
            assert response.status_code == 200, response.text
            response_data = response.json()
            assert response_data == {
                "task_instances": [
                    {
                        "dag_id": self.DAG_ID,
                        "dag_display_name": self.DAG_DISPLAY_NAME,
                        "dag_version": {
                            "bundle_name": "dags-folder",
                            "bundle_url": None,
                            "bundle_version": None,
                            "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                            "dag_display_name": "example_python_operator",
                            "dag_id": "example_python_operator",
                            "id": response_data["task_instances"][0]["dag_version"]["id"],
                            "version_number": 1,
                        },
                        "duration": 10000.0,
                        "end_date": "2020-01-03T00:00:00Z",
                        "logical_date": "2020-01-01T00:00:00Z",
                        "id": response_data["task_instances"][0]["id"],
                        "executor": None,
                        "executor_config": "{}",
                        "hostname": "",
                        "map_index": map_index,
                        "max_tries": 0,
                        "note": new_note_value,
                        "operator": "PythonOperator",
                        "operator_name": "PythonOperator",
                        "pid": 100,
                        "pool": "default_pool",
                        "pool_slots": 1,
                        "priority_weight": 9,
                        "queue": "default_queue",
                        "queued_when": None,
                        "scheduled_when": None,
                        "start_date": "2020-01-02T00:00:00Z",
                        "state": "running",
                        "task_id": self.TASK_ID,
                        "task_display_name": self.TASK_ID,
                        "try_number": 0,
                        "unixname": getuser(),
                        "dag_run_id": self.RUN_ID,
                        "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                        "rendered_map_index": str(map_index),
                        "run_after": "2020-01-01T00:00:00Z",
                        "trigger": None,
                        "triggerer_job": None,
                    }
                ],
                "total_entries": 1,
            }

            _check_task_instance_note(
                session,
                response_data["task_instances"][0]["id"],
                {"content": new_note_value, "user_id": "test"},
            )

    def test_set_note_should_respond_200_mapped_task_summary_with_rtif(self, test_client, session):
        """Verify we don't duplicate rows through join to RTIF"""
        tis = self.create_task_instances(session)
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(
                task=old_ti.task, run_id=old_ti.run_id, map_index=idx, dag_version_id=old_ti.dag_version_id
            )
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.commit()

        new_note_value = "My super cool TaskInstance note"
        response = test_client.patch(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={"note": new_note_value},
        )
        assert response.status_code == 200, response.text
        response_data = response.json()

        assert response_data["total_entries"] == 3

        for map_index in range(1, 3):
            response_ti = response_data["task_instances"][map_index]
            assert response_ti == {
                "dag_id": self.DAG_ID,
                "dag_display_name": self.DAG_DISPLAY_NAME,
                "dag_version": {
                    "bundle_name": "dags-folder",
                    "bundle_url": None,
                    "bundle_version": None,
                    "created_at": response_ti["dag_version"]["created_at"],
                    "dag_display_name": "example_python_operator",
                    "dag_id": "example_python_operator",
                    "id": response_ti["dag_version"]["id"],
                    "version_number": 1,
                },
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00Z",
                "logical_date": "2020-01-01T00:00:00Z",
                "id": response_ti["id"],
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "map_index": map_index,
                "max_tries": 0,
                "note": new_note_value,
                "operator": "PythonOperator",
                "operator_name": "PythonOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 9,
                "queue": "default_queue",
                "queued_when": None,
                "scheduled_when": None,
                "start_date": "2020-01-02T00:00:00Z",
                "state": "running",
                "task_id": self.TASK_ID,
                "task_display_name": self.TASK_ID,
                "try_number": 0,
                "unixname": getuser(),
                "dag_run_id": self.RUN_ID,
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "rendered_map_index": str(map_index),
                "run_after": "2020-01-01T00:00:00Z",
                "trigger": None,
                "triggerer_job": None,
            }

            _check_task_instance_note(
                session, response_ti["id"], {"content": new_note_value, "user_id": "test"}
            )

    def test_set_note_should_respond_200_when_note_is_empty(self, test_client, session):
        tis = self.create_task_instances(session)
        for ti in tis:
            ti.task_instance_note = None
            session.add(ti)
        session.commit()
        new_note_value = "My super cool TaskInstance note."
        response = test_client.patch(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={"note": new_note_value},
        )
        assert response.status_code == 200, response.text
        response_data = response.json()
        response_ti = response_data["task_instances"][0]
        assert response_ti["note"] == new_note_value
        _check_task_instance_note(session, response_ti["id"], {"content": new_note_value, "user_id": "test"})

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_instance_state")
    def test_should_raise_409_for_updating_same_task_instance_state(
        self, mock_set_ti_state, test_client, session
    ):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = None

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": "success",
            },
        )
        assert response.status_code == 409
        assert "Task id print_the_context is already in success state" in response.text


class TestPatchTaskInstanceDryRun(TestTaskInstanceEndpoint):
    ENDPOINT_URL = "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
    NEW_STATE = "failed"
    DAG_ID = "example_python_operator"
    TASK_ID = "print_the_context"
    RUN_ID = "TEST_DAG_RUN_ID"
    DAG_DISPLAY_NAME = "example_python_operator"

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_instance_state")
    def test_should_call_mocked_api(self, mock_set_ti_state, test_client, session):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.map_index == -1,
            )
        ).all()

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "task_instances": [
                {
                    "dag_id": self.DAG_ID,
                    "dag_display_name": self.DAG_DISPLAY_NAME,
                    "dag_version": {
                        "bundle_name": "dags-folder",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][0]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "dag_run_id": self.RUN_ID,
                    "logical_date": "2020-01-01T00:00:00Z",
                    "task_id": self.TASK_ID,
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "id": response_data["task_instances"][0]["id"],
                    "map_index": -1,
                    "max_tries": 0,
                    "note": "placeholder-note",
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 9,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_display_name": self.TASK_ID,
                    "try_number": 0,
                    "unixname": getuser(),
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                }
            ],
            "total_entries": 1,
        }

        mock_set_ti_state.assert_called_once_with(
            commit=False,
            downstream=False,
            upstream=False,
            future=False,
            map_indexes=None,
            past=False,
            run_id=self.RUN_ID,
            session=mock.ANY,
            state=self.NEW_STATE,
            task_id=self.TASK_ID,
        )

    @pytest.mark.parametrize(
        "payload",
        [
            {
                "new_state": "success",
            },
            {
                "note": "something",
            },
            {
                "new_state": "success",
                "note": "something",
            },
        ],
    )
    def test_should_not_update(self, test_client, session, payload):
        self.create_task_instances(session)

        task_before = test_client.get(self.ENDPOINT_URL).json()

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json=payload,
        )

        assert response.status_code == 200
        assert [ti["task_id"] for ti in response.json()["task_instances"]] == ["print_the_context"]

        task_after = test_client.get(self.ENDPOINT_URL).json()

        assert task_before == task_after

        _check_task_instance_note(session, task_after["id"], {"content": "placeholder-note", "user_id": None})

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={},
        )
        assert response.status_code == 403

    def test_should_not_update_mapped_task_instance(self, test_client, session):
        map_index = 1
        tis = self.create_task_instances(session)
        ti = TaskInstance(
            task=tis[0].task, run_id=tis[0].run_id, map_index=map_index, dag_version_id=tis[0].dag_version_id
        )
        ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
        session.add(ti)
        session.commit()

        task_before = test_client.get(f"{self.ENDPOINT_URL}/{map_index}").json()

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/{map_index}/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )

        assert response.status_code == 200
        assert [ti["task_id"] for ti in response.json()["task_instances"]] == ["print_the_context"]

        task_after = test_client.get(f"{self.ENDPOINT_URL}/{map_index}").json()

        assert task_before == task_after
        _check_task_instance_note(session, task_after["id"], None)

    def test_should_not_update_mapped_task_instance_summary(self, test_client, session):
        map_indexes = [1, 2, 3]
        tis = self.create_task_instances(session)
        for map_index in map_indexes:
            ti = TaskInstance(
                task=tis[0].task,
                run_id=tis[0].run_id,
                map_index=map_index,
                state="running",
                dag_version_id=tis[0].dag_version_id,
            )
            ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
            session.add(ti)

        session.delete(tis[0])
        session.commit()

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )

        assert response.status_code == 200
        assert response.json()["total_entries"] == len(map_indexes)

        for map_index in map_indexes:
            task_after = test_client.get(f"{self.ENDPOINT_URL}/{map_index}").json()
            assert task_after["note"] is None
            assert task_after["state"] == "running"
            _check_task_instance_note(session, task_after["id"], None)

    @pytest.mark.parametrize(
        ("error", "code", "payload"),
        [
            [
                [
                    "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `print_the_context` and map_index: `-1` was not found"
                ],
                404,
                {
                    "new_state": "failed",
                },
            ]
        ],
    )
    def test_should_handle_errors(self, error, code, payload, test_client, session):
        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run?map_index=-1",
            json=payload,
        )
        assert response.status_code == code
        assert response.json()["detail"] == error

    def test_should_200_for_unknown_fields(self, test_client, session):
        self.create_task_instances(session)
        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 200

    def test_should_raise_404_for_non_existent_dag(self, test_client):
        response = test_client.patch(
            "/dags/non-existent-dag/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "The Dag with ID: `non-existent-dag` was not found"}

    def test_should_raise_404_for_non_existent_task_in_dag(self, test_client):
        response = test_client.patch(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/non_existent_task/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404
        assert response.json() == {
            "detail": "Task 'non_existent_task' not found in DAG 'example_python_operator'"
        }

    def test_should_raise_404_not_found_dag(self, test_client):
        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404

    def test_should_raise_404_not_found_task(self, test_client):
        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404

    @pytest.mark.parametrize(
        ("payload", "expected"),
        [
            (
                {
                    "new_state": "failede",
                },
                f"'failede' is not one of ['{State.SUCCESS}', '{State.FAILED}', '{State.SKIPPED}']",
            ),
            (
                {
                    "new_state": "queued",
                },
                f"'queued' is not one of ['{State.SUCCESS}', '{State.FAILED}', '{State.SKIPPED}']",
            ),
        ],
    )
    def test_should_raise_422_for_invalid_task_instance_state(self, payload, expected, test_client, session):
        self.create_task_instances(session)
        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json=payload,
        )
        assert response.status_code == 422
        assert response.json() == {
            "detail": [
                {
                    "type": "value_error",
                    "loc": ["body", "new_state"],
                    "msg": f"Value error, {expected}",
                    "input": payload["new_state"],
                    "ctx": {"error": {}},
                }
            ]
        }

    @pytest.mark.parametrize(
        ("new_state", "expected_status_code", "expected_json", "set_ti_state_call_count"),
        [
            (
                "failed",
                200,
                {
                    "task_instances": [
                        {
                            "dag_id": "example_python_operator",
                            "dag_display_name": "example_python_operator",
                            "dag_version": {
                                "bundle_name": "dags-folder",
                                "bundle_url": None,
                                "bundle_version": None,
                                "created_at": mock.ANY,
                                "dag_display_name": "example_python_operator",
                                "dag_id": "example_python_operator",
                                "id": mock.ANY,
                                "version_number": 1,
                            },
                            "dag_run_id": "TEST_DAG_RUN_ID",
                            "logical_date": "2020-01-01T00:00:00Z",
                            "task_id": "print_the_context",
                            "duration": 10000.0,
                            "end_date": "2020-01-03T00:00:00Z",
                            "executor": None,
                            "executor_config": "{}",
                            "hostname": "",
                            "id": mock.ANY,
                            "map_index": -1,
                            "max_tries": 0,
                            "note": "placeholder-note",
                            "operator": "PythonOperator",
                            "operator_name": "PythonOperator",
                            "pid": 100,
                            "pool": "default_pool",
                            "pool_slots": 1,
                            "priority_weight": 9,
                            "queue": "default_queue",
                            "queued_when": None,
                            "scheduled_when": None,
                            "start_date": "2020-01-02T00:00:00Z",
                            "state": "running",
                            "task_display_name": "print_the_context",
                            "try_number": 0,
                            "unixname": getuser(),
                            "rendered_fields": {},
                            "rendered_map_index": None,
                            "run_after": "2020-01-01T00:00:00Z",
                            "trigger": None,
                            "triggerer_job": None,
                        }
                    ],
                    "total_entries": 1,
                },
                1,
            ),
            (
                None,
                422,
                {
                    "detail": [
                        {
                            "type": "value_error",
                            "loc": ["body", "new_state"],
                            "msg": "Value error, 'new_state' should not be empty",
                            "input": None,
                            "ctx": {"error": {}},
                        }
                    ]
                },
                0,
            ),
        ],
    )
    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_instance_state")
    def test_update_mask_should_call_mocked_api(
        self,
        mock_set_ti_state,
        test_client,
        session,
        new_state,
        expected_status_code,
        expected_json,
        set_ti_state_call_count,
    ):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.map_index == -1,
            )
        ).all()

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            params={"update_mask": "new_state"},
            json={
                "new_state": new_state,
            },
        )
        response_data = response.json()
        if expected_status_code == 200:
            expected_json["task_instances"][0]["dag_version"]["created_at"] = response_data["task_instances"][
                0
            ]["dag_version"]["created_at"]
            expected_json["task_instances"][0]["dag_version"]["id"] = response_data["task_instances"][0][
                "dag_version"
            ]["id"]
            expected_json["task_instances"][0]["id"] = response_data["task_instances"][0]["id"]
        assert response.status_code == expected_status_code
        assert response_data == expected_json
        assert mock_set_ti_state.call_count == set_ti_state_call_count

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_instance_state")
    def test_should_return_empty_list_for_updating_same_task_instance_state(
        self, mock_set_ti_state, test_client, session
    ):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = None

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={
                "new_state": "success",
            },
        )
        assert response.status_code == 200
        assert response.json() == {"task_instances": [], "total_entries": 0}


class TestDeleteTaskInstance(TestTaskInstanceEndpoint):
    DAG_ID = "example_python_operator"
    TASK_ID = "print_the_context"
    RUN_ID = "TEST_DAG_RUN_ID"
    ENDPOINT_URL = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}"

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete(self.ENDPOINT_URL)
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete(self.ENDPOINT_URL)
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("test_url", "setup_needed", "expected_error"),
        [
            (
                f"/dags/non_existent_dag/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}",
                False,
                "The Task Instance with dag_id: `non_existent_dag`, run_id: `TEST_DAG_RUN_ID`, task_id: `print_the_context` and map_index: `-1` was not found",
            ),
            (
                f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/non_existent_task",
                True,
                "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `non_existent_task` and map_index: `-1` was not found",
            ),
            (
                f"/dags/{DAG_ID}/dagRuns/NON_EXISTENT_DAG_RUN/taskInstances/{TASK_ID}",
                True,
                "The Task Instance with dag_id: `example_python_operator`, run_id: `NON_EXISTENT_DAG_RUN`, task_id: `print_the_context` and map_index: `-1` was not found",
            ),
        ],
    )
    def test_should_respond_404_for_non_existent_resources(
        self, test_client, session, test_url, setup_needed, expected_error
    ):
        if setup_needed:
            self.create_task_instances(session)
        response = test_client.delete(test_url)
        assert response.status_code == 404
        assert response.json()["detail"] == expected_error

    @pytest.mark.parametrize(
        ("task_instances", "map_index", "expected_status_code", "expected_remaining"),
        [
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                -1,
                200,
                None,
                id="normal-success-state",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.RUNNING}],
                -1,
                200,
                None,
                id="normal-running-state",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.FAILED}],
                -1,
                200,
                None,
                id="normal-failed-state",
            ),
            pytest.param(
                [
                    {"task_id": TASK_ID, "map_index": 1},
                    {"task_id": TASK_ID, "map_index": 2},
                    {"task_id": TASK_ID, "map_index": 3},
                ],
                2,
                200,
                {1, 3},
                id="mapped-task-deletion",
            ),
            pytest.param(
                [{"task_id": TASK_ID}],
                1,
                404,
                set(),
                id="non-mapped-task-with-map-index",
            ),
        ],
    )
    def test_should_handle_task_instance_deletion(
        self,
        test_client,
        session,
        task_instances,
        map_index,
        expected_status_code,
        expected_remaining,
    ):
        self.create_task_instances(session, task_instances=task_instances)

        base_query = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.DAG_ID,
            TaskInstance.task_id == self.TASK_ID,
            TaskInstance.run_id == self.RUN_ID,
        )

        if map_index == -1:
            initial_ti = base_query.filter(TaskInstance.map_index == -1).first()
            assert initial_ti is not None
        else:
            initial_tis = base_query.filter(TaskInstance.map_index != -1).all()
            if any(isinstance(ti, dict) and "map_index" in ti for ti in task_instances):
                expected_map_indexes = {ti["map_index"] for ti in task_instances if "map_index" in ti}
                actual_map_indexes = {ti.map_index for ti in initial_tis}
                assert actual_map_indexes == expected_map_indexes
            else:
                assert len(initial_tis) == 0

        response = test_client.delete(
            self.ENDPOINT_URL,
            params={"map_index": map_index} if map_index != -1 else None,
        )
        assert response.status_code == expected_status_code

        if expected_status_code == 404:
            assert (
                response.json()["detail"]
                == f"The Task Instance with dag_id: `{self.DAG_ID}`, run_id: `{self.RUN_ID}`, task_id: `{self.TASK_ID}` and map_index: `{map_index}` was not found"
            )
        else:
            if map_index == -1:
                deleted_ti = base_query.filter(TaskInstance.map_index == -1).first()
                assert deleted_ti is None
            else:
                remaining_tis = base_query.filter(TaskInstance.map_index != -1).all()
                if expected_remaining is not None:
                    assert set(ti.map_index for ti in remaining_tis) == expected_remaining


class TestBulkTaskInstances(TestTaskInstanceEndpoint):
    DAG_ID = "example_python_operator"
    TASK_ID = "print_the_context"
    RUN_ID = "TEST_DAG_RUN_ID"
    ENDPOINT_URL = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances"
    BASH_DAG_ID = "example_bash_operator"
    BASH_TASK_ID = "also_run_this"
    WILDCARD_ENDPOINT = "/dags/~/dagRuns/~/taskInstances"

    @pytest.mark.parametrize(
        ("default_ti", "actions", "expected_results", "endpoint_url", "setup_dags"),
        [
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                TASK_ID,
                            ],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]"],
                        "errors": [],
                    }
                },
                None,
                None,
                id="delete-success",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                {
                                    "task_id": TASK_ID,
                                    "map_index": -1,
                                },
                            ],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]"],
                        "errors": [],
                    }
                },
                None,
                None,
                id="delete-with-entity-success",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                "non_existent_task",
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [],
                        "errors": [],
                    }
                },
                None,
                None,
                id="delete-skip",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                {
                                    "task_id": "non_existent_task",
                                    "map_index": -1,
                                },
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [],
                        "errors": [],
                    }
                },
                None,
                None,
                id="delete-with-entity-skip",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                "non_existent_task",
                            ],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [],
                        "errors": [
                            {
                                "error": f"No task instances found for dag_id: {DAG_ID}, run_id: {RUN_ID}, task_id: non_existent_task",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                None,
                None,
                id="delete-failure",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                {
                                    "task_id": "non_existent_task",
                                    "map_index": -1,
                                },
                            ],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [],
                        "errors": [
                            {
                                "error": f"The task instances with these identifiers: [{{'dag_id': '{DAG_ID}', 'dag_run_id': '{RUN_ID}', 'task_id': 'non_existent_task', 'map_index': -1}}] were not found",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                None,
                None,
                id="delete-with-entity-failure",
            ),
            pytest.param(
                [
                    {"task_id": TASK_ID, "state": State.SUCCESS, "map_index": 0},
                    {"task_id": TASK_ID, "state": State.SUCCESS, "map_index": 1},
                    {"task_id": TASK_ID, "state": State.SUCCESS, "map_index": 2},
                ],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                {"task_id": TASK_ID, "map_index": None},
                            ],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [
                            f"{DAG_ID}.{RUN_ID}.{TASK_ID}[0]",
                            f"{DAG_ID}.{RUN_ID}.{TASK_ID}[1]",
                            f"{DAG_ID}.{RUN_ID}.{TASK_ID}[2]",
                        ],
                        "errors": [],
                    }
                },
                None,
                None,
                id="delete-all-map-indexes",
            ),
            pytest.param(
                [
                    {"task_id": TASK_ID, "state": State.SUCCESS},
                    {"task_id": "another_task", "state": State.SUCCESS},
                ],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [TASK_ID, {"task_id": "another_task", "map_index": -1}],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [
                            f"{DAG_ID}.{RUN_ID}.another_task[-1]",
                            f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]",
                        ],
                        "errors": [],
                    }
                },
                None,
                None,
                id="mixed-string-and-object",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.RUNNING}],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": TASK_ID,
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                },
                            ],
                        }
                    ]
                },
                {
                    "update": {
                        "success": [f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]"],
                        "errors": [],
                    }
                },
                None,
                None,
                id="update-success",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.RUNNING}],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": "non_existent_task",
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                },
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [],
                    }
                },
                None,
                None,
                id="update-skip",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.RUNNING, "map_index": 100}],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": TASK_ID,
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                    "map_index": 100,
                                },
                            ],
                        }
                    ]
                },
                {
                    "update": {
                        "success": [f"{DAG_ID}.{RUN_ID}.{TASK_ID}[100]"],
                        "errors": [],
                    }
                },
                None,
                None,
                id="update-success-mapped-task",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.RUNNING}],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": "non_existent_task",
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                },
                            ],
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [
                            {
                                "error": f"The Task Instance with dag_id: `{DAG_ID}`, run_id: `{RUN_ID}`, task_id: `non_existent_task` and map_index: `None` was not found",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                None,
                None,
                id="update-failure",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.RUNNING}],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": TASK_ID,
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                    "map_index": -100,
                                },
                            ],
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [
                            {
                                "error": f"The Task Instance with dag_id: `{DAG_ID}`, run_id: `{RUN_ID}`, task_id: `{TASK_ID}` and map_index: `-100` was not found",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                None,
                None,
                id="update-failure-mapped-task",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.RUNNING}],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": TASK_ID,
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                    "map_index": -100,
                                },
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [],
                    }
                },
                None,
                None,
                id="update-failure-mapped-task-with-skip",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": "non_existent_task",
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                },
                            ],
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "task_id": TASK_ID,
                                    "new_state": "failed",
                                    "note": "test",
                                    "include_upstream": True,
                                    "include_downstream": True,
                                    "include_future": True,
                                    "include_past": True,
                                },
                            ],
                        },
                        {"action": "delete", "entities": [TASK_ID]},
                        {"action": "delete", "entities": ["non_existent_task"]},
                    ],
                },
                {
                    "update": {
                        "success": [f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]"],
                        "errors": [
                            {
                                "error": f"The Task Instance with dag_id: `{DAG_ID}`, run_id: `{RUN_ID}`, task_id: `non_existent_task` and map_index: `None` was not found",
                                "status_code": 404,
                            }
                        ],
                    },
                    "delete": {
                        "success": [f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]"],
                        "errors": [
                            {
                                "error": f"No task instances found for dag_id: {DAG_ID}, run_id: {RUN_ID}, task_id: non_existent_task",
                                "status_code": 404,
                            }
                        ],
                    },
                },
                None,
                None,
                id="update-delete-success",
            ),
            pytest.param(
                [{"task_id": TASK_ID, "state": State.SUCCESS}],
                {
                    "actions": [{"action": "create", "entities": []}],
                },
                {
                    "create": {
                        "success": [],
                        "errors": [
                            {"error": "Task instances bulk create is not supported", "status_code": 405}
                        ],
                    }
                },
                None,
                None,
                id="create-failure",
            ),
            pytest.param(
                [
                    {"task_id": BASH_TASK_ID, "state": State.SUCCESS},
                    {"task_id": TASK_ID, "state": State.SUCCESS},
                ],
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                {
                                    "dag_id": BASH_DAG_ID,
                                    "dag_run_id": RUN_ID,
                                    "task_id": BASH_TASK_ID,
                                },
                                {
                                    "dag_id": DAG_ID,
                                    "dag_run_id": RUN_ID,
                                    "task_id": TASK_ID,
                                },
                            ],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [
                            f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]",
                            f"{BASH_DAG_ID}.{RUN_ID}.{BASH_TASK_ID}[-1]",
                        ],
                        "errors": [],
                    }
                },
                WILDCARD_ENDPOINT,
                [BASH_DAG_ID, DAG_ID],
                id="wildcard-delete-across-dags",
            ),
            pytest.param(
                [
                    {"task_id": BASH_TASK_ID, "state": State.RUNNING},
                    {"task_id": TASK_ID, "state": State.RUNNING},
                ],
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "dag_id": BASH_DAG_ID,
                                    "dag_run_id": RUN_ID,
                                    "task_id": BASH_TASK_ID,
                                    "new_state": "success",
                                },
                                {
                                    "dag_id": DAG_ID,
                                    "dag_run_id": RUN_ID,
                                    "task_id": TASK_ID,
                                    "new_state": "success",
                                },
                            ],
                        }
                    ]
                },
                {
                    "update": {
                        "success": [
                            f"{BASH_DAG_ID}.{RUN_ID}.{BASH_TASK_ID}[-1]",
                            f"{DAG_ID}.{RUN_ID}.{TASK_ID}[-1]",
                        ],
                        "errors": [],
                    }
                },
                WILDCARD_ENDPOINT,
                [BASH_DAG_ID, DAG_ID],
                id="wildcard-update-across-dags",
            ),
        ],
    )
    def test_bulk_task_instances(
        self, test_client, session, default_ti, actions, expected_results, endpoint_url, setup_dags
    ):
        # Setup task instances
        if setup_dags:
            for dag_id in setup_dags:
                self.create_task_instances(
                    session, task_instances=default_ti, dag_id=dag_id, update_extras=True
                )
        else:
            self.create_task_instances(session, task_instances=default_ti)

        url = endpoint_url or self.ENDPOINT_URL
        response = test_client.patch(url, json=actions)
        assert response.status_code == 200
        response_data = response.json()
        for task_id, value in expected_results.items():
            assert sorted(response_data[task_id]) == sorted(value)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(self.ENDPOINT_URL, json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(self.ENDPOINT_URL, json={})
        assert response.status_code == 403

    def test_should_respond_422(self, test_client):
        response = test_client.patch(self.ENDPOINT_URL, json={})
        assert response.status_code == 422
