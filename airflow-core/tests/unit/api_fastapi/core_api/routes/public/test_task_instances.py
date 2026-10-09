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
import math
import os
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import pendulum
import pytest
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql.selectable import Select

from airflow._shared.secrets_masker import mask_secret
from airflow._shared.state import TaskScope
from airflow._shared.timezones.timezone import datetime
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.core_api.services.public import task_instances as task_instances_service
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models import DagModel, DagRun, Log, TaskInstance
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dynamic_region import LOOP_DECISION_KEY, DynamicRegion
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.models.taskinstance import clear_loop_task_instances, clear_task_instances, uuid7
from airflow.models.team import Team
from airflow.models.trigger import Trigger
from airflow.models.xcom import XComModelV2
from airflow.sdk import BaseOperator, TaskGroup, task, task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.state.metastore import MetastoreBackend
from airflow.utils.platform import getuser
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.api_fastapi import _check_task_instance_note
from tests_common.test_utils.asserts import assert_queries_count, count_queries
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_runs,
    clear_db_teams,
    clear_rendered_ti_fields,
)
from tests_common.test_utils.logs import check_last_log
from tests_common.test_utils.mapping import expand_mapped_task_instances, push_mapped_length
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.taskinstance import create_task_instance
from tests_common.test_utils.team import attach_dag_to_team
from unit.listeners.class_listener import ClassBasedListener

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
                kwargs: dict[str, Any] = self.ti_init | {"map_index": mi}
                ti = TaskInstance(task=tasks[i], **kwargs, dag_version_id=dag_version.id)
                session.add(ti)
                ti.dag_run = dr
                ti.try_number = 1
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
            clear_task_instances(tis, session=session)
            successors = []
            for ti in tis:
                current = session.scalar(
                    select(TaskInstance).where(
                        TaskInstance.dag_id == ti.dag_id,
                        TaskInstance.task_id == ti.task_id,
                        TaskInstance.run_id == ti.run_id,
                        TaskInstance.map_index == ti.map_index,
                    )
                )
                assert current.id != ti.id
                assert current.try_number == 2
                current.queue = "default_queue"
                successors.append(current)
                session.flush()
            tis = successors
        session.commit()
        return tis


class TestGetTaskInstance(TestTaskInstanceEndpoint):
    def test_removed_mapped_regional_instances_are_still_listed(self, test_client, dag_maker, session):
        with dag_maker("removed-mapped", serialized=True):
            MockOperator(task_id="kept")

            @task
            def mapped(value):
                return value

            mapped.expand(value=[1, 2])
        dr = dag_maker.create_dagrun()
        with dag_maker(dag_id=dr.dag_id, serialized=True):
            MockOperator(task_id="kept")
        latest_version_id = DagVersion.get_latest_version(dr.dag_id, session=session).id
        removed = [ti for ti in dr.task_instances if ti.task_id == "mapped"]
        assert len(removed) == 2
        for ti in removed:
            ti.state = TaskInstanceState.REMOVED
            ti.dag_version_id = latest_version_id
        session.commit()
        collection_url = f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances"

        response = test_client.get(collection_url)

        assert response.status_code == 200
        entries = {(ti["task_id"], ti["map_index"]) for ti in response.json()["task_instances"]}
        assert entries == {("kept", -1), ("mapped", 0), ("mapped", 1)}
        response = test_client.get(f"{collection_url}/mapped/0")
        assert response.status_code == 200
        assert response.json()["map_index"] == 0

    def test_should_respond_200(self, test_client, session):
        self.create_task_instances(session)
        # Update ti and set operator to None to
        # test that operator field is nullable.
        # This prevents issue when users upgrade to 2.0+
        # from 1.10.x
        # https://github.com/apache/airflow/issues/14421
        session.execute(update(TaskInstance).values(operator=None))
        session.commit()
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        response_data = response.json()
        assert response.status_code == 200
        assert response_data == {
            "dag_id": "example_python_operator",
            "dag_version": {
                "bundle_name": "apache-airflow-providers-standard-example-dags",
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
            "region_id": "00000000-0000-0000-0000-000000000000",
            "region_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": None,
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 14,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "running",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 1,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "rendered_map_index": None,
            "run_after": "2020-01-01T00:00:00Z",
            "trigger": None,
            "triggerer_job": None,
            "team_name": None,
            "state_reason": None,
        }

    def test_should_include_state_reason(self, test_client, session):
        self.create_task_instances(session, task_instances=[{"retry_reason": "auth error, do not retry"}])
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 200
        assert response.json()["state_reason"] == "auth error, do not retry"

    @pytest.fixture
    def masked_secret(self):
        """The masker is a cached process global, so drop the pattern again for the next test."""
        from airflow._shared.secrets_masker import _secrets_masker

        masker = _secrets_masker()
        patterns, replacer = set(masker.patterns), masker.replacer
        mask_secret("hunter2")
        yield
        masker.patterns, masker.replacer = patterns, replacer

    @pytest.mark.enable_redact
    def test_should_redact_secrets_in_state_reason(self, test_client, session, masked_secret):
        """A policy may compose the reason from an unredacted exception, so mask on the way out."""
        self.create_task_instances(
            session, task_instances=[{"retry_reason": "auth: the token hunter2 expired"}]
        )
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 200
        assert response.json()["state_reason"] == "auth: the token *** expired"

    @conf_vars({("core", "multi_team"): "True"})
    def test_should_include_team_name(self, test_client, session):
        self.create_task_instances(session)
        with attach_dag_to_team(
            session, "example_python_operator", bundle_name="team-bundle-ti", team_name="team-ti"
        ):
            response = test_client.get(
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
            )
            assert response.status_code == 200
            assert response.json()["team_name"] == "team-ti"

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
    def test_should_respond_200_with_versions(self, test_client, run_id, expected_version_number):
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
            "region_id": "00000000-0000-0000-0000-000000000000",
            "region_index": -1,
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
            "team_name": None,
            "state_reason": None,
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
                "bundle_name": "apache-airflow-providers-standard-example-dags",
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
            "region_id": "00000000-0000-0000-0000-000000000000",
            "region_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 14,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "deferred",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 1,
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
                "bundle_names": None,
                "dag_display_name": None,
                "dag_id": None,
                "end_date": None,
                "job_type": "TriggererJob",
                "state": "running",
                "team_names": [],
                "unixname": getuser(),
            },
            "team_name": None,
            "state_reason": None,
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
                "bundle_name": "apache-airflow-providers-standard-example-dags",
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
            "region_id": "00000000-0000-0000-0000-000000000000",
            "region_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 14,
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
            "rendered_fields": {},
            "rendered_map_index": None,
            "run_after": "2020-01-01T00:00:00Z",
            "trigger": None,
            "triggerer_job": None,
            "team_name": None,
            "state_reason": None,
        }

    def test_should_respond_200_task_instance_with_rendered(self, test_client, session):
        tis = self.create_task_instances(session)
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
                "bundle_name": "apache-airflow-providers-standard-example-dags",
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
            "region_id": "00000000-0000-0000-0000-000000000000",
            "region_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
            "operator_name": "PythonOperator",
            "pid": 100,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 14,
            "queue": "default_queue",
            "queued_when": None,
            "scheduled_when": None,
            "start_date": "2020-01-02T00:00:00Z",
            "state": "running",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 1,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "rendered_map_index": None,
            "run_after": "2020-01-01T00:00:00Z",
            "trigger": None,
            "triggerer_job": None,
            "team_name": None,
            "state_reason": None,
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
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note", "try_number"]:
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
        for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note", "try_number"]:
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
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note", "try_number"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
            session.flush()
            session.add(RTIF(ti, render_templates=False))
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
                    "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                "region_id": "00000000-0000-0000-0000-000000000000",
                "region_index": map_index,
                "max_tries": 0,
                "note": "placeholder-note",
                "operator": "PythonOperator",
                "operator_name": "PythonOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 14,
                "queue": "default_queue",
                "queued_when": None,
                "scheduled_when": None,
                "start_date": "2020-01-02T00:00:00Z",
                "state": "running",
                "task_id": "print_the_context",
                "task_display_name": "print_the_context",
                "try_number": 1,
                "unixname": getuser(),
                "dag_run_id": "TEST_DAG_RUN_ID",
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "rendered_map_index": str(map_index),
                "run_after": "2020-01-01T00:00:00Z",
                "trigger": None,
                "triggerer_job": None,
                "team_name": None,
                "state_reason": None,
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

    @conf_vars({("core", "multi_team"): "True"})
    def test_should_include_team_name(self, test_client, session):
        self.create_task_instances(session)
        with attach_dag_to_team(
            session,
            "example_python_operator",
            bundle_name="team-bundle-mapped-ti",
            team_name="team-mapped-ti",
        ):
            response = test_client.get(
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/-1",
            )
            assert response.status_code == 200
            assert response.json()["team_name"] == "team-mapped-ti"


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
            push_mapped_length(
                dr.get_task_instance(task1.task_id, session=session), list(range(count)), session=session
            )

            if count:
                # Remove the map_index=-1 TI when we're creating other TIs
                session.execute(
                    delete(TaskInstance).where(
                        TaskInstance.dag_id == mapped.dag_id,
                        TaskInstance.task_id == mapped.task_id,
                        TaskInstance.run_id == dr.run_id,
                    )
                )

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
            dagbag = DagBag(os.devnull)
            dagbag.dags = {dag_id: dag_maker.dag}
            sync_bag_to_db(dagbag, "dags-folder", None)
            session.flush()

            expand_mapped_task_instances(sdag.task_dict[mapped.task_id], dr.run_id, session=session)

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
        with assert_queries_count(5):
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
                {"order_by": "state", "limit": 108},  # Maximum page limit will limit result to 100 items.
                list(range(5, 25)) + list(range(25, 105)),
            ),
            (
                {"order_by": "-state", "limit": 100},
                list(range(5)[::-1]) + list(range(25, 110)[::-1]) + list(range(15, 25)[::-1]),
            ),
            ({"order_by": "logical_date", "limit": 100}, list(range(100))),
            ({"order_by": "-logical_date", "limit": 100}, list(range(109, 9, -1))),
            ({"order_by": "data_interval_start", "limit": 100}, list(range(100))),
            ({"order_by": "-data_interval_start", "limit": 100}, list(range(109, 9, -1))),
            # Compound sort (_rendered_map_index ASC, map_index ASC): all TIs have NULL
            # _rendered_map_index in this fixture so they all tie on the first key and
            # are ordered by map_index integer — 0, 1, 2, ..., 99 (not lexicographic).
            ({"order_by": "rendered_map_index", "limit": 100}, list(range(100))),
            (
                {"order_by": "-rendered_map_index", "limit": 100},
                list(range(109, 9, -1)),
            ),
        ],
    )
    def test_mapped_instances_order(
        self, test_client, session, params, expected_map_indexes, one_task_with_many_mapped_tis
    ):
        from airflow.configuration import conf

        with assert_queries_count(5):
            response = test_client.get(
                "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
                params=params,
            )

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 110
        assert len(body["task_instances"]) == min(params["limit"], conf.getint("api", "maximum_page_limit"))
        assert expected_map_indexes == [ti["map_index"] for ti in body["task_instances"]]

    @pytest.mark.parametrize(
        ("filter_param", "value", "expected_map_indexes"),
        [
            # Prefix match (index-friendly).
            ("rendered_map_index_prefix_pattern", "table_", [0, 1]),
            ("rendered_map_index_prefix_pattern", "metrics", [2, 3]),
            ("rendered_map_index_prefix_pattern", "nope", []),
            # Fallback to str(map_index) when _rendered_map_index is NULL.
            # Prefix "10" matches "10" and "100".."109".
            ("rendered_map_index_prefix_pattern", "10", [10, *range(100, 110)]),
            # Substring match (advanced).
            ("rendered_map_index_pattern", "table_orders", [0]),
            ("rendered_map_index_pattern", "table_orders|metrics_daily", [0, 2]),
            ("rendered_map_index_pattern", "_users", [1]),
            ("rendered_map_index_pattern", "nope", []),
        ],
    )
    def test_rendered_map_index_filter(
        self,
        test_client,
        session,
        one_task_with_many_mapped_tis,
        filter_param,
        value,
        expected_map_indexes,
    ):
        rendered_by_map_index = {
            0: "table_orders",
            1: "table_users",
            2: "metrics_daily",
            3: "metrics_hourly",
        }
        for map_index, rendered in rendered_by_map_index.items():
            ti = session.scalars(
                select(TaskInstance).where(
                    TaskInstance.task_id == "task_2", TaskInstance.map_index == map_index
                )
            ).first()
            ti._rendered_map_index = rendered
        session.commit()

        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={filter_param: value, "order_by": "map_index"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == len(expected_map_indexes)
        assert [ti["map_index"] for ti in body["task_instances"]] == expected_map_indexes

    def test_rendered_map_index_order_without_template_numeric(self, test_client, session, dag_maker):
        """map_index values beyond 9 must sort numerically, not lexicographically.

        Without the compound sort the SQL expression falls back to
        CAST(map_index AS String), producing "0","1","10","11","2"... instead
        of 0, 1, 2, ..., 10, 11.
        """
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={"numeric_order_dag": {"success": 12, "failed": 0, "running": 0}},
        )

        response = test_client.get(
            "/dags/numeric_order_dag/dagRuns/run_numeric_order_dag/taskInstances/task_2/listMapped",
            params={"order_by": "rendered_map_index", "limit": 20},
        )
        assert response.status_code == 200
        body = response.json()
        # Numeric order: 0, 1, 2, ..., 11.
        # Lexicographic order would be: 0, 1, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9.
        assert [ti["map_index"] for ti in body["task_instances"]] == list(range(12))

    def test_rendered_map_index_order_with_template(self, test_client, session, one_task_with_mapped_tis):
        """Custom map_index_template labels must be sorted alphabetically."""
        # one_task_with_mapped_tis creates 3 TIs: map_index 0, 1, 2.
        labels = {0: "zebra", 1: "apple", 2: "mango"}
        for map_index, label in labels.items():
            ti = session.scalar(
                select(TaskInstance).where(
                    TaskInstance.task_id == "task_2",
                    TaskInstance.map_index == map_index,
                )
            )
            ti._rendered_map_index = label
        session.commit()

        response = test_client.get(
            "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"order_by": "rendered_map_index"},
        )
        assert response.status_code == 200
        body = response.json()
        # Alphabetical order: "apple" (1), "mango" (2), "zebra" (0).
        assert [ti["map_index"] for ti in body["task_instances"]] == [1, 2, 0]
        assert [ti["rendered_map_index"] for ti in body["task_instances"]] == [
            "apple",
            "mango",
            "zebra",
        ]

    def test_rendered_map_index_order_stable_regardless_of_uuid_order(self, test_client, session, dag_maker):
        """
        Results must be ordered by integer map_index regardless of UUID insertion order.

        The compound sort ([_rendered_map_index, map_index]) makes the integer
        map_index the effective tiebreaker when no map_index_template is set.
        This verifies that even when UUIDs are assigned out of map_index order
        (as happens during retries), the response is still sorted 0, 1, 2, ...
        """
        self.create_dag_runs_with_mapped_tasks(
            dag_maker,
            session,
            dags={"retry_dag": {"success": 5, "failed": 0, "running": 0}},
        )

        # Assign newer (larger) UUIDs to map_index 1 and 3, simulating retry
        # ordering where some TIs received their UUIDs after others.  The sort
        # result must still follow integer map_index order, not UUID order.
        for map_index in [1, 3]:
            session.execute(
                update(TaskInstance)
                .where(
                    TaskInstance.dag_id == "retry_dag",
                    TaskInstance.task_id == "task_2",
                    TaskInstance.map_index == map_index,
                )
                .values(id=uuid7())
            )
        session.commit()

        response = test_client.get(
            "/dags/retry_dag/dagRuns/run_retry_dag/taskInstances/task_2/listMapped",
            params={"order_by": "rendered_map_index", "limit": 50},
        )
        assert response.status_code == 200
        body = response.json()
        # All 5 TIs must be on the first page in map_index order.
        assert body["total_entries"] == 5
        assert [ti["map_index"] for ti in body["task_instances"]] == [0, 1, 2, 3, 4]

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

    def test_no_duplicate_joins_in_get_mapped_task_instances_query(
        self, one_task_with_mapped_tis, test_client
    ):
        """Regression test for #62027: the get_mapped_task_instances endpoint must not emit duplicate JOINs."""
        from sqlalchemy import event

        import airflow.settings

        executed_statements: list[str] = []

        def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_statements.append(statement.upper())

        event.listen(airflow.settings.engine, "before_cursor_execute", capture)
        try:
            response = test_client.get(
                "/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            )
        finally:
            event.remove(airflow.settings.engine, "before_cursor_execute", capture)

        assert response.status_code == 200

        ti_queries = [s for s in executed_statements if "FROM TASK_INSTANCE" in s and "JOIN DAG_RUN" in s]
        assert ti_queries, "Expected at least one query selecting from task_instance with JOIN dag_run"
        for q in ti_queries:
            assert q.count("JOIN DAG_RUN") == 1, "dag_run must appear exactly once in JOINs"
            if "JOIN DAG_VERSION" in q:
                assert q.count("JOIN DAG_VERSION") == 1, "dag_version must appear exactly once in JOINs"


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
                6,
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
                6,
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
                6,
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
                6,
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
                6,
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
                8,
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
                4,
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
                4,
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
                8,
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
                8,
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
                8,
                id="test null states with no filter",
            ),
            pytest.param(
                [{"start_date": None, "end_date": None}],
                True,
                "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances",
                {"start_date_gte": DEFAULT_DATETIME_STR_1},
                1,
                8,
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
                8,
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
                4,
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
                4,
                id="test pool_name_pattern filter",
            ),
            pytest.param(
                [
                    {"pool": "test_pool_1"},
                    {"pool": "test_pool_2"},
                    {"pool": "test_pool_3"},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"pool_name_prefix_pattern": "test_pool"},
                3,
                4,
                id="test pool_name_prefix_pattern filter",
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
                8,
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
                4,
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
                4,
                id="test queue_name_pattern filter",
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
                {"queue_name_prefix_pattern": "test"},
                3,
                4,
                id="test queue_name_prefix_pattern filter",
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
                8,
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
                4,
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
                4,
                id="test task_display_name_pattern filter",
            ),
            pytest.param(
                [
                    {"_task_display_property_value": "task_name_1"},
                    {"_task_display_property_value": "task_name_2"},
                    {"_task_display_property_value": "task_not_match_name_3"},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"task_display_name_prefix_pattern": "task_name"},
                2,
                4,
                id="test task_display_name_prefix_pattern filter",
            ),
            pytest.param(
                "task_group_test",
                True,
                ("/dags/example_task_group/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"task_group_id": "section_1"},
                3,
                8,
                id="test task_group filter with exact match",
            ),
            pytest.param(
                "task_group_test",
                True,
                ("/dags/example_task_group/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"task_group_id": "section_2"},
                4,  # section_2 has 4 tasks: task_1 + inner_section_2 (task_2, task_3, task_4)
                8,
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
                4,
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
                4,
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
                4,
                id="test multiple version numbers filter",
            ),
            pytest.param(
                [
                    {"try_number": 0, "state": None},
                    {"try_number": 0, "state": None},
                    {"try_number": 1},
                    {"try_number": 1},
                    {"try_number": 1},
                    {"try_number": 2},
                ],
                True,
                ("/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"try_number": [0, 1]},
                5,
                8,
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
                4,
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
                4,
                id="test operator_name_pattern filter",
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
                {"operator_name_prefix_pattern": "Custom"},
                2,
                4,
                id="test operator_name_prefix_pattern filter",
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
                4,
                id="test map_index filter",
            ),
            pytest.param(
                [
                    {"map_index": 0, "_rendered_map_index": "table_orders"},
                    {"map_index": 1, "_rendered_map_index": "table_users"},
                    {"map_index": 2, "_rendered_map_index": None},
                    {"map_index": 3, "_rendered_map_index": None},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"rendered_map_index_prefix_pattern": "table_"},
                2,
                4,
                id="test rendered_map_index_prefix_pattern filter",
            ),
            pytest.param(
                [
                    {"map_index": 0, "_rendered_map_index": "table_orders"},
                    {"map_index": 1, "_rendered_map_index": "table_users"},
                    {"map_index": 2, "_rendered_map_index": None},
                    {"map_index": 3, "_rendered_map_index": None},
                ],
                True,
                "/dags/~/dagRuns/~/taskInstances",
                {"rendered_map_index_pattern": "_users|table_orders"},
                2,
                4,
                id="test rendered_map_index_pattern filter",
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
                4,
                id="test run_id_pattern filter",
            ),
            pytest.param(
                [
                    {},
                ],
                True,
                ("/dags/~/dagRuns/~/taskInstances"),
                {"run_id_prefix_pattern": "TEST_DAG_"},
                1,
                4,
                id="test run_id_prefix_pattern filter",
            ),
            pytest.param(
                "dag_id_pattern_test",  # Special marker for multi-DAG test
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_pattern": "example_python_operator"},
                14,  # Based on test failure - example_python_operator creates 14 task instances
                4,
                id="test dag_id_pattern exact match",
            ),
            pytest.param(
                "dag_id_pattern_test",  # Special marker for multi-DAG test
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_pattern": "example_%"},
                22,  # Based on test failure - both DAGs together create 22 task instances
                4,
                id="test dag_id_pattern wildcard prefix",
            ),
            pytest.param(
                "dag_id_pattern_test",  # Special marker for multi-DAG test
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_pattern": "%skip%"},
                8,  # Based on test failure - example_skip_dag creates 8 task instances
                4,
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
            pytest.param(
                "dag_id_pattern_test",
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_prefix_pattern": "example_python_operator"},
                14,
                4,
                id="test dag_id_prefix_pattern exact match",
            ),
            pytest.param(
                "dag_id_pattern_test",
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_prefix_pattern": "example_"},
                22,
                4,
                id="test dag_id_prefix_pattern prefix",
            ),
            pytest.param(
                "dag_id_pattern_test",
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_prefix_pattern": "example_skip"},
                8,
                4,
                id="test dag_id_prefix_pattern specific prefix",
            ),
            pytest.param(
                "dag_id_pattern_test",
                False,
                "/dags/~/dagRuns/~/taskInstances",
                {"dag_id_prefix_pattern": "nonexistent"},
                0,
                3,
                id="test dag_id_prefix_pattern no match",
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

    def test_dag_id_required_when_dag_run_id_specified(self, test_client):
        # dag_run_id is not unique - it requires dag_id to identify a specific dag_run
        response = test_client.get("/dags/~/dagRuns/some_run_id/taskInstances")
        assert response.status_code == 400
        assert response.json() == {"detail": "dag_id is required when dag_run_id is specified"}

    def test_bad_state(self, test_client):
        response = test_client.get("/dags/~/dagRuns/~/taskInstances", params={"state": "invalid"})
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Invalid value for state. Valid values are {', '.join(TaskInstanceState)}"
        )

    def test_no_duplicate_joins_in_get_task_instances_query(self, test_client, session):
        """Regression test for #62027: the get_task_instances endpoint must not emit duplicate JOINs.

        Combining explicit join() with joinedload() on the same tables causes SQLAlchemy
        to emit duplicate JOINs (dag_run twice, dag_version twice). By relying solely on
        joinedload via eager_load_TI_and_TIH_for_validation, each table must appear
        exactly once in the SQL emitted by the real endpoint.
        """
        from sqlalchemy import event

        import airflow.settings

        self.create_task_instances(session)

        executed_statements: list[str] = []

        def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_statements.append(statement.upper())

        event.listen(airflow.settings.engine, "before_cursor_execute", capture)
        try:
            response = test_client.get("/dags/~/dagRuns/~/taskInstances")
        finally:
            event.remove(airflow.settings.engine, "before_cursor_execute", capture)

        assert response.status_code == 200

        # Find all statements that query task_instance joined with dag_run
        ti_queries = [s for s in executed_statements if "FROM TASK_INSTANCE" in s and "JOIN DAG_RUN" in s]
        assert ti_queries, "Expected at least one query selecting from task_instance with JOIN dag_run"
        for q in ti_queries:
            assert q.count("JOIN DAG_RUN") == 1, "dag_run must appear exactly once in JOINs"
            if "JOIN DAG_VERSION" in q:
                assert q.count("JOIN DAG_VERSION") == 1, "dag_version must appear exactly once in JOINs"

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
        count = session.scalar(
            select(func.count())
            .select_from(TaskInstance)
            .where(TaskInstance.dag_id == "example_python_operator")
        )
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

        ti_count = session.scalar(
            select(func.count()).select_from(TaskInstance).where(TaskInstance.dag_id == dag_id)
        )

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

    @pytest.mark.parametrize(
        ("order_by", "expected_map_indexes"),
        [
            # All four TIs have explicit labels so alphabetical order is
            # consistent across databases (no NULL ordering differences).
            # Labels: "analytics"(3), "events"(2), "table_orders"(0), "table_users"(1)
            ("rendered_map_index", [3, 2, 0, 1]),
            ("-rendered_map_index", [1, 0, 2, 3]),
        ],
    )
    def test_should_respond_200_for_rendered_map_index_order(
        self, test_client, session, order_by, expected_map_indexes
    ):
        self.create_task_instances(
            session,
            update_extras=True,
            task_instances=[
                {"map_index": 0, "_rendered_map_index": "table_orders"},
                {"map_index": 1, "_rendered_map_index": "table_users"},
                {"map_index": 2, "_rendered_map_index": "events"},
                {"map_index": 3, "_rendered_map_index": "analytics"},
            ],
        )
        response = test_client.get("/dags/~/dagRuns/~/taskInstances", params={"order_by": order_by})
        assert response.status_code == 200
        assert [ti["map_index"] for ti in response.json()["task_instances"]] == expected_map_indexes

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
        ti_count = session.scalar(
            select(func.count()).select_from(TaskInstance).where(TaskInstance.dag_id == dag_id)
        )
        assert response_batch1.json()["total_entries"] == response_batch2.json()["total_entries"] == ti_count
        assert (num_entries_batch1 + num_entries_batch2) == ti_count
        assert response_batch1 != response_batch2

    def test_cursor_pagination_first_page(self, test_client, session):
        """First page with cursor='' returns cursor response without needing a real token."""
        dag_id = "example_python_operator"
        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(5)
            ],
            dag_id=dag_id,
        )
        response = test_client.get(
            "/dags/~/dagRuns/~/taskInstances",
            params={"limit": 3, "order_by": ["map_index"], "cursor": ""},
        )
        assert response.status_code == 200, response.json()
        body = response.json()
        assert body["next_cursor"] is not None
        assert body["previous_cursor"] is None
        assert body["total_entries"] == 5
        assert body["total_entries_limit"] == 50_000
        assert len(body["task_instances"]) == 3

    def test_cursor_pagination_returns_cursor_response(self, test_client, session):
        """When cursor param is provided, response has cursor fields and a bounded total_entries."""
        dag_id = "example_python_operator"
        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(5)
            ],
            dag_id=dag_id,
        )
        # First page in cursor mode (empty cursor)
        response1 = test_client.get(
            "/dags/~/dagRuns/~/taskInstances",
            params={"limit": 3, "order_by": ["map_index"], "cursor": ""},
        )
        assert response1.status_code == 200
        body1 = response1.json()
        assert body1["total_entries"] == 5
        assert body1["total_entries_limit"] == 50_000
        assert len(body1["task_instances"]) == 3
        next_cursor = body1["next_cursor"]
        assert next_cursor is not None

        # Second (last) page using next_cursor from first page — only 2 TIs remain
        response2 = test_client.get(
            "/dags/~/dagRuns/~/taskInstances",
            params={"limit": 100, "cursor": next_cursor, "order_by": ["map_index"]},
        )
        assert response2.status_code == 200
        body2 = response2.json()
        assert body2["next_cursor"] is None
        assert body2["previous_cursor"] is not None
        assert body2["total_entries"] == 5
        assert body2["total_entries_limit"] == 50_000

    def test_cursor_pagination_forward_and_backward_consistency(self, test_client, session):
        """Walk all pages forward via next_cursor, then backward via previous_cursor, and compare."""
        dag_id = "example_python_operator"
        total_tis = 13
        page_size = 4
        max_pages = math.ceil(total_tis / page_size)
        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(total_tis)
            ],
            dag_id=dag_id,
        )

        # -- Walk forward collecting pages --
        forward_ids: list[str] = []
        forward_pages: list[dict] = []
        cursor_token = ""
        for _ in range(max_pages):
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances",
                params={"limit": page_size, "order_by": ["map_index"], "cursor": cursor_token},
            )
            assert response.status_code == 200, response.json()
            body = response.json()
            assert body["total_entries"] == total_tis
            assert body["total_entries_limit"] == 50_000
            forward_pages.append(body)
            forward_ids.extend(ti["id"] for ti in body["task_instances"])

            cursor_token = body.get("next_cursor")
            if cursor_token is None:
                break

        # Sanity: all TIs collected, no overlaps, multiple pages
        assert len(forward_ids) == total_tis
        assert len(forward_ids) == len(set(forward_ids)), "Forward pages should not overlap"
        assert len(forward_pages) == 4

        # Boundary cursors
        assert forward_pages[0]["previous_cursor"] is None, "First page should have no previous_cursor"
        assert forward_pages[-1]["next_cursor"] is None, "Last page should have no next_cursor"

        # -- Walk backward from the last page using previous_cursor --
        backward_ids: list[str] = []
        cursor_token = forward_pages[-1]["previous_cursor"]
        assert cursor_token is not None, "Last page should provide a previous_cursor"

        for _ in range(max_pages):
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances",
                params={"limit": page_size, "order_by": ["map_index"], "cursor": cursor_token},
            )
            assert response.status_code == 200, response.json()
            body = response.json()
            backward_ids = [ti["id"] for ti in body["task_instances"]] + backward_ids

            cursor_token = body.get("previous_cursor")
            if cursor_token is None:
                break

        # Backward walk covers all items except the last page (already collected).
        # Order must match exactly — no re-sorting needed if pagination is correct.
        all_backward = backward_ids + [ti["id"] for ti in forward_pages[-1]["task_instances"]]
        assert all_backward == forward_ids, (
            "Walking backward + last page should produce the same TIs in the same order as walking forward"
        )

    def test_cursor_pagination_invalid_token(self, test_client, session):
        """Invalid cursor token returns 400."""
        self.create_task_instances(session)
        response = test_client.get(
            "/dags/~/dagRuns/~/taskInstances",
            params={"cursor": "this-is-not-valid", "order_by": ["map_index"]},
        )
        assert response.status_code == 400

    def test_cursor_pagination_order_by_run_after_roundtrips(self, test_client, session):
        """
        Sorting by ``run_after`` (a column-form ``to_replace`` backed by an association proxy)
        must not raise a 500 when ``has_next=true``.  Regression for
        https://github.com/apache/airflow/issues/67970.

        Verify the full cursor round-trip: the first page must include a ``next_cursor``,
        and following that cursor must return the remaining TIs without overlap.
        """
        dag_id = "example_python_operator"
        total_tis = 5
        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(total_tis)
            ],
            dag_id=dag_id,
        )
        # First page — limit < total so next_cursor must be present
        response1 = test_client.get(
            "/dags/~/dagRuns/~/taskInstances",
            params={"limit": 3, "order_by": ["-run_after"], "cursor": ""},
        )
        assert response1.status_code == 200, response1.json()
        body1 = response1.json()
        assert len(body1["task_instances"]) == 3
        next_cursor = body1["next_cursor"]
        assert next_cursor is not None, "next_cursor must be present when more rows exist"

        # Second page — follow the cursor; must not 500 and must return remaining TIs
        response2 = test_client.get(
            "/dags/~/dagRuns/~/taskInstances",
            params={"limit": 10, "order_by": ["-run_after"], "cursor": next_cursor},
        )
        assert response2.status_code == 200, response2.json()
        body2 = response2.json()
        ids1 = {ti["id"] for ti in body1["task_instances"]}
        ids2 = {ti["id"] for ti in body2["task_instances"]}
        assert ids1.isdisjoint(ids2), "Pages must not overlap"
        assert len(ids1) + len(ids2) == total_tis

    def test_cursor_pagination_nullable_sort_column_returns_all_rows(self, test_client, session):
        """Cursor pagination sorted by a nullable column must not silently drop rows.

        With NULLs present, the keyset predicate and the ORDER BY can disagree on NULL
        placement, so every row on one side of the NULL/non-NULL boundary is dropped
        without error.
        """
        dag_id = "example_python_operator"
        # Three TIs with NULL start_date and three with distinct values, so both the NULL and
        # the non-NULL block span more than one page and the boundary is crossed mid-walk.
        self.create_task_instances(
            session,
            task_instances=[
                {"start_date": None},
                {"start_date": None},
                {"start_date": None},
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=1)},
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=2)},
                {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=3)},
            ],
            dag_id=dag_id,
        )

        # Full set via offset pagination (returns everything).
        full = test_client.get("/dags/~/dagRuns/~/taskInstances", params={"limit": 100})
        assert full.status_code == 200, full.json()
        full_ids = {ti["id"] for ti in full.json()["task_instances"]}
        assert len(full_ids) == 6

        # Walk every page forward via cursor, sorted by the nullable column.
        collected: list[str] = []
        cursor_token: str | None = ""
        for _ in range(20):
            resp = test_client.get(
                "/dags/~/dagRuns/~/taskInstances",
                params={"limit": 2, "order_by": ["start_date"], "cursor": cursor_token},
            )
            assert resp.status_code == 200, resp.json()
            body = resp.json()
            collected.extend(ti["id"] for ti in body["task_instances"])
            cursor_token = body.get("next_cursor")
            if cursor_token is None:
                break

        assert len(collected) == len(set(collected)), "cursor pages overlapped"
        assert set(collected) == full_ids, "cursor pagination dropped rows across the NULL boundary"

    def test_cursor_pagination_forward_backward_consistency_nullable(self, test_client, session):
        """Forward then backward walk over a nullable column must agree, NULLs included.

        Backward pagination flips the sort direction, re-deriving the keyset bounds; this
        guards that NULL placement stays consistent in both directions.
        """
        dag_id = "example_python_operator"
        page_size = 3
        # 3 NULL start_dates + 5 distinct values -> NULL block and non-NULL block both span pages.
        self.create_task_instances(
            session,
            task_instances=[{"start_date": None} for _ in range(3)]
            + [{"start_date": DEFAULT_DATETIME_1 + dt.timedelta(minutes=(i + 1))} for i in range(5)],
            dag_id=dag_id,
        )

        forward_ids: list[str] = []
        forward_pages: list[dict] = []
        cursor_token: str | None = ""
        for _ in range(20):
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances",
                params={"limit": page_size, "order_by": ["start_date"], "cursor": cursor_token},
            )
            assert response.status_code == 200, response.json()
            body = response.json()
            forward_pages.append(body)
            forward_ids.extend(ti["id"] for ti in body["task_instances"])
            cursor_token = body.get("next_cursor")
            if cursor_token is None:
                break

        assert len(forward_ids) == 8
        assert len(forward_ids) == len(set(forward_ids)), "Forward pages should not overlap"
        assert forward_pages[0]["previous_cursor"] is None

        backward_ids: list[str] = []
        cursor_token = forward_pages[-1]["previous_cursor"]
        assert cursor_token is not None
        for _ in range(20):
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances",
                params={"limit": page_size, "order_by": ["start_date"], "cursor": cursor_token},
            )
            assert response.status_code == 200, response.json()
            body = response.json()
            backward_ids = [ti["id"] for ti in body["task_instances"]] + backward_ids
            cursor_token = body.get("previous_cursor")
            if cursor_token is None:
                break

        all_backward = backward_ids + [ti["id"] for ti in forward_pages[-1]["task_instances"]]
        assert all_backward == forward_ids, "Backward walk + last page must match the forward walk exactly"

    @conf_vars({("core", "multi_team"): "True"})
    def test_should_include_team_name(self, test_client, session):
        self.create_task_instances(session)
        with attach_dag_to_team(
            session, "example_python_operator", bundle_name="team-bundle-tis", team_name="team-tis"
        ):
            response = test_client.get(f"/dags/{'example_python_operator'}/dagRuns/~/taskInstances")
            assert response.status_code == 200
            body = response.json()
            assert body["task_instances"]
            assert all(ti["team_name"] == "team-tis" for ti in body["task_instances"])

    @conf_vars({("core", "multi_team"): "True"})
    def test_should_filter_by_team(self, test_client, session):
        self.create_task_instances(session)
        with attach_dag_to_team(
            session,
            "example_python_operator",
            bundle_name="team-bundle-tis-filter",
            team_name="team-tis-filter",
        ):
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances", params={"teams": ["team-tis-filter"]}
            )
            assert response.status_code == 200
            body = response.json()
            assert body["total_entries"] > 0
            assert all(ti["dag_id"] == "example_python_operator" for ti in body["task_instances"])

            # A team with no Dags returns nothing.
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances", params={"teams": ["nonexistent-team"]}
            )
            assert response.status_code == 200
            assert response.json()["total_entries"] == 0


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
        with assert_queries_count(5):
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

        ti_count = session.scalar(
            select(func.count()).select_from(TaskInstance).where(TaskInstance.dag_id == dag_id)
        )

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
                22,
                22,
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
        ti_count = 10
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

    def test_no_duplicate_joins_in_get_task_instances_batch_query(self, test_client, session):
        """Regression test for #62027: the get_task_instances_batch endpoint must not emit duplicate JOINs."""
        from sqlalchemy import event

        import airflow.settings

        self.create_task_instances(session)

        executed_statements: list[str] = []

        def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_statements.append(statement.upper())

        event.listen(airflow.settings.engine, "before_cursor_execute", capture)
        try:
            response = test_client.post("/dags/~/dagRuns/~/taskInstances/list", json={})
        finally:
            event.remove(airflow.settings.engine, "before_cursor_execute", capture)

        assert response.status_code == 200

        ti_queries = [s for s in executed_statements if "FROM TASK_INSTANCE" in s and "JOIN DAG_RUN" in s]
        assert ti_queries, "Expected at least one query selecting from task_instance with JOIN dag_run"
        for q in ti_queries:
            assert q.count("JOIN DAG_RUN") == 1, "dag_run must appear exactly once in JOINs"
            if "JOIN DAG_VERSION" in q:
                assert q.count("JOIN DAG_VERSION") == 1, "dag_version must appear exactly once in JOINs"


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
            "priority_weight": 14,
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
                "bundle_name": "apache-airflow-providers-standard-example-dags",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "state_reason": None,
        }

    def test_should_include_state_reason_from_history(self, test_client, session):
        self.create_task_instances(
            session,
            task_instances=[{"state": State.SUCCESS, "retry_reason": "auth error, do not retry"}],
            with_ti_history=True,
        )
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1"
        )
        assert response.status_code == 200
        assert response.json()["state_reason"] == "auth error, do not retry"

    @pytest.fixture
    def masked_secret(self):
        from airflow._shared.secrets_masker import _secrets_masker

        masker = _secrets_masker()
        patterns, replacer = set(masker.patterns), masker.replacer
        mask_secret("hunter2")
        yield
        masker.patterns, masker.replacer = patterns, replacer

    @pytest.mark.enable_redact
    def test_should_redact_secrets_in_state_reason_from_history(self, test_client, session, masked_secret):
        self.create_task_instances(
            session,
            task_instances=[{"state": State.SUCCESS, "retry_reason": "auth: the token hunter2 expired"}],
            with_ti_history=True,
        )
        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1"
        )
        assert response.status_code == 200
        assert response.json()["state_reason"] == "auth: the token *** expired"

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
            "priority_weight": 14,
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
                "bundle_name": "apache-airflow-providers-standard-example-dags",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "state_reason": None,
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
            ti.try_number = 1
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note", "try_number"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
            session.flush()
            session.add(RTIF(ti, render_templates=False))
        session.commit()
        tis = session.scalars(select(TaskInstance)).all()
        # Record the task instance history
        from airflow.models.taskinstance import clear_task_instances

        successors = clear_task_instances(tis, session)
        for old_ti, ti in zip(tis, successors):
            if ti.map_index > 0:
                assert old_ti.working_set is None
                assert old_ti.try_number == 1
                assert ti.id != old_ti.id
                assert ti.try_number == 2
                ti.queue = "default_queue"
                session.merge(ti)
        session.commit()
        tis = session.scalars(select(TaskInstance)).all()
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
                "priority_weight": 14,
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
                    "bundle_name": "apache-airflow-providers-standard-example-dags",
                    "bundle_url": None,
                    "bundle_version": None,
                    "created_at": response_data["dag_version"]["created_at"],
                    "dag_display_name": "example_python_operator",
                    "dag_id": "example_python_operator",
                    "id": response_data["dag_version"]["id"],
                    "version_number": 1,
                },
                "state_reason": None,
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
        successor = ti.prepare_db_for_next_try(session=session)
        successor.state = None
        session.flush()
        ti.duration = 10000
        ti.end_date = self.default_time + dt.timedelta(days=2)
        session.merge(ti)
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
            "priority_weight": 14,
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
                "bundle_name": "apache-airflow-providers-standard-example-dags",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "state_reason": None,
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
            "priority_weight": 14,
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
                "bundle_name": "apache-airflow-providers-standard-example-dags",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": response_data["dag_version"]["created_at"],
                "dag_display_name": "example_python_operator",
                "dag_id": "example_python_operator",
                "id": response_data["dag_version"]["id"],
                "version_number": 1,
            },
            "state_reason": None,
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
    def test_should_respond_200_with_versions(self, test_client, run_id, expected_version_number, session):
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
            "state_reason": None,
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
    @pytest.mark.parametrize("one_run", [False, True])
    def test_broad_clear_uses_pinned_loop_after_latest_definition_removes_it(
        self, test_client, dag_maker, session, one_run
    ):
        @task_group
        def body():
            MockOperator(task_id="member")

        with dag_maker(dag_id="changed_loop", serialized=True):
            loop = create_loop(body, max_iterations=2)
        dr = dag_maker.create_dagrun()
        root = session.scalar(select(DynamicRegion).where(DynamicRegion.dag_id == dr.dag_id))
        for ti in dr.task_instances:
            ti.state = State.SUCCESS
        for loop_task in loop.iter_tasks():
            session.add(
                TaskInstance(
                    task=loop_task,
                    run_id=dr.run_id,
                    dag_version_id=dr.created_dag_version_id,
                    region_id=root.id,
                    region_index=1,
                    state=State.SUCCESS,
                )
            )
        session.commit()
        suffix_ids = {ti.id for ti in dr.get_task_instances(session=session) if ti.region_index == 1}
        dag_id, run_id, gate_id = dr.dag_id, dr.run_id, loop.gate_task_id
        with dag_maker(dag_id=dag_id, serialized=True, session=session):
            MockOperator(task_id="replacement")
        session.commit()

        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json={
                "task_ids": [gate_id],
                "dry_run": False,
                "only_failed": False,
                **({"dag_run_id": run_id} if one_run else {}),
            },
        )

        assert response.status_code == 200, response.text
        session.expire_all()
        assert len(suffix_ids) == 2
        for identity in suffix_ids:
            archived = session.get(TaskInstance, identity)
            assert (archived.working_set, archived.archived_reason) == (None, "superseded")
        assert (
            session.scalar(
                select(func.count()).select_from(DynamicRegion).where(DynamicRegion.dag_id == dag_id)
            )
            == 2
        )

    @pytest.mark.parametrize(
        ("selection", "expected_task_ids"),
        [
            pytest.param({"task_ids": ["normal_t"]}, {"setup_t", "normal_t", "teardown_t"}, id="by-task"),
            pytest.param({"task_ids": ["setup_t"]}, {"setup_t", "teardown_t"}, id="setup"),
        ],
    )
    def test_clear_in_dag_with_loop_includes_setups_and_teardowns(
        self, test_client, dag_maker, session, selection, expected_task_ids
    ):
        @task_group
        def body():
            MockOperator(task_id="work")

        with dag_maker("clear_loop_setup_teardown", serialized=True):
            create_loop(body, max_iterations=2)
            setup_t = MockOperator(task_id="setup_t").as_setup()
            normal_t = MockOperator(task_id="normal_t")
            teardown_t = MockOperator(task_id="teardown_t").as_teardown(setups=setup_t)
            setup_t >> normal_t >> teardown_t
        dr = dag_maker.create_dagrun()

        response = test_client.post(
            f"/dags/{dr.dag_id}/clearTaskInstances",
            json={"dag_run_id": dr.run_id, "dry_run": True, "only_failed": False, **selection},
        )

        assert response.status_code == 200, response.text
        assert {ti["task_id"] for ti in response.json()["task_instances"]} == expected_task_ids

    @pytest.mark.parametrize(
        ("selection", "expected_task_ids"),
        [
            pytest.param(
                {"task_ids": ["b"], "include_downstream": True}, {"b", "c"}, id="downstream-added-task"
            ),
            pytest.param({"task_group_id": "late_group"}, {"late_group.d"}, id="group-added-later"),
        ],
    )
    def test_run_clear_uses_latest_dag_structure_for_unversioned_bundle(
        self, test_client, dag_maker, session, selection, expected_task_ids
    ):
        with dag_maker("clear_unversioned_bundle", serialized=True):
            MockOperator(task_id="a") >> MockOperator(task_id="b")
        dr = dag_maker.create_dagrun()
        dag_id, run_id = dr.dag_id, dr.run_id
        with dag_maker(dag_id=dag_id, serialized=True, session=session):
            MockOperator(task_id="a") >> MockOperator(task_id="b") >> MockOperator(task_id="c")
            with TaskGroup("late_group"):
                MockOperator(task_id="d")
        version = DagVersion.get_latest_version(dag_id, session=session)
        session.add_all(
            TaskInstance(
                task=dag_maker.serialized_dag.get_task(task_id),
                run_id=run_id,
                dag_version_id=version.id,
                state=State.SUCCESS,
            )
            for task_id in ("c", "late_group.d")
        )
        session.commit()

        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json={"dag_run_id": run_id, "dry_run": True, "only_failed": False, **selection},
        )

        assert response.status_code == 200, response.text
        assert {ti["task_id"] for ti in response.json()["task_instances"]} == expected_task_ids

    @pytest.mark.parametrize("exact", [False, True])
    def test_task_name_clear_refreshes_execution_after_waiting_for_dagrun_lock(
        self, test_client, dag_maker, session, mocker, exact
    ):
        with dag_maker(serialized=True):
            MockOperator(task_id="task")
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("task", session=session)
        ti.state = State.SUCCESS
        session.commit()
        run_id, dag_id, original_id = dr.run_id, dr.dag_id, ti.id
        execute = Session._execute_internal
        bind = session.get_bind()
        replacement_id = None

        def overlap(request_session, statement, *args, **kwargs):
            nonlocal replacement_id
            run_lock = (
                isinstance(statement, Select)
                and statement._for_update_arg is not None
                and any(getattr(table, "name", None) == "dag_run" for table in statement.get_final_froms())
            )
            if run_lock and replacement_id is None:
                replacement_id = original_id
                with Session(bind=bind) as other:
                    successor = clear_task_instances([other.get(TaskInstance, original_id)], session=other)[0]
                    successor.state = State.SUCCESS
                    other.commit()
                    replacement_id = successor.id
            return execute(request_session, statement, *args, **kwargs)

        mocker.patch.object(Session, "_execute_internal", autospec=True, side_effect=overlap)
        selection = {"task_instance_ids": [str(original_id)]} if exact else {"task_ids": ["task"]}
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json={"dag_run_id": run_id, **selection, "dry_run": False, "only_failed": False},
        )

        assert response.status_code == (409 if exact else 200), response.text
        if not exact:
            assert response.json()["total_entries"] == 1
        session.expire_all()
        assert session.get(TaskInstance, original_id).working_set is None
        assert (session.get(TaskInstance, replacement_id).working_set is True) is exact

    def test_exact_clear_rejects_missing_execution_without_changing_run(
        self, test_client, dag_maker, session
    ):
        with dag_maker(serialized=True):
            MockOperator(task_id="task")
        dr = dag_maker.create_dagrun()
        session.commit()
        before = {ti.id for ti in dr.get_task_instances(session=session)}

        response = test_client.post(
            f"/dags/{dr.dag_id}/clearTaskInstances",
            json={
                "dag_run_id": dr.run_id,
                "task_instance_ids": [str(uuid7())],
                "dry_run": False,
                "only_failed": False,
            },
        )

        assert response.status_code == 404
        session.expire_all()
        assert dr.clear_number == 0
        assert {ti.id for ti in dr.get_task_instances(session=session)} == before

    @pytest.mark.parametrize(
        "selection",
        [
            {"task_instance_ids": []},
            {"task_instance_ids": ["00000000-0000-0000-0000-000000000001"]},
            {
                "task_instance_ids": ["00000000-0000-0000-0000-000000000001"],
                "dag_run_id": "run",
                "task_ids": ["task"],
            },
            {
                "task_instance_ids": ["00000000-0000-0000-0000-000000000001"],
                "dag_run_id": "run",
                "task_group_id": "group",
            },
            {
                "task_instance_ids": ["00000000-0000-0000-0000-000000000001"],
                "dag_run_id": "run",
                "include_future": True,
            },
            {
                "task_instance_ids": ["00000000-0000-0000-0000-000000000001"],
                "dag_run_id": "run",
                "include_past": True,
            },
            {"dag_run_id": "run", "whole_expansion_ids": ["00000000-0000-0000-0000-000000000001"]},
        ],
    )
    def test_exact_clear_rejects_conflicting_scope(self, test_client, selection):
        response = test_client.post("/dags/example_python_operator/clearTaskInstances", json=selection)
        assert response.status_code == 422

    @pytest.mark.parametrize("downstream", [False, True])
    @pytest.mark.parametrize("later", [False, True])
    @pytest.mark.parametrize("dry_run", [False, True])
    @pytest.mark.parametrize("only_failed", [False, True])
    def test_exact_loop_clear_keeps_iteration_scope(
        self, test_client, dag_maker, session, downstream, later, dry_run, only_failed
    ):
        @task_group
        def body():
            MockOperator(task_id="member")

        with dag_maker(serialized=True):
            loop = create_loop(body, max_iterations=3)
            loop >> MockOperator(task_id="outside")
        dr = dag_maker.create_dagrun()
        root = session.scalar(
            select(DynamicRegion).where(
                DynamicRegion.dag_id == dr.dag_id, DynamicRegion.node_id == loop.group_id
            )
        )
        for index in (1, 2):
            for loop_task in loop.iter_tasks():
                session.add(
                    TaskInstance(
                        task=loop_task,
                        run_id=dr.run_id,
                        dag_version_id=dr.created_dag_version_id,
                        region_id=root.id,
                        region_index=index,
                        state=State.SUCCESS,
                    )
                )
        session.commit()
        tis = list(dr.get_task_instances(session=session))
        seed = next(ti for ti in tis if ti.task_id == "body.member" and ti.region_index == 1)
        if only_failed:
            seed.state = State.FAILED
            session.commit()
        before = {(ti.id, ti.state, ti.try_number) for ti in tis}
        coordinates = {ti.id: (ti.task_id, str(ti.region_id), ti.region_index) for ti in tis}

        response = test_client.post(
            f"/dags/{dr.dag_id}/clearTaskInstances",
            json={
                "dag_run_id": dr.run_id,
                "dry_run": dry_run,
                "task_instance_ids": [str(seed.id)],
                "only_failed": only_failed,
                "include_downstream": downstream,
                "include_later_loop_iterations": later,
            },
        )

        assert response.status_code == 200, response.text
        expected = {seed.id}
        if downstream and not only_failed:
            expected.update(
                ti.id
                for ti in tis
                if ti.task_id == "outside"
                or (ti.region_index == 1 and ti.task_id == loop.gate_task_id)
                or (later and ti.region_id == root.id and ti.region_index > 1)
            )
        assert {
            (row["task_id"], row["region_id"], row["region_index"])
            for row in response.json()["task_instances"]
        } == {coordinates[value] for value in expected}
        session.expire_all()
        if dry_run:
            assert {
                (ti.id, ti.state, ti.try_number) for ti in dr.get_task_instances(session=session)
            } == before
        else:
            for identity, state, try_number in before:
                if identity not in expected:
                    retained = session.get(TaskInstance, identity)
                    assert (retained.state, retained.try_number) == (state, try_number)
                elif state in (State.SUCCESS, State.FAILED):
                    historical = session.get(TaskInstance, identity)
                    assert historical.working_set is None
                    archived = coordinates[identity][2] > 1 and downstream and later
                    assert historical.archived_reason == ("superseded" if archived else "retry")

    def test_loop_clear_reports_conflict_when_a_worker_archived_the_execution_first(
        self, test_client, dag_maker, session, mocker
    ):
        @task_group
        def body():
            MockOperator(task_id="member")

        with dag_maker(serialized=True):
            loop = create_loop(body, max_iterations=3)
        dr = dag_maker.create_dagrun()
        root = session.scalar(
            select(DynamicRegion).where(
                DynamicRegion.dag_id == dr.dag_id, DynamicRegion.node_id == loop.group_id
            )
        )
        for loop_task in loop.iter_tasks():
            session.add(
                TaskInstance(
                    task=loop_task,
                    run_id=dr.run_id,
                    dag_version_id=dr.created_dag_version_id,
                    region_id=root.id,
                    region_index=1,
                    state=State.SUCCESS,
                )
            )
        session.commit()
        gate = next(
            ti
            for ti in dr.get_task_instances(session=session)
            if ti.task_id == loop.gate_task_id and ti.region_index == 0
        )
        mocker.patch.object(
            TaskInstance,
            "archive",
            autospec=True,
            side_effect=ValueError("An archived task instance cannot be archived again"),
        )

        response = test_client.post(
            f"/dags/{dr.dag_id}/clearTaskInstances",
            json={
                "dag_run_id": dr.run_id,
                "dry_run": False,
                "only_failed": False,
                "task_instance_ids": [str(gate.id)],
                "include_later_loop_iterations": True,
            },
        )

        assert response.status_code == 409, response.text

    @pytest.mark.parametrize("downstream", [False, True])
    @pytest.mark.parametrize("later", [False, True])
    @pytest.mark.parametrize("seed_task", ["body.improve", "body.evaluate", "gate"])
    @pytest.mark.parametrize("exact", [False, True])
    @pytest.mark.parametrize("only_failed", [False, True])
    def test_loop_clear_dry_run_reports_what_the_real_clear_replaces(
        self, test_client, dag_maker, session, downstream, later, seed_task, exact, only_failed
    ):
        @task_group
        def body():
            improve = MockOperator(task_id="improve")
            improve >> MockOperator(task_id="evaluate")

        with dag_maker(serialized=True):
            loop = create_loop(body, max_iterations=4)
            loop >> MockOperator(task_id="finished")
        dr = dag_maker.create_dagrun()
        root = session.scalar(
            select(DynamicRegion).where(
                DynamicRegion.dag_id == dr.dag_id, DynamicRegion.node_id == loop.group_id
            )
        )
        for index in (1, 2, 3):
            for loop_task in loop.iter_tasks():
                session.add(
                    TaskInstance(
                        task=loop_task,
                        run_id=dr.run_id,
                        dag_version_id=dr.created_dag_version_id,
                        region_id=root.id,
                        region_index=index,
                        state=State.SUCCESS,
                    )
                )
        for ti in dr.get_task_instances(session=session):
            ti.state = State.FAILED if only_failed and ti.region_index in (1, 2) else State.SUCCESS
        session.commit()
        task_id = loop.gate_task_id if seed_task == "gate" else seed_task

        def build_payload():
            seed = session.scalar(
                select(TaskInstance).where(
                    TaskInstance.run_id == dr.run_id,
                    TaskInstance.task_id == task_id,
                    TaskInstance.region_index == 1,
                    TaskInstance.working_set.is_(True),
                )
            )
            return {
                "dag_run_id": dr.run_id,
                "only_failed": only_failed,
                "include_downstream": downstream,
                "include_later_loop_iterations": later,
                **({"task_instance_ids": [str(seed.id)]} if exact else {"task_ids": [task_id]}),
            }

        test_client.post(f"/dags/{dr.dag_id}/clearTaskInstances", json={**build_payload(), "dry_run": False})
        for ti in session.scalars(select(TaskInstance).where(TaskInstance.working_set.is_(True))):
            ti.state = State.FAILED if only_failed and ti.region_index in (1, 2) else State.SUCCESS
        session.commit()
        payload = build_payload()
        archived_before = set(
            session.scalars(
                select(TaskInstance.id)
                .where(TaskInstance.working_set.is_(None))
                .execution_options(include_all_attempts=True)
            )
        )
        dry = test_client.post(f"/dags/{dr.dag_id}/clearTaskInstances", json={**payload, "dry_run": True})
        real = test_client.post(f"/dags/{dr.dag_id}/clearTaskInstances", json={**payload, "dry_run": False})

        assert dry.status_code == 200, dry.text
        assert real.status_code == 200, real.text
        session.expire_all()
        archived_ids = {
            ti.id
            for ti in session.scalars(
                select(TaskInstance)
                .where(
                    TaskInstance.dag_id == dr.dag_id,
                    TaskInstance.working_set.is_(None),
                )
                .execution_options(include_all_attempts=True)
            )
        } - archived_before
        reported = {
            (row["task_id"], row["region_id"], row["region_index"]) for row in dry.json()["task_instances"]
        }
        replaced = {
            (ti.task_id, str(ti.region_id), ti.region_index)
            for ti in session.scalars(
                select(TaskInstance)
                .where(TaskInstance.id.in_(archived_ids))
                .execution_options(include_all_attempts=True)
            )
        }
        assert reported == replaced
        assert {
            (row["task_id"], row["region_id"], row["region_index"]) for row in real.json()["task_instances"]
        } >= reported

    @pytest.mark.parametrize("new_note", [None, "Reason for clearing", ""])
    @pytest.mark.parametrize("whole", [False, True])
    @pytest.mark.parametrize("exact", [False, True])
    def test_legacy_whole_clear_retains_affected_execution_and_note(
        self, test_client, dag_maker, session, new_note, whole, exact
    ):
        with dag_maker(dag_id="legacy_clear_response", serialized=True):
            MockOperator.partial(task_id="mapped").expand(arg2=[1])
        dr = dag_maker.create_dagrun()
        session.execute(delete(TaskInstance).where(TaskInstance.dag_id == dr.dag_id))
        session.execute(delete(DynamicRegion).where(DynamicRegion.dag_id == dr.dag_id))
        ti = TaskInstance(
            dag_maker.serialized_dag.get_task("mapped"),
            run_id=dr.run_id,
            dag_version_id=dr.created_dag_version_id,
            region_index=0,
            state=State.SUCCESS,
        )
        ti.note = ("Original execution note", "test")
        session.add(ti)
        session.commit()
        old_id = ti.id
        selection = (
            {"task_instance_ids": [str(old_id)], "whole_expansion_ids": [str(old_id)] if whole else []}
            if exact
            else {"task_ids": ["mapped"] if whole else [["mapped", 0]]}
        )

        response = test_client.post(
            f"/dags/{dr.dag_id}/clearTaskInstances",
            json={
                "dry_run": False,
                "reset_dag_runs": False,
                "only_failed": False,
                "dag_run_id": dr.run_id,
                **selection,
                "note": new_note,
            },
        )

        assert response.status_code == 200, response.text
        expected_note = "Original execution note" if new_note is None else new_note or None
        body = response.json()
        assert body["total_entries"] == 1
        assert (body["task_instances"][0]["id"] == str(old_id)) is whole
        assert body["task_instances"][0]["note"] == expected_note
        session.expire_all()
        history = session.get(TaskInstance, old_id)
        assert history.working_set is None
        assert history.archived_reason == ("superseded" if whole else "retry")
        assert history.note == (expected_note if whole else "Original execution note")
        assert (
            session.scalar(
                select(func.count()).select_from(DynamicRegion).where(DynamicRegion.dag_id == dr.dag_id)
            )
            == whole
        )

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
        "payload",
        [
            pytest.param(
                {"only_failed": True, "only_running": True},
                id="only_failed_and_only_running",
            ),
            pytest.param(
                {"start_date": "2024-01-02T00:00:00Z", "end_date": "2024-01-01T00:00:00Z"},
                id="start_date_after_end_date",
            ),
            pytest.param(
                {
                    "start_date": "2024-01-01T00:00:00Z",
                    "end_date": "2024-01-02T00:00:00Z",
                    "dag_run_id": "run_1",
                },
                id="dag_run_id_with_start_and_end_date",
            ),
            pytest.param(
                {"start_date": "2024-01-01T00:00:00Z", "dag_run_id": "run_1"},
                id="dag_run_id_with_start_date",
            ),
            pytest.param(
                {"end_date": "2024-01-01T00:00:00Z", "dag_run_id": "run_1"},
                id="dag_run_id_with_end_date",
            ),
            pytest.param({"task_ids": []}, id="empty_task_ids"),
        ],
    )
    def test_should_respond_422_on_invalid_body(self, test_client, payload):
        response = test_client.post("/dags/example_python_operator/clearTaskInstances", json=payload)
        assert response.status_code == 422

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
            [],
            mock.ANY,
            DagRunState.QUEUED,
            prevent_running_task=False,
            run_on_latest_version=False,
            whole_task_keys=set(),
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

        failed_dag_runs = session.scalar(
            select(func.count()).select_from(DagRun).where(DagRun.state == "failed")
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
                    "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                "region_id": "00000000-0000-0000-0000-000000000000",
                "region_index": -1,
                "max_tries": 0,
                "note": "placeholder-note",
                "operator": "PythonOperator",
                "operator_name": "PythonOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 14,
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
                "team_name": None,
                "state_reason": None,
                "try_number": 1,
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

        logs = session.scalar(
            select(func.count())
            .select_from(Log)
            .where(Log.dag_id == dag_id, Log.run_id == dag_run_id, Log.event == event)
        )

        assert response.status_code == 200
        assert logs == audit_log_count

    @pytest.mark.db_test
    def test_clear_sets_note_on_task_instances(self, test_client, session):
        """Test that a note is set on cleared task instances when note is provided."""
        dag_id = "example_python_operator"
        note_value = "Cleared by automation"
        payload = {
            "dry_run": False,
            "reset_dag_runs": False,
            "only_failed": True,
            "only_running": False,
            "note": note_value,
        }
        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=[{"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED}],
            update_extras=False,
        )
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["total_entries"] == 1
        ti_id = response_data["task_instances"][0]["id"]
        _check_task_instance_note(session, ti_id, {"content": note_value, "user_id": "test"})

    @pytest.mark.db_test
    def test_clear_without_note_does_not_set_note(self, test_client, session):
        """Test that existing note is preserved on cleared task instances when note is not provided."""
        dag_id = "example_python_operator"
        payload = {
            "dry_run": False,
            "reset_dag_runs": False,
            "only_failed": True,
            "only_running": False,
        }
        old_ti = self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=[{"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED}],
            update_extras=False,
        )[0]
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["total_entries"] == 1
        ti_id = response_data["task_instances"][0]["id"]
        assert ti_id != str(old_ti.id)
        _check_task_instance_note(session, old_ti.id, {"content": "placeholder-note", "user_id": None})
        _check_task_instance_note(session, ti_id, {"content": "placeholder-note", "user_id": None})

    @pytest.mark.db_test
    def test_clear_dry_run_does_not_set_note(self, test_client, session):
        """Test that a note is NOT updated when dry_run=True even if note is provided."""
        dag_id = "example_python_operator"
        note_value = "Should not be set"
        payload = {
            "dry_run": True,
            "reset_dag_runs": False,
            "only_failed": True,
            "only_running": False,
            "note": note_value,
        }
        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=[{"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED}],
            update_extras=False,
        )
        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["total_entries"] == 1
        ti_id = response_data["task_instances"][0]["id"]
        _check_task_instance_note(session, ti_id, {"content": "placeholder-note", "user_id": None})

    def _seed_task_state(self, session, dag_id, task_id=None, map_index=None):
        """Store one task state key for the TI matching the given task_id/map_index."""
        stmt = select(TaskInstance).where(TaskInstance.dag_id == dag_id)
        if task_id is not None:
            stmt = stmt.where(TaskInstance.task_id == task_id)
        if map_index is not None:
            stmt = stmt.where(TaskInstance.map_index == map_index)
        ti = session.scalars(stmt).one()
        MetastoreBackend().set(
            TaskScope(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id, map_index=ti.map_index),
            "job_id",
            "app_1234",
            session=session,
        )
        session.commit()

    def _task_state_rows(self, session, dag_id, task_id=None, map_index=None):
        stmt = select(TaskStateStoreModel).where(TaskStateStoreModel.dag_id == dag_id)
        if task_id is not None:
            stmt = stmt.where(TaskStateStoreModel.task_id == task_id)
        if map_index is not None:
            stmt = stmt.where(TaskStateStoreModel.region_index == map_index)
        return session.scalars(stmt).all()

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("payload_extra", "expect_kept"),
        [
            pytest.param({}, False, id="default-discards"),
            pytest.param({"keep_task_state": True}, True, id="keep-preserves"),
            pytest.param({"keep_task_state": False}, False, id="explicit-false-discards"),
        ],
    )
    def test_clear_task_state_store(self, test_client, session, payload_extra, expect_kept):
        """Clearing one mapped index discards only that index's task state, unless kept."""
        dag_id = "example_task_mapping_second_order"
        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=[
                {"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED},
                {
                    "logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1),
                    "state": State.FAILED,
                    "map_indexes": (0, 1),
                },
            ],
            update_extras=False,
        )
        self._seed_task_state(session, dag_id, "times_2", 0)
        self._seed_task_state(session, dag_id, "times_2", 1)
        assert self._task_state_rows(session, dag_id, "times_2", 0)
        assert self._task_state_rows(session, dag_id, "times_2", 1)

        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json={
                "dry_run": False,
                "reset_dag_runs": False,
                "only_failed": True,
                "task_ids": [["times_2", 0]],
                **payload_extra,
            },
        )
        assert response.status_code == 200

        session.expire_all()
        assert bool(self._task_state_rows(session, dag_id, "times_2", 0)) is expect_kept
        # The untargeted map index is never touched, regardless of keep_task_state.
        assert self._task_state_rows(session, dag_id, "times_2", 1)

    @pytest.mark.db_test
    def test_clear_dry_run_does_not_discard_task_state(self, test_client, session):
        """A dry run previews the clear and must not touch task state."""
        dag_id = "example_python_operator"
        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=[{"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED}],
            update_extras=False,
        )
        self._seed_task_state(session, dag_id)

        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json={"dry_run": True, "reset_dag_runs": False, "only_failed": True},
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == 1

        session.expire_all()
        assert self._task_state_rows(session, dag_id)

    @pytest.mark.db_test
    @mock.patch.object(MetastoreBackend, "clear", side_effect=RuntimeError("boom"))
    def test_clear_task_state_store_discard_failure_fails_request(self, mock_clear, test_client, session):
        """A backend.clear() failure fails the whole request instead of reporting a partial clear as success."""
        dag_id = "example_python_operator"
        self.create_task_instances(
            session,
            dag_id=dag_id,
            task_instances=[{"logical_date": DEFAULT_DATETIME_1, "state": State.FAILED}],
            update_extras=False,
        )
        self._seed_task_state(session, dag_id)

        with pytest.raises(RuntimeError, match="boom"):
            test_client.post(
                f"/dags/{dag_id}/clearTaskInstances",
                json={"dry_run": False, "reset_dag_runs": False, "only_failed": True},
            )

        session.expire_all()
        assert self._task_state_rows(session, dag_id)
        ti = session.scalars(select(TaskInstance).where(TaskInstance.dag_id == dag_id)).one()
        assert ti.state == State.FAILED

    @pytest.mark.parametrize(
        ("task_group_id", "expected_task_ids"),
        [
            pytest.param(
                "section_1",
                ["section_1.task_1", "section_1.task_2", "section_1.task_3"],
                id="flat group",
            ),
            pytest.param(
                "section_2",
                [
                    "section_2.task_1",
                    "section_2.inner_section_2.task_2",
                    "section_2.inner_section_2.task_3",
                    "section_2.inner_section_2.task_4",
                ],
                id="nested group resolves recursively",
            ),
        ],
    )
    def test_clear_by_task_group_id_targets_every_task_in_the_group(
        self, test_client, session, task_group_id, expected_task_ids
    ):
        """Clearing by task_group_id resolves the whole group server-side from the dag structure."""
        self.create_task_instances(session, dag_id="example_task_group")
        response = test_client.post(
            "/dags/example_task_group/clearTaskInstances",
            json={
                "dry_run": True,
                "only_failed": False,
                "dag_run_id": "TEST_DAG_RUN_ID",
                "task_group_id": task_group_id,
            },
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["total_entries"] == len(expected_task_ids)
        assert sorted(ti["task_id"] for ti in response_data["task_instances"]) == sorted(expected_task_ids)

    def test_clear_by_task_group_id_not_found(self, test_client, session):
        """An unknown task_group_id returns 404."""
        self.create_task_instances(session, dag_id="example_task_group")
        response = test_client.post(
            "/dags/example_task_group/clearTaskInstances",
            json={"dry_run": True, "dag_run_id": "TEST_DAG_RUN_ID", "task_group_id": "nonexistent_group"},
        )
        assert response.status_code == 404
        assert "nonexistent_group" in response.json()["detail"]

    def test_clear_rejects_both_task_ids_and_task_group_id(self, test_client, session):
        """task_ids and task_group_id are mutually exclusive."""
        self.create_task_instances(session, dag_id="example_task_group")
        response = test_client.post(
            "/dags/example_task_group/clearTaskInstances",
            json={
                "dry_run": True,
                "dag_run_id": "TEST_DAG_RUN_ID",
                "task_ids": ["section_1.task_1"],
                "task_group_id": "section_1",
            },
        )
        assert response.status_code == 422

    def test_clear_by_task_group_id_handles_group_larger_than_page_size(
        self, test_client, dag_maker, session
    ):
        """A group with more tasks than the API page size is cleared in full (regression for #59235)."""
        group_size = 60  # deliberately larger than the default 50-row page the UI used to cap at
        dag_id = "large_task_group_dag"
        with dag_maker(session=session, dag_id=dag_id, start_date=DEFAULT_DATETIME_1, serialized=True):
            with TaskGroup("big_group"):
                for index in range(group_size):
                    BaseOperator(task_id=f"task_{index}")
        dr = dag_maker.create_dagrun(
            run_id="run_large_group",
            logical_date=DEFAULT_DATETIME_1,
            data_interval=(DEFAULT_DATETIME_1, DEFAULT_DATETIME_2),
        )
        DagBundlesManager().sync_bundles_to_db()
        dagbag = DagBag(os.devnull)
        dagbag.dags = {dag_id: dag_maker.dag}
        sync_bag_to_db(dagbag, "dags-folder", None)
        session.flush()

        response = test_client.post(
            f"/dags/{dag_id}/clearTaskInstances",
            json={
                "dry_run": True,
                "only_failed": False,
                "dag_run_id": dr.run_id,
                "task_group_id": "big_group",
            },
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == group_size


class TestGetTaskInstanceTries(TestTaskInstanceEndpoint):
    def test_history_without_dag_version_serializes_null(self, test_client, session):
        self.create_task_instances(
            session=session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True
        )
        historical = session.scalar(
            select(TaskInstance)
            .where(TaskInstance.working_set.is_(None))
            .execution_options(include_all_attempts=True)
        )
        historical.dag_version_id = None
        session.commit()

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )

        assert response.status_code == 200
        attempts = {item["try_number"]: item for item in response.json()["task_instances"]}
        assert attempts[1]["dag_version"] is None
        assert attempts[2]["dag_version"] is not None

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
                    "priority_weight": 14,
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
                        "bundle_name": "apache-airflow-providers-standard-example-dags",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][0]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "state_reason": None,
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
                    "priority_weight": 14,
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
                        "bundle_name": "apache-airflow-providers-standard-example-dags",
                        "bundle_url": None,
                        "bundle_version": None,
                        "created_at": response_data["task_instances"][1]["dag_version"]["created_at"],
                        "dag_display_name": "example_python_operator",
                        "dag_id": "example_python_operator",
                        "id": response_data["task_instances"][1]["dag_version"]["id"],
                        "version_number": 1,
                    },
                    "state_reason": None,
                },
            ],
            "total_entries": 2,
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

    def test_tries_include_failed_and_pending_retry(self, test_client, session):
        self.create_task_instances(
            session=session, task_instances=[{"state": State.FAILED}], with_ti_history=True
        )
        ti = session.scalars(select(TaskInstance)).one()
        ti.state = State.UP_FOR_RETRY
        session.commit()

        response = test_client.get(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["total_entries"] == 2
        assert sorted(
            (task_try["try_number"], task_try["state"]) for task_try in response_data["task_instances"]
        ) == [(1, "failed"), (2, "up_for_retry")]

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
        tis = session.scalars(select(TaskInstance)).all()

        # Record the task instance history
        from airflow.models.taskinstance import clear_task_instances

        successors = clear_task_instances(tis, session)
        for old_ti, ti in zip(tis, successors):
            if ti.map_index > 0:
                assert old_ti.working_set is None
                assert old_ti.try_number == 1
                assert ti.id != old_ti.id
                assert ti.try_number == 2
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
                        "priority_weight": 14,
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
                            "bundle_name": "apache-airflow-providers-standard-example-dags",
                            "bundle_url": None,
                            "bundle_version": None,
                            "created_at": response_data["task_instances"][0]["dag_version"]["created_at"],
                            "dag_display_name": "example_python_operator",
                            "dag_id": "example_python_operator",
                            "id": response_data["task_instances"][0]["dag_version"]["id"],
                            "version_number": 1,
                        },
                        "state_reason": None,
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
                        "priority_weight": 14,
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
                            "bundle_name": "apache-airflow-providers-standard-example-dags",
                            "bundle_url": None,
                            "bundle_version": None,
                            "created_at": response_data["task_instances"][1]["dag_version"]["created_at"],
                            "dag_display_name": "example_python_operator",
                            "dag_id": "example_python_operator",
                            "id": response_data["task_instances"][1]["dag_version"]["id"],
                            "version_number": 1,
                        },
                        "state_reason": None,
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
    def test_should_respond_200_with_versions(self, test_client, run_id, expected_version_number):
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
            "state_reason": None,
        }


class TestRegionalTaskStateControls(TestTaskInstanceEndpoint):
    @pytest.fixture
    def loop_instances(self, dag_maker, session):
        @task_group
        def body():
            MockOperator(task_id="first") >> MockOperator(task_id="last")

        with dag_maker("manual-loop", serialized=True):
            loop = create_loop(body, max_iterations=3, until=lambda loop: True)
        dr = dag_maker.create_dagrun(run_id="manual-run")
        root = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
        for index in (1, 2):
            for operator in loop.iter_tasks():
                session.add(
                    TaskInstance(
                        task=operator,
                        run_id=dr.run_id,
                        dag_version_id=dr.created_dag_version_id,
                        region_id=root.id,
                        region_index=index,
                    )
                )
        session.flush()
        tis = {(ti.task_id, ti.region_index): ti for ti in dr.get_task_instances(session=session)}
        for ti in tis.values():
            ti.set_state(State.FAILED, session=session)
        session.commit()
        return dr, loop, root, tis

    @pytest.mark.parametrize("keep_task_state", [False, True])
    def test_clear_discards_task_state_of_the_selected_loop_iteration_only(
        self, test_client, session, loop_instances, keep_task_state
    ):
        dr, loop, root, tis = loop_instances
        backend = MetastoreBackend()
        for index in (1, 2):
            backend.set(
                TaskScope(
                    dag_id=dr.dag_id,
                    run_id=dr.run_id,
                    task_id="body.first",
                    region_id=root.id,
                    region_index=index,
                ),
                "job_id",
                "external-job",
                session=session,
            )
        session.commit()

        response = test_client.post(
            f"/dags/{dr.dag_id}/clearTaskInstances",
            json={
                "dry_run": False,
                "dag_run_id": dr.run_id,
                "task_instance_ids": [str(tis["body.first", 1].id)],
                "keep_task_state": keep_task_state,
            },
        )

        assert response.status_code == 200, response.text
        session.expire_all()
        assert set(
            session.scalars(
                select(TaskStateStoreModel.region_index).where(
                    TaskStateStoreModel.dag_id == dr.dag_id,
                    TaskStateStoreModel.task_id == "body.first",
                )
            )
        ) == ({1, 2} if keep_task_state else {2})

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_exact_loop_mark_does_not_change_other_iterations(
        self, test_client, session, loop_instances, dry_run
    ):
        dr, loop, root, tis = loop_instances
        url = f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/body.first"

        response = test_client.patch(
            url + ("/dry_run" if dry_run else ""),
            json={"new_state": "success", "region_id": str(root.id), "region_index": 1},
        )

        assert response.status_code == 200, response.text
        assert [
            (item["task_id"], item["map_index"], item["region_index"])
            for item in response.json()["task_instances"]
        ] == [("body.first", -1, 1)]
        session.expire_all()
        for index in (0, 2):
            assert tis["body.first", index].state == State.FAILED
            assert tis["body.last", index].state == State.FAILED
        assert tis["body.first", 1].state == (State.FAILED if dry_run else State.SUCCESS)
        current_last = session.scalar(
            select(TaskInstance).where(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.run_id == dr.run_id,
                TaskInstance.task_id == "body.last",
                TaskInstance.region_id == root.id,
                TaskInstance.region_index == 1,
                TaskInstance.working_set.is_(True),
            )
        )
        assert current_last.state == (State.FAILED if dry_run else None)

    def test_loop_mark_without_coordinates_is_ambiguous(self, test_client, loop_instances):
        dr, loop, root, tis = loop_instances

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/body.first",
            json={"new_state": "success"},
        )

        assert response.status_code == 409, response.text

    @pytest.mark.parametrize(
        ("map_index", "expected_status"),
        [
            pytest.param(7, 400, id="conflicting"),
            pytest.param(-1, 200, id="default"),
            pytest.param(1, 200, id="matching"),
        ],
    )
    def test_loop_mark_rejects_map_index_conflicting_with_region_index(
        self, test_client, loop_instances, map_index, expected_status
    ):
        dr, loop, root, tis = loop_instances

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/body.first/{map_index}",
            json={"new_state": "success", "region_id": str(root.id), "region_index": 1},
        )

        assert response.status_code == expected_status, response.text
        if expected_status == 400:
            assert response.json()["detail"] == "map_index conflicts with region_index"

    @pytest.mark.parametrize("coordinates_in_query", [False, True])
    def test_exact_loop_note_changes_only_selected_execution(
        self, test_client, session, loop_instances, coordinates_in_query
    ):
        dr, loop, root, tis = loop_instances
        coordinates = {"region_id": str(root.id), "region_index": 1}

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/body.first",
            json={"note": "selected iteration", **({} if coordinates_in_query else coordinates)},
            params=coordinates if coordinates_in_query else {},
        )

        assert response.status_code == 200, response.text
        session.expire_all()
        assert tis["body.first", 1].note == "selected iteration"
        assert tis["body.first", 0].note is None
        assert tis["body.first", 2].note is None

    def test_manual_gate_success_after_rewind_stops_without_consuming_continue(
        self, test_client, session, loop_instances
    ):
        dr, loop, root, tis = loop_instances
        clear_loop_task_instances([tis[loop.gate_task_id, 0]], downstream=False, session=session)
        gate = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.task_id == loop.gate_task_id,
                TaskInstance.region_index == 0,
                TaskInstance.working_set.is_(True),
            )
        ).one()
        gate_id = gate.id
        session.add(XComModelV2(task_instance_id=gate_id, key=LOOP_DECISION_KEY, value="continue"))
        session.commit()

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/{gate.task_id}",
            json={"new_state": "success", "region_id": str(root.id), "region_index": 0},
        )

        assert response.status_code == 200, response.text
        session.expire_all()
        assert session.get(TaskInstance, gate_id).state == State.SUCCESS
        assert all(ti.region_index == 0 for ti in dr.get_task_instances(session=session))
        assert not session.scalar(select(XComModelV2).where(XComModelV2.task_instance_id == gate_id))

    @pytest.mark.parametrize("outcome", ["success", "failed", "skipped"])
    def test_manual_terminal_settles_superseded_running_execution(
        self, test_client, session, loop_instances, outcome
    ):
        dr, loop, root, tis = loop_instances
        archiving = tis["body.first", 2]
        archiving.state = State.RUNNING
        session.flush()
        archiving_id = archiving.id
        clear_loop_task_instances([tis[loop.gate_task_id, 0]], downstream=False, session=session)
        assert archiving.state == State.RESTARTING
        session.commit()

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/{archiving.task_id}",
            json={"new_state": outcome, "region_id": str(root.id), "region_index": 2},
        )

        assert response.status_code == 200, response.text
        assert response.json()["task_instances"][0]["state"] == outcome
        session.expire_all()
        history = session.get(TaskInstance, archiving_id)
        assert history.working_set is None
        assert history.state == outcome
        assert history.archived_reason == "superseded"
        assert history.end_date is not None

    @pytest.mark.parametrize(
        ("body", "params"),
        [
            ({"region_index": 1}, {}),
            ({}, {"region_index": 1}),
            (
                {"region_index": 1, "region_id": "00000000-0000-0000-0000-000000000000"},
                {"region_index": 2, "region_id": "00000000-0000-0000-0000-000000000000"},
            ),
            (
                {
                    "region_index": 1,
                    "region_id": "00000000-0000-0000-0000-000000000000",
                    "include_past": True,
                },
                {},
            ),
        ],
    )
    def test_invalid_coordinate_control_is_rejected(self, test_client, loop_instances, body, params):
        dr, loop, root, tis = loop_instances

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/body.first",
            json={"new_state": "success", **body},
            params=params,
        )

        assert response.status_code == 400, response.text

    @pytest.mark.parametrize("action", ["update", "delete"])
    def test_bulk_exact_passes_have_distinct_execution_results(
        self, test_client, session, loop_instances, action
    ):
        dr, loop, root, tis = loop_instances
        ids = {str(tis["body.first", index].id) for index in (0, 2)}
        entities = [
            {
                "task_id": "body.first",
                "map_index": -1,
                "region_id": str(root.id),
                "region_index": index,
                **({"new_state": "success"} if action == "update" else {}),
            }
            for index in (0, 2)
        ]

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances",
            json={"actions": [{"action": action, "entities": entities}]},
        )

        assert response.status_code == 200, response.text
        assert not response.json()[action]["errors"]
        assert set(response.json()[action]["success"]) == ids
        session.expire_all()
        assert tis["body.first", 1].state == State.FAILED
        live = {str(ti.id): ti.state for ti in dr.get_task_instances(session=session)}
        if action == "delete":
            assert not ids & live.keys()
        else:
            assert all(live[ti_id] == State.SUCCESS for ti_id in ids)

    def test_bulk_regional_entities_do_not_lock_their_run_again(self, test_client, loop_instances, mocker):
        dr, loop, root, tis = loop_instances
        lock_runs = mocker.spy(task_instances_service, "_lock_patch_runs")
        entities = [
            {
                "task_id": "body.first",
                "map_index": -1,
                "region_id": str(root.id),
                "region_index": index,
                "new_state": "success",
            }
            for index in (0, 2)
        ]

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances",
            json={"actions": [{"action": "update", "entities": entities}]},
        )

        assert response.status_code == 200, response.text
        lock_runs.assert_not_called()

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_group_coordinates_select_one_loop_pass(self, test_client, session, loop_instances, dry_run):
        dr, loop, root, tis = loop_instances

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskGroupInstances/body"
            + ("/dry_run" if dry_run else ""),
            json={"new_state": "success", "region_id": str(root.id), "region_index": 1},
        )

        assert response.status_code == 200, response.text
        assert {item["id"] for item in response.json()["task_instances"]} == {
            str(ti.id) for (task_id, index), ti in tis.items() if index == 1
        }
        session.expire_all()
        for (_task_id, index), ti in tis.items():
            assert ti.state == (State.SUCCESS if index == 1 and not dry_run else State.FAILED)

    @pytest.mark.parametrize("action", ["update", "delete"])
    @pytest.mark.parametrize("map_index", [None, -1])
    def test_bulk_loop_request_requires_coordinates(
        self, test_client, session, loop_instances, action, map_index
    ):
        dr, loop, root, tis = loop_instances
        before = {ti.id: ti.state for ti in tis.values()}
        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances",
            json={
                "actions": [
                    {
                        "action": action,
                        "entities": [
                            {
                                "task_id": "body.first",
                                "map_index": map_index,
                                **({"new_state": "success"} if action == "update" else {}),
                            }
                        ],
                    }
                ]
            },
        )

        assert response.status_code == 200, response.text
        assert response.json()[action]["errors"][0]["status_code"] == 409
        assert not response.json()[action]["success"]
        session.expire_all()
        assert {ti.id: ti.state for ti in dr.get_task_instances(session=session)} == before

    def test_group_pass_includes_mapped_descendants(self, test_client, dag_maker, session):
        @task_group
        def body():
            MockOperator.partial(task_id="mapped").expand(arg2=[1, 2]) >> MockOperator(task_id="last")

        with dag_maker("mapped-manual-loop", serialized=True):
            loop = create_loop(body, max_iterations=2)
            loop >> MockOperator(task_id="outside")
        dr = dag_maker.create_dagrun(run_id="manual-run")
        root = session.scalar(select(DynamicRegion).where(DynamicRegion.node_id == loop.group_id))
        session.commit()

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskGroupInstances/body",
            json={"new_state": "success", "region_id": str(root.id), "region_index": 0},
        )

        assert response.status_code == 200, response.text
        values = response.json()["task_instances"]
        assert {(ti["task_id"], ti["map_index"]) for ti in values} == {
            ("body.mapped", 0),
            ("body.mapped", 1),
            ("body.last", -1),
            (loop.gate_task_id, -1),
        }
        session.expire_all()
        assert all(
            ti.state == (None if ti.task_id == "outside" else State.SUCCESS)
            for ti in dr.get_task_instances(session=session)
        )

    @conf_vars({("state_store", "clear_on_success"): "True"})
    def test_regional_success_preserves_other_state_scope_and_notifies_listener(
        self, test_client, session, loop_instances, listener_manager
    ):
        dr, loop, root, tis = loop_instances
        backend = MetastoreBackend()
        scopes = [
            TaskScope(
                dag_id=dr.dag_id,
                run_id=dr.run_id,
                task_id="body.first",
                region_id=root.id,
                region_index=index,
            )
            for index in (0, 1)
        ]
        for scope in scopes:
            backend.set(scope, "job_id", "external-job", session=session)
        session.commit()
        listener = ClassBasedListener()
        listener_manager(listener)

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/body.first",
            json={
                "new_state": "success",
                "note": "completed manually",
                "region_id": str(root.id),
                "region_index": 1,
            },
        )

        assert response.status_code == 200, response.text
        assert listener.state == [TaskInstanceState.SUCCESS]
        assert listener.ti_note_at_listener == "completed manually"
        assert set(
            session.scalars(
                select(TaskStateStoreModel.region_index).where(
                    TaskStateStoreModel.dag_id == dr.dag_id,
                    TaskStateStoreModel.task_id == "body.first",
                )
            )
        ) == {0}

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_regional_downstream_mark_uses_same_scope_as_preview(
        self, test_client, session, loop_instances, dry_run
    ):
        dr, loop, root, tis = loop_instances

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskInstances/body.first"
            + ("/dry_run" if dry_run else ""),
            json={
                "new_state": "success",
                "include_downstream": True,
                "region_id": str(root.id),
                "region_index": 1,
            },
        )

        assert response.status_code == 200, response.text
        assert {item["id"] for item in response.json()["task_instances"]} == {
            str(ti.id) for (_task_id, index), ti in tis.items() if index == 1
        }
        session.expire_all()
        assert all(
            ti.state == (State.SUCCESS if index == 1 and not dry_run else State.FAILED)
            for (_task_id, index), ti in tis.items()
        )

    @pytest.mark.parametrize("remove_group", [False, True])
    def test_regional_group_uses_execution_version_after_definition_changes(
        self, test_client, session, dag_maker, loop_instances, remove_group
    ):
        dr, loop, root, tis = loop_instances

        @task_group(group_id="replacement" if remove_group else "body")
        def changed():
            MockOperator(task_id="first")

        with dag_maker(dr.dag_id, serialized=True, session=session):
            create_loop(changed, max_iterations=3, until=lambda loop: True)
        session.commit()

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskGroupInstances/body",
            json={"new_state": "success", "region_id": str(root.id), "region_index": 1},
        )

        assert response.status_code == 200, response.text
        assert {item["id"] for item in response.json()["task_instances"]} == {
            str(ti.id) for (_task_id, index), ti in tis.items() if index == 1
        }

    def test_group_terminal_response_retains_executions_archived_by_manual_settlement(
        self, test_client, session, loop_instances
    ):
        dr, loop, root, tis = loop_instances
        draining = [tis["body.first", 2], tis[loop.gate_task_id, 2]]
        for ti in draining:
            ti.state = State.RUNNING
        session.flush()
        ids = {str(ti.id) for ti in draining}
        clear_loop_task_instances([tis[loop.gate_task_id, 0]], downstream=False, session=session)
        session.commit()

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskGroupInstances/body",
            json={"new_state": "skipped", "region_id": str(root.id), "region_index": 2},
        )

        assert response.status_code == 200, response.text
        assert {item["id"] for item in response.json()["task_instances"]} == ids
        assert all(item["state"] == "skipped" for item in response.json()["task_instances"])

    def test_unscoped_group_rejects_pinned_loop_when_latest_group_is_plain(
        self, test_client, session, dag_maker, loop_instances
    ):
        dr, loop, root, tis = loop_instances
        before = {ti.id: ti.state for ti in tis.values()}
        with dag_maker(dr.dag_id, serialized=True, session=session):
            with TaskGroup(group_id="body"):
                MockOperator(task_id="first") >> MockOperator(task_id="last")
        session.commit()

        response = test_client.patch(
            f"/dags/{dr.dag_id}/dagRuns/{dr.run_id}/taskGroupInstances/body",
            json={"new_state": "success"},
        )

        assert response.status_code == 409, response.text
        session.expire_all()
        assert {ti.id: ti.state for ti in dr.get_task_instances(session=session)} == before


class TestPatchTaskInstance(TestTaskInstanceEndpoint):
    ENDPOINT_URL = "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
    NEW_STATE = "failed"
    DAG_ID = "example_python_operator"
    DAG_DISPLAY_NAME = "example_python_operator"
    TASK_ID = "print_the_context"
    RUN_ID = "TEST_DAG_RUN_ID"

    @pytest.mark.parametrize(
        ("state", "listener_state"),
        [
            ("success", [TaskInstanceState.SUCCESS]),
            ("failed", [TaskInstanceState.FAILED]),
            ("skipped", [TaskInstanceState.SKIPPED]),
            ("running", []),
        ],
    )
    def test_patch_task_instance_notifies_listeners(
        self, test_client, session, state, listener_state, listener_manager
    ):
        from unit.listeners.class_listener import ClassBasedListener

        self.create_task_instances(session)

        listener = ClassBasedListener()
        listener_manager(listener)
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

    def test_patch_task_instance_listener_sees_note_when_note_and_state_both_patched(
        self, test_client, session, listener_manager
    ):
        from unit.listeners.class_listener import ClassBasedListener

        self.create_task_instances(session)

        listener = ClassBasedListener()
        listener_manager(listener)
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success", "note": "listener_note"},
        )
        assert response.status_code == 200
        assert listener.ti_note_at_listener == "listener_note"

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
                        "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                    "region_id": "00000000-0000-0000-0000-000000000000",
                    "region_index": -1,
                    "max_tries": 0,
                    "note": "placeholder-note",
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 14,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_display_name": self.TASK_ID,
                    "try_number": 1,
                    "unixname": getuser(),
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                    "team_name": None,
                    "state_reason": None,
                }
            ],
            "total_entries": 1,
            "total_entries_limit": None,
            "next_cursor": None,
            "previous_cursor": None,
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
            "detail": "Task 'non_existent_task' not found in Dag 'example_python_operator'"
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
                                "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                            "region_id": "00000000-0000-0000-0000-000000000000",
                            "region_index": -1,
                            "max_tries": 0,
                            "note": "placeholder-note",
                            "operator": "PythonOperator",
                            "operator_name": "PythonOperator",
                            "pid": 100,
                            "pool": "default_pool",
                            "pool_slots": 1,
                            "priority_weight": 14,
                            "queue": "default_queue",
                            "queued_when": None,
                            "scheduled_when": None,
                            "start_date": "2020-01-02T00:00:00Z",
                            "state": "running",
                            "task_display_name": "print_the_context",
                            "try_number": 1,
                            "unixname": getuser(),
                            "rendered_fields": {},
                            "rendered_map_index": None,
                            "run_after": "2020-01-01T00:00:00Z",
                            "trigger": None,
                            "triggerer_job": None,
                            "team_name": None,
                            "state_reason": None,
                        }
                    ],
                    "total_entries": 1,
                    "total_entries_limit": None,
                    "next_cursor": None,
                    "previous_cursor": None,
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
                        "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                    "region_id": "00000000-0000-0000-0000-000000000000",
                    "region_index": -1,
                    "max_tries": 0,
                    "note": new_note_value,
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 14,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_id": self.TASK_ID,
                    "task_display_name": self.TASK_ID,
                    "try_number": 1,
                    "unixname": getuser(),
                    "dag_run_id": self.RUN_ID,
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                    "team_name": None,
                    "state_reason": None,
                }
            ],
            "total_entries": 1,
            "total_entries_limit": None,
            "next_cursor": None,
            "previous_cursor": None,
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
                        "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                    "region_id": "00000000-0000-0000-0000-000000000000",
                    "region_index": -1,
                    "max_tries": 0,
                    "note": new_note_value,
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 14,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_id": self.TASK_ID,
                    "task_display_name": self.TASK_ID,
                    "try_number": 1,
                    "unixname": getuser(),
                    "dag_run_id": self.RUN_ID,
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                    "team_name": None,
                    "state_reason": None,
                }
            ],
            "total_entries": 1,
            "total_entries_limit": None,
            "next_cursor": None,
            "previous_cursor": None,
        }

        _check_task_instance_note(
            session, response_data["task_instances"][0]["id"], {"content": new_note_value, "user_id": "test"}
        )

    def test_set_note_should_respond_200_for_unversioned_task_instance(self, test_client, session):
        self.create_task_instances(session)
        session.execute(update(TaskInstance).values(dag_version_id=None))
        session.execute(update(DagRun).values(created_dag_version_id=None))
        session.commit()

        response = test_client.patch(
            "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={"note": "unversioned note"},
        )

        assert response.status_code == 200, response.text
        assert response.json()["task_instances"][0]["note"] == "unversioned note"

    def test_set_empty_note_removes_existing_note(self, test_client, session):
        self.create_task_instances(session)
        url = "/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"

        set_response = test_client.patch(url, json={"note": "a note to remove"})
        assert set_response.status_code == 200, set_response.text
        ti_id = set_response.json()["task_instances"][0]["id"]
        _check_task_instance_note(session, ti_id, {"content": "a note to remove", "user_id": "test"})

        clear_response = test_client.patch(url, json={"note": ""})
        assert clear_response.status_code == 200, clear_response.text
        assert clear_response.json()["task_instances"][0]["note"] is None
        _check_task_instance_note(session, ti_id, None)

    def test_set_note_should_respond_200_mapped_task_with_rtif(self, test_client, session):
        """Verify we don't duplicate rows through join to RTIF"""
        tis = self.create_task_instances(session)
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(
                task=old_ti.task, run_id=old_ti.run_id, map_index=idx, dag_version_id=old_ti.dag_version_id
            )
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note", "try_number"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
            session.flush()
            session.add(RTIF(ti, render_templates=False))
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
                            "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                        "region_id": "00000000-0000-0000-0000-000000000000",
                        "region_index": map_index,
                        "max_tries": 0,
                        "note": new_note_value,
                        "operator": "PythonOperator",
                        "operator_name": "PythonOperator",
                        "pid": 100,
                        "pool": "default_pool",
                        "pool_slots": 1,
                        "priority_weight": 14,
                        "queue": "default_queue",
                        "queued_when": None,
                        "scheduled_when": None,
                        "start_date": "2020-01-02T00:00:00Z",
                        "state": "running",
                        "task_id": self.TASK_ID,
                        "task_display_name": self.TASK_ID,
                        "try_number": 1,
                        "unixname": getuser(),
                        "dag_run_id": self.RUN_ID,
                        "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                        "rendered_map_index": str(map_index),
                        "run_after": "2020-01-01T00:00:00Z",
                        "trigger": None,
                        "triggerer_job": None,
                        "team_name": None,
                        "state_reason": None,
                    }
                ],
                "total_entries": 1,
                "total_entries_limit": None,
                "next_cursor": None,
                "previous_cursor": None,
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
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note", "try_number"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
            session.flush()
            session.add(RTIF(ti, render_templates=False))
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
                    "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                "region_id": "00000000-0000-0000-0000-000000000000",
                "region_index": map_index,
                "max_tries": 0,
                "note": new_note_value,
                "operator": "PythonOperator",
                "operator_name": "PythonOperator",
                "pid": 100,
                "pool": "default_pool",
                "pool_slots": 1,
                "priority_weight": 14,
                "queue": "default_queue",
                "queued_when": None,
                "scheduled_when": None,
                "start_date": "2020-01-02T00:00:00Z",
                "state": "running",
                "task_id": self.TASK_ID,
                "task_display_name": self.TASK_ID,
                "try_number": 1,
                "unixname": getuser(),
                "dag_run_id": self.RUN_ID,
                "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
                "rendered_map_index": str(map_index),
                "run_after": "2020-01-01T00:00:00Z",
                "trigger": None,
                "triggerer_job": None,
                "team_name": None,
                "state_reason": None,
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

    @pytest.mark.db_test
    @conf_vars({("state_store", "clear_on_success"): "True"})
    def test_patch_task_instance_to_success_clears_task_state(self, test_client, session):
        """When clear_on_success=True, task_state rows are deleted after manual mark-as-success."""
        self.create_task_instances(session)
        ti = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
            )
        ).one()

        backend = MetastoreBackend()
        scope = TaskScope(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id, map_index=ti.map_index)
        backend.set(scope, "job_id", "app_1234", session=session)
        session.commit()

        assert session.scalars(
            select(TaskStateStoreModel).where(TaskStateStoreModel.task_id == self.TASK_ID)
        ).all()

        test_client.patch(self.ENDPOINT_URL, json={"new_state": "success"})

        session.expire_all()
        assert not session.scalars(
            select(TaskStateStoreModel).where(TaskStateStoreModel.task_id == self.TASK_ID)
        ).all()

    @pytest.mark.db_test
    @conf_vars({("state_store", "clear_on_success"): "True"})
    def test_patch_task_instance_to_failed_does_not_clear_task_state(self, test_client, session):
        """Task state rows are preserved when manually marking a TI as FAILED."""
        self.create_task_instances(session)
        ti = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
            )
        ).one()

        backend = MetastoreBackend()
        scope = TaskScope(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id, map_index=ti.map_index)
        backend.set(scope, "job_id", "app_1234", session=session)
        session.commit()

        test_client.patch(self.ENDPOINT_URL, json={"new_state": "failed"})

        session.expire_all()
        assert session.scalars(
            select(TaskStateStoreModel).where(TaskStateStoreModel.task_id == self.TASK_ID)
        ).all()

    @pytest.mark.db_test
    @conf_vars({("state_store", "clear_on_success"): "False"})
    def test_patch_task_instance_to_success_skips_clear_when_config_disabled(self, test_client, session):
        """Task state rows are preserved on manual mark-as-success when clear_on_success=False."""
        self.create_task_instances(session)
        ti = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
            )
        ).one()

        backend = MetastoreBackend()
        scope = TaskScope(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id, map_index=ti.map_index)
        backend.set(scope, "job_id", "app_1234", session=session)
        session.commit()

        test_client.patch(self.ENDPOINT_URL, json={"new_state": "success"})

        session.expire_all()
        assert session.scalars(
            select(TaskStateStoreModel).where(TaskStateStoreModel.task_id == self.TASK_ID)
        ).all()


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
                        "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                    "region_id": "00000000-0000-0000-0000-000000000000",
                    "region_index": -1,
                    "max_tries": 0,
                    "note": "placeholder-note",
                    "operator": "PythonOperator",
                    "operator_name": "PythonOperator",
                    "pid": 100,
                    "pool": "default_pool",
                    "pool_slots": 1,
                    "priority_weight": 14,
                    "queue": "default_queue",
                    "queued_when": None,
                    "scheduled_when": None,
                    "start_date": "2020-01-02T00:00:00Z",
                    "state": "running",
                    "task_display_name": self.TASK_ID,
                    "try_number": 1,
                    "unixname": getuser(),
                    "rendered_fields": {},
                    "rendered_map_index": None,
                    "run_after": "2020-01-01T00:00:00Z",
                    "trigger": None,
                    "triggerer_job": None,
                    "team_name": None,
                    "state_reason": None,
                }
            ],
            "total_entries": 1,
            "total_entries_limit": None,
            "next_cursor": None,
            "previous_cursor": None,
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
            "detail": "Task 'non_existent_task' not found in Dag 'example_python_operator'"
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
                                "bundle_name": "apache-airflow-providers-standard-example-dags",
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
                            "region_id": "00000000-0000-0000-0000-000000000000",
                            "region_index": -1,
                            "max_tries": 0,
                            "note": "placeholder-note",
                            "operator": "PythonOperator",
                            "operator_name": "PythonOperator",
                            "pid": 100,
                            "pool": "default_pool",
                            "pool_slots": 1,
                            "priority_weight": 14,
                            "queue": "default_queue",
                            "queued_when": None,
                            "scheduled_when": None,
                            "start_date": "2020-01-02T00:00:00Z",
                            "state": "running",
                            "task_display_name": "print_the_context",
                            "try_number": 1,
                            "unixname": getuser(),
                            "rendered_fields": {},
                            "rendered_map_index": None,
                            "run_after": "2020-01-01T00:00:00Z",
                            "trigger": None,
                            "triggerer_job": None,
                            "team_name": None,
                            "state_reason": None,
                        }
                    ],
                    "total_entries": 1,
                    "total_entries_limit": None,
                    "next_cursor": None,
                    "previous_cursor": None,
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
        assert response.json() == {
            "task_instances": [],
            "total_entries": 0,
            "total_entries_limit": None,
            "next_cursor": None,
            "previous_cursor": None,
        }


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

        base_stmt = select(TaskInstance).where(
            TaskInstance.dag_id == self.DAG_ID,
            TaskInstance.task_id == self.TASK_ID,
            TaskInstance.run_id == self.RUN_ID,
        )

        if map_index == -1:
            initial_ti = session.scalars(base_stmt.where(TaskInstance.map_index == -1)).first()
            assert initial_ti is not None
        else:
            initial_tis = session.scalars(base_stmt.where(TaskInstance.map_index != -1)).all()
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
                deleted_ti = session.scalars(base_stmt.where(TaskInstance.map_index == -1)).first()
                assert deleted_ti is None
            else:
                remaining_tis = session.scalars(base_stmt.where(TaskInstance.map_index != -1)).all()
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

    @pytest.mark.parametrize("delete_mode", ["single", "bulk-exact", "bulk-all"])
    def test_delete_removes_current_and_archived_tries(self, test_client, session, delete_mode):
        current_tis = self.create_task_instances(
            session,
            task_instances=[{"task_id": self.TASK_ID, "state": State.SUCCESS, "map_indexes": (0, 1, 2)}],
            with_ti_history=True,
        )
        session.delete(next(ti for ti in current_tis if ti.map_index == 2))
        session.flush()
        task_rows = session.scalars(
            select(TaskInstance)
            .where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id == self.TASK_ID,
            )
            .execution_options(include_all_attempts=True)
        ).all()
        ids_by_index = {
            map_index: {ti.id for ti in task_rows if ti.map_index == map_index} for map_index in (0, 1, 2)
        }
        assert [len(ids_by_index[index]) for index in (0, 1, 2)] == [2, 2, 1]
        session.commit()

        if delete_mode == "single":
            assert (
                test_client.delete(f"{self.ENDPOINT_URL}/{self.TASK_ID}", params={"map_index": 2}).status_code
                == 404
            )
            assert (
                session.scalar(
                    select(TaskInstance.id)
                    .where(TaskInstance.id.in_(ids_by_index[2]))
                    .execution_options(include_all_attempts=True)
                )
                in ids_by_index[2]
            )
            response = test_client.delete(f"{self.ENDPOINT_URL}/{self.TASK_ID}", params={"map_index": 0})
        else:
            entity = {"task_id": self.TASK_ID}
            if delete_mode == "bulk-exact":
                entity["map_index"] = 0
            response = test_client.patch(
                self.ENDPOINT_URL,
                json={"actions": [{"action": "delete", "entities": [entity]}]},
            )
        assert response.status_code == 200
        if delete_mode != "single":
            expected_indexes = {0, 1} if delete_mode == "bulk-all" else {0}
            assert set(response.json()["delete"]["success"]) == {
                f"{self.DAG_ID}.{self.RUN_ID}.{self.TASK_ID}[{index}]" for index in expected_indexes
            }

        session.expire_all()
        remaining_ids = ids_by_index[1] | ids_by_index[2] if delete_mode != "bulk-all" else set()
        assert (
            set(
                session.scalars(
                    select(TaskInstance.id)
                    .where(
                        TaskInstance.dag_id == self.DAG_ID,
                        TaskInstance.run_id == self.RUN_ID,
                        TaskInstance.task_id == self.TASK_ID,
                    )
                    .execution_options(include_all_attempts=True)
                )
            )
            == remaining_ids
        )

    @pytest.fixture(autouse=True)
    def clean_db(self, session):
        clear_db_runs()
        yield
        clear_db_teams()
        clear_db_runs()

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
            if setup_dags == [self.BASH_DAG_ID, self.DAG_ID]:
                self.create_task_instances(
                    session,
                    task_instances=[{"task_id": self.BASH_TASK_ID, "state": default_ti[0]["state"]}],
                    dag_id=self.BASH_DAG_ID,
                    update_extras=True,
                )
                self.create_task_instances(
                    session,
                    task_instances=[{"task_id": self.TASK_ID, "state": default_ti[1]["state"]}],
                    dag_id=self.DAG_ID,
                    update_extras=True,
                )
            else:
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

    @pytest.mark.parametrize(
        ("map_index", "new_state"),
        [
            pytest.param(0, "failed", id="mapped-ti-map-index-0-failed"),
            pytest.param(1, "failed", id="mapped-ti-map-index-1-failed"),
            pytest.param(2, "success", id="mapped-ti-map-index-2-success"),
        ],
    )
    def test_bulk_update_mapped_task_instance_state_is_persisted(
        self, test_client, session, map_index, new_state
    ):
        """Verify that bulk-updating a specific mapped TI actually persists the new state in the DB."""
        self.create_task_instances(
            session,
            task_instances=[{"state": State.RUNNING, "map_indexes": (0, 1, 2)}],
        )

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {
                                "task_id": self.TASK_ID,
                                "map_index": map_index,
                                "new_state": new_state,
                            }
                        ],
                    }
                ]
            },
        )
        assert response.status_code == 200
        assert response.json()["update"]["success"] == [
            f"{self.DAG_ID}.{self.RUN_ID}.{self.TASK_ID}[{map_index}]"
        ]

        session.expire_all()
        # Verify only the targeted mapped TI changed state; others remain unchanged.
        for mi in [0, 1, 2]:
            ti = session.scalar(
                select(TaskInstance).where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.run_id == self.RUN_ID,
                    TaskInstance.task_id == self.TASK_ID,
                    TaskInstance.map_index == mi,
                )
            )
            assert ti is not None
            if mi == map_index:
                assert ti.state == new_state, f"Expected map_index={mi} to be {new_state!r}, got {ti.state!r}"
            else:
                assert ti.state == State.RUNNING, (
                    f"Expected map_index={mi} to remain running, got {ti.state!r}"
                )

    def test_bulk_task_instances_rejects_unauthorized_dag_ids_from_request_body(self, test_client, session):
        restricted_bundle_name = "restricted-bundle-update"
        restricted_team_name = "restricted-team-update"
        self.create_task_instances(
            session,
            task_instances=[{"task_id": self.BASH_TASK_ID, "state": State.RUNNING}],
            dag_id=self.BASH_DAG_ID,
            update_extras=True,
        )
        self.create_task_instances(
            session,
            task_instances=[{"task_id": self.TASK_ID, "state": State.RUNNING}],
            dag_id=self.DAG_ID,
            update_extras=True,
        )
        restricted_bundle = DagBundleModel(name=restricted_bundle_name)
        restricted_team = Team(name=restricted_team_name)
        restricted_bundle.teams.append(restricted_team)
        session.add_all([restricted_bundle, restricted_team])
        session.flush()
        session.execute(
            update(DagModel)
            .where(DagModel.dag_id == self.BASH_DAG_ID)
            .values(bundle_name=restricted_bundle_name)
        )
        session.commit()

        auth_manager = test_client.app.state.auth_manager
        token = auth_manager._get_token_signer().generate(
            auth_manager.serialize_user(
                SimpleAuthManagerUser(username="limited-user", role="user", teams=[]),
            )
        )
        response = test_client.patch(
            self.WILDCARD_ENDPOINT,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {
                                "dag_id": self.BASH_DAG_ID,
                                "dag_run_id": self.RUN_ID,
                                "task_id": self.BASH_TASK_ID,
                                "new_state": "success",
                            },
                            {
                                "dag_id": self.DAG_ID,
                                "dag_run_id": self.RUN_ID,
                                "task_id": self.TASK_ID,
                                "new_state": "success",
                            },
                        ],
                    }
                ]
            },
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        assert response.json()["update"]["success"] == [f"{self.DAG_ID}.{self.RUN_ID}.{self.TASK_ID}[-1]"]
        assert response.json()["update"]["errors"] == [
            {
                "error": f"User is not authorized to update task instances for DAG '{self.BASH_DAG_ID}'",
                "status_code": 403,
            }
        ]

    def test_bulk_delete_rejects_unauthorized_dag_ids_from_request_body(self, test_client, session):
        restricted_bundle_name = "restricted-bundle-delete"
        restricted_team_name = "restricted-team-delete"
        self.create_task_instances(
            session,
            task_instances=[{"task_id": self.BASH_TASK_ID, "state": State.SUCCESS}],
            dag_id=self.BASH_DAG_ID,
            update_extras=True,
        )
        self.create_task_instances(
            session,
            task_instances=[{"task_id": self.TASK_ID, "state": State.SUCCESS}],
            dag_id=self.DAG_ID,
            update_extras=True,
        )
        restricted_bundle = DagBundleModel(name=restricted_bundle_name)
        restricted_team = Team(name=restricted_team_name)
        restricted_bundle.teams.append(restricted_team)
        session.add_all([restricted_bundle, restricted_team])
        session.flush()
        session.execute(
            update(DagModel)
            .where(DagModel.dag_id == self.BASH_DAG_ID)
            .values(bundle_name=restricted_bundle_name)
        )
        session.commit()

        auth_manager = test_client.app.state.auth_manager
        token = auth_manager._get_token_signer().generate(
            auth_manager.serialize_user(
                SimpleAuthManagerUser(username="limited-user", role="user", teams=[]),
            )
        )
        response = test_client.patch(
            self.WILDCARD_ENDPOINT,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": [
                            {
                                "dag_id": self.BASH_DAG_ID,
                                "dag_run_id": self.RUN_ID,
                                "task_id": self.BASH_TASK_ID,
                            },
                            {
                                "dag_id": self.DAG_ID,
                                "dag_run_id": self.RUN_ID,
                                "task_id": self.TASK_ID,
                            },
                        ],
                    }
                ]
            },
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 200
        assert response.json()["delete"]["success"] == [f"{self.DAG_ID}.{self.RUN_ID}.{self.TASK_ID}[-1]"]
        assert response.json()["delete"]["errors"] == [
            {
                "error": f"User is not authorized to delete task instances for DAG '{self.BASH_DAG_ID}'",
                "status_code": 403,
            }
        ]

    @pytest.mark.parametrize("task_count", [5, 10, 20])
    def test_bulk_delete_query_count_scales_linearly_with_task_count(self, test_client, session, task_count):
        # Each extra task instance adds one coordinate DELETE, with no per-instance re-SELECT.
        QUERIES_PER_TASK_INSTANCE = 1
        BASE_QUERY_COUNT = 3

        self.create_task_instances(
            session,
            task_instances=[{"state": State.RUNNING, "map_indexes": tuple(range(task_count))}],
        )
        request_body = {
            "actions": [
                {
                    "action": "delete",
                    "entities": [
                        {"task_id": self.TASK_ID, "map_index": map_index} for map_index in range(task_count)
                    ],
                    "action_on_non_existence": "fail",
                }
            ]
        }

        with count_queries() as result:
            response = test_client.patch(self.ENDPOINT_URL, json=request_body)

        assert response.status_code == 200
        assert len(response.json()["delete"]["success"]) == task_count

        query_count = sum(result.values())
        expected_query_count = BASE_QUERY_COUNT + task_count * QUERIES_PER_TASK_INSTANCE
        assert query_count == expected_query_count, (
            f"Bulk-delete query count {query_count} does not match expected {expected_query_count} "
            f"for {task_count} task instances. "
            f"A regression that re-queries each task instance would give "
            f"~{BASE_QUERY_COUNT + task_count * 2} queries instead."
        )

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(self.ENDPOINT_URL, json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(self.ENDPOINT_URL, json={})
        assert response.status_code == 403

    def test_should_respond_422(self, test_client):
        response = test_client.patch(self.ENDPOINT_URL, json={})
        assert response.status_code == 422

    def test_bulk_update_note_of_unversioned_task_instance(self, test_client, session):
        self.create_task_instances(session, task_instances=[{"state": State.RUNNING}])
        session.execute(update(TaskInstance).values(dag_version_id=None))
        session.execute(update(DagRun).values(created_dag_version_id=None))
        session.commit()

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [{"task_id": self.TASK_ID, "note": "unversioned note"}],
                    }
                ]
            },
        )

        assert response.status_code == 200, response.text
        assert response.json()["update"] == {
            "success": [f"{self.DAG_ID}.{self.RUN_ID}.{self.TASK_ID}[-1]"],
            "errors": [],
        }

    def test_bulk_update_listener_sees_note_when_note_and_state_both_patched(
        self, test_client, session, listener_manager
    ):
        from unit.listeners.class_listener import ClassBasedListener

        self.create_task_instances(session, task_instances=[{"state": State.RUNNING}])

        listener = ClassBasedListener()
        listener_manager(listener)
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {
                                "task_id": self.TASK_ID,
                                "new_state": "success",
                                "note": "listener_note",
                            }
                        ],
                    }
                ]
            },
        )
        assert response.status_code == 200
        assert listener.ti_note_at_listener == "listener_note"


class TestPatchTaskGroup(TestTaskInstanceEndpoint):
    DAG_ID = "example_task_group"
    RUN_ID = "TEST_DAG_RUN_ID"
    GROUP_ID = "section_1"
    BASE_URL = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskGroupInstances"
    ENDPOINT_URL = f"{BASE_URL}/{GROUP_ID}"

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_group_state")
    def test_patch_task_group_success(self, mock_set_tg_state, test_client, session):
        """Test that patching a task group sets state for all tasks in the group."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        tis = (
            session.scalars(
                select(TaskInstance)
                .options(joinedload(TaskInstance.rendered_task_instance_fields))
                .where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.run_id == self.RUN_ID,
                    TaskInstance.task_id.in_(["section_1.task_1", "section_1.task_2", "section_1.task_3"]),
                )
            )
            .unique()
            .all()
        )

        mock_set_tg_state.return_value = list(tis)

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success"},
        )
        assert response.status_code == 200
        response_data = response.json()
        assert mock_set_tg_state.call_count == 1
        call_kwargs = mock_set_tg_state.call_args.kwargs
        assert call_kwargs["group_id"] == self.GROUP_ID
        assert call_kwargs["state"] == "success"
        assert response_data["total_entries"] == 3
        response_task_ids = sorted(ti["task_id"] for ti in response_data["task_instances"])
        assert response_task_ids == ["section_1.task_1", "section_1.task_2", "section_1.task_3"]
        for ti in response_data["task_instances"]:
            assert ti["dag_id"] == self.DAG_ID
            assert ti["dag_run_id"] == self.RUN_ID

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_group_state")
    def test_patch_task_group_failed_state(self, mock_set_tg_state, test_client, session):
        """Test that patching a task group with failed state works."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        tis = (
            session.scalars(
                select(TaskInstance)
                .options(joinedload(TaskInstance.rendered_task_instance_fields))
                .where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.run_id == self.RUN_ID,
                    TaskInstance.task_id.in_(["section_1.task_1", "section_1.task_2", "section_1.task_3"]),
                )
            )
            .unique()
            .all()
        )

        mock_set_tg_state.return_value = list(tis)

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "failed"},
        )
        assert response.status_code == 200
        call_kwargs = mock_set_tg_state.call_args.kwargs
        assert call_kwargs["state"] == "failed"
        response_data = response.json()
        assert response_data["total_entries"] == 3
        response_task_ids = sorted(ti["task_id"] for ti in response_data["task_instances"])
        assert response_task_ids == ["section_1.task_1", "section_1.task_2", "section_1.task_3"]
        for ti in response_data["task_instances"]:
            assert ti["dag_id"] == self.DAG_ID
            assert ti["dag_run_id"] == self.RUN_ID

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_group_state")
    def test_patch_task_group_nested(self, mock_set_tg_state, test_client, session):
        """Test that patching a nested task group includes tasks from inner groups."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        tis = (
            session.scalars(
                select(TaskInstance)
                .options(joinedload(TaskInstance.rendered_task_instance_fields))
                .where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.run_id == self.RUN_ID,
                )
            )
            .unique()
            .all()
        )

        mock_set_tg_state.return_value = list(tis)

        # section_2 contains task_1, and inner_section_2 which contains task_2, task_3, task_4
        url = f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskGroupInstances/section_2"
        response = test_client.patch(
            url,
            json={"new_state": "success"},
        )
        assert response.status_code == 200
        assert mock_set_tg_state.call_count == 1
        call_kwargs = mock_set_tg_state.call_args.kwargs
        assert call_kwargs["group_id"] == "section_2"

    def test_patch_task_group_not_found(self, test_client, session):
        """Test that requesting a non-existent task group returns 404."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        url = f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskGroupInstances/nonexistent_group"
        response = test_client.patch(
            url,
            json={"new_state": "success"},
        )
        assert response.status_code == 404
        assert "nonexistent_group" in response.json()["detail"]

    def test_patch_task_group_invalid_state(self, test_client, session):
        """Test that an invalid new_state returns 422."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "invalid_state"},
        )
        assert response.status_code == 422

    def test_patch_task_group_dag_not_found(self, test_client, session):
        """Test that requesting a non-existent DAG returns 404."""
        url = f"/dags/nonexistent_dag/dagRuns/{self.RUN_ID}/taskGroupInstances/{self.GROUP_ID}"
        response = test_client.patch(
            url,
            json={"new_state": "success"},
        )
        assert response.status_code == 404

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(self.ENDPOINT_URL, json={"new_state": "success"})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(self.ENDPOINT_URL, json={"new_state": "success"})
        assert response.status_code == 403

    def test_query_count_does_not_scale_with_task_group_size(self, test_client, session):
        """Test that query count does not grow excessively with task group size."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        url_section_1 = f"{self.BASE_URL}/section_1"
        url_section_2 = f"{self.BASE_URL}/section_2"

        # --- section_1 (3 tasks) ---
        with count_queries() as result_section_1:
            response = test_client.patch(url_section_1, json={"new_state": "success"})
        assert response.status_code == 200

        # Reset TI states so the next call has work to do
        for ti in session.scalars(
            select(TaskInstance).where(TaskInstance.dag_id == self.DAG_ID, TaskInstance.run_id == self.RUN_ID)
        ):
            ti.state = State.RUNNING
        session.commit()

        # --- section_2 (4 tasks including nested inner_section_2) ---
        with count_queries() as result_section_2:
            response = test_client.patch(url_section_2, json={"new_state": "success"})
        assert response.status_code == 200

        count_section_1 = sum(result_section_1.values())
        count_section_2 = sum(result_section_2.values())
        per_task_overhead = count_section_2 - count_section_1
        assert per_task_overhead <= 2, (
            f"Adding one task should add at most a few queries for per-TI state updates, "
            f"got {per_task_overhead} (section_1={count_section_1}, section_2={count_section_2})"
        )

    def test_patch_task_group_updates_ti_states_in_db(self, test_client, session):
        """Test that patching a task group actually updates task instance states in the database."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        group_task_ids = ["section_1.task_1", "section_1.task_2", "section_1.task_3"]

        # Verify all TIs in the group start as RUNNING
        tis_before = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(group_task_ids),
            )
        ).all()
        assert all(ti.state == State.RUNNING for ti in tis_before)

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success"},
        )
        assert response.status_code == 200

        # Verify states were actually updated in the database
        session.expire_all()
        tis_after = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(group_task_ids),
            )
        ).all()
        assert len(tis_after) == 3
        for ti in tis_after:
            assert ti.state == TaskInstanceState.SUCCESS, (
                f"Expected {ti.task_id} to be SUCCESS, got {ti.state}"
            )

    def test_include_downstream_affects_downstream_tis(self, test_client, session):
        """Test that include_downstream=True also sets state on downstream task instances."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        # section_1 is upstream of section_2 and end
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success", "include_downstream": True},
        )
        assert response.status_code == 200

        session.expire_all()
        # section_2 tasks and end are downstream of section_1 and should be affected
        downstream_task_ids = [
            "section_2.task_1",
            "section_2.inner_section_2.task_2",
            "section_2.inner_section_2.task_3",
            "section_2.inner_section_2.task_4",
            "end",
        ]
        downstream_tis = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(downstream_task_ids),
            )
        ).all()
        assert len(downstream_tis) > 0
        for ti in downstream_tis:
            assert ti.state == TaskInstanceState.SUCCESS, (
                f"Expected downstream {ti.task_id} to be SUCCESS, got {ti.state}"
            )

    def test_include_upstream_affects_upstream_tis(self, test_client, session):
        """Test that include_upstream=True also sets state on upstream task instances."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        # section_1 is downstream of start
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success", "include_upstream": True},
        )
        assert response.status_code == 200

        session.expire_all()
        start_ti = session.scalar(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id == "start",
            )
        )
        assert start_ti is not None
        assert start_ti.state == TaskInstanceState.SUCCESS, (
            f"Expected upstream 'start' to be SUCCESS, got {start_ti.state}"
        )

    def test_clears_failed_downstream_tasks(self, test_client, session):
        """Test that setting a group to success clears failed downstream tasks."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        # Put downstream tasks in failed/upstream_failed state
        downstream_task_ids = [
            "section_2.task_1",
            "section_2.inner_section_2.task_2",
            "section_2.inner_section_2.task_3",
            "section_2.inner_section_2.task_4",
        ]
        session.execute(
            update(TaskInstance)
            .where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(downstream_task_ids),
            )
            .values(state=TaskInstanceState.UPSTREAM_FAILED)
        )
        session.commit()

        # Set section_1 to success — should clear downstream failed tasks
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success"},
        )
        assert response.status_code == 200

        session.expire_all()
        downstream_tis = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(downstream_task_ids),
            )
        ).all()
        assert {ti.task_id for ti in downstream_tis} == set(downstream_task_ids)
        for ti in downstream_tis:
            assert ti.state != TaskInstanceState.UPSTREAM_FAILED, (
                f"Expected {ti.task_id} to be cleared from upstream_failed, got {ti.state}"
            )

    def test_409_when_all_tis_already_in_target_state(self, test_client, session):
        """Test that 409 is returned when all TIs are already in the target state."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        group_task_ids = ["section_1.task_1", "section_1.task_2", "section_1.task_3"]
        session.execute(
            update(TaskInstance)
            .where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(group_task_ids),
            )
            .values(state=TaskInstanceState.SUCCESS)
        )
        session.commit()

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success"},
        )
        assert response.status_code == 409

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_group_state")
    def test_includes_upstream_downstream_parameters(self, mock_set_tg_state, test_client, session):
        """Test that include_upstream and include_downstream parameters are passed through."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        tis = (
            session.scalars(
                select(TaskInstance)
                .options(joinedload(TaskInstance.rendered_task_instance_fields))
                .where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.run_id == self.RUN_ID,
                    TaskInstance.task_id.in_(["section_1.task_1", "section_1.task_2", "section_1.task_3"]),
                )
            )
            .unique()
            .all()
        )

        mock_set_tg_state.return_value = list(tis[:1])

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": "success",
                "include_upstream": True,
                "include_downstream": True,
                "include_future": True,
                "include_past": True,
            },
        )
        assert response.status_code == 200

        # Verify the parameters were passed to set_task_group_state
        call_kwargs = mock_set_tg_state.call_args.kwargs
        assert call_kwargs["upstream"] is True
        assert call_kwargs["downstream"] is True
        assert call_kwargs["future"] is True
        assert call_kwargs["past"] is True

    def test_patch_task_group_note_only(self, test_client, session):
        """Test that patching only the note updates notes for all TIs in the group without changing state."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        group_task_ids = ["section_1.task_1", "section_1.task_2", "section_1.task_3"]
        note_value = "group note"

        response = test_client.patch(
            self.ENDPOINT_URL,
            params={"update_mask": "note"},
            json={"note": note_value},
        )
        assert response.status_code == 200, response.text
        response_data = response.json()
        assert response_data["total_entries"] == 3
        response_task_ids = sorted(ti["task_id"] for ti in response_data["task_instances"])
        assert response_task_ids == group_task_ids
        for ti in response_data["task_instances"]:
            assert ti["note"] == note_value

        tis_after = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(group_task_ids),
            )
        ).all()
        for ti in tis_after:
            assert ti.state == State.RUNNING
            _check_task_instance_note(session, ti.id, {"content": note_value, "user_id": "test"})

    def test_patch_task_group_state_and_note(self, test_client, session):
        """Test that patching both new_state and note applies both, including reflecting the note in the response."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        group_task_ids = ["section_1.task_1", "section_1.task_2", "section_1.task_3"]
        note_value = "marking task group as failed"

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "failed", "note": note_value},
        )
        assert response.status_code == 200, response.text
        response_data = response.json()
        assert response_data["total_entries"] == 3
        response_task_ids = sorted(ti["task_id"] for ti in response_data["task_instances"])
        assert response_task_ids == group_task_ids
        for ti in response_data["task_instances"]:
            assert ti["state"] == TaskInstanceState.FAILED
            assert ti["note"] == note_value

        session.expire_all()
        tis_after = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(group_task_ids),
            )
        ).all()
        for ti in tis_after:
            assert ti.state == TaskInstanceState.FAILED
            _check_task_instance_note(session, ti.id, {"content": note_value, "user_id": "test"})

    def test_patch_task_group_listener_sees_note_when_note_and_state_both_patched(
        self, test_client, session, listener_manager
    ):
        from unit.listeners.class_listener import ClassBasedListener

        self.create_task_instances(session, dag_id=self.DAG_ID)

        listener = ClassBasedListener()
        listener_manager(listener)
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "failed", "note": "listener_note"},
        )
        assert response.status_code == 200
        assert listener.ti_note_at_listener == "listener_note"


class TestPatchTaskGroupDryRun(TestTaskInstanceEndpoint):
    DAG_ID = "example_task_group"
    RUN_ID = "TEST_DAG_RUN_ID"
    GROUP_ID = "section_1"
    BASE_URL = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskGroupInstances"
    ENDPOINT_URL = f"{BASE_URL}/{GROUP_ID}/dry_run"

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.set_task_group_state")
    def test_dry_run_returns_affected_tis_without_committing(self, mock_set_tg_state, test_client, session):
        """Test that dry run returns TIs that would be affected without committing."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        tis = (
            session.scalars(
                select(TaskInstance)
                .options(joinedload(TaskInstance.rendered_task_instance_fields))
                .where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.run_id == self.RUN_ID,
                    TaskInstance.task_id.in_(["section_1.task_1", "section_1.task_2", "section_1.task_3"]),
                )
            )
            .unique()
            .all()
        )

        mock_set_tg_state.return_value = list(tis)

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success"},
        )
        assert response.status_code == 200
        assert mock_set_tg_state.call_count == 1
        # Verify commit=False was passed for dry run
        call_kwargs = mock_set_tg_state.call_args.kwargs
        assert call_kwargs["commit"] is False

    def test_dry_run_query_count_does_not_scale(self, test_client, session):
        """Test that dry_run query count does not grow excessively with task group size."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        url_section_1 = f"{self.BASE_URL}/section_1/dry_run"
        url_section_2 = f"{self.BASE_URL}/section_2/dry_run"

        # --- section_1 (3 tasks) ---
        with count_queries() as result_section_1:
            response = test_client.patch(url_section_1, json={"new_state": "success"})
        assert response.status_code == 200

        # --- section_2 (4 tasks including nested inner_section_2) ---
        with count_queries() as result_section_2:
            response = test_client.patch(url_section_2, json={"new_state": "success"})
        assert response.status_code == 200

        count_section_1 = sum(result_section_1.values())
        count_section_2 = sum(result_section_2.values())
        per_task_overhead = count_section_2 - count_section_1
        assert per_task_overhead <= 2, (
            f"Adding one task should add at most a few queries for per-TI state updates, "
            f"got {per_task_overhead} (section_1={count_section_1}, section_2={count_section_2})"
        )

    def test_dry_run_does_not_update_ti_states_in_db(self, test_client, session):
        """Test that dry run does not actually modify task instance states in the database."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        group_task_ids = ["section_1.task_1", "section_1.task_2", "section_1.task_3"]

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "success"},
        )
        assert response.status_code == 200

        # Verify states were NOT changed in the database
        session.expire_all()
        tis_after = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.task_id.in_(group_task_ids),
            )
        ).all()
        for ti in tis_after:
            assert ti.state == State.RUNNING, (
                f"Expected {ti.task_id} to remain RUNNING after dry run, got {ti.state}"
            )

    def test_dry_run_task_group_not_found(self, test_client, session):
        """Test that requesting a non-existent task group returns 404."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        url = f"/dags/{self.DAG_ID}/dagRuns/{self.RUN_ID}/taskGroupInstances/nonexistent_group/dry_run"
        response = test_client.patch(
            url,
            json={"new_state": "success"},
        )
        assert response.status_code == 404

    def test_dry_run_invalid_state(self, test_client, session):
        """Test that an invalid new_state returns 422."""
        self.create_task_instances(session, dag_id=self.DAG_ID)

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={"new_state": "invalid_state"},
        )
        assert response.status_code == 422

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(self.ENDPOINT_URL, json={"new_state": "success"})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(self.ENDPOINT_URL, json={"new_state": "success"})
        assert response.status_code == 403
