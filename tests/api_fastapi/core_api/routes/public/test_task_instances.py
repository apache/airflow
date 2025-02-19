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
from unittest import mock

import pendulum
import pytest
from sqlalchemy import select

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models import DagRun, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagbag import DagBag
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.taskmap import TaskMap
from airflow.models.trigger import Trigger
from airflow.utils.platform import getuser
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import (
    clear_db_runs,
    clear_rendered_ti_fields,
)
from tests_common.test_utils.mock_operators import MockOperator

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
        logical_date = self.ti_init.pop("logical_date", self.default_time)
        dr = None

        tis = []
        for i in range(counter):
            if task_instances is None:
                pass
            elif update_extras:
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
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )

        assert response.status_code == 200
        assert response.json() == {
            "dag_id": "example_python_operator",
            "dag_version": None,
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": mock.ANY,
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": None,
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

    @pytest.mark.parametrize(
        "run_id, expected_version_number",
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_respond_200_with_versions(self, test_client, run_id, expected_version_number):
        response = test_client.get(
            f"/public/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1"
        )

        assert response.status_code == 200
        assert response.json() == {
            "id": mock.ANY,
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
            "dag_run_id": run_id,
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
                "id": mock.ANY,
                "version_number": expected_version_number,
                "dag_id": "dag_with_multiple_versions",
                "bundle_name": "dag_maker",
                "bundle_version": f"some_commit_hash{expected_version_number}",
                "bundle_url": f"fakeprotocol://test_host.github.com/tree/some_commit_hash{expected_version_number}",
                "created_at": mock.ANY,
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
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        data = response.json()

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
            "dag_version": None,
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": mock.ANY,
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
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
            },
            "triggerer_job": {
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
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 200
        assert response.json() == {
            "dag_id": "example_python_operator",
            "dag_version": None,
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": mock.ANY,
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
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
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 200

        assert response.json() == {
            "dag_id": "example_python_operator",
            "dag_version": None,
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": mock.ANY,
            "map_index": -1,
            "max_tries": 0,
            "note": "placeholder-note",
            "operator": "PythonOperator",
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
            "rendered_fields": {"op_args": "()", "op_kwargs": {}, "templates_dict": None},
            "rendered_map_index": None,
            "run_after": "2020-01-01T00:00:00Z",
            "trigger": None,
            "triggerer_job": None,
        }

    def test_raises_404_for_nonexistent_task_instance(self, test_client):
        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 404
        assert response.json() == {
            "detail": "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID` and task_id: `print_the_context` was not found"
        }

    def test_raises_404_for_mapped_task_instance_with_multiple_indexes(self, test_client, session):
        tis = self.create_task_instances(session)

        old_ti = tis[0]

        for index in range(3):
            ti = TaskInstance(task=old_ti.task, run_id=old_ti.run_id, map_index=index)
            for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
                setattr(ti, attr, getattr(old_ti, attr))
            session.add(ti)
        session.delete(old_ti)
        session.commit()

        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "Task instance is mapped, add the map_index value to the URL"}

    def test_raises_404_for_mapped_task_instance_with_one_index(self, test_client, session):
        tis = self.create_task_instances(session)

        old_ti = tis[0]

        ti = TaskInstance(task=old_ti.task, run_id=old_ti.run_id, map_index=2)
        for attr in ["duration", "end_date", "pid", "start_date", "state", "queue", "note"]:
            setattr(ti, attr, getattr(old_ti, attr))
        session.add(ti)
        session.delete(old_ti)
        session.commit()

        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "Task instance is mapped, add the map_index value to the URL"}


class TestGetMappedTaskInstance(TestTaskInstanceEndpoint):
    def test_should_respond_200_mapped_task_instance_with_rtif(self, test_client, session):
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
            response = test_client.get(
                "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                f"/print_the_context/{map_index}",
            )
            assert response.status_code == 200

            assert response.json() == {
                "dag_id": "example_python_operator",
                "dag_version": None,
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00Z",
                "logical_date": "2020-01-01T00:00:00Z",
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "id": mock.ANY,
                "map_index": map_index,
                "max_tries": 0,
                "note": "placeholder-note",
                "operator": "PythonOperator",
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
                "rendered_fields": {"op_args": "()", "op_kwargs": {}, "templates_dict": None},
                "rendered_map_index": None,
                "run_after": "2020-01-01T00:00:00Z",
                "trigger": None,
                "triggerer_job": None,
            }

    def test_should_respond_404_wrong_map_index(self, test_client, session):
        self.create_task_instances(session)

        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
            "/print_the_context/10",
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
            with dag_maker(session=session, dag_id=dag_id, start_date=DEFAULT_DATETIME_1):
                task1 = BaseOperator(task_id="op1")
                mapped = MockOperator.partial(task_id="task_2", executor="default").expand(arg2=task1.output)

            dr = dag_maker.create_dagrun(
                run_id=f"run_{dag_id}",
                logical_date=DEFAULT_DATETIME_1,
                data_interval=(DEFAULT_DATETIME_1, DEFAULT_DATETIME_2),
            )

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
                ti = TaskInstance(mapped, run_id=dr.run_id, map_index=index, state=state)
                setattr(ti, "start_date", DEFAULT_DATETIME_1)
                session.add(ti)

            DagBundlesManager().sync_bundles_to_db()
            dagbag = DagBag(os.devnull, include_examples=False)
            dagbag.dags = {dag_id: dag_maker.dag}
            dagbag.sync_to_db("dags-folder", None)
            session.flush()

            TaskMap.expand_mapped_task(mapped, dr.run_id, session=session)

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

    def test_should_respond_404(self, test_client):
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "DAG mapped_tis not found"}

    def test_should_respond_200(self, one_task_with_many_mapped_tis, test_client):
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
        )

        assert response.status_code == 200
        assert response.json()["total_entries"] == 110
        assert len(response.json()["task_instances"]) == 100

    def test_offset_limit(self, test_client, one_task_with_many_mapped_tis):
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"offset": 4, "limit": 10},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 110
        assert len(body["task_instances"]) == 10
        assert list(range(4, 14)) == [ti["map_index"] for ti in body["task_instances"]]

    @pytest.mark.parametrize(
        "params , expected_map_indexes",
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
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params=params,
        )

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 110
        assert len(body["task_instances"]) == params["limit"]
        assert expected_map_indexes == [ti["map_index"] for ti in body["task_instances"]]

    @pytest.mark.parametrize(
        "params, expected_map_indexes",
        [
            ({"order_by": "rendered_map_index", "limit": 108}, [0] + list(range(1, 108))),  # Asc
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

        ti.rendered_map_index = "a"

        session.commit()

        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params=params,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 110
        assert len(body["task_instances"]) == params["limit"]
        assert expected_map_indexes == [ti["map_index"] for ti in body["task_instances"]]

    def test_with_date(self, test_client, one_task_with_mapped_tis):
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"start_date_gte": DEFAULT_DATETIME_1},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 3
        assert len(body["task_instances"]) == 3

        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"start_date_gte": DEFAULT_DATETIME_2},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0
        assert body["task_instances"] == []

    def test_with_logical_date(self, test_client, one_task_with_mapped_tis):
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"logical_date_gte": DEFAULT_DATETIME_1},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 3
        assert len(body["task_instances"]) == 3

        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params={"logical_date_gte": DEFAULT_DATETIME_2},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0
        assert body["task_instances"] == []

    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_task_instance_count",
        [
            ({"state": "success"}, 3, 3),
            ({"state": "running"}, 0, 0),
            ({"pool": "default_pool"}, 3, 3),
            ({"pool": "test_pool"}, 0, 0),
            ({"queue": "default"}, 3, 3),
            ({"queue": "test_queue"}, 0, 0),
            ({"executor": "default"}, 3, 3),
            ({"executor": "no_exec"}, 0, 0),
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
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
            params=query_params,
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert len(body["task_instances"]) == expected_task_instance_count

    def test_with_zero_mapped(self, test_client, one_task_with_zero_mapped_tis, session):
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/task_2/listMapped",
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0
        assert body["task_instances"] == []

    def test_should_raise_404_not_found_for_nonexistent_task(
        self, one_task_with_zero_mapped_tis, test_client
    ):
        response = test_client.get(
            "/public/dags/mapped_tis/dagRuns/run_mapped_tis/taskInstances/nonexistent_task/listMapped",
        )
        assert response.status_code == 404
        assert response.json()["detail"] == "Task id nonexistent_task not found"


class TestGetTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "task_instances, update_extras, url, params, expected_ti",
        [
            pytest.param(
                [
                    {"logical_date": DEFAULT_DATETIME_1},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"logical_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                False,
                "/public/dags/example_python_operator/dagRuns/~/taskInstances",
                {"logical_date_lte": DEFAULT_DATETIME_1},
                1,
                id="test logical date filter",
            ),
            pytest.param(
                [
                    {"start_date": DEFAULT_DATETIME_1},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=1)},
                    {"start_date": DEFAULT_DATETIME_1 + dt.timedelta(days=2)},
                ],
                True,
                "/public/dags/example_python_operator/dagRuns/~/taskInstances",
                {"start_date_gte": DEFAULT_DATETIME_1, "start_date_lte": DEFAULT_DATETIME_STR_2},
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
                "/public/dags/example_python_operator/dagRuns/~/taskInstances?",
                {"end_date_gte": DEFAULT_DATETIME_1, "end_date_lte": DEFAULT_DATETIME_STR_2},
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
                "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances",
                {"duration_gte": 100, "duration_lte": 200},
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
                "/public/dags/~/dagRuns/~/taskInstances",
                {"duration_gte": 100, "duration_lte": 200},
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
                ("/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"state": ["running", "queued", "none"]},
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
                ("/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {},
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
                ("/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"pool": ["test_pool_1", "test_pool_2"]},
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
                "/public/dags/~/dagRuns/~/taskInstances",
                {"pool": ["test_pool_1", "test_pool_2"]},
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
                "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances",
                {"queue": ["test_queue_1", "test_queue_2"]},
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
                "/public/dags/~/dagRuns/~/taskInstances",
                {"queue": ["test_queue_1", "test_queue_2"]},
                2,
                id="test queue filter ~",
            ),
            pytest.param(
                [
                    {"executor": "test_exec_1"},
                    {"executor": "test_exec_2"},
                    {"executor": "test_exec_3"},
                ],
                True,
                ("/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"),
                {"executor": ["test_exec_1", "test_exec_2"]},
                2,
                id="test_executor_filter",
            ),
            pytest.param(
                [
                    {"executor": "test_exec_1"},
                    {"executor": "test_exec_2"},
                    {"executor": "test_exec_3"},
                ],
                True,
                "/public/dags/~/dagRuns/~/taskInstances",
                {"executor": ["test_exec_1", "test_exec_2"]},
                2,
                id="test executor filter ~",
            ),
            pytest.param(
                [
                    {"_task_display_property_value": "task_name_1"},
                    {"_task_display_property_value": "task_name_2"},
                    {"_task_display_property_value": "task_not_match_name_3"},
                ],
                True,
                ("/public/dags/~/dagRuns/~/taskInstances"),
                {"task_display_name_pattern": "task_name"},
                2,
                id="test task_display_name_pattern filter",
            ),
            pytest.param(
                [
                    {"task_id": "task_match_id_1"},
                    {"task_id": "task_match_id_2"},
                    {"task_id": "task_match_id_3"},
                ],
                True,
                ("/public/dags/~/dagRuns/~/taskInstances"),
                {"task_id": "task_match_id_2"},
                1,
                id="test task_id filter",
            ),
            pytest.param(
                [
                    {},
                ],
                True,
                ("/public/dags/~/dagRuns/~/taskInstances"),
                {"version_number": [2]},
                2,
                id="test version number filter",
            ),
            pytest.param(
                [
                    {},
                ],
                True,
                ("/public/dags/~/dagRuns/~/taskInstances"),
                {"version_number": [1, 2, 3]},
                6,
                id="test multiple version numbers filter",
            ),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_respond_200(
        self, test_client, task_instances, update_extras, url, params, expected_ti, session
    ):
        self.create_task_instances(
            session,
            update_extras=update_extras,
            task_instances=task_instances,
        )
        response = test_client.get(url, params=params)
        if params == {"task_id_pattern": "task_match_id"}:
            import pprint

            pprint.pprint(response.json())
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_ti
        assert len(response.json()["task_instances"]) == expected_ti

    def test_not_found(self, test_client):
        response = test_client.get("/public/dags/invalid/dagRuns/~/taskInstances")
        assert response.status_code == 404
        assert response.json() == {"detail": "DAG with dag_id: `invalid` was not found"}

        response = test_client.get("/public/dags/~/dagRuns/invalid/taskInstances")
        assert response.status_code == 404
        assert response.json() == {"detail": "DagRun with run_id: `invalid` was not found"}

    def test_bad_state(self, test_client):
        response = test_client.get("/public/dags/~/dagRuns/~/taskInstances", params={"state": "invalid"})
        assert response.status_code == 422
        assert (
            response.json()["detail"]
            == f"Invalid value for state. Valid values are {', '.join(TaskInstanceState)}"
        )

    @pytest.mark.xfail(reason="permissions not implemented yet.")
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
        response = test_client.get("/public/dags/~/dagRuns/~/taskInstances")
        assert response.status_code == 200
        assert response.json()["total_entries"] == 3
        assert len(response.json()["task_instances"]) == 3

    def test_should_respond_200_for_dag_id_filter(self, test_client, session):
        self.create_task_instances(session)
        self.create_task_instances(session, dag_id="example_skip_dag")
        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/~/taskInstances",
        )

        assert response.status_code == 200
        count = session.query(TaskInstance).filter(TaskInstance.dag_id == "example_python_operator").count()
        assert count == response.json()["total_entries"]
        assert count == len(response.json()["task_instances"])

    @pytest.mark.parametrize(
        "order_by_field, base_date",
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
        response_asc = test_client.get(
            "/public/dags/~/dagRuns/~/taskInstances", params={"order_by": order_by_field}
        )
        assert response_asc.status_code == 200
        assert response_asc.json()["total_entries"] == ti_count
        assert len(response_asc.json()["task_instances"]) == ti_count

        # Descending order
        response_desc = test_client.get(
            "/public/dags/~/dagRuns/~/taskInstances", params={"order_by": f"-{order_by_field}"}
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
            "/public/dags/~/dagRuns/~/taskInstances", params={"limit": 5, "offset": 0, "dag_ids": [dag_id]}
        )
        assert response_batch1.status_code == 200, response_batch1.json()
        num_entries_batch1 = len(response_batch1.json()["task_instances"])
        assert num_entries_batch1 == 5
        assert len(response_batch1.json()["task_instances"]) == 5

        # 5 items after that
        response_batch2 = test_client.get(
            "/public/dags/~/dagRuns/~/taskInstances", params={"limit": 5, "offset": 5, "dag_ids": [dag_id]}
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
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/dependencies",
        )
        assert response.status_code == 200, response.text
        assert response.json() == {"dependencies": []}

    @pytest.mark.parametrize(
        "state, dependencies",
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
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/dependencies",
        )
        assert response.status_code == 200, response.text
        assert response.json() == dependencies

    def test_should_respond_dependencies_mapped(self, test_client, session):
        tis = self.create_task_instances(
            session, task_instances=[{"state": State.SCHEDULED}], update_extras=True
        )
        old_ti = tis[0]

        ti = TaskInstance(task=old_ti.task, run_id=old_ti.run_id, map_index=0, state=old_ti.state)
        session.add(ti)
        session.commit()

        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
            "print_the_context/0/dependencies",
        )
        assert response.status_code == 200, response.text


class TestGetTaskInstancesBatch(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "task_instances, update_extras, payload, expected_ti_count",
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
        response = test_client.post(
            "/public/dags/~/dagRuns/~/taskInstances/list",
            json=payload,
        )
        body = response.json()
        assert response.status_code == 200, body
        assert expected_ti_count == body["total_entries"]
        assert expected_ti_count == len(body["task_instances"])

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
            "/public/dags/~/dagRuns/~/taskInstances/list",
            json={"order_by": "start_date", "dag_ids": [dag_id]},
        )
        assert response_asc.status_code == 200, response_asc.json()
        assert response_asc.json()["total_entries"] == ti_count
        assert len(response_asc.json()["task_instances"]) == ti_count

        # Descending order
        response_desc = test_client.post(
            "/public/dags/~/dagRuns/~/taskInstances/list",
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
            "/public/dags/~/dagRuns/~/taskInstances/list",
            json=payload,
        )
        body = response.json()
        assert response.status_code == 200, body
        assert expected_ti_count == body["total_entries"]
        assert expected_ti_count == len(body["task_instances"])

    @pytest.mark.parametrize(
        "payload, expected_ti, total_ti",
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
            "/public/dags/~/dagRuns/~/taskInstances/list",
            json=payload,
        )
        assert response.status_code == 200
        assert len(response.json()["task_instances"]) == expected_ti
        assert response.json()["total_entries"] == total_ti

    def test_should_raise_400_for_no_json(self, test_client):
        response = test_client.post(
            "/public/dags/~/dagRuns/~/taskInstances/list",
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

    def test_should_respond_422_for_non_wildcard_path_parameters(self, test_client):
        response = test_client.post(
            "/public/dags/non_wildcard/dagRuns/~/taskInstances/list",
        )
        assert response.status_code == 422
        assert "Input should be '~'" in str(response.json()["detail"])

        response = test_client.post(
            "/public/dags/~/dagRuns/non_wildcard/taskInstances/list",
        )
        assert response.status_code == 422
        assert "Input should be '~'" in str(response.json()["detail"])

    @pytest.mark.parametrize(
        "payload, expected",
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
            "/public/dags/~/dagRuns/~/taskInstances/list",
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
            "/public/dags/~/dagRuns/~/taskInstances/list",
            json={"page_limit": 5, "page_offset": 0},
        )
        assert response_batch1.status_code == 200, response_batch1.json()
        num_entries_batch1 = len(response_batch1.json()["task_instances"])
        assert num_entries_batch1 == 5
        assert len(response_batch1.json()["task_instances"]) == 5

        # 5 items after that
        response_batch2 = test_client.post(
            "/public/dags/~/dagRuns/~/taskInstances/list",
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
            "/public/dags/~/dagRuns/~/taskInstances/list",
            json={},
        )

        num_entries_batch3 = len(response_batch3.json()["task_instances"])
        assert num_entries_batch3 == ti_count
        assert len(response_batch3.json()["task_instances"]) == ti_count


class TestGetTaskInstanceTry(TestTaskInstanceEndpoint):
    def test_should_respond_200(self, test_client, session):
        self.create_task_instances(session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True)
        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1"
        )
        assert response.status_code == 200
        assert response.json() == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "PythonOperator",
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
            "dag_version": None,
        }

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_should_respond_200_with_different_try_numbers(self, test_client, try_number, session):
        self.create_task_instances(session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True)
        response = test_client.get(
            f"/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/{try_number}",
        )

        assert response.status_code == 200
        assert response.json() == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0 if try_number == 1 else 1,
            "operator": "PythonOperator",
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
            "dag_version": None,
        }

    @pytest.mark.parametrize("try_number", [1, 2])
    def test_should_respond_200_with_mapped_task_at_different_try_numbers(
        self, test_client, try_number, session
    ):
        tis = self.create_task_instances(session, task_instances=[{"state": State.FAILED}])
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(task=old_ti.task, run_id=old_ti.run_id, map_index=idx)
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
                "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                f"/print_the_context/{map_index}/tries/{try_number}",
            )
            assert response.status_code == 200

            assert response.json() == {
                "dag_id": "example_python_operator",
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00Z",
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "map_index": map_index,
                "max_tries": 0 if try_number == 1 else 1,
                "operator": "PythonOperator",
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
                "dag_version": None,
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
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1",
        )
        assert response.status_code == 200
        data = response.json()

        assert data == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "PythonOperator",
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
            "dag_version": None,
        }

    def test_should_respond_200_with_task_state_in_removed(self, test_client, session):
        self.create_task_instances(
            session, task_instances=[{"state": State.REMOVED}], update_extras=True, with_ti_history=True
        )
        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries/1",
        )
        assert response.status_code == 200

        assert response.json() == {
            "dag_id": "example_python_operator",
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "operator": "PythonOperator",
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
            "dag_version": None,
        }

    def test_raises_404_for_nonexistent_task_instance(self, test_client, session):
        self.create_task_instances(session)
        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/nonexistent_task/tries/0"
        )
        assert response.status_code == 404

        assert response.json() == {
            "detail": "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `nonexistent_task`, try_number: `0` and map_index: `-1` was not found"
        }

    @pytest.mark.parametrize(
        "run_id, expected_version_number",
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_respond_200_with_versions(self, test_client, run_id, expected_version_number):
        response = test_client.get(
            f"/public/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1/tries/0"
        )
        assert response.status_code == 200
        assert response.json() == {
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
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
                "bundle_url": f"fakeprotocol://test_host.github.com/tree/some_commit_hash{expected_version_number}",
                "created_at": mock.ANY,
            },
        }


class TestPostClearTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.parametrize(
        "main_dag, task_instances, request_dag, payload, expected_ti",
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
                    "task_ids": [["print_the_context", 0], "sleep_for_1"],
                },
                2,
                id="clear mapped task and unmapped tasks together",
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
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{request_dag}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_ti

    @pytest.mark.parametrize(
        "main_dag, task_instances, request_dag, payload, expected_ti",
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
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{request_dag}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 422

    @mock.patch("airflow.api_fastapi.core_api.routes.public.task_instances.clear_task_instances")
    def test_clear_taskinstance_is_called_with_queued_dr_state(self, mock_clearti, test_client, session):
        """Test that if reset_dag_runs is True, then clear_task_instances is called with State.QUEUED"""
        self.create_task_instances(session)
        dag_id = "example_python_operator"
        payload = {"reset_dag_runs": True, "dry_run": False}
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200

        # dag (3rd argument) is a different session object. Manually asserting that the dag_id
        # is the same.
        mock_clearti.assert_called_once_with([], mock.ANY, mock.ANY, DagRunState.QUEUED)
        assert mock_clearti.call_args[0][2].dag_id == dag_id

    def test_clear_taskinstance_is_called_with_invalid_task_ids(self, test_client, session):
        """Test that dagrun is running when invalid task_ids are passed to clearTaskInstances API."""
        dag_id = "example_python_operator"
        tis = self.create_task_instances(session)
        dagrun = tis[0].get_dagrun()
        assert dagrun.state == "running"

        payload = {"dry_run": False, "reset_dag_runs": True, "task_ids": [""]}
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{dag_id}/clearTaskInstances",
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
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{dag_id}/clearTaskInstances",
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
        "target_logical_date, response_logical_date",
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
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 200
        expected_response = [
            {
                "dag_id": "example_python_operator",
                "dag_version": None,
                "dag_run_id": "TEST_DAG_RUN_ID_0",
                "task_id": "print_the_context",
                "duration": mock.ANY,
                "end_date": mock.ANY,
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "id": mock.ANY,
                "logical_date": response_logical_date,
                "map_index": -1,
                "max_tries": 0,
                "note": "placeholder-note",
                "operator": "PythonOperator",
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
                "unixname": mock.ANY,
            },
        ]
        assert response.json()["task_instances"] == expected_response
        assert response.json()["total_entries"] == 1

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
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{dag_id}/clearTaskInstances",
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
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            f"/public/dags/{dag_id}/clearTaskInstances",
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
            f"/public/dags/{dag_id}/clearTaskInstances",
            json=payload,
        )

        assert response.status_code == 404
        assert f"Dag Run id TEST_DAG_RUN_ID_100 not found in dag {dag_id}" in response.text

    @pytest.mark.parametrize(
        "payload, expected",
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
        self.dagbag.sync_to_db("dags-folder", None)
        response = test_client.post(
            "/public/dags/example_python_operator/clearTaskInstances",
            json=payload,
        )
        assert response.status_code == 422
        assert response.json() == expected

    def test_raises_404_for_non_existent_dag(self, test_client):
        response = test_client.post(
            "/public/dags/non-existent-dag/clearTaskInstances",
            json={
                "dry_run": False,
                "reset_dag_runs": True,
                "only_failed": False,
                "only_running": True,
            },
        )
        assert response.status_code == 404
        assert "DAG non-existent-dag not found" in response.text


class TestGetTaskInstanceTries(TestTaskInstanceEndpoint):
    def test_should_respond_200(self, test_client, session):
        self.create_task_instances(
            session=session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True
        )
        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == 2  # The task instance and its history
        assert len(response.json()["task_instances"]) == 2
        assert response.json() == {
            "task_instances": [
                {
                    "dag_id": "example_python_operator",
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 0,
                    "operator": "PythonOperator",
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
                    "dag_version": None,
                },
                {
                    "dag_id": "example_python_operator",
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 1,
                    "operator": "PythonOperator",
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
                    "dag_version": None,
                },
            ],
            "total_entries": 2,
        }

    def test_ti_in_retry_state_not_returned(self, test_client, session):
        self.create_task_instances(
            session=session, task_instances=[{"state": State.SUCCESS}], with_ti_history=True
        )
        ti = session.query(TaskInstance).one()
        ti.state = State.UP_FOR_RETRY
        session.merge(ti)
        session.commit()

        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/tries"
        )
        assert response.status_code == 200
        assert response.json()["total_entries"] == 1
        assert len(response.json()["task_instances"]) == 1
        assert response.json() == {
            "task_instances": [
                {
                    "dag_id": "example_python_operator",
                    "duration": 10000.0,
                    "end_date": "2020-01-03T00:00:00Z",
                    "executor": None,
                    "executor_config": "{}",
                    "hostname": "",
                    "map_index": -1,
                    "max_tries": 0,
                    "operator": "PythonOperator",
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
                    "dag_version": None,
                },
            ],
            "total_entries": 1,
        }

    def test_mapped_task_should_respond_200(self, test_client, session):
        tis = self.create_task_instances(session, task_instances=[{"state": State.FAILED}])
        old_ti = tis[0]
        for idx in (1, 2):
            ti = TaskInstance(task=old_ti.task, run_id=old_ti.run_id, map_index=idx)
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
            response = test_client.get(
                "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances"
                f"/print_the_context/{map_index}/tries",
            )
            assert response.status_code == 200
            assert (
                response.json()["total_entries"] == 2
            )  # the mapped task was cleared. So both the task instance and its history
            assert len(response.json()["task_instances"]) == 2
            assert response.json() == {
                "task_instances": [
                    {
                        "dag_id": "example_python_operator",
                        "duration": 10000.0,
                        "end_date": "2020-01-03T00:00:00Z",
                        "executor": None,
                        "executor_config": "{}",
                        "hostname": "",
                        "map_index": map_index,
                        "max_tries": 0,
                        "operator": "PythonOperator",
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
                        "dag_version": None,
                    },
                    {
                        "dag_id": "example_python_operator",
                        "duration": 10000.0,
                        "end_date": "2020-01-03T00:00:00Z",
                        "executor": None,
                        "executor_config": "{}",
                        "hostname": "",
                        "map_index": map_index,
                        "max_tries": 1,
                        "operator": "PythonOperator",
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
                        "dag_version": None,
                    },
                ],
                "total_entries": 2,
            }

    def test_raises_404_for_nonexistent_task_instance(self, test_client, session):
        self.create_task_instances(session)
        response = test_client.get(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/non_existent_task/tries"
        )
        assert response.status_code == 404

        assert response.json() == {
            "detail": "The Task Instance with dag_id: `example_python_operator`, run_id: `TEST_DAG_RUN_ID`, task_id: `non_existent_task` and map_index: `-1` was not found"
        }

    @pytest.mark.parametrize(
        "run_id, expected_version_number",
        [
            ("run1", 1),
            ("run2", 2),
            ("run3", 3),
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_should_respond_200_with_versions(self, test_client, run_id, expected_version_number):
        response = test_client.get(
            f"/public/dags/dag_with_multiple_versions/dagRuns/{run_id}/taskInstances/task1/tries"
        )
        assert response.status_code == 200

        assert response.json()["task_instances"][0] == {
            "task_id": "task1",
            "dag_id": "dag_with_multiple_versions",
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
                "bundle_url": f"fakeprotocol://test_host.github.com/tree/some_commit_hash{expected_version_number}",
                "created_at": mock.ANY,
            },
        }


class TestPatchTaskInstance(TestTaskInstanceEndpoint):
    ENDPOINT_URL = (
        "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
    )
    NEW_STATE = "failed"
    DAG_ID = "example_python_operator"
    TASK_ID = "print_the_context"
    RUN_ID = "TEST_DAG_RUN_ID"

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
    def test_should_call_mocked_api(self, mock_set_ti_state, test_client, session):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.DAG_ID,
                TaskInstance.task_id == self.TASK_ID,
                TaskInstance.run_id == self.RUN_ID,
                TaskInstance.map_index == -1,
            )
        ).one_or_none()

        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 200
        assert response.json() == {
            "dag_id": self.DAG_ID,
            "dag_version": None,
            "dag_run_id": self.RUN_ID,
            "logical_date": "2020-01-01T00:00:00Z",
            "task_id": self.TASK_ID,
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

        mock_set_ti_state.assert_called_once_with(
            commit=True,
            downstream=False,
            upstream=False,
            future=False,
            map_indexes=[-1],
            past=False,
            run_id=self.RUN_ID,
            session=mock.ANY,
            state=self.NEW_STATE,
            task_id=self.TASK_ID,
        )

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
        ti = TaskInstance(task=tis[0].task, run_id=tis[0].run_id, map_index=map_index)
        ti.rendered_task_instance_fields = RTIF(ti, render_templates=False)
        session.add(ti)
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

    @pytest.mark.parametrize(
        "error, code, payload",
        [
            [
                (
                    "Task Instance not found for dag_id=example_python_operator"
                    ", run_id=TEST_DAG_RUN_ID, task_id=print_the_context"
                ),
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
            "/public/dags/non-existent-dag/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "DAG non-existent-dag not found"}

    def test_should_raise_404_for_non_existent_task_in_dag(self, test_client):
        response = test_client.patch(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/non_existent_task",
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
        "payload, expected",
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
        "new_state,expected_status_code,expected_json,set_ti_state_call_count",
        [
            (
                "failed",
                200,
                {
                    "dag_id": "example_python_operator",
                    "dag_version": None,
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
    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
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
        ).one_or_none()

        response = test_client.patch(
            self.ENDPOINT_URL,
            params={"update_mask": "new_state"},
            json={
                "new_state": new_state,
            },
        )
        assert response.status_code == expected_status_code
        assert response.json() == expected_json
        assert mock_set_ti_state.call_count == set_ti_state_call_count

    @pytest.mark.parametrize(
        "new_note_value",
        [
            "My super cool TaskInstance note.",
            None,
        ],
    )
    def test_update_mask_set_note_should_respond_200(self, test_client, session, new_note_value):
        self.create_task_instances(session)
        response = test_client.patch(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            params={"update_mask": "note"},
            json={"note": new_note_value},
        )
        assert response.status_code == 200, response.text
        assert response.json() == {
            "dag_id": self.DAG_ID,
            "dag_version": None,
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "id": mock.ANY,
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "note": new_note_value,
            "operator": "PythonOperator",
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

    def test_set_note_should_respond_200(self, test_client, session):
        self.create_task_instances(session)
        new_note_value = "My super cool TaskInstance note."
        response = test_client.patch(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={"note": new_note_value},
        )
        assert response.status_code == 200, response.text
        assert response.json() == {
            "dag_id": self.DAG_ID,
            "dag_version": None,
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "id": mock.ANY,
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "map_index": -1,
            "max_tries": 0,
            "note": new_note_value,
            "operator": "PythonOperator",
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

    def test_set_note_should_respond_200_mapped_task_instance_with_rtif(self, test_client, session):
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
            response = test_client.patch(
                "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/"
                f"print_the_context/{map_index}",
                json={"note": new_note_value},
            )
            assert response.status_code == 200, response.text

            assert response.json() == {
                "dag_id": self.DAG_ID,
                "dag_version": None,
                "duration": 10000.0,
                "end_date": "2020-01-03T00:00:00Z",
                "logical_date": "2020-01-01T00:00:00Z",
                "id": mock.ANY,
                "executor": None,
                "executor_config": "{}",
                "hostname": "",
                "map_index": map_index,
                "max_tries": 0,
                "note": new_note_value,
                "operator": "PythonOperator",
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
                "rendered_fields": {"op_args": "()", "op_kwargs": {}, "templates_dict": None},
                "rendered_map_index": None,
                "run_after": "2020-01-01T00:00:00Z",
                "trigger": None,
                "triggerer_job": None,
            }

    def test_set_note_should_respond_200_when_note_is_empty(self, test_client, session):
        tis = self.create_task_instances(session)
        for ti in tis:
            ti.task_instance_note = None
            session.add(ti)
        session.commit()
        new_note_value = "My super cool TaskInstance note."
        response = test_client.patch(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context",
            json={"note": new_note_value},
        )
        assert response.status_code == 200, response.text
        assert response.json()["note"] == new_note_value

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
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
    ENDPOINT_URL = (
        "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context"
    )
    NEW_STATE = "failed"
    DAG_ID = "example_python_operator"
    TASK_ID = "print_the_context"
    RUN_ID = "TEST_DAG_RUN_ID"

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
    def test_should_call_mocked_api(self, mock_set_ti_state, test_client, session):
        self.create_task_instances(session)

        mock_set_ti_state.return_value = [
            session.scalars(
                select(TaskInstance).where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.task_id == self.TASK_ID,
                    TaskInstance.run_id == self.RUN_ID,
                    TaskInstance.map_index == -1,
                )
            ).one_or_none()
        ]

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 200
        assert response.json() == {
            "task_instances": [
                {
                    "dag_id": self.DAG_ID,
                    "dag_version": None,
                    "dag_run_id": self.RUN_ID,
                    "logical_date": "2020-01-01T00:00:00Z",
                    "task_id": self.TASK_ID,
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
            map_indexes=[-1],
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

    def test_should_not_update_mapped_task_instance(self, test_client, session):
        map_index = 1
        tis = self.create_task_instances(session)
        ti = TaskInstance(task=tis[0].task, run_id=tis[0].run_id, map_index=map_index)
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

    @pytest.mark.parametrize(
        "error, code, payload",
        [
            [
                (
                    "Task Instance not found for dag_id=example_python_operator"
                    ", run_id=TEST_DAG_RUN_ID, task_id=print_the_context"
                ),
                404,
                {
                    "new_state": "failed",
                },
            ]
        ],
    )
    def test_should_handle_errors(self, error, code, payload, test_client, session):
        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
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
            "/public/dags/non-existent-dag/dagRuns/TEST_DAG_RUN_ID/taskInstances/print_the_context/dry_run",
            json={
                "new_state": self.NEW_STATE,
            },
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "DAG non-existent-dag not found"}

    def test_should_raise_404_for_non_existent_task_in_dag(self, test_client):
        response = test_client.patch(
            "/public/dags/example_python_operator/dagRuns/TEST_DAG_RUN_ID/taskInstances/non_existent_task/dry_run",
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
        "payload, expected",
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
        "new_state,expected_status_code,expected_json,set_ti_state_call_count",
        [
            (
                "failed",
                200,
                {
                    "task_instances": [
                        {
                            "dag_id": "example_python_operator",
                            "dag_version": None,
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
    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
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

        mock_set_ti_state.return_value = [
            session.scalars(
                select(TaskInstance).where(
                    TaskInstance.dag_id == self.DAG_ID,
                    TaskInstance.task_id == self.TASK_ID,
                    TaskInstance.run_id == self.RUN_ID,
                    TaskInstance.map_index == -1,
                )
            ).one_or_none()
        ]

        response = test_client.patch(
            f"{self.ENDPOINT_URL}/dry_run",
            params={"update_mask": "new_state"},
            json={
                "new_state": new_state,
            },
        )
        assert response.status_code == expected_status_code
        assert response.json() == expected_json
        assert mock_set_ti_state.call_count == set_ti_state_call_count

    @mock.patch("airflow.models.dag.DAG.set_task_instance_state")
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
