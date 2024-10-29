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

import pendulum
import pytest

from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models import DagRun, TaskInstance
from airflow.models.dagbag import DagBag
from airflow.models.renderedtifields import RenderedTaskInstanceFields as RTIF
from airflow.models.trigger import Trigger
from airflow.utils.platform import getuser
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_runs, clear_rendered_ti_fields

pytestmark = pytest.mark.db_test


DEFAULT_DATETIME_1 = datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
DEFAULT_DATETIME_STR_1 = "2020-01-01T00:00:00+00:00"
DEFAULT_DATETIME_STR_2 = "2020-01-02T00:00:00+00:00"

QUOTED_DEFAULT_DATETIME_STR_1 = urllib.parse.quote(DEFAULT_DATETIME_STR_1)
QUOTED_DEFAULT_DATETIME_STR_2 = urllib.parse.quote(DEFAULT_DATETIME_STR_2)


class TestTaskInstanceEndpoint:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, session) -> None:
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
        clear_db_runs()
        clear_rendered_ti_fields()
        dagbag = DagBag(include_examples=True, read_dags_from_db=False)
        dagbag.sync_to_db()
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
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
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
            "start_date": "2020-01-02T00:00:00Z",
            "state": "running",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "rendered_map_index": None,
            "trigger": None,
            "triggerer_job": None,
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
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
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
            "start_date": "2020-01-02T00:00:00Z",
            "state": "deferred",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
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
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
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
            "start_date": "2020-01-02T00:00:00Z",
            "state": "removed",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {},
            "rendered_map_index": None,
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
            "duration": 10000.0,
            "end_date": "2020-01-03T00:00:00Z",
            "logical_date": "2020-01-01T00:00:00Z",
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
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
            "start_date": "2020-01-02T00:00:00Z",
            "state": "running",
            "task_id": "print_the_context",
            "task_display_name": "print_the_context",
            "try_number": 0,
            "unixname": getuser(),
            "dag_run_id": "TEST_DAG_RUN_ID",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "rendered_map_index": None,
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
