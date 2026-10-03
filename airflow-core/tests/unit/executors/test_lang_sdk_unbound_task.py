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
"""A stub task queued without its artifact fails in the task runtime, with the reason in its task log."""

from __future__ import annotations

import json

import pytest
from sqlalchemy import select

from airflow.executors.workloads import ExecuteTask
from airflow.models.taskinstance import TaskInstance
from airflow.sdk import task
from airflow.sdk.execution_time.coordinator import reset_coordinator_manager
from airflow.sdk.execution_time.supervisor import InProcessTestSupervisor, supervise_task
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test

UNBOUND_MESSAGE = (
    "Task 'load' of Dag 'etl' has no task handler artifact, and its Dag file '{dag_file}' is not an "
    "artifact that ExecutableCoordinator runs. Queue 'golang' routes it to a Lang-SDK coordinator, so it "
    "must be a @task.stub task the Dag processor bound to an artifact, or a task of a Dag defined in a "
    "Lang SDK. Check the import errors of '{dag_file}', and that the scheduler has the same [sdk] "
    "configuration as the Dag processor."
)


class TestUnboundStubTask:
    @pytest.fixture(autouse=True)
    def _clean_db(self):
        def clean():
            reset_coordinator_manager()
            clear_db_runs()
            clear_db_dags()

        clean()
        yield
        clean()

    @pytest.fixture
    def run_unbound_stub_task(self, dag_maker, session, configure_dag_bundles, tmp_path):
        def run(*, retries: int) -> tuple[TaskInstance, list[dict], str]:
            dags = tmp_path / "dags"
            dags.mkdir()
            with dag_maker("etl", session=session):

                @task.stub(task_id="load", queue="golang", retries=retries)
                def load(): ...

                load()
            ti = dag_maker.create_dagrun(session=session).get_task_instances(session=session)[0]
            ti.state = TaskInstanceState.QUEUED
            session.commit()
            dag_file = ti.dag_model.relative_fileloc
            # A Python Dag file: no artifact binds this task, and the file is not one a Go coordinator runs.
            (dags / dag_file).write_text("# A Python Dag file\n")

            coordinator = {"classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator"}
            sdk_config = {
                ("sdk", "coordinators"): json.dumps({"go": coordinator}),
                ("sdk", "queue_to_coordinator"): json.dumps({"golang": "go"}),
                ("logging", "base_log_folder"): str(tmp_path / "logs"),
            }
            workload = ExecuteTask.make(ti)
            with configure_dag_bundles({ti.dag_model.bundle_name: dags}), conf_vars(sdk_config):
                reset_coordinator_manager()
                exit_code = supervise_task(
                    ti=workload.ti,  # type: ignore[arg-type]
                    bundle_info=workload.bundle_info,
                    dag_rel_path=workload.dag_rel_path,
                    token="",
                    log_path=workload.log_path,
                    client=InProcessTestSupervisor._api_client(),
                )
            assert exit_code == 1

            session.expire_all()
            # A retry gives the task instance a new id.
            reported = session.scalars(
                select(TaskInstance).where(
                    TaskInstance.dag_id == "etl",
                    TaskInstance.task_id == "load",
                    TaskInstance.run_id == "test",
                )
            ).one()
            task_log = [
                json.loads(line)
                for line in (tmp_path / "logs" / workload.log_path).read_text().splitlines()
                if line
            ]
            return reported, task_log, dag_file

        return run

    def test_the_try_fails_with_the_reason_in_its_state_reason_and_task_log(self, run_unbound_stub_task):
        ti, task_log, dag_file = run_unbound_stub_task(retries=0)

        expected = UNBOUND_MESSAGE.format(dag_file=dag_file)
        assert ti.state == TaskInstanceState.FAILED
        assert ti.retry_reason == expected[:500]
        assert [(e["level"], e["event"], e["reason"]) for e in task_log if "reason" in e] == [
            ("error", "Cannot run the task's Lang-SDK artifact", expected)
        ]

    def test_the_try_is_retried_when_the_task_has_retries_left(self, run_unbound_stub_task):
        ti, task_log, dag_file = run_unbound_stub_task(retries=1)

        assert ti.state == TaskInstanceState.UP_FOR_RETRY
        assert ti.retry_reason == UNBOUND_MESSAGE.format(dag_file=dag_file)[:500]
