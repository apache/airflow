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

from typing import TYPE_CHECKING

import pytest

from airflow.providers.remote.models.remote_job import RemoteJob, RemoteJobModel
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


class TestRemoteJob:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.query(RemoteJobModel).delete()

    def test_reserve_task_no_job(self):
        job = RemoteJob.reserve_task("worker")
        assert job is None

    def test_reserve_task_has_one(self, session: Session):
        rjm = RemoteJobModel(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            state=TaskInstanceState.QUEUED,
            queue="default",
            command=str(["hello", "world"]),
            queued_dttm=timezone.utcnow(),
        )
        session.add(rjm)
        session.commit()

        job = RemoteJob.reserve_task("worker")
        assert job
        assert job.remote_worker == "worker"
        assert job.queue == "default"
        assert job.dag_id == "test_dag"
        assert job.task_id == "test_task"
        assert job.run_id == "test_run"

        jobs: list[RemoteJobModel] = session.query(RemoteJobModel).all()
        assert len(jobs) == 1
        assert jobs[0].state == TaskInstanceState.RUNNING
        assert jobs[0].remote_worker == "worker"
        assert jobs[0].queue == "default"
        assert jobs[0].dag_id == "test_dag"
        assert jobs[0].task_id == "test_task"
        assert jobs[0].run_id == "test_run"

    def test_set_state(self, session: Session):
        rjm = RemoteJobModel(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            state=TaskInstanceState.RUNNING,
            queue="default",
            command=str(["hello", "world"]),
            queued_dttm=timezone.utcnow(),
        )
        session.add(rjm)
        session.commit()

        RemoteJob.set_state(rjm.key, TaskInstanceState.FAILED)

        jobs: list[RemoteJobModel] = session.query(RemoteJobModel).all()
        assert len(jobs) == 1
        assert jobs[0].state == TaskInstanceState.FAILED
        assert jobs[0].last_update
        assert jobs[0].queue == "default"
        assert jobs[0].dag_id == "test_dag"
        assert jobs[0].task_id == "test_task"
        assert jobs[0].run_id == "test_run"
