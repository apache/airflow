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

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select

from airflow.providers.common.compat.sdk import timezone
from airflow.providers.edge3.cli.worker import EdgeWorker
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel, EdgeWorkerState
from airflow.providers.edge3.worker_api.datamodels import WorkerQueueUpdateBody, WorkerStateBody
from airflow.providers.edge3.worker_api.routes.worker import (
    _assert_version,
    register,
    set_state,
    update_queues,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


class TestWorkerApiRoutes:
    @pytest.fixture
    def cli_worker(self, tmp_path: Path) -> EdgeWorker:
        test_worker = EdgeWorker(str(tmp_path / "mock.pid"), "mock", None, 8, 5, 5)
        return test_worker

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.execute(delete(EdgeWorkerModel))

    def test_assert_version(self):
        from airflow import __version__ as airflow_version
        from airflow.providers.edge3 import __version__ as edge_provider_version

        with pytest.raises(HTTPException):
            _assert_version({})

        with pytest.raises(HTTPException):
            _assert_version({"airflow_version": airflow_version})

        with pytest.raises(HTTPException):
            _assert_version({"edge_provider_version": edge_provider_version})

        with pytest.raises(HTTPException):
            _assert_version({"airflow_version": "1.2.3", "edge_provider_version": edge_provider_version})

        with pytest.raises(HTTPException):
            _assert_version({"airflow_version": airflow_version, "edge_provider_version": "2023.10.07"})

        _assert_version({"airflow_version": airflow_version, "edge_provider_version": edge_provider_version})

    @pytest.mark.parametrize(
        "input_queues",
        [
            pytest.param(None, id="empty-queues"),
            pytest.param(["default", "default2"], id="with-queues"),
        ],
    )
    def test_register(self, session: Session, input_queues: list[str] | None, cli_worker: EdgeWorker):
        body = WorkerStateBody(
            state=EdgeWorkerState.STARTING,
            jobs_active=0,
            queues=input_queues,
            sysinfo=cli_worker._get_sysinfo(),
        )
        register("test_worker", body, session)
        session.commit()

        worker: list[EdgeWorkerModel] = session.scalars(select(EdgeWorkerModel)).all()
        assert len(worker) == 1
        assert worker[0].worker_name == "test_worker"
        if input_queues:
            assert worker[0].queues == input_queues
        else:
            assert worker[0].queues is None

    @pytest.mark.parametrize(
        ("existing_state", "should_raise"),
        [
            pytest.param(EdgeWorkerState.RUNNING, True, id="duplicate-running"),
            pytest.param(EdgeWorkerState.IDLE, True, id="duplicate-idle"),
            pytest.param(EdgeWorkerState.STARTING, True, id="duplicate-starting"),
            pytest.param(EdgeWorkerState.TERMINATING, True, id="duplicate-terminating"),
            pytest.param(EdgeWorkerState.MAINTENANCE_MODE, True, id="duplicate-maintenance"),
            pytest.param(EdgeWorkerState.OFFLINE, False, id="reuse-offline"),
            pytest.param(EdgeWorkerState.UNKNOWN, False, id="reuse-unknown"),
            pytest.param(EdgeWorkerState.OFFLINE_MAINTENANCE, False, id="reuse-offline-maintenance"),
        ],
    )
    def test_register_duplicate_worker(
        self, session: Session, existing_state: EdgeWorkerState, should_raise: bool, cli_worker: EdgeWorker
    ):
        # Create an existing worker
        existing_worker = EdgeWorkerModel(
            worker_name="test_worker",
            state=existing_state,
            queues=["default"],
            first_online=timezone.utcnow(),
        )
        session.add(existing_worker)
        session.commit()

        # Try to register a new worker with the same name
        body = WorkerStateBody(
            state=EdgeWorkerState.STARTING,
            jobs_active=0,
            queues=["default"],
            sysinfo=cli_worker._get_sysinfo(),
        )

        if should_raise:
            with pytest.raises(HTTPException) as exc_info:
                register("test_worker", body, session)
            assert exc_info.value.status_code == 409
            assert "already active" in str(exc_info.value.detail).lower()
        else:
            # Should succeed for offline/unknown states
            register("test_worker", body, session)
            session.commit()
            worker = session.execute(
                select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == "test_worker")
            ).scalar_one_or_none()
            assert worker is not None
            # State should be updated (or redefined based on redefine_state logic)
            assert worker.state is not None

    @pytest.mark.parametrize(
        ("worker_state", "body_state", "expected_state"),
        [
            pytest.param(
                EdgeWorkerState.MAINTENANCE_REQUEST,
                EdgeWorkerState.MAINTENANCE_PENDING,
                EdgeWorkerState.MAINTENANCE_PENDING,
                id="stay_maintenance_pending",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_REQUEST,
                EdgeWorkerState.MAINTENANCE_MODE,
                EdgeWorkerState.MAINTENANCE_MODE,
                id="stay_maintenance_mode",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_REQUEST,
                EdgeWorkerState.RUNNING,
                EdgeWorkerState.MAINTENANCE_REQUEST,
                id="maintenance_request",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_EXIT,
                EdgeWorkerState.MAINTENANCE_PENDING,
                EdgeWorkerState.RUNNING,
                id="exit_maintenance_pending",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_EXIT,
                EdgeWorkerState.MAINTENANCE_MODE,
                EdgeWorkerState.IDLE,
                id="exit_maintenance_mode",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_EXIT,
                EdgeWorkerState.RUNNING,
                EdgeWorkerState.RUNNING,
                id="no_exit",
            ),
            pytest.param(
                EdgeWorkerState.RUNNING, EdgeWorkerState.IDLE, EdgeWorkerState.IDLE, id="no_maintenance"
            ),
            pytest.param(
                EdgeWorkerState.OFFLINE_MAINTENANCE,
                EdgeWorkerState.STARTING,
                EdgeWorkerState.MAINTENANCE_REQUEST,
                id="maintenance_starting",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_MODE,
                EdgeWorkerState.STARTING,
                EdgeWorkerState.MAINTENANCE_REQUEST,
                id="maintenance_crash",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_PENDING,
                EdgeWorkerState.STARTING,
                EdgeWorkerState.MAINTENANCE_REQUEST,
                id="maintenance_crash_2",
            ),
            pytest.param(
                EdgeWorkerState.MAINTENANCE_REQUEST,
                EdgeWorkerState.STARTING,
                EdgeWorkerState.MAINTENANCE_REQUEST,
                id="maintenance_crash_3",
            ),
        ],
    )
    def test_redefine_state(
        self, worker_state: EdgeWorkerState, body_state: EdgeWorkerState, expected_state: EdgeWorkerState
    ):
        from airflow.providers.edge3.worker_api.routes.worker import redefine_state

        assert redefine_state(worker_state, body_state) == expected_state

    def test_set_state(self, session: Session, cli_worker: EdgeWorker):
        queues = ["default", "default2"]
        rwm = EdgeWorkerModel(
            worker_name="test2_worker",
            state=EdgeWorkerState.IDLE,
            queues=queues,
            first_online=timezone.utcnow(),
        )
        session.add(rwm)
        session.commit()

        body = WorkerStateBody(
            state=EdgeWorkerState.RUNNING,
            jobs_active=1,
            queues=["default2"],
            sysinfo=cli_worker._get_sysinfo(),
        )
        return_queues = set_state("test2_worker", body, session).queues

        worker: list[EdgeWorkerModel] = session.scalars(select(EdgeWorkerModel)).all()
        assert len(worker) == 1
        assert worker[0].worker_name == "test2_worker"
        assert worker[0].state == EdgeWorkerState.RUNNING
        assert worker[0].queues == queues
        assert return_queues == ["default", "default2"]

    @pytest.mark.parametrize(
        ("add_queues", "remove_queues", "expected_queues"),
        [
            pytest.param(None, None, ["init"], id="no-changes"),
            pytest.param(
                ["queue1", "queue2"], ["queue1", "queue_not_existing"], ["init", "queue2"], id="add-remove"
            ),
            pytest.param(["init"], None, ["init"], id="check-duplicated"),
        ],
    )
    def test_update_queues(
        self,
        session: Session,
        add_queues: list[str] | None,
        remove_queues: list[str] | None,
        expected_queues: list[str],
    ):
        rwm = EdgeWorkerModel(
            worker_name="test2_worker",
            state=EdgeWorkerState.IDLE,
            queues=["init"],
            first_online=timezone.utcnow(),
        )
        session.add(rwm)
        session.commit()
        body = WorkerQueueUpdateBody(new_queues=add_queues, remove_queues=remove_queues)
        update_queues("test2_worker", body, session)
        worker: list[EdgeWorkerModel] = session.scalars(select(EdgeWorkerModel)).all()
        assert len(worker) == 1
        assert worker[0].worker_name == "test2_worker"
        assert len(expected_queues) == len(worker[0].queues or [])
        for expected_queue in expected_queues:
            assert expected_queue in (worker[0].queues or [])
