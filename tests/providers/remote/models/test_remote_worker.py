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

from airflow.providers.remote.cli.remote_command import _get_sysinfo
from airflow.providers.remote.models.remote_worker import (
    RemoteWorker,
    RemoteWorkerModel,
    RemoteWorkerState,
    RemoteWorkerVersionException,
)
from airflow.utils import timezone

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


class TestRemoteWorker:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.query(RemoteWorkerModel).delete()

    def test_assert_version(self):
        from airflow import __version__ as airflow_version
        from airflow.providers.remote import __version__ as remote_provider_version

        with pytest.raises(RemoteWorkerVersionException):
            RemoteWorker.assert_version({})

        with pytest.raises(RemoteWorkerVersionException):
            RemoteWorker.assert_version({"airflow_version": airflow_version})

        with pytest.raises(RemoteWorkerVersionException):
            RemoteWorker.assert_version({"remote_provider_version": remote_provider_version})

        with pytest.raises(RemoteWorkerVersionException):
            RemoteWorker.assert_version(
                {"airflow_version": "1.2.3", "remote_provider_version": remote_provider_version}
            )

        with pytest.raises(RemoteWorkerVersionException):
            RemoteWorker.assert_version(
                {"airflow_version": airflow_version, "remote_provider_version": "2023.10.07"}
            )

        RemoteWorker.assert_version(
            {"airflow_version": airflow_version, "remote_provider_version": remote_provider_version}
        )

    def test_register_worker(self, session: Session):
        RemoteWorker.register_worker(
            "test_worker", RemoteWorkerState.STARTING, queues=None, sysinfo=_get_sysinfo()
        )

        worker: list[RemoteWorkerModel] = session.query(RemoteWorkerModel).all()
        assert len(worker) == 1
        assert worker[0].worker_name == "test_worker"

    def test_set_state(self, session: Session):
        rwm = RemoteWorkerModel(
            worker_name="test2_worker",
            state=RemoteWorkerState.IDLE,
            queues=["default"],
            first_online=timezone.utcnow(),
        )
        session.add(rwm)
        session.commit()

        RemoteWorker.set_state("test2_worker", RemoteWorkerState.RUNNING, 1, _get_sysinfo())

        worker: list[RemoteWorkerModel] = session.query(RemoteWorkerModel).all()
        assert len(worker) == 1
        assert worker[0].worker_name == "test2_worker"
        assert worker[0].state == RemoteWorkerState.RUNNING
