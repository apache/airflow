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
from sqlalchemy import delete

from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel, EdgeWorkerState

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Plugin endpoint is not used in Airflow 3.0+")
class TestUiApiRoutes:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, session: Session):
        session.execute(delete(EdgeWorkerModel))
        session.add(EdgeWorkerModel(worker_name="worker1", queues=["default"], state=EdgeWorkerState.RUNNING))
        session.commit()

    def test_worker(self, session: Session):
        from airflow.providers.edge3.worker_api.routes.ui import worker

        worker_response = worker(session=session)
        assert worker_response is not None
        assert worker_response.total_entries == 1
        assert len(worker_response.workers) == 1
        assert worker_response.workers[0].worker_name == "worker1"
