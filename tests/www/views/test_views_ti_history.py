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

import pytest

from airflow.models.taskinstancehistory import TaskInstanceHistory
from tests.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


class TestTIHistoryEndpoint:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        clear_db_runs()
        yield
        clear_db_runs()


class TestGetTIHistory(TestTIHistoryEndpoint):
    def test_should_respond_200(self, admin_client, create_task_instance, session):
        ti = create_task_instance(dag_id="test_dag", task_id="test_task", run_id="test_run_id")
        TaskInstanceHistory.record_ti(ti)
        assert session.query(TaskInstanceHistory).count() == 1
        response = admin_client.get(
            "/object/task_instance_history?dag_id=test_dag&task_id=test_task&run_id=test_run_id"
        )
        assert response.status_code == 200
        assert len(response.json) == 1
