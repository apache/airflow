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

from unittest.mock import patch

import pytest

from airflow._shared.timezones import timezone
from airflow.api_fastapi.common.dagbag import dag_bag_from_app
from airflow.models.dagbag import DBDagBag
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = pytest.mark.db_test


DEFAULT_START_DATE = timezone.parse("2024-10-31T11:00:00Z")
DEFAULT_END_DATE = timezone.parse("2024-10-31T12:00:00Z")


@pytest.fixture
def ver_client(client):
    client.headers["Airflow-API-Version"] = "2025-04-28"
    return client


class TestTIUpdateState:
    def setup_method(self):
        clear_db_assets()
        clear_db_runs()

    def teardown_method(self):
        clear_db_assets()
        clear_db_runs()

    @pytest.mark.parametrize(
        ("mock_indexes", "expected_response_indexes"),
        [
            pytest.param(
                [("task_a", 5), ("task_b", 10)],
                {"task_a": 5, "task_b": 10},
                id="plain ints",
            ),
            pytest.param(
                [("task_a", [3, 4]), ("task_b", [9])],
                {"task_a": 3, "task_b": 9},
                id="list of ints",
            ),
            pytest.param(
                [
                    ("task_a", None),
                ],
                {"task_a": None},
                id="task has no upstreams",
            ),
            pytest.param(
                [("task_a", None), ("task_b", [6, 7]), ("task_c", 2)],
                {"task_a": None, "task_b": 6, "task_c": 2},
                id="mixed types",
            ),
        ],
    )
    @patch("airflow.api_fastapi.execution_api.routes.task_instances._get_upstream_map_indexes")
    def test_ti_run(
        self,
        mock_get_upstream_map_indexes,
        ver_client,
        session,
        create_task_instance,
        time_machine,
        mock_indexes,
        expected_response_indexes,
        get_execution_app,
    ):
        """
        Test that this version of the endpoint works.

        Later versions modified the type of upstream_map_indexes.
        """
        mock_get_upstream_map_indexes.return_value = mock_indexes

        instant_str = "2024-09-30T12:00:00Z"
        instant = timezone.parse(instant_str)
        time_machine.move_to(instant, tick=False)

        ti = create_task_instance(
            task_id="test_ti_run_state_to_running",
            state=State.QUEUED,
            session=session,
            start_date=instant,
        )

        dagbag = DBDagBag()
        execution_app = get_execution_app(ver_client)
        execution_app.dependency_overrides[dag_bag_from_app] = lambda: dagbag
        session.commit()

        response = ver_client.patch(
            f"/execution/task-instances/{ti.id}/run",
            json={
                "state": "running",
                "hostname": "random-hostname",
                "unixname": "random-unixname",
                "pid": 100,
                "start_date": instant_str,
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "dag_run": {
                "dag_id": "dag",
                "run_id": "test",
                "clear_number": 0,
                "logical_date": instant_str,
                "data_interval_start": instant.subtract(days=1).to_iso8601_string(),
                "data_interval_end": instant_str,
                "run_after": instant_str,
                "start_date": instant_str,
                "end_date": None,
                "run_type": "manual",
                "conf": {},
                "consumed_asset_events": [],
            },
            "task_reschedule_count": 0,
            "upstream_map_indexes": expected_response_indexes,
            "max_tries": 0,
            "should_retry": False,
            "variables": [],
            "connections": [],
            "xcom_keys_to_clear": [],
        }
