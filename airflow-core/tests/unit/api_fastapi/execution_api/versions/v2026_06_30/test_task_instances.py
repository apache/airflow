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

from airflow._shared.timezones import timezone
from airflow.models.asset import AssetActive, AssetEvent, AssetModel
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"
TIMESTAMP = timezone.parse(TIMESTAMP_STR)
PARTITION_DATE = timezone.parse("2026-05-20T01:00:00")

RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "h",
    "unixname": "u",
    "pid": 1,
    "start_date": TIMESTAMP_STR,
}


@pytest.fixture
def old_ver_client(client):
    """Last released execution API before this bundle -- the version Task SDK 1.2.x sends."""
    client.headers["Airflow-API-Version"] = "2026-04-06"
    return client


class TestPartitionDateFieldBackwardCompat:
    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_strips_partition_date_from_dag_run(
        self, old_ver_client, session, create_task_instance
    ):
        ti = create_task_instance(
            task_id="test_partition_date_downgrade",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.partition_key = "2026-05-20"
        ti.dag_run.partition_date = PARTITION_DATE
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        dag_run = response.json()["dag_run"]
        assert dag_run["partition_key"] == "2026-05-20"
        assert "partition_date" not in dag_run

    def test_head_version_includes_partition_date_field(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_partition_date_head",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.partition_key = "2026-05-20"
        ti.dag_run.partition_date = PARTITION_DATE
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        dag_run = response.json()["dag_run"]
        assert dag_run["partition_key"] == "2026-05-20"
        assert dag_run["partition_date"] == PARTITION_DATE.isoformat().replace("+00:00", "Z")


class TestConsumedEventPartitionKeyBackwardCompat:
    """``partition_key`` on consumed asset events is stripped for clients older than 2026-06-30."""

    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()
        clear_db_assets()

    def teardown_method(self):
        clear_db_runs()
        clear_db_assets()

    def _create_ti_with_consumed_event(self, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_consumed_event_partition_key_compat",
            state=State.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        asset = AssetModel(name="upstream", uri="s3://bucket/upstream", group="asset", extra={})
        session.add_all([asset, AssetActive.for_asset(asset)])
        session.flush()
        ti.dag_run.partition_key = "2026-05-20"
        ti.dag_run.consumed_asset_events.append(
            AssetEvent(asset_id=asset.id, source_dag_id="src", source_run_id="r1", partition_key="2024-01-15")
        )
        session.commit()
        return ti

    def test_old_version_strips_event_key_but_keeps_dag_run_key(
        self, old_ver_client, session, create_task_instance
    ):
        ti = self._create_ti_with_consumed_event(session, create_task_instance)

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        dag_run = response.json()["dag_run"]
        events = dag_run["consumed_asset_events"]
        assert events
        assert all("partition_key" not in event for event in events)
        # The DagRun-level field really was released at 2026-04-06 and must survive.
        assert dag_run["partition_key"] == "2026-05-20"

    @pytest.mark.parametrize("api_version", [None, "2026-06-30"])
    def test_partition_key_kept_at_2026_06_30_and_newer(
        self, client, session, create_task_instance, api_version
    ):
        """The gate must not over-move: 1.3.x clients send 2026-06-30 and do carry this field."""
        if api_version is not None:
            client.headers["Airflow-API-Version"] = api_version
        ti = self._create_ti_with_consumed_event(session, create_task_instance)

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        dag_run = response.json()["dag_run"]
        assert [event["partition_key"] for event in dag_run["consumed_asset_events"]] == ["2024-01-15"]
        assert dag_run["partition_key"] == "2026-05-20"

    def test_schema_gates_event_partition_key_at_2026_06_30(self, client):
        """
        The schema half of the gate must move with the converter half, or the served
        OpenAPI contradicts the wire format.
        """
        old_schema = client.get("/execution/openapi.json?version=2026-04-06").json()["components"]["schemas"]
        new_schema = client.get("/execution/openapi.json?version=2026-06-30").json()["components"]["schemas"]

        assert "partition_key" not in old_schema["AssetEventDagRunReference"]["properties"]
        assert "partition_key" in new_schema["AssetEventDagRunReference"]["properties"]
