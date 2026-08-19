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

from datetime import datetime

import pytest
from sqlalchemy import delete

from airflow._shared.timezones import timezone
from airflow.models.asset import AssetActive, AssetEvent, AssetModel
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.parse("2021-01-01T00:00:00")


@pytest.fixture
def ver_client(client):
    """Newest released Task SDK pins this version; it requires a non-null start_date."""
    client.headers["Airflow-API-Version"] = "2026-06-30"
    return client


@pytest.fixture
def asset_with_queued_created_dagrun(session):
    asset = AssetModel(
        id=1,
        name="test_asset",
        uri="s3://bucket/key",
        group="asset",
        extra={},
        created_at=DEFAULT_DATE,
        updated_at=DEFAULT_DATE,
    )
    session.add_all([asset, AssetActive.for_asset(asset)])
    event = AssetEvent(
        id=1,
        asset_id=1,
        timestamp=datetime(2021, 1, 2, tzinfo=timezone.utc),
        extra={},
        source_dag_id="source_dag",
        source_task_id="source_task",
        source_run_id="source_run",
        source_map_index=-1,
        partition_key=None,
    )
    session.add(event)
    event.created_dagruns.append(
        DagRun(
            dag_id="created_dag",
            run_id="queued_run",
            logical_date=DEFAULT_DATE,
            state=DagRunState.QUEUED,
            run_type=DagRunType.ASSET_TRIGGERED,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        )
    )
    session.commit()

    yield asset

    session.execute(delete(AssetEvent))
    session.execute(delete(DagRun))
    session.execute(delete(AssetActive))
    session.execute(delete(AssetModel))
    session.commit()


@pytest.mark.usefixtures("asset_with_queued_created_dagrun")
def test_created_dagrun_start_date_is_never_null(ver_client):
    """A queued created Dag run must still report a non-null start_date at this version.

    Clients of this version declare ``start_date`` non-nullable and reject the whole response
    when it is null, so the value falls back to ``run_after``.
    """
    response = ver_client.get("/execution/asset-events/by-asset", params={"name": "test_asset", "uri": None})

    assert response.status_code == 200
    created = response.json()["asset_events"][0]["created_dagruns"][0]
    assert created["start_date"] is not None
    assert "run_after" not in created
