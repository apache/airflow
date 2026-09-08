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
from airflow.models.asset import AssetActive, AssetAliasModel, AssetEvent, AssetModel
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.parse("2021-01-01T00:00:00")


@pytest.fixture
def ver_client(client):
    """A version older than the one that introduced partition_key on nested Dag runs."""
    client.headers["Airflow-API-Version"] = "2025-11-05"
    return client


@pytest.fixture
def asset_event_with_created_dagrun(session):
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
            run_id="created_run",
            logical_date=DEFAULT_DATE,
            state=DagRunState.SUCCESS,
            run_type=DagRunType.ASSET_TRIGGERED,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        )
    )
    alias = AssetAliasModel(id=1, name="test_alias")
    alias.asset_events = [event]
    alias.assets.append(asset)
    session.add(alias)
    session.commit()

    yield asset

    session.execute(delete(AssetEvent))
    session.execute(delete(DagRun))
    session.execute(delete(AssetAliasModel))
    session.execute(delete(AssetActive))
    session.execute(delete(AssetModel))
    session.commit()


@pytest.mark.usefixtures("asset_event_with_created_dagrun")
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("by-asset", {"name": "test_asset", "uri": None}),
        ("by-asset-alias", {"name": "test_alias"}),
    ],
)
def test_nested_created_dagruns_hide_partition_key(ver_client, path, params):
    """Nested Dag runs must be migrated too, not only the events that carry them.

    The nested schema for this version forbids unknown keys, so leaving ``partition_key`` on a
    created Dag run fails response serialization and the client receives a 500.
    """
    response = ver_client.get(f"/execution/asset-events/{path}", params=params)

    assert response.status_code == 200
    events = response.json()["asset_events"]
    assert events, "fixture must produce at least one event"
    for event in events:
        assert "partition_key" not in event
        for created in event["created_dagruns"]:
            assert "partition_key" not in created
