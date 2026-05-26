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

from airflow._shared.timezones import timezone
from airflow.models.asset import AssetActive, AssetEvent, AssetModel

from tests_common.test_utils.db import clear_db_assets, clear_db_dags, clear_db_runs

DEFAULT_DATE = timezone.parse("2021-01-01T00:00:00")
PARTITION_DATE = timezone.parse("2026-05-20T01:00:00")

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def _clear_db():
    clear_db_assets()
    clear_db_runs()
    clear_db_dags()
    yield
    clear_db_assets()
    clear_db_runs()
    clear_db_dags()


@pytest.fixture
def old_ver_client(client):
    """Last released execution API before AddPartitionDateField was applied."""
    client.headers["Airflow-API-Version"] = "2026-06-16"
    return client


@pytest.fixture
def test_asset(session):
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
    session.commit()
    return asset


@pytest.fixture
def test_asset_event(session, test_asset):
    event = AssetEvent(
        id=1,
        asset_id=test_asset.id,
        extra={},
        source_dag_id="producer",
        source_task_id="emit",
        source_run_id="scheduled__1",
        source_map_index=-1,
        timestamp=datetime(2021, 1, 1, tzinfo=timezone.utc),
        partition_key="2026-05-20T01:00:00",
        partition_date=PARTITION_DATE,
    )
    session.add(event)
    session.commit()
    return event


@pytest.mark.usefixtures("test_asset_event")
def test_partition_date_stripped_at_top_level_for_older_clients(old_ver_client):
    """``partition_date`` should not appear in the AssetEventResponse for ``2026-06-16``."""
    response = old_ver_client.get(
        "/execution/asset-events/by-asset",
        params={"name": "test_asset", "uri": "s3://bucket/key"},
    )
    assert response.status_code == 200
    [event] = response.json()["asset_events"]
    assert "partition_key" in event
    assert "partition_date" not in event


def test_partition_date_stripped_from_created_dagruns_for_older_clients(
    old_ver_client, dag_maker, session, test_asset
):
    """``partition_date`` should also be stripped from nested ``created_dagruns`` entries."""
    with dag_maker(dag_id="consumer_dag", schedule=None, session=session):
        from airflow.sdk import BaseOperator

        BaseOperator(task_id="task")
    dag_run = dag_maker.create_dagrun(
        partition_key="2026-05-20",
        partition_date=PARTITION_DATE,
    )
    event = AssetEvent(
        id=1,
        asset_id=test_asset.id,
        extra={},
        source_dag_id="producer",
        source_task_id="emit",
        source_run_id="scheduled__1",
        source_map_index=-1,
        timestamp=datetime(2021, 1, 1, tzinfo=timezone.utc),
        partition_key="2026-05-20T01:00:00",
        partition_date=PARTITION_DATE,
    )
    event.created_dagruns.append(dag_run)
    session.add(event)
    session.commit()

    response = old_ver_client.get(
        "/execution/asset-events/by-asset",
        params={"name": "test_asset", "uri": "s3://bucket/key"},
    )
    assert response.status_code == 200
    [response_event] = response.json()["asset_events"]
    assert response_event["created_dagruns"], "fixture must produce at least one consumer DagRun"
    for dag_ref in response_event["created_dagruns"]:
        assert "partition_key" in dag_ref
        assert "partition_date" not in dag_ref
