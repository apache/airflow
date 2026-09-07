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
from airflow.models.asset import AssetActive, AssetAliasModel, AssetEvent, AssetModel
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = pytest.mark.db_test

RUN_AFTER = timezone.datetime(2021, 1, 1)
STARTED_AT = timezone.datetime(2021, 1, 2)


@pytest.fixture
def old_ver_client(client):
    """Last released execution API in which ``DagRunAssetReference.start_date`` was required."""
    client.headers["Airflow-API-Version"] = "2026-06-30"
    return client


@pytest.fixture
def asset_event_with_created_dagruns(session):
    asset = AssetModel(id=1, name="test_asset", uri="s3://bucket/key", group="asset", extra={})
    alias = AssetAliasModel(id=1, name="test_alias")
    alias.assets.append(asset)
    event = AssetEvent(id=1, asset_id=1, timestamp=RUN_AFTER, extra={}, partition_key=None)
    alias.asset_events.append(event)
    queued_run = DagRun(
        dag_id="created_dag",
        run_id="queued_run",
        state=DagRunState.QUEUED,
        run_type=DagRunType.ASSET_TRIGGERED,
        run_after=RUN_AFTER,
    )
    started_run = DagRun(
        dag_id="created_dag",
        run_id="started_run",
        state=DagRunState.RUNNING,
        run_type=DagRunType.ASSET_TRIGGERED,
        run_after=RUN_AFTER,
        start_date=STARTED_AT,
    )
    event.created_dagruns.extend([queued_run, started_run])
    session.add_all([asset, AssetActive.for_asset(asset), alias, event])
    session.commit()
    yield
    clear_db_runs()
    clear_db_assets()


@pytest.mark.usefixtures("asset_event_with_created_dagruns")
@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/execution/asset-events/by-asset", {"name": "test_asset", "uri": None}),
        ("/execution/asset-events/by-asset-alias", {"name": "test_alias"}),
    ],
)
def test_created_dagruns_always_have_start_date_and_no_run_after(old_ver_client, path, params):
    response = old_ver_client.get(path, params=params)

    assert response.status_code == 200
    created = {run["run_id"]: run for run in response.json()["asset_events"][0]["created_dagruns"]}
    assert created["queued_run"]["start_date"] == RUN_AFTER.isoformat().replace("+00:00", "Z")
    assert created["started_run"]["start_date"] == STARTED_AT.isoformat().replace("+00:00", "Z")
    assert all("run_after" not in run for run in created.values())
