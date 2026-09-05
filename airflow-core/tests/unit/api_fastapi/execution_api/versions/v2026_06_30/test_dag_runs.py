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
from airflow.utils.state import DagRunState

from tests_common.test_utils.db import clear_db_assets

pytestmark = pytest.mark.db_test

ADDED_IN_2026_06_30 = frozenset({"team_name", "partition_date"})


@pytest.fixture
def old_ver_client(client):
    """Last released execution API before ``team_name`` and ``partition_date`` were added."""
    client.headers["Airflow-API-Version"] = "2026-04-06"
    return client


@pytest.fixture
def dag_runs(session, dag_maker):
    with dag_maker(dag_id="test_dag_run_fields", session=session, serialized=True):
        pass
    run1 = dag_maker.create_dagrun(
        state=DagRunState.SUCCESS,
        logical_date=timezone.datetime(2025, 1, 1),
        run_id="run1",
    )
    # A populated value proves the converters strip the field, not just drop a null key.
    run1.partition_date = timezone.datetime(2025, 1, 1)
    dag_maker.create_dagrun(
        state=DagRunState.SUCCESS,
        logical_date=timezone.datetime(2025, 1, 10),
        run_id="run2",
    )
    session.commit()


@pytest.mark.usefixtures("dag_runs")
def test_get_dag_run_omits_new_fields(old_ver_client):
    response = old_ver_client.get("/execution/dag-runs/test_dag_run_fields/run1")

    assert response.status_code == 200
    assert not ADDED_IN_2026_06_30 & response.json().keys()


@pytest.mark.usefixtures("dag_runs")
def test_get_previous_dag_run_omits_new_fields(old_ver_client):
    response = old_ver_client.get(
        "/execution/dag-runs/previous",
        params={
            "dag_id": "test_dag_run_fields",
            "logical_date": timezone.datetime(2025, 1, 10).isoformat(),
        },
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "run1"
    assert not ADDED_IN_2026_06_30 & response.json().keys()


@pytest.mark.usefixtures("dag_runs")
def test_get_previous_dag_run_without_a_match(old_ver_client):
    response = old_ver_client.get(
        "/execution/dag-runs/previous",
        params={
            "dag_id": "test_dag_run_fields",
            "logical_date": timezone.datetime(2024, 1, 1).isoformat(),
        },
    )

    assert response.status_code == 200
    assert response.json() is None


@pytest.fixture
def dag_run_with_consumed_event(session, dag_maker):
    """A Dag run that consumed an asset event carrying a ``partition_key``."""
    # dag_maker resets Dag/DagRun tables between tests but not asset tables, and this fixture
    # commits (the API request needs to see the row), so a leftover asset from a prior test
    # using this fixture would collide on the name/uri unique constraint.
    clear_db_assets()
    with dag_maker(dag_id="test_dag_run_consumed_event", session=session, serialized=True):
        pass
    run = dag_maker.create_dagrun(
        state=DagRunState.SUCCESS,
        logical_date=timezone.datetime(2025, 1, 1),
        run_id="run1",
    )
    asset = AssetModel(name="upstream", uri="s3://bucket/upstream", group="asset", extra={})
    session.add_all([asset, AssetActive.for_asset(asset)])
    session.flush()
    run.consumed_asset_events.append(
        AssetEvent(asset_id=asset.id, source_dag_id="src", source_run_id="r1", partition_key="2024-01-15")
    )
    session.commit()
    yield
    clear_db_assets()


@pytest.mark.usefixtures("dag_run_with_consumed_event")
def test_get_dag_run_strips_consumed_event_partition_key(old_ver_client):
    """A bare-DagRun route must not leak an event-level partition_key to a pre-2026-06-30 client."""
    response = old_ver_client.get("/execution/dag-runs/test_dag_run_consumed_event/run1")

    assert response.status_code == 200
    assert all("partition_key" not in event for event in response.json()["consumed_asset_events"])


@pytest.mark.usefixtures("dag_run_with_consumed_event")
def test_get_previous_dag_run_strips_consumed_event_partition_key(old_ver_client):
    """Same invariant via /previous, which also returns a bare DagRun."""
    response = old_ver_client.get(
        "/execution/dag-runs/previous",
        params={
            "dag_id": "test_dag_run_consumed_event",
            "logical_date": timezone.datetime(2025, 1, 2).isoformat(),
        },
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == "run1"
    assert all("partition_key" not in event for event in response.json()["consumed_asset_events"])
