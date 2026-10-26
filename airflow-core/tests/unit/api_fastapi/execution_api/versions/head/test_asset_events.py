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

import httpx2
import pytest
from sqlalchemy import delete

from airflow._shared.timezones import timezone
from airflow.models.asset import AssetActive, AssetAliasModel, AssetEvent, AssetModel, AssetWatcherModel
from airflow.models.dagrun import DagRun
from airflow.models.trigger import Trigger
from airflow.sdk.api.client import Client as ExecutionAPIClient
from airflow.triggers.base import AssetTriggerEvent, TriggerEvent
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars

DEFAULT_DATE = timezone.parse("2021-01-01T00:00:00")

pytestmark = pytest.mark.db_test


@pytest.fixture
def test_asset_events(session):
    def make_timestamp(day):
        return datetime(2021, 1, day, tzinfo=timezone.utc)

    common = {
        "asset_id": 1,
        "extra": {"foo": "bar"},
        "source_dag_id": "foo",
        "source_task_id": "bar",
        "source_run_id": "custom",
        "source_map_index": -1,
        "partition_key": None,
    }

    events = [AssetEvent(id=i, timestamp=make_timestamp(i), **common) for i in (1, 2, 3)]
    session.add_all(events)
    session.commit()
    yield events

    for event in events:
        session.delete(event)
    session.commit()


@pytest.fixture
def test_asset(session):
    asset = AssetModel(
        id=1,
        name="test_get_asset_by_name",
        uri="s3://bucket/key",
        group="asset",
        extra={"foo": "bar"},
        created_at=DEFAULT_DATE,
        updated_at=DEFAULT_DATE,
    )
    asset_active = AssetActive.for_asset(asset)
    session.add_all([asset, asset_active])
    session.commit()

    yield asset

    session.delete(asset)
    session.delete(asset_active)
    session.commit()


@pytest.fixture
def test_asset_alias(session, test_asset_events, test_asset):
    alias = AssetAliasModel(id=1, name="test_alias")
    alias.asset_events = test_asset_events
    alias.assets.append(test_asset)
    session.add(alias)
    session.commit()

    yield alias

    session.delete(alias)
    session.commit()


class TestGetAssetEventByAsset:
    @pytest.mark.parametrize("partition_key", [None, "2026-10-16"])
    def test_get_watcher_event_through_task_sdk(self, client, session, test_asset, partition_key, request):
        trigger = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs={})
        session.add(trigger)
        session.flush()

        def cleanup():
            session.execute(delete(AssetEvent).where(AssetEvent.asset_id == test_asset.id))
            session.execute(delete(AssetWatcherModel).where(AssetWatcherModel.trigger_id == trigger.id))
            session.execute(delete(Trigger).where(Trigger.id == trigger.id))
            session.commit()

        request.addfinalizer(cleanup)
        test_asset.add_trigger(trigger, "partition_watcher")
        session.flush()
        payload = {"partition_key": "payload-only", "changed": True}
        event = (
            TriggerEvent(payload)
            if partition_key is None
            else AssetTriggerEvent(payload, partition_key=partition_key)
        )
        Trigger.submit_event(trigger.id, event, session=session)
        session.commit()
        session.expire_all()

        def read_event(request: httpx2.Request) -> httpx2.Response:
            response = client.get(
                request.url.path,
                params=list(request.url.params.multi_items()),
                headers=dict(request.headers),
            )
            return httpx2.Response(response.status_code, headers=response.headers, content=response.content)

        with ExecutionAPIClient(
            base_url="http://testserver/execution/",
            token="fake",
            transport=httpx2.MockTransport(read_event),
        ) as sdk_client:
            result = sdk_client.asset_events.get(name=test_asset.name)

        assert len(result.asset_events) == 1
        returned_event = result.asset_events[0]
        assert returned_event.partition_key == partition_key
        assert returned_event.extra == {"from_trigger": True, "payload": payload}
        assert isinstance(returned_event.timestamp, datetime)

    @pytest.mark.usefixtures("test_asset")
    def test_get_by_asset_with_created_dagrun_without_start_date(self, client, session, test_asset_events):
        created_dagrun = DagRun(
            dag_id="created_dag",
            run_id="queued_run",
            logical_date=DEFAULT_DATE,
            state=DagRunState.QUEUED,
            run_type=DagRunType.ASSET_TRIGGERED,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
        )
        test_asset_events[0].created_dagruns.append(created_dagrun)
        session.commit()

        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": "test_get_asset_by_name", "uri": None},
        )

        assert response.status_code == 200
        assert response.json()["asset_events"][0]["created_dagruns"][0]["start_date"] is None

    @pytest.mark.parametrize(
        ("uri", "name"),
        [
            (None, "test_get_asset_by_name"),
            ("s3://bucket/key", None),
            ("s3://bucket/key", "test_get_asset_by_name"),
        ],
    )
    @pytest.mark.usefixtures("test_asset", "test_asset_events")
    def test_get_by_asset(self, uri, name, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": name, "uri": uri},
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 1,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "created_dagruns": [],
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "timestamp": "2021-01-01T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 2,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-02T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 3,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-03T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }

    @pytest.mark.parametrize(
        ("uri", "name"),
        [
            (None, "test_get_asset_by_name"),
            ("s3://bucket/key", None),
            ("s3://bucket/key", "test_get_asset_by_name"),
        ],
    )
    @pytest.mark.usefixtures("test_asset", "test_asset_events")
    def test_get_by_asset_with_after_filter(self, uri, name, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": name, "uri": uri, "after": "2021-01-02T00:00:00Z"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 2,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-02T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 3,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-03T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }

    @pytest.mark.parametrize(
        ("uri", "name"),
        [
            (None, "test_get_asset_by_name"),
            ("s3://bucket/key", None),
            ("s3://bucket/key", "test_get_asset_by_name"),
        ],
    )
    @pytest.mark.usefixtures("test_asset", "test_asset_events")
    def test_get_by_asset_with_before_filter(self, uri, name, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": name, "uri": uri, "before": "2021-01-02T00:00:00Z"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 1,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-01T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 2,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-02T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }

    @pytest.mark.parametrize(
        ("uri", "name"),
        [
            (None, "test_get_asset_by_name"),
            ("s3://bucket/key", None),
            ("s3://bucket/key", "test_get_asset_by_name"),
        ],
    )
    @pytest.mark.usefixtures("test_asset", "test_asset_events")
    def test_get_by_asset_with_before_and_after_filters(self, uri, name, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": name,
                "uri": uri,
                "before": "2021-01-02T12:00:00Z",
                "after": "2021-01-01T12:00:00Z",
            },
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 2,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-02T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }

    @pytest.mark.parametrize(
        ("uri", "name"),
        [
            (None, "test_get_asset_by_name"),
            ("s3://bucket/key", None),
            ("s3://bucket/key", "test_get_asset_by_name"),
        ],
    )
    @pytest.mark.usefixtures("test_asset", "test_asset_events")
    def test_get_by_asset_with_descending_order(self, uri, name, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": name, "uri": uri, "ascending": False},
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 3,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-03T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 2,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-02T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 1,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-01T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }

    @pytest.mark.parametrize(
        ("uri", "name"),
        [
            (None, "test_get_asset_by_name"),
            ("s3://bucket/key", None),
            ("s3://bucket/key", "test_get_asset_by_name"),
        ],
    )
    @pytest.mark.usefixtures("test_asset", "test_asset_events")
    def test_get_by_asset_get_first(self, uri, name, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": name, "uri": uri, "limit": 1},
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 1,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-01T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }

    @pytest.mark.parametrize(
        ("uri", "name"),
        [
            (None, "test_get_asset_by_name"),
            ("s3://bucket/key", None),
            ("s3://bucket/key", "test_get_asset_by_name"),
        ],
    )
    @pytest.mark.usefixtures("test_asset", "test_asset_events")
    def test_get_by_asset_get_last(self, uri, name, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": name, "uri": uri, "limit": 1, "ascending": False},
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 3,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-03T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }


class TestGetAssetEventByAssetAlias:
    @pytest.mark.usefixtures("test_asset_alias")
    def test_get_by_asset(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "test_alias"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "asset_events": [
                {
                    "id": 1,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-01T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 2,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-02T00:00:00Z",
                    "partition_key": None,
                },
                {
                    "id": 3,
                    "extra": {"foo": "bar"},
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "created_dagruns": [],
                    "timestamp": "2021-01-03T00:00:00Z",
                    "partition_key": None,
                },
            ]
        }


class TestGetAssetEventByAssetPartitionKey:
    """Tests for partition_key_regexp_pattern regex filter on execution API.

    Patterns are written to work consistently across PostgreSQL (~),
    MySQL (REGEXP), and SQLite (re.match). They are not necessarily
    ^-anchored; some cases use explicit prefixes such as ``.*`` to
    achieve SQLite ``re.match``-compatible behavior.
    """

    @pytest.fixture(autouse=True)
    def _enable_regexp_query_filters(self):
        with conf_vars({("api", "regexp_query_timeout"): "30"}):
            yield

    @pytest.fixture
    def test_partitioned_events(self, session, test_asset):
        def make_timestamp(day):
            return datetime(2021, 1, day, tzinfo=timezone.utc)

        common = {
            "asset_id": 1,
            "extra": {"foo": "bar"},
            "source_dag_id": "foo",
            "source_task_id": "bar",
            "source_run_id": "custom",
            "source_map_index": -1,
        }

        events = [
            AssetEvent(id=10, timestamp=make_timestamp(1), partition_key="2024-01-01", **common),
            AssetEvent(id=11, timestamp=make_timestamp(2), partition_key="2024-01-02", **common),
            AssetEvent(id=12, timestamp=make_timestamp(3), partition_key="us|2024-01-01", **common),
            AssetEvent(id=13, timestamp=make_timestamp(4), partition_key="eu|2024-01-01", **common),
            AssetEvent(id=14, timestamp=make_timestamp(5), partition_key="apac|2024-03-20", **common),
            AssetEvent(id=15, timestamp=make_timestamp(6), partition_key=None, **common),
        ]
        session.add_all(events)
        session.commit()
        yield events

        for event in events:
            session.delete(event)
        session.commit()

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_with_exact_partition_key(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key_regexp_pattern": "^2024-01-01$",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 1
        assert data["asset_events"][0]["partition_key"] == "2024-01-01"

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_with_prefix_partition_key(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key_regexp_pattern": "^2024-01-",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 2

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_with_composite_partition_key_multiple_regions(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key_regexp_pattern": r"^(us|eu)\|2024-01-.*",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 2
        keys = {e["partition_key"] for e in data["asset_events"]}
        assert keys == {"us|2024-01-01", "eu|2024-01-01"}

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_with_composite_partition_key_date_across_regions(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key_regexp_pattern": r".*\|2024-01-01$",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 2
        keys = {e["partition_key"] for e in data["asset_events"]}
        assert keys == {"us|2024-01-01", "eu|2024-01-01"}

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_with_partition_key_no_match(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key_regexp_pattern": "^2025-",
            },
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 0

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_without_partition_key_returns_all(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": "test_get_asset_by_name", "uri": None},
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 6

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_with_partition_key_and_time_range(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key_regexp_pattern": "^2024-01-",
                "after": "2021-01-01T12:00:00Z",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 1
        assert data["asset_events"][0]["partition_key"] == "2024-01-02"

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_with_invalid_regex_returns_400(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key_regexp_pattern": "[invalid(regex",
            },
        )
        assert response.status_code == 400
        assert "Invalid regular expression" in response.json()["detail"]

    def test_get_by_asset_with_pattern_disabled_returns_400(self, client):
        with conf_vars({("api", "regexp_query_timeout"): "0"}):
            response = client.get(
                "/execution/asset-events/by-asset",
                params={
                    "name": "test_get_asset_by_name",
                    "uri": None,
                    "partition_key_regexp_pattern": "^2021-",
                },
            )
        assert response.status_code == 400
        assert "disabled" in response.json()["detail"]

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    @pytest.mark.parametrize(
        ("partition_key", "expected_keys"),
        [
            ("2024-01-01", ["2024-01-01"]),
            ("us|2024-01-01", ["us|2024-01-01"]),
            ("nonexistent", []),
        ],
    )
    def test_get_by_asset_exact_partition_key(self, client, partition_key, expected_keys):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key": partition_key,
            },
        )
        assert response.status_code == 200
        keys = [event["partition_key"] for event in response.json()["asset_events"]]
        assert keys == expected_keys

    @pytest.mark.usefixtures("test_asset", "test_partitioned_events")
    def test_get_by_asset_partition_key_and_pattern_combined(self, client):
        # Both filters are allowed and combine with AND: a disjoint pair yields no results.
        response = client.get(
            "/execution/asset-events/by-asset",
            params={
                "name": "test_get_asset_by_name",
                "uri": None,
                "partition_key": "2024-01-01",
                "partition_key_regexp_pattern": "^2025-",
            },
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 0


class TestGetAssetEventByAssetAliasPartitionKey:
    """Tests for partition_key_regexp_pattern regex filter on by-asset-alias endpoint.

    All patterns use ^-anchored regex so they work consistently across
    PostgreSQL (~), MySQL (REGEXP), and SQLite (re.match).
    """

    @pytest.fixture(autouse=True)
    def _enable_regexp_query_filters(self):
        with conf_vars({("api", "regexp_query_timeout"): "30"}):
            yield

    @pytest.fixture
    def test_partitioned_alias_events(self, session, test_asset):
        def make_timestamp(day):
            return datetime(2021, 1, day, tzinfo=timezone.utc)

        common = {
            "asset_id": 1,
            "extra": {"foo": "bar"},
            "source_dag_id": "foo",
            "source_task_id": "bar",
            "source_run_id": "custom",
            "source_map_index": -1,
        }

        events = [
            AssetEvent(id=20, timestamp=make_timestamp(1), partition_key="us|2024-01-01", **common),
            AssetEvent(id=21, timestamp=make_timestamp(2), partition_key="eu|2024-01-01", **common),
            AssetEvent(id=22, timestamp=make_timestamp(3), partition_key=None, **common),
        ]
        session.add_all(events)
        session.commit()

        alias = AssetAliasModel(id=10, name="partitioned_alias")
        alias.asset_events = events
        alias.assets.append(test_asset)
        session.add(alias)
        session.commit()

        yield events, alias

        session.delete(alias)
        for event in events:
            session.delete(event)
        session.commit()

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_with_partition_key_region(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "partitioned_alias", "partition_key_regexp_pattern": r"^us\|.*"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 1
        assert data["asset_events"][0]["partition_key"] == "us|2024-01-01"

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_with_partition_key_all_regions(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "partitioned_alias", "partition_key_regexp_pattern": r".*\|2024-01-01$"},
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 2

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_without_partition_key_returns_all(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "partitioned_alias"},
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 3

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_with_invalid_regex_returns_400(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "partitioned_alias", "partition_key_regexp_pattern": "[invalid(regex"},
        )
        assert response.status_code == 400
        assert "Invalid regular expression" in response.json()["detail"]

    def test_get_by_alias_with_pattern_disabled_returns_400(self, client):
        with conf_vars({("api", "regexp_query_timeout"): "0"}):
            response = client.get(
                "/execution/asset-events/by-asset-alias",
                params={"name": "partitioned_alias", "partition_key_regexp_pattern": "^us"},
            )
        assert response.status_code == 400
        assert "disabled" in response.json()["detail"]

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_with_exact_partition_key_regex(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "partitioned_alias", "partition_key_regexp_pattern": r"^us\|2024-01-01$"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 1
        assert data["asset_events"][0]["partition_key"] == "us|2024-01-01"

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_exact_partition_key(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "partitioned_alias", "partition_key": "us|2024-01-01"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 1
        assert data["asset_events"][0]["partition_key"] == "us|2024-01-01"

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_exact_partition_key_no_match(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "partitioned_alias", "partition_key": "nonexistent"},
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 0

    @pytest.mark.usefixtures("test_asset", "test_partitioned_alias_events")
    def test_get_by_alias_partition_key_and_pattern_combined(self, client):
        # Both filters are allowed and combine with AND.
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={
                "name": "partitioned_alias",
                "partition_key": "us|2024-01-01",
                "partition_key_regexp_pattern": r"^us\|",
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 1
        assert data["asset_events"][0]["partition_key"] == "us|2024-01-01"


class TestGetAssetEventByAssetExtraFilter:
    @pytest.fixture
    def test_events_with_extra(self, session):
        asset = AssetModel(
            id=1,
            name="test_asset",
            uri="s3://bucket/key",
            group="asset",
            extra={},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
        )
        asset_active = AssetActive.for_asset(asset)
        session.add_all([asset, asset_active])
        session.flush()

        events = [
            AssetEvent(
                id=1,
                asset_id=1,
                extra={"region": "us", "env": "prod"},
                source_dag_id="d1",
                source_task_id="t1",
                source_run_id="r1",
                source_map_index=-1,
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                id=2,
                asset_id=1,
                extra={"region": "eu", "env": "prod"},
                source_dag_id="d1",
                source_task_id="t1",
                source_run_id="r2",
                source_map_index=-1,
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                id=3,
                asset_id=1,
                extra={"region": "us", "env": "staging"},
                source_dag_id="d1",
                source_task_id="t1",
                source_run_id="r3",
                source_map_index=-1,
                timestamp=DEFAULT_DATE,
            ),
        ]
        session.add_all(events)
        session.commit()
        yield events
        for e in events:
            session.delete(e)
        session.delete(asset_active)
        session.delete(asset)
        session.commit()

    @pytest.mark.usefixtures("test_events_with_extra")
    def test_filter_by_extra_key_value(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params=[("name", "test_asset"), ("uri", "s3://bucket/key"), ("extra", "region=us")],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 2
        assert all(e["extra"]["region"] == "us" for e in data["asset_events"])

    @pytest.mark.usefixtures("test_events_with_extra")
    def test_filter_by_extra_env(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params=[("name", "test_asset"), ("uri", "s3://bucket/key"), ("extra", "env=prod")],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 2

    @pytest.mark.usefixtures("test_events_with_extra")
    def test_no_extra_filter_returns_all(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params={"name": "test_asset", "uri": "s3://bucket/key"},
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 3

    @pytest.mark.usefixtures("test_events_with_extra")
    def test_filter_nonexistent_key(self, client):
        response = client.get(
            "/execution/asset-events/by-asset",
            params=[("name", "test_asset"), ("uri", "s3://bucket/key"), ("extra", "missing=val")],
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 0

    @pytest.mark.usefixtures("test_events_with_extra")
    @pytest.mark.parametrize(
        ("extra_params", "expected_count"),
        [
            ([("extra", "region=us"), ("extra", "env=prod")], 1),
            ([("extra", "region=eu"), ("extra", "env=prod")], 1),
            ([("extra", "region=us"), ("extra", "env=staging")], 1),
            ([("extra", "region=eu"), ("extra", "env=staging")], 0),
        ],
    )
    def test_filter_multiple_extra_keys(self, client, extra_params, expected_count):
        response = client.get(
            "/execution/asset-events/by-asset",
            params=[("name", "test_asset"), ("uri", "s3://bucket/key"), *extra_params],
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == expected_count


class TestGetAssetEventByAssetAliasExtraFilter:
    @pytest.fixture
    def test_alias_events_with_extra(self, session):
        asset = AssetModel(
            id=1,
            name="test_asset",
            uri="s3://bucket/key",
            group="asset",
            extra={},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
        )
        asset_active = AssetActive.for_asset(asset)
        session.add_all([asset, asset_active])
        session.flush()

        events = [
            AssetEvent(
                id=1,
                asset_id=1,
                extra={"tier": "gold", "region": "us"},
                source_dag_id="d1",
                source_task_id="t1",
                source_run_id="r1",
                source_map_index=-1,
                timestamp=DEFAULT_DATE,
            ),
            AssetEvent(
                id=2,
                asset_id=1,
                extra={"tier": "silver", "region": "eu"},
                source_dag_id="d1",
                source_task_id="t1",
                source_run_id="r2",
                source_map_index=-1,
                timestamp=DEFAULT_DATE,
            ),
        ]
        session.add_all(events)
        session.flush()

        alias = AssetAliasModel(id=1, name="test_alias")
        alias.asset_events = events
        alias.assets.append(asset)
        session.add(alias)
        session.commit()
        yield events
        session.delete(alias)
        for e in events:
            session.delete(e)
        session.delete(asset_active)
        session.delete(asset)
        session.commit()

    @pytest.mark.usefixtures("test_alias_events_with_extra")
    def test_filter_by_extra_key_value(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params=[("name", "test_alias"), ("extra", "tier=gold")],
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["asset_events"]) == 1
        assert data["asset_events"][0]["extra"]["tier"] == "gold"

    @pytest.mark.usefixtures("test_alias_events_with_extra")
    def test_no_extra_filter_returns_all(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params={"name": "test_alias"},
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == 2

    @pytest.mark.usefixtures("test_alias_events_with_extra")
    @pytest.mark.parametrize(
        ("extra_params", "expected_count"),
        [
            ([("extra", "tier=gold"), ("extra", "region=us")], 1),
            ([("extra", "tier=gold"), ("extra", "region=eu")], 0),
            ([("extra", "tier=silver"), ("extra", "region=eu")], 1),
        ],
    )
    def test_filter_multiple_extra_keys(self, client, extra_params, expected_count):
        response = client.get(
            "/execution/asset-events/by-asset-alias",
            params=[("name", "test_alias"), *extra_params],
        )
        assert response.status_code == 200
        assert len(response.json()["asset_events"]) == expected_count
