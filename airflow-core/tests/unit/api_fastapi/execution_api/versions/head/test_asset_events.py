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
from airflow.models.asset import AssetActive, AssetAliasModel, AssetEvent, AssetModel

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
