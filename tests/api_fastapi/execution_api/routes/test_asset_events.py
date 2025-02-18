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

from airflow.models.asset import AssetActive, AssetAliasModel, AssetEvent, AssetModel
from airflow.utils import timezone

DEFAULT_DATE = timezone.parse("2021-01-01T00:00:00")

pytestmark = pytest.mark.db_test


class TestGetAssetByName:
    def test_get_asset_event_by_name_uri(self, client, session):
        asset = AssetModel(
            id=1,
            name="test_get_asset_by_name",
            uri="s3://bucket/key",
            group="asset",
            extra={"foo": "bar"},
            created_at=DEFAULT_DATE,
            updated_at=DEFAULT_DATE,
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
                    "timestamp": "2021-01-01T00:00:00Z",
                },
            ]
        }


class TestGetAssetEventByAssetAlias:
    @pytest.mark.usefixtures("test_asset_alias")
    def test_get_by_asset(self, client):
        response = client.get(
            "/execution/asset-events/by-asset-name-uri",
            params={"name": "test_get_asset_by_name", "uri": "s3://bucket/key"},
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
                },
                {
                    "id": 2,
                    "asset_id": 1,
                    "uri": "s3://bucket/key",
                    "name": "test_get_asset_by_name",
                    "group": "asset",
                    "extra": {"foo": "bar"},
                    "asset": {
                        "extra": {"foo": "bar"},
                        "group": "asset",
                        "name": "test_get_asset_by_name",
                        "uri": "s3://bucket/key",
                    },
                    "source_task_id": "bar",
                    "source_dag_id": "foo",
                    "source_run_id": "custom",
                    "source_map_index": -1,
                    "created_dagruns": [],
                    "timestamp": "2021-01-01T00:00:00Z",
                },
            ]
        }
