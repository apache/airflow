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

from airflow.models.asset import AssetActive, AssetModel
from airflow.utils import timezone

DEFAULT_DATE = timezone.parse("2021-01-01T00:00:00")

pytestmark = pytest.mark.db_test


class TestGetAssetByName:
    def test_get_asset_by_name(self, client, session):
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

        response = client.get("/execution/assets/by-name", params={"name": "test_get_asset_by_name"})

        assert response.status_code == 200
        assert response.json() == {
            "name": "test_get_asset_by_name",
            "uri": "s3://bucket/key",
            "group": "asset",
            "extra": {"foo": "bar"},
        }

        session.delete(asset)
        session.delete(asset_active)
        session.commit()

    def test_asset_name_not_found(self, client):
        response = client.get("/execution/assets/by-name", params={"name": "non_existent"})

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Asset with name non_existent not found",
                "reason": "not_found",
            }
        }


class TestGetAssetByUri:
    def test_get_asset_by_uri(self, client, session):
        asset = AssetModel(
            name="test_get_asset_by_uri",
            uri="s3://bucket/key",
            group="asset",
            extra={"foo": "bar"},
        )

        asset_active = AssetActive.for_asset(asset)

        session.add_all([asset, asset_active])
        session.commit()

        response = client.get("/execution/assets/by-uri", params={"uri": "s3://bucket/key"})

        assert response.status_code == 200
        assert response.json() == {
            "name": "test_get_asset_by_uri",
            "uri": "s3://bucket/key",
            "group": "asset",
            "extra": {"foo": "bar"},
        }

        session.delete(asset)
        session.delete(asset_active)
        session.commit()

    def test_asset_uri_not_found(self, client):
        response = client.get("/execution/assets/by-uri", params={"uri": "non_existent"})

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Asset with URI non_existent not found",
                "reason": "not_found",
            }
        }
