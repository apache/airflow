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

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, select

from airflow.models.asset import AssetActive, AssetModel
from airflow.models.asset_state import AssetStateModel
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

_BY_NAME_VALUE = "/execution/state/asset/by-name/value"
_BY_NAME_CLEAR = "/execution/state/asset/by-name/clear"
_BY_URI_VALUE = "/execution/state/asset/by-uri/value"
_BY_URI_CLEAR = "/execution/state/asset/by-uri/clear"


@pytest.fixture(autouse=True)
def reset_state_tables():
    with create_session() as session:
        session.execute(delete(AssetStateModel))
        session.execute(delete(AssetModel))


@pytest.fixture
def asset(session: Session) -> AssetModel:
    asset = AssetModel(name="test_asset", uri="s3://bucket/test", group="asset")
    session.add(asset)
    session.flush()
    session.add(AssetActive.for_asset(asset))
    session.commit()
    return asset


@pytest.fixture
def inactive_asset(session: Session) -> AssetModel:
    """An asset row with no asset_active entry — simulates a removed asset."""
    asset = AssetModel(name="inactive_asset", uri="s3://bucket/inactive", group="asset")
    session.add(asset)
    session.commit()
    return asset


class TestGetAssetStateByName:
    def test_get_returns_value(self, client: TestClient, asset: AssetModel):
        client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        response = client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"})

        assert response.status_code == 200
        assert response.json() == {"value": "2026-04-29"}

    def test_get_missing_key_returns_404(self, client: TestClient, asset: AssetModel):
        response = client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "never_set"})

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"

    def test_get_asset_name_with_slashes(self, client: TestClient, session):
        slashed = AssetModel(name="team/sales/orders", uri="s3://bucket/team/sales", group="asset")
        session.add(slashed)
        session.flush()
        session.add(AssetActive.for_asset(slashed))
        session.commit()

        client.put(_BY_NAME_VALUE, params={"name": slashed.name, "key": "wm"}, json={"value": "x"})
        response = client.get(_BY_NAME_VALUE, params={"name": slashed.name, "key": "wm"})

        assert response.status_code == 200
        assert response.json() == {"value": "x"}

    def test_get_unknown_asset_returns_404(self, client: TestClient):
        response = client.get(_BY_NAME_VALUE, params={"name": "nonexistent", "key": "wm"})

        assert response.status_code == 404


class TestPutAssetStateByName:
    def test_put_creates_row(self, client: TestClient, asset: AssetModel, session: Session):
        response = client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        assert response.status_code == 204
        row = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == asset.id,
                AssetStateModel.key == "watermark",
            )
        )
        assert row is not None
        assert row.value == "2026-04-29"

    def test_put_overwrites_existing(self, client: TestClient, asset: AssetModel):
        client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "2026-04-28"}
        )

        response = client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        assert response.status_code == 204
        assert client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}).json() == {
            "value": "2026-04-29"
        }

    def test_put_empty_body_returns_422(self, client: TestClient, asset: AssetModel):
        response = client.put(_BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={})

        assert response.status_code == 422

    def test_put_unknown_asset_returns_404(self, client: TestClient):
        response = client.put(
            _BY_NAME_VALUE, params={"name": "nonexistent", "key": "watermark"}, json={"value": "x"}
        )

        assert response.status_code == 404


class TestDeleteAssetStateByName:
    def test_delete_removes_key(self, client: TestClient, asset: AssetModel):
        client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        response = client.delete(_BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"})

        assert response.status_code == 204
        assert client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}).status_code == 404

    def test_delete_missing_key_is_noop(self, client: TestClient, asset: AssetModel):
        response = client.delete(_BY_NAME_VALUE, params={"name": asset.name, "key": "never_existed"})

        assert response.status_code == 204


class TestClearAssetStateByName:
    def test_clear_removes_all_keys(self, client: TestClient, asset: AssetModel):
        for k, v in [("watermark", "a"), ("last_id", "b"), ("schema_hash", "c")]:
            client.put(_BY_NAME_VALUE, params={"name": asset.name, "key": k}, json={"value": v})

        response = client.delete(_BY_NAME_CLEAR, params={"name": asset.name})

        assert response.status_code == 204
        with create_session() as session:
            row = session.scalar(select(AssetStateModel).where(AssetStateModel.asset_id == asset.id))
            assert row is None


class TestGetAssetStateByUri:
    def test_get_returns_value(self, client: TestClient, asset: AssetModel):
        client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        response = client.get(_BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"})

        assert response.status_code == 200
        assert response.json() == {"value": "2026-04-29"}

    def test_get_missing_key_returns_404(self, client: TestClient, asset: AssetModel):
        response = client.get(_BY_URI_VALUE, params={"uri": asset.uri, "key": "never_set"})

        assert response.status_code == 404

    def test_get_unknown_uri_returns_404(self, client: TestClient):
        response = client.get(_BY_URI_VALUE, params={"uri": "s3://nonexistent/path", "key": "wm"})

        assert response.status_code == 404


class TestPutAssetStateByUri:
    def test_put_creates_row(self, client: TestClient, asset: AssetModel, session: Session):
        response = client.put(
            _BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        assert response.status_code == 204
        row = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == asset.id,
                AssetStateModel.key == "watermark",
            )
        )
        assert row is not None
        assert row.value == "2026-04-29"

    def test_put_unknown_uri_returns_404(self, client: TestClient):
        response = client.put(
            _BY_URI_VALUE, params={"uri": "s3://nonexistent/path", "key": "wm"}, json={"value": "x"}
        )

        assert response.status_code == 404


class TestDeleteAssetStateByUri:
    def test_delete_removes_key(self, client: TestClient, asset: AssetModel):
        client.put(_BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"}, json={"value": "2026-04-29"})

        response = client.delete(_BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"})

        assert response.status_code == 204
        assert client.get(_BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"}).status_code == 404


class TestClearAssetStateByUri:
    def test_clear_removes_all_keys(self, client: TestClient, asset: AssetModel):
        for k, v in [("watermark", "a"), ("last_id", "b")]:
            client.put(_BY_URI_VALUE, params={"uri": asset.uri, "key": k}, json={"value": v})

        response = client.delete(_BY_URI_CLEAR, params={"uri": asset.uri})

        assert response.status_code == 204
        with create_session() as session:
            row = session.scalar(select(AssetStateModel).where(AssetStateModel.asset_id == asset.id))
            assert row is None


class TestInactiveAssetRejected:
    """An asset row without a corresponding asset_active entry is treated as not found."""

    def test_get_inactive_asset_by_name_returns_404(self, client: TestClient, inactive_asset: AssetModel):
        response = client.get(_BY_NAME_VALUE, params={"name": inactive_asset.name, "key": "watermark"})
        assert response.status_code == 404

    def test_get_inactive_asset_by_uri_returns_404(self, client: TestClient, inactive_asset: AssetModel):
        response = client.get(_BY_URI_VALUE, params={"uri": inactive_asset.uri, "key": "watermark"})
        assert response.status_code == 404
