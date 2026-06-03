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

import json
from typing import TYPE_CHECKING

import pytest
from fastapi import Request
from sqlalchemy import delete, select

from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.models.asset import AssetActive, AssetModel
from airflow.models.asset_store import AssetStoreModel
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance

    from tests_common.pytest_plugin import CreateTaskInstance

pytestmark = pytest.mark.db_test

_BY_NAME_VALUE = "/execution/store/asset/by-name/value"


def _create_asset_store_row(asset_id: int, key: str, value: str) -> None:
    """Insert an AssetStoreModel row directly in the database."""
    with create_session() as s:
        s.add(AssetStoreModel(asset_id=asset_id, key=key, value=json.dumps(value)))


_BY_NAME_CLEAR = "/execution/store/asset/by-name/clear"
_BY_URI_VALUE = "/execution/store/asset/by-uri/value"
_BY_URI_CLEAR = "/execution/store/asset/by-uri/clear"


@pytest.fixture(autouse=True)
def reset_state_tables():
    with create_session() as session:
        session.execute(delete(AssetStoreModel))
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
        _create_asset_store_row(asset.id, "watermark", "2026-04-29")

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

        _create_asset_store_row(slashed.id, "wm", "x")
        response = client.get(_BY_NAME_VALUE, params={"name": slashed.name, "key": "wm"})

        assert response.status_code == 200
        assert response.json() == {"value": "x"}

    def test_get_unknown_asset_returns_404(self, client: TestClient):
        response = client.get(_BY_NAME_VALUE, params={"name": "nonexistent", "key": "wm"})

        assert response.status_code == 404


class TestPutAssetStateByName:
    @pytest.fixture(autouse=True)
    def setup_ti_auth(
        self,
        exec_app: FastAPI,
        create_task_instance: CreateTaskInstance,
    ):
        """Create a real TI and wire the mock auth to use its UUID so the route can look up writer info."""
        self._ti: TaskInstance = create_task_instance()

        async def _auth(request: Request) -> TIToken:
            return TIToken(id=self._ti.id, claims=TIClaims(scope="execution"))

        exec_app.dependency_overrides[require_auth] = _auth

    def test_put_creates_row(self, client: TestClient, asset: AssetModel, session: Session):
        response = client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        assert response.status_code == 204
        row = session.scalar(
            select(AssetStoreModel).where(
                AssetStoreModel.asset_id == asset.id,
                AssetStoreModel.key == "watermark",
            )
        )
        assert row is not None
        # DB stores JSON-encoded string
        assert row.value == '"2026-04-29"'

    def test_put_records_writer(self, client: TestClient, asset: AssetModel, session: Session):
        """PUT writes about writer fields resolved from the JWT token's TI."""
        response = client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": "v"}
        )
        assert response.status_code == 204

        row = session.scalar(
            select(AssetStoreModel).where(
                AssetStoreModel.asset_id == asset.id,
                AssetStoreModel.key == "watermark",
            )
        )
        assert row is not None
        assert row.last_updated_by_kind == "task"
        assert row.last_updated_by_dag_id == self._ti.dag_id
        assert row.last_updated_by_run_id == self._ti.run_id
        assert row.last_updated_by_task_id == self._ti.task_id
        assert row.last_updated_by_map_index == self._ti.map_index

    def test_put_int_value_roundtrip(self, client: TestClient, asset: AssetModel):
        response = client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "total_runs"}, json={"value": 5}
        )
        assert response.status_code == 204
        assert client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "total_runs"}).json() == {
            "value": 5
        }

    def test_put_dict_value_roundtrip(self, client: TestClient, asset: AssetModel):
        response = client.put(
            _BY_NAME_VALUE,
            params={"name": asset.name, "key": "last_run"},
            json={"value": {"rows": 1234, "status": "ok"}},
        )
        assert response.status_code == 204
        assert client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "last_run"}).json() == {
            "value": {"rows": 1234, "status": "ok"}
        }

    def test_put_list_value_roundtrip(self, client: TestClient, asset: AssetModel):
        response = client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "ids"}, json={"value": [1, 2, 3]}
        )
        assert response.status_code == 204
        assert client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "ids"}).json() == {
            "value": [1, 2, 3]
        }

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

    def test_put_null_value_returns_422(self, client: TestClient, asset: AssetModel):
        response = client.put(
            _BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}, json={"value": None}
        )
        assert response.status_code == 422

    @pytest.mark.parametrize("bad_float", [float("nan"), float("inf"), float("-inf")])
    def test_put_non_finite_float_returns_422(self, client: TestClient, asset: AssetModel, bad_float: float):
        with pytest.raises(ValueError, match="Out of range float values are not JSON compliant"):
            _ = client.put(
                _BY_NAME_VALUE,
                params={"name": asset.name, "key": "watermark"},
                content=json.dumps({"value": bad_float}, allow_nan=True).encode(),
                headers={"Content-Type": "application/json"},
            )

    def test_put_unknown_asset_returns_404(self, client: TestClient):
        response = client.put(
            _BY_NAME_VALUE, params={"name": "nonexistent", "key": "watermark"}, json={"value": "x"}
        )

        assert response.status_code == 404


class TestDeleteAssetStateByName:
    def test_delete_removes_key(self, client: TestClient, asset: AssetModel):
        _create_asset_store_row(asset.id, "watermark", "2026-04-29")

        response = client.delete(_BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"})

        assert response.status_code == 204
        assert client.get(_BY_NAME_VALUE, params={"name": asset.name, "key": "watermark"}).status_code == 404

    def test_delete_missing_key_is_noop(self, client: TestClient, asset: AssetModel):
        response = client.delete(_BY_NAME_VALUE, params={"name": asset.name, "key": "never_existed"})

        assert response.status_code == 204


class TestClearAssetStateByName:
    def test_clear_removes_all_keys(self, client: TestClient, asset: AssetModel):
        for k, v in [("watermark", "a"), ("last_id", "b"), ("schema_hash", "c")]:
            _create_asset_store_row(asset.id, k, v)

        response = client.delete(_BY_NAME_CLEAR, params={"name": asset.name})

        assert response.status_code == 204
        with create_session() as session:
            row = session.scalar(select(AssetStoreModel).where(AssetStoreModel.asset_id == asset.id))
            assert row is None


class TestGetAssetStateByUri:
    def test_get_returns_value(self, client: TestClient, asset: AssetModel):
        _create_asset_store_row(asset.id, "watermark", "2026-04-29")

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
    @pytest.fixture(autouse=True)
    def setup_ti_auth(
        self,
        exec_app: FastAPI,
        create_task_instance: CreateTaskInstance,
    ):
        """Create a real TI and wire the mock auth to use its UUID so the route can look up writer info."""
        self._ti: TaskInstance = create_task_instance()

        async def _auth(request: Request) -> TIToken:
            return TIToken(id=self._ti.id, claims=TIClaims(scope="execution"))

        exec_app.dependency_overrides[require_auth] = _auth

    def test_put_creates_row(self, client: TestClient, asset: AssetModel, session: Session):
        response = client.put(
            _BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"}, json={"value": "2026-04-29"}
        )

        assert response.status_code == 204
        row = session.scalar(
            select(AssetStoreModel).where(
                AssetStoreModel.asset_id == asset.id,
                AssetStoreModel.key == "watermark",
            )
        )
        assert row is not None
        assert row.value == '"2026-04-29"'

    def test_put_records_writer(self, client: TestClient, asset: AssetModel, session: Session):
        """PUT writes writer fields resolved from the JWT token's TI."""
        response = client.put(
            _BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"}, json={"value": "v"}
        )
        assert response.status_code == 204

        row = session.scalar(
            select(AssetStoreModel).where(
                AssetStoreModel.asset_id == asset.id,
                AssetStoreModel.key == "watermark",
            )
        )
        assert row is not None
        assert row.last_updated_by_kind == "task"
        assert row.last_updated_by_dag_id == self._ti.dag_id
        assert row.last_updated_by_run_id == self._ti.run_id
        assert row.last_updated_by_task_id == self._ti.task_id
        assert row.last_updated_by_map_index == self._ti.map_index

    def test_put_unknown_uri_returns_404(self, client: TestClient):
        response = client.put(
            _BY_URI_VALUE, params={"uri": "s3://nonexistent/path", "key": "wm"}, json={"value": "x"}
        )

        assert response.status_code == 404


class TestDeleteAssetStateByUri:
    def test_delete_removes_key(self, client: TestClient, asset: AssetModel):
        _create_asset_store_row(asset.id, "watermark", "2026-04-29")

        response = client.delete(_BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"})

        assert response.status_code == 204
        assert client.get(_BY_URI_VALUE, params={"uri": asset.uri, "key": "watermark"}).status_code == 404


class TestClearAssetStateByUri:
    def test_clear_removes_all_keys(self, client: TestClient, asset: AssetModel):
        for k, v in [("watermark", "a"), ("last_id", "b")]:
            _create_asset_store_row(asset.id, k, v)

        response = client.delete(_BY_URI_CLEAR, params={"uri": asset.uri})

        assert response.status_code == 204
        with create_session() as session:
            row = session.scalar(select(AssetStoreModel).where(AssetStoreModel.asset_id == asset.id))
            assert row is None


class TestInactiveAssetRejected:
    """An asset row without a corresponding asset_active entry is treated as not found."""

    def test_get_inactive_asset_by_name_returns_404(self, client: TestClient, inactive_asset: AssetModel):
        response = client.get(_BY_NAME_VALUE, params={"name": inactive_asset.name, "key": "watermark"})
        assert response.status_code == 404

    def test_get_inactive_asset_by_uri_returns_404(self, client: TestClient, inactive_asset: AssetModel):
        response = client.get(_BY_URI_VALUE, params={"uri": inactive_asset.uri, "key": "watermark"})
        assert response.status_code == 404
