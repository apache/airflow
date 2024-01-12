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

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models.pool import Pool
from airflow.security import permissions
from airflow.utils.session import provide_session
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools

pytestmark = pytest.mark.db_test


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_POOL),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_POOL),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_POOL),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestBasePoolEndpoints:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_pools()

    def teardown_method(self) -> None:
        clear_db_pools()


class TestGetPools(TestBasePoolEndpoints):
    def test_response_200(self, session):
        pool_model = Pool(pool="test_pool_a", slots=3, include_deferred=True)
        session.add(pool_model)
        session.commit()
        result = session.query(Pool).all()
        assert len(result) == 2  # accounts for the default pool as well
        response = self.client.get("/api/v1/pools", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert {
            "pools": [
                {
                    "name": "default_pool",
                    "slots": 128,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "open_slots": 128,
                    "description": "Default pool",
                    "include_deferred": False,
                },
                {
                    "name": "test_pool_a",
                    "slots": 3,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "open_slots": 3,
                    "description": None,
                    "include_deferred": True,
                },
            ],
            "total_entries": 2,
        } == response.json

    def test_response_200_with_order_by(self, session):
        pool_model = Pool(pool="test_pool_a", slots=3, include_deferred=True)
        session.add(pool_model)
        session.commit()
        result = session.query(Pool).all()
        assert len(result) == 2  # accounts for the default pool as well
        response = self.client.get("/api/v1/pools?order_by=slots", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert {
            "pools": [
                {
                    "name": "test_pool_a",
                    "slots": 3,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "open_slots": 3,
                    "description": None,
                    "include_deferred": True,
                },
                {
                    "name": "default_pool",
                    "slots": 128,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "open_slots": 128,
                    "description": "Default pool",
                    "include_deferred": False,
                },
            ],
            "total_entries": 2,
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/pools")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get("/api/v1/pools", environ_overrides={"REMOTE_USER": "test_no_permissions"})
        assert response.status_code == 403


class TestGetPoolsPagination(TestBasePoolEndpoints):
    @pytest.mark.parametrize(
        "url, expected_pool_ids",
        [
            # Offset test data
            ("/api/v1/pools?offset=1", [f"test_pool{i}" for i in range(1, 101)]),
            ("/api/v1/pools?offset=3", [f"test_pool{i}" for i in range(3, 103)]),
            # Limit test data
            ("/api/v1/pools?limit=2", ["default_pool", "test_pool1"]),
            ("/api/v1/pools?limit=1", ["default_pool"]),
            # Limit and offset test data
            (
                "/api/v1/pools?limit=100&offset=1",
                [f"test_pool{i}" for i in range(1, 101)],
            ),
            ("/api/v1/pools?limit=2&offset=1", ["test_pool1", "test_pool2"]),
            (
                "/api/v1/pools?limit=3&offset=2",
                ["test_pool2", "test_pool3", "test_pool4"],
            ),
        ],
    )
    @provide_session
    def test_limit_and_offset(self, url, expected_pool_ids, session):
        pools = [Pool(pool=f"test_pool{i}", slots=1, include_deferred=False) for i in range(1, 121)]
        session.add_all(pools)
        session.commit()
        result = session.query(Pool).count()
        assert result == 121  # accounts for default pool as well
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        pool_ids = [pool["name"] for pool in response.json["pools"]]
        assert pool_ids == expected_pool_ids

    def test_should_respect_page_size_limit_default(self, session):
        pools = [Pool(pool=f"test_pool{i}", slots=1, include_deferred=False) for i in range(1, 121)]
        session.add_all(pools)
        session.commit()
        result = session.query(Pool).count()
        assert result == 121
        response = self.client.get("/api/v1/pools", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert len(response.json["pools"]) == 100

    def test_should_raise_400_for_invalid_orderby(self, session):
        pools = [Pool(pool=f"test_pool{i}", slots=1, include_deferred=False) for i in range(1, 121)]
        session.add_all(pools)
        session.commit()
        result = session.query(Pool).count()
        assert result == 121
        response = self.client.get(
            "/api/v1/pools?order_by=open_slots", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        msg = "Ordering with 'open_slots' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        pools = [Pool(pool=f"test_pool{i}", slots=1, include_deferred=False) for i in range(1, 200)]
        session.add_all(pools)
        session.commit()
        result = session.query(Pool).count()
        assert result == 200
        response = self.client.get("/api/v1/pools?limit=180", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert len(response.json["pools"]) == 150


class TestGetPool(TestBasePoolEndpoints):
    def test_response_200(self, session):
        pool_model = Pool(pool="test_pool_a", slots=3, include_deferred=True)
        session.add(pool_model)
        session.commit()
        response = self.client.get("/api/v1/pools/test_pool_a", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert {
            "name": "test_pool_a",
            "slots": 3,
            "occupied_slots": 0,
            "running_slots": 0,
            "queued_slots": 0,
            "scheduled_slots": 0,
            "deferred_slots": 0,
            "open_slots": 3,
            "description": None,
            "include_deferred": True,
        } == response.json

    def test_response_404(self):
        response = self.client.get("/api/v1/pools/invalid_pool", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 404
        assert {
            "detail": "Pool with name:'invalid_pool' not found",
            "status": 404,
            "title": "Not Found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/pools/default_pool")

        assert_401(response)


class TestDeletePool(TestBasePoolEndpoints):
    def test_response_204(self, session):
        pool_name = "test_pool"
        pool_instance = Pool(pool=pool_name, slots=3, include_deferred=False)
        session.add(pool_instance)
        session.commit()

        response = self.client.delete(f"api/v1/pools/{pool_name}", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 204
        # Check if the pool is deleted from the db
        response = self.client.get(f"api/v1/pools/{pool_name}", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 404

    def test_response_404(self):
        response = self.client.delete("api/v1/pools/invalid_pool", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 404
        assert {
            "detail": "Pool with name:'invalid_pool' not found",
            "status": 404,
            "title": "Not Found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self, session):
        pool_name = "test_pool"
        pool_instance = Pool(pool=pool_name, slots=3, include_deferred=False)
        session.add(pool_instance)
        session.commit()

        response = self.client.delete(f"api/v1/pools/{pool_name}")

        assert_401(response)

        # Should still exists
        response = self.client.get(f"/api/v1/pools/{pool_name}", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200


class TestPostPool(TestBasePoolEndpoints):
    def test_response_200(self):
        response = self.client.post(
            "api/v1/pools",
            json={"name": "test_pool_a", "slots": 3, "description": "test pool", "include_deferred": True},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert {
            "name": "test_pool_a",
            "slots": 3,
            "occupied_slots": 0,
            "running_slots": 0,
            "queued_slots": 0,
            "scheduled_slots": 0,
            "deferred_slots": 0,
            "open_slots": 3,
            "description": "test pool",
            "include_deferred": True,
        } == response.json

    def test_response_409(self, session):
        pool_name = "test_pool_a"
        pool_instance = Pool(pool=pool_name, slots=3, include_deferred=False)
        session.add(pool_instance)
        session.commit()
        response = self.client.post(
            "api/v1/pools",
            json={"name": "test_pool_a", "slots": 3, "include_deferred": False},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 409
        assert {
            "detail": f"Pool: {pool_name} already exists",
            "status": 409,
            "title": "Conflict",
            "type": EXCEPTIONS_LINK_MAP[409],
        } == response.json

    @pytest.mark.parametrize(
        "request_json, error_detail",
        [
            pytest.param(
                {"slots": 3},
                "Missing required property(ies): ['name']",
                id="for missing pool name",
            ),
            pytest.param(
                {"name": "invalid_pool"},
                "Missing required property(ies): ['slots']",
                id="for missing slots",
            ),
            pytest.param(
                {},
                "Missing required property(ies): ['name', 'slots']",
                id="for missing pool name AND slots AND include_deferred",
            ),
            pytest.param(
                {"name": "invalid_pool", "slots": 3, "extra_field_1": "extra"},
                "{'extra_field_1': ['Unknown field.']}",
                id="for extra fields",
            ),
        ],
    )
    def test_response_400(self, request_json, error_detail):
        response = self.client.post(
            "api/v1/pools", json=request_json, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        assert {
            "detail": error_detail,
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        } == response.json

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post("api/v1/pools", json={"name": "test_pool_a", "slots": 3})

        assert_401(response)


class TestPatchPool(TestBasePoolEndpoints):
    def test_response_200(self, session):
        pool = Pool(pool="test_pool", slots=2, include_deferred=True)
        session.add(pool)
        session.commit()
        response = self.client.patch(
            "api/v1/pools/test_pool",
            json={"name": "test_pool_a", "slots": 3, "include_deferred": False},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert {
            "occupied_slots": 0,
            "queued_slots": 0,
            "name": "test_pool_a",
            "open_slots": 3,
            "running_slots": 0,
            "scheduled_slots": 0,
            "deferred_slots": 0,
            "slots": 3,
            "description": None,
            "include_deferred": False,
        } == response.json

    @pytest.mark.parametrize(
        "error_detail, request_json",
        [
            # Missing properties
            ("Missing required property(ies): ['name']", {"slots": 3}),
            ("Missing required property(ies): ['slots']", {"name": "test_pool_a"}),
            ("Missing required property(ies): ['name', 'slots']", {}),
            # Extra properties
            (
                "{'extra_field': ['Unknown field.']}",
                {"name": "test_pool_a", "slots": 3, "include_deferred": True, "extra_field": "extra"},
            ),
        ],
    )
    @provide_session
    def test_response_400(self, error_detail, request_json, session):
        pool = Pool(pool="test_pool", slots=2, include_deferred=False)
        session.add(pool)
        session.commit()
        response = self.client.patch(
            "api/v1/pools/test_pool", json=request_json, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        assert {
            "detail": error_detail,
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        } == response.json

    def test_not_found_when_no_pool_available(self):
        response = self.client.patch(
            "api/v1/pools/test_pool",
            json={"name": "test_pool_a", "slots": 3},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert {
            "detail": "Pool with name:'test_pool' not found",
            "status": 404,
            "title": "Not Found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raises_401_unauthenticated(self, session):
        pool = Pool(pool="test_pool", slots=2, include_deferred=False)
        session.add(pool)
        session.commit()

        response = self.client.patch(
            "api/v1/pools/test_pool",
            json={"name": "test_pool_a", "slots": 3},
        )

        assert_401(response)


class TestModifyDefaultPool(TestBasePoolEndpoints):
    def test_delete_400(self):
        response = self.client.delete("api/v1/pools/default_pool", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 400
        assert {
            "detail": "Default Pool can't be deleted",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        } == response.json

    @pytest.mark.parametrize(
        "status_code, url, json, expected_response",
        [
            pytest.param(
                400,
                "api/v1/pools/default_pool",
                {"name": "test_pool_a", "slots": 3, "include_deferred": False},
                {
                    "detail": "Default Pool's name can't be modified",
                    "status": 400,
                    "title": "Bad Request",
                    "type": EXCEPTIONS_LINK_MAP[400],
                },
                id="400 No update mask",
            ),
            pytest.param(
                400,
                "api/v1/pools/default_pool?update_mask=name, slots",
                {"name": "test_pool_a", "slots": 3, "include_deferred": False},
                {
                    "detail": "Default Pool's name can't be modified",
                    "status": 400,
                    "title": "Bad Request",
                    "type": EXCEPTIONS_LINK_MAP[400],
                },
                id="400 Update mask with both fields",
            ),
            pytest.param(
                200,
                "api/v1/pools/default_pool?update_mask=slots",
                {"name": "test_pool_a", "slots": 3},
                {
                    "occupied_slots": 0,
                    "queued_slots": 0,
                    "name": "default_pool",
                    "open_slots": 3,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "slots": 3,
                    "description": "Default pool",
                    "include_deferred": False,
                },
                id="200 Update mask with slots",
            ),
            pytest.param(
                200,
                "api/v1/pools/default_pool?update_mask=include_deferred",
                {"name": "test_pool_a", "include_deferred": True},
                {
                    "occupied_slots": 0,
                    "queued_slots": 0,
                    "name": "default_pool",
                    "open_slots": 128,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "slots": 128,
                    "description": "Default pool",
                    "include_deferred": True,
                },
                id="200 Update mask with include_deferred",
            ),
            pytest.param(
                200,
                "api/v1/pools/default_pool?update_mask=slots,include_deferred",
                {"name": "test_pool_a", "slots": 3, "include_deferred": True},
                {
                    "occupied_slots": 0,
                    "queued_slots": 0,
                    "name": "default_pool",
                    "open_slots": 3,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "slots": 3,
                    "description": "Default pool",
                    "include_deferred": True,
                },
                id="200 Update mask with slots AND include_deferred",
            ),
            pytest.param(
                200,
                "api/v1/pools/default_pool?update_mask=name,slots",
                {"name": "default_pool", "slots": 3},
                {
                    "occupied_slots": 0,
                    "queued_slots": 0,
                    "name": "default_pool",
                    "open_slots": 3,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "slots": 3,
                    "description": "Default pool",
                    "include_deferred": False,
                },
                id="200 Update mask with slots and name",
            ),
            pytest.param(
                200,
                "api/v1/pools/default_pool",
                {
                    "name": "default_pool",
                    "slots": 3,
                    "include_deferred": True,
                },
                {
                    "occupied_slots": 0,
                    "queued_slots": 0,
                    "name": "default_pool",
                    "open_slots": 3,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "deferred_slots": 0,
                    "slots": 3,
                    "description": "Default pool",
                    "include_deferred": True,
                },
                id="200 no update mask",
            ),
        ],
    )
    def test_patch(self, status_code, url, json, expected_response):
        response = self.client.patch(url, json=json, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == status_code
        assert response.json == expected_response


class TestPatchPoolWithUpdateMask(TestBasePoolEndpoints):
    @pytest.mark.parametrize(
        "url, patch_json, expected_name, expected_slots, expected_include_deferred",
        [
            (
                "api/v1/pools/test_pool?update_mask=name, slots",
                {"name": "test_pool_a", "slots": 2},
                "test_pool_a",
                2,
                False,
            ),
            (
                "api/v1/pools/test_pool?update_mask=name",
                {"name": "test_pool_a", "slots": 2},
                "test_pool_a",
                3,
                False,
            ),
            (
                "api/v1/pools/test_pool?update_mask=slots",
                {"name": "test_pool_a", "slots": 2},
                "test_pool",
                2,
                False,
            ),
            (
                "api/v1/pools/test_pool?update_mask=slots",
                {"slots": 2},
                "test_pool",
                2,
                False,
            ),
            (
                "api/v1/pools/test_pool?update_mask=include_deferred",
                {"include_deferred": True},
                "test_pool",
                3,
                True,
            ),
        ],
    )
    @provide_session
    def test_response_200(
        self, url, patch_json, expected_name, expected_slots, expected_include_deferred, session
    ):
        pool = Pool(pool="test_pool", slots=3, include_deferred=False)
        session.add(pool)
        session.commit()
        response = self.client.patch(url, json=patch_json, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert {
            "name": expected_name,
            "slots": expected_slots,
            "occupied_slots": 0,
            "running_slots": 0,
            "queued_slots": 0,
            "scheduled_slots": 0,
            "deferred_slots": 0,
            "open_slots": expected_slots,
            "description": None,
            "include_deferred": expected_include_deferred,
        } == response.json

    @pytest.mark.parametrize(
        "error_detail, url, patch_json",
        [
            pytest.param(
                "Property is read-only - 'occupied_slots'",
                "api/v1/pools/test_pool?update_mask=slots, name, occupied_slots",
                {"name": "test_pool_a", "slots": 2, "occupied_slots": 1},
                id="Patching read only field",
            ),
            pytest.param(
                "Property is read-only - 'queued_slots'",
                "api/v1/pools/test_pool?update_mask=slots, name, queued_slots",
                {"name": "test_pool_a", "slots": 2, "queued_slots": 1},
                id="Patching read only field",
            ),
            pytest.param(
                "Invalid field: names in update mask",
                "api/v1/pools/test_pool?update_mask=slots, names,",
                {"name": "test_pool_a", "slots": 2},
                id="Invalid update mask",
            ),
            pytest.param(
                "Invalid field: slot in update mask",
                "api/v1/pools/test_pool?update_mask=slot, name,",
                {"name": "test_pool_a", "slots": 2},
                id="Invalid update mask",
            ),
        ],
    )
    @provide_session
    def test_response_400(self, error_detail, url, patch_json, session):
        pool = Pool(pool="test_pool", slots=3, include_deferred=False)
        session.add(pool)
        session.commit()
        response = self.client.patch(url, json=patch_json, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 400
        assert {
            "detail": error_detail,
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        } == response.json
