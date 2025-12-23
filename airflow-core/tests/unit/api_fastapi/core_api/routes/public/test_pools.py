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

from unittest import mock

import pytest
from sqlalchemy import func, select

from airflow.models.pool import Pool
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_pools
from tests_common.test_utils.logs import check_last_log

pytestmark = pytest.mark.db_test

POOL1_NAME = "pool1"
POOL1_SLOT = 3
POOL1_INCLUDE_DEFERRED = True


POOL2_NAME = "pool2"
POOL2_SLOT = 10
POOL2_INCLUDE_DEFERRED = False
POOL2_DESCRIPTION = "Some Description"


POOL3_NAME = "pool3/with_slashes"
POOL3_SLOT = 5
POOL3_INCLUDE_DEFERRED = False
POOL3_DESCRIPTION = "Some Description"


@provide_session
def _create_pools(session) -> None:
    pool1 = Pool(pool=POOL1_NAME, slots=POOL1_SLOT, include_deferred=POOL1_INCLUDE_DEFERRED)
    pool2 = Pool(pool=POOL2_NAME, slots=POOL2_SLOT, include_deferred=POOL2_INCLUDE_DEFERRED)
    pool3 = Pool(
        pool=POOL3_NAME,
        slots=POOL3_SLOT,
        include_deferred=POOL3_INCLUDE_DEFERRED,
        description=POOL3_DESCRIPTION,
    )
    session.add_all([pool1, pool2, pool3])


class TestPoolsEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_pools()

    def teardown_method(self) -> None:
        clear_db_pools()

    def create_pools(self):
        _create_pools()


class TestDeletePool(TestPoolsEndpoint):
    def test_delete_should_respond_204(self, test_client, session):
        self.create_pools()
        pools = session.scalars(select(Pool)).all()
        assert len(pools) == 4
        response = test_client.delete(f"/pools/{POOL1_NAME}")
        assert response.status_code == 204
        pools = session.scalars(select(Pool)).all()
        assert len(pools) == 3
        check_last_log(session, dag_id=None, event="delete_pool", logical_date=None)

    def test_delete_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete(f"/pools/{POOL1_NAME}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete(f"/pools/{POOL1_NAME}")
        assert response.status_code == 403

    def test_delete_should_respond_400(self, test_client):
        response = test_client.delete("/pools/default_pool")
        assert response.status_code == 400
        body = response.json()
        assert body["detail"] == "Default Pool can't be deleted"

    def test_delete_should_respond_404(self, test_client):
        response = test_client.delete(f"/pools/{POOL1_NAME}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Pool with name: `{POOL1_NAME}` was not found" == body["detail"]

    def test_delete_pool3_should_respond_204(self, test_client, session):
        """Test deleting POOL3 with forward slash in name"""
        self.create_pools()
        pools = session.scalars(select(Pool)).all()
        assert len(pools) == 4
        response = test_client.delete(f"/pools/{POOL3_NAME}")
        assert response.status_code == 204
        pools = session.scalars(select(Pool)).all()
        assert len(pools) == 3
        check_last_log(session, dag_id=None, event="delete_pool", logical_date=None)


class TestGetPool(TestPoolsEndpoint):
    def test_get_should_respond_200(self, test_client, session):
        self.create_pools()
        response = test_client.get(f"/pools/{POOL1_NAME}")
        assert response.status_code == 200
        assert response.json() == {
            "deferred_slots": 0,
            "description": None,
            "include_deferred": True,
            "name": "pool1",
            "occupied_slots": 0,
            "open_slots": 3,
            "queued_slots": 0,
            "running_slots": 0,
            "scheduled_slots": 0,
            "slots": 3,
        }

    def test_get_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/pools/{POOL1_NAME}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/pools/{POOL1_NAME}")
        assert response.status_code == 403

    def test_get_should_respond_404(self, test_client):
        response = test_client.get(f"/pools/{POOL1_NAME}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Pool with name: `{POOL1_NAME}` was not found" == body["detail"]

    def test_get_pool3_should_respond_200(self, test_client, session):
        """Test getting POOL3 with forward slash in name"""
        self.create_pools()
        response = test_client.get(f"/pools/{POOL3_NAME}")
        assert response.status_code == 200
        assert response.json() == {
            "deferred_slots": 0,
            "description": "Some Description",
            "include_deferred": False,
            "name": "pool3/with_slashes",
            "occupied_slots": 0,
            "open_slots": 5,
            "queued_slots": 0,
            "running_slots": 0,
            "scheduled_slots": 0,
            "slots": 5,
        }


class TestGetPools(TestPoolsEndpoint):
    @pytest.mark.parametrize(
        ("query_params", "expected_total_entries", "expected_ids"),
        [
            # Filters
            ({}, 4, [Pool.DEFAULT_POOL_NAME, POOL1_NAME, POOL2_NAME, POOL3_NAME]),
            ({"limit": 1}, 4, [Pool.DEFAULT_POOL_NAME]),
            ({"limit": 1, "offset": 1}, 4, [POOL1_NAME]),
            # Sort
            ({"order_by": "-id"}, 4, [POOL3_NAME, POOL2_NAME, POOL1_NAME, Pool.DEFAULT_POOL_NAME]),
            ({"order_by": "id"}, 4, [Pool.DEFAULT_POOL_NAME, POOL1_NAME, POOL2_NAME, POOL3_NAME]),
            ({"order_by": "name"}, 4, [Pool.DEFAULT_POOL_NAME, POOL1_NAME, POOL2_NAME, POOL3_NAME]),
            # Search
            (
                {"pool_name_pattern": "~"},
                4,
                [Pool.DEFAULT_POOL_NAME, POOL1_NAME, POOL2_NAME, POOL3_NAME],
            ),
            ({"pool_name_pattern": "default"}, 1, [Pool.DEFAULT_POOL_NAME]),
        ],
    )
    def test_should_respond_200(
        self, test_client, session, query_params, expected_total_entries, expected_ids
    ):
        self.create_pools()
        response = test_client.get("/pools", params=query_params)
        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [pool["name"] for pool in body["pools"]] == expected_ids

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/pools", params={"pool_name_pattern": "~"})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/pools", params={"pool_name_pattern": "~"})
        assert response.status_code == 403

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_pools")
    def test_should_call_get_authorized_pools(self, mock_get_authorized_pools, test_client):
        self.create_pools()
        mock_get_authorized_pools.return_value = {Pool.DEFAULT_POOL_NAME, POOL1_NAME}
        response = test_client.get("/pools")
        mock_get_authorized_pools.assert_called_once_with(user=mock.ANY, method="GET")
        assert response.status_code == 200
        body = response.json()

        assert body["total_entries"] == 2
        assert [pool["name"] for pool in body["pools"]] == [Pool.DEFAULT_POOL_NAME, POOL1_NAME]


class TestPatchPool(TestPoolsEndpoint):
    @pytest.mark.parametrize(
        ("pool_name", "query_params", "body", "expected_status_code", "expected_response"),
        [
            # Error
            (
                Pool.DEFAULT_POOL_NAME,
                {},
                {},
                400,
                {"detail": "Only slots and included_deferred can be modified on Default Pool"},
            ),
            (
                Pool.DEFAULT_POOL_NAME,
                {"update_mask": ["description"]},
                {"pool": Pool.DEFAULT_POOL_NAME},
                400,
                {"detail": "Only slots and included_deferred can be modified on Default Pool"},
            ),
            (
                "unknown_pool",
                {},
                {"pool": "unknown_pool"},
                404,
                {"detail": "The Pool with name: `unknown_pool` was not found"},
            ),
            # Pool name can't be updated
            (
                POOL1_NAME,
                {},
                {"pool": "pool1_updated"},
                400,
                {"detail": "Invalid body, pool name from request body doesn't match uri parameter"},
            ),
            (
                POOL1_NAME,
                {},
                {"pool": POOL1_NAME},
                422,
                {
                    "detail": [
                        {
                            "input": {"pool": POOL1_NAME},
                            "loc": ["slots"],
                            "msg": "Field required",
                            "type": "missing",
                        },
                        {
                            "input": {"pool": POOL1_NAME},
                            "loc": ["include_deferred"],
                            "msg": "Field required",
                            "type": "missing",
                        },
                    ],
                },
            ),
            # Partial body on default_pool
            (
                Pool.DEFAULT_POOL_NAME,
                {"update_mask": ["slots"]},
                {"slots": 150, "name": Pool.DEFAULT_POOL_NAME, "include_deferred": True},
                200,
                {
                    "deferred_slots": 0,
                    "description": "Default pool",
                    "include_deferred": False,
                    "name": "default_pool",
                    "occupied_slots": 0,
                    "open_slots": 150,
                    "queued_slots": 0,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "slots": 150,
                },
            ),
            # Partial body on default_pool alternate
            (
                Pool.DEFAULT_POOL_NAME,
                {"update_mask": ["slots", "include_deferred"]},
                {"pool": Pool.DEFAULT_POOL_NAME, "slots": 150, "include_deferred": True},
                200,
                {
                    "deferred_slots": 0,
                    "description": "Default pool",
                    "include_deferred": True,
                    "name": "default_pool",
                    "occupied_slots": 0,
                    "open_slots": 150,
                    "queued_slots": 0,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "slots": 150,
                },
            ),
            # Full body
            (
                POOL1_NAME,
                {},
                {
                    "slots": 8,
                    "description": "Description Updated",
                    "name": POOL1_NAME,
                    "include_deferred": False,
                },
                200,
                {
                    "deferred_slots": 0,
                    "description": "Description Updated",
                    "include_deferred": False,
                    "name": POOL1_NAME,
                    "occupied_slots": 0,
                    "open_slots": 8,
                    "queued_slots": 0,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "slots": 8,
                },
            ),
        ],
    )
    def test_should_respond_200(
        self, test_client, session, pool_name, query_params, body, expected_status_code, expected_response
    ):
        self.create_pools()
        response = test_client.patch(f"/pools/{pool_name}", params=query_params, json=body)
        assert response.status_code == expected_status_code

        body = response.json()

        if response.status_code == 422:
            for error in body["detail"]:
                # pydantic version can vary in tests (lower constraints), we do not assert the url.
                del error["url"]

        assert body == expected_response
        if response.status_code == 200:
            check_last_log(session, dag_id=None, event="patch_pool", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(f"/pools/{POOL1_NAME}", params={}, json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(f"/pools/{POOL1_NAME}", params={}, json={})
        assert response.status_code == 403

    def test_patch_pool3_should_respond_200(self, test_client, session):
        """Test patching POOL3 with forward slash in name"""
        self.create_pools()
        body = {
            "slots": 10,
            "description": "Updated Description",
            "name": POOL3_NAME,
            "include_deferred": True,
        }
        response = test_client.patch(f"/pools/{POOL3_NAME}", json=body)
        assert response.status_code == 200
        expected_response = {
            "deferred_slots": 0,
            "description": "Updated Description",
            "include_deferred": True,
            "name": "pool3/with_slashes",
            "occupied_slots": 0,
            "open_slots": 10,
            "queued_slots": 0,
            "running_slots": 0,
            "scheduled_slots": 0,
            "slots": 10,
        }
        assert response.json() == expected_response
        check_last_log(session, dag_id=None, event="patch_pool", logical_date=None)


class TestPostPool(TestPoolsEndpoint):
    @pytest.mark.parametrize(
        ("body", "expected_status_code", "expected_response"),
        [
            (
                {"name": "my_pool", "slots": 11},
                201,
                {
                    "name": "my_pool",
                    "slots": 11,
                    "description": None,
                    "include_deferred": False,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "open_slots": 11,
                    "deferred_slots": 0,
                },
            ),
            (
                {"name": "my_pool", "slots": 11, "include_deferred": True, "description": "Some description"},
                201,
                {
                    "name": "my_pool",
                    "slots": 11,
                    "description": "Some description",
                    "include_deferred": True,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "open_slots": 11,
                    "deferred_slots": 0,
                },
            ),
        ],
    )
    def test_should_respond_200(self, test_client, session, body, expected_status_code, expected_response):
        self.create_pools()
        n_pools = session.scalar(select(func.count()).select_from(Pool))
        response = test_client.post("/pools", json=body)
        assert response.status_code == expected_status_code

        assert response.json() == expected_response
        assert session.scalar(select(func.count()).select_from(Pool)) == n_pools + 1
        check_last_log(session, dag_id=None, event="post_pool", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/pools", json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/pools", json={})
        assert response.status_code == 403

    @pytest.mark.parametrize(
        (
            "body",
            "first_expected_status_code",
            "first_expected_response",
            "second_expected_status_code",
            "second_expected_response",
        ),
        [
            (
                {"name": "my_pool", "slots": 11},
                201,
                {
                    "name": "my_pool",
                    "slots": 11,
                    "description": None,
                    "include_deferred": False,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "scheduled_slots": 0,
                    "open_slots": 11,
                    "deferred_slots": 0,
                },
                409,
                None,
            ),
        ],
    )
    def test_should_response_409(
        self,
        test_client,
        session,
        body,
        first_expected_status_code,
        first_expected_response,
        second_expected_status_code,
        second_expected_response,
    ):
        self.create_pools()
        n_pools = session.scalar(select(func.count()).select_from(Pool))
        response = test_client.post("/pools", json=body)
        assert response.status_code == first_expected_status_code
        assert response.json() == first_expected_response
        assert session.scalar(select(func.count()).select_from(Pool)) == n_pools + 1
        response = test_client.post("/pools", json=body)
        assert response.status_code == second_expected_status_code
        if second_expected_status_code == 201:
            assert response.json() == second_expected_response
        else:
            response_json = response.json()
            assert "detail" in response_json
            assert list(response_json["detail"].keys()) == ["reason", "statement", "orig_error", "message"]

        assert session.scalar(select(func.count()).select_from(Pool)) == n_pools + 1


class TestBulkPools(TestPoolsEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        ("actions", "expected_results"),
        [
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {"name": "pool3", "slots": 10, "description": "New Description"},
                                {"name": "pool4", "slots": 20, "description": "New Description"},
                            ],
                            "action_on_existence": "fail",
                        }
                    ]
                },
                {"create": {"success": ["pool3", "pool4"], "errors": []}},
                id="test_successful_create",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {"name": "pool3", "slots": 10, "description": "New Description"},
                                {"name": "pool1", "slots": 20, "description": "New Description"},
                            ],
                            "action_on_existence": "skip",
                        }
                    ]
                },
                {"create": {"success": ["pool3"], "errors": []}},
                id="test_successful_create_with_skip",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {"name": "pool3", "slots": 10, "description": "New Description"},
                                {"name": "pool2", "slots": 20, "description": "New Description"},
                            ],
                            "action_on_existence": "overwrite",
                        }
                    ]
                },
                {"create": {"success": ["pool3", "pool2"], "errors": []}},
                id="test_successful_create_with_overwrite",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool2", "slots": 20, "description": "New Description"}],
                            "action_on_existence": "fail",
                        }
                    ]
                },
                {
                    "create": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The pools with these pool names: {'pool2'} already exist.",
                                "status_code": 409,
                            }
                        ],
                    }
                },
                id="test_create_conflict",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool2",
                                    "slots": 10,
                                    "description": "New Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {"update": {"success": ["pool2"], "errors": []}},
                id="test_successful_update",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool4",
                                    "slots": 20,
                                    "description": "New Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {"update": {"success": [], "errors": []}},
                id="test_update_with_skip",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool4",
                                    "slots": 10,
                                    "description": "New Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The pools with these pool names: {'pool4'} were not found.",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                id="test_update_not_found",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool1",
                                    "slots": 50,
                                    "description": "Updated description",
                                    "include_deferred": False,
                                }
                            ],
                            "update_mask": ["slots", "description"],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {
                    "update": {"success": ["pool1"], "errors": []},
                },
                id="test_update_with_valid_update_mask",
            ),
            pytest.param(
                {"actions": [{"action": "delete", "entities": ["pool1"], "action_on_non_existence": "skip"}]},
                {"delete": {"success": ["pool1"], "errors": []}},
                id="test_successful_delete",
            ),
            pytest.param(
                {"actions": [{"action": "delete", "entities": ["pool3"], "action_on_non_existence": "skip"}]},
                {"delete": {"success": [], "errors": []}},
                id="test_delete_with_skip",
            ),
            pytest.param(
                {"actions": [{"action": "delete", "entities": ["pool4"], "action_on_non_existence": "fail"}]},
                {
                    "delete": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The pools with these pool names: {'pool4'} were not found.",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                id="test_delete_not_found",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool6", "slots": 10, "description": "New Description"}],
                            "action_on_existence": "skip",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool1",
                                    "slots": 10,
                                    "description": "New Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {"action": "delete", "entities": ["pool2"], "action_on_non_existence": "skip"},
                    ]
                },
                {
                    "create": {"success": ["pool6"], "errors": []},
                    "update": {"success": ["pool1"], "errors": []},
                    "delete": {"success": ["pool2"], "errors": []},
                },
                id="test_create_update_delete",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool1", "slots": 10, "description": "New Description"}],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool1",
                                    "slots": 100,
                                    "description": "New Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {"action": "delete", "entities": ["pool4"], "action_on_non_existence": "skip"},
                    ]
                },
                {
                    "create": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The pools with these pool names: {'pool1'} already exist.",
                                "status_code": 409,
                            }
                        ],
                    },
                    "update": {"success": ["pool1"], "errors": []},
                    "delete": {"success": [], "errors": []},
                },
                id="test_create_update_delete_with_fail",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool1", "slots": 10, "description": "New Description"}],
                            "action_on_existence": "skip",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool5",
                                    "slots": 10,
                                    "description": "New Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "skip",
                        },
                        {"action": "delete", "entities": ["pool5"], "action_on_non_existence": "skip"},
                    ]
                },
                {
                    "create": {"success": [], "errors": []},
                    "update": {"success": [], "errors": []},
                    "delete": {"success": [], "errors": []},
                },
                id="test_create_update_delete_with_skip",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool5", "slots": 10, "description": "New Description"}],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool5",
                                    "slots": 100,
                                    "description": "New test Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {"action": "delete", "entities": ["pool5"], "action_on_non_existence": "fail"},
                    ]
                },
                {
                    "create": {"success": ["pool5"], "errors": []},
                    "update": {"success": ["pool5"], "errors": []},
                    "delete": {"success": ["pool5"], "errors": []},
                },
                id="test_dependent_actions",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {"name": "pool1", "slots": 100, "description": "New test Description"}
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool1",
                                    "slots": 100,
                                    "description": "New test Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "create",
                            "entities": [
                                {"name": "pool5", "slots": 100, "description": "New test Description"}
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool8",
                                    "slots": 100,
                                    "description": "New test Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                        {"action": "delete", "entities": ["pool2"], "action_on_non_existence": "fail"},
                        {
                            "action": "create",
                            "entities": [
                                {"name": "pool6", "slots": 100, "description": "New test Description"}
                            ],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool9",
                                    "slots": 100,
                                    "description": "New test Description",
                                    "include_deferred": False,
                                }
                            ],
                            "action_on_non_existence": "fail",
                        },
                    ]
                },
                {
                    "create": {
                        "success": ["pool5", "pool6"],
                        "errors": [
                            {
                                "error": "The pools with these pool names: {'pool1'} already exist.",
                                "status_code": 409,
                            }
                        ],
                    },
                    "update": {
                        "success": ["pool1"],
                        "errors": [
                            {
                                "error": "The pools with these pool names: {'pool8'} were not found.",
                                "status_code": 404,
                            },
                            {
                                "error": "The pools with these pool names: {'pool9'} were not found.",
                                "status_code": 404,
                            },
                        ],
                    },
                    "delete": {"success": ["pool2"], "errors": []},
                },
                id="test_repeated_actions",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool6", "slots": 5, "description": "Initial Description"}],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "name": "pool6",
                                    "slots": 50,
                                    "description": "Masked Update Description",
                                    "include_deferred": False,
                                }
                            ],
                            "update_mask": ["slots"],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "delete",
                            "entities": ["pool6"],
                            "action_on_non_existence": "fail",
                        },
                    ]
                },
                {
                    "create": {"success": ["pool6"], "errors": []},
                    "update": {"success": ["pool6"], "errors": []},
                    "delete": {"success": ["pool6"], "errors": []},
                },
                id="test_dependent_actions_with_update_mask",
            ),
        ],
    )
    def test_bulk_pools(self, test_client, actions, expected_results, session):
        self.create_pools()
        response = test_client.patch("/pools", json=actions)
        response_data = response.json()
        for key, value in expected_results.items():
            assert response_data[key] == value
        check_last_log(session, dag_id=None, event="bulk_pools", logical_date=None)

    def test_update_mask_preserves_other_fields(self, test_client, session):
        # Arrange: create a pool with initial values
        self.create_pools()

        # Act: update only the "slots" field via update_mask
        response = test_client.patch(
            "/pools",
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {
                                "name": "pool1",
                                "slots": 50,
                                "description": "Should not be updated",
                                "include_deferred": False,
                            }
                        ],
                        "update_mask": ["slots"],  # only slots should update
                        "action_on_non_existence": "fail",
                    }
                ]
            },
        )
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["update"]["success"] == ["pool1"]

        # Assert: fetch from DB and check only masked field changed
        updated_pool = session.scalars(select(Pool).where(Pool.pool == "pool1")).one()
        assert updated_pool.slots == 50  # updated
        assert updated_pool.description is None  # unchanged
        assert updated_pool.include_deferred is True  # unchanged

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch("/pools", json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(
            "/pools",
            json={
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {"pool": "pool1", "slots": 1},
                        ],
                    },
                ]
            },
        )
        assert response.status_code == 403
