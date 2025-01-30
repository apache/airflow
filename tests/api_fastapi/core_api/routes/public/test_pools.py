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

from airflow.models.pool import Pool
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_pools

pytestmark = pytest.mark.db_test

POOL1_NAME = "pool1"
POOL1_SLOT = 3
POOL1_INCLUDE_DEFERRED = True


POOL2_NAME = "pool2"
POOL2_SLOT = 10
POOL2_INCLUDE_DEFERRED = False
POOL2_DESCRIPTION = "Some Description"


@provide_session
def _create_pools(session) -> None:
    pool1 = Pool(pool=POOL1_NAME, slots=POOL1_SLOT, include_deferred=POOL1_INCLUDE_DEFERRED)
    pool2 = Pool(pool=POOL2_NAME, slots=POOL2_SLOT, include_deferred=POOL2_INCLUDE_DEFERRED)
    session.add_all([pool1, pool2])


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
        pools = session.query(Pool).all()
        assert len(pools) == 3
        response = test_client.delete(f"/public/pools/{POOL1_NAME}")
        assert response.status_code == 204
        pools = session.query(Pool).all()
        assert len(pools) == 2

    def test_delete_should_respond_400(self, test_client):
        response = test_client.delete("/public/pools/default_pool")
        assert response.status_code == 400
        body = response.json()
        assert body["detail"] == "Default Pool can't be deleted"

    def test_delete_should_respond_404(self, test_client):
        response = test_client.delete(f"/public/pools/{POOL1_NAME}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Pool with name: `{POOL1_NAME}` was not found" == body["detail"]


class TestGetPool(TestPoolsEndpoint):
    def test_get_should_respond_200(self, test_client, session):
        self.create_pools()
        response = test_client.get(f"/public/pools/{POOL1_NAME}")
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

    def test_get_should_respond_404(self, test_client):
        response = test_client.get(f"/public/pools/{POOL1_NAME}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Pool with name: `{POOL1_NAME}` was not found" == body["detail"]


class TestGetPools(TestPoolsEndpoint):
    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_ids",
        [
            # Filters
            ({}, 3, [Pool.DEFAULT_POOL_NAME, POOL1_NAME, POOL2_NAME]),
            ({"limit": 1}, 3, [Pool.DEFAULT_POOL_NAME]),
            ({"limit": 1, "offset": 1}, 3, [POOL1_NAME]),
            # Sort
            ({"order_by": "-id"}, 3, [POOL2_NAME, POOL1_NAME, Pool.DEFAULT_POOL_NAME]),
            ({"order_by": "id"}, 3, [Pool.DEFAULT_POOL_NAME, POOL1_NAME, POOL2_NAME]),
        ],
    )
    def test_should_respond_200(
        self, test_client, session, query_params, expected_total_entries, expected_ids
    ):
        self.create_pools()
        response = test_client.get("/public/pools", params=query_params)
        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [pool["name"] for pool in body["pools"]] == expected_ids


class TestPatchPool(TestPoolsEndpoint):
    @pytest.mark.parametrize(
        "pool_name, query_params, body, expected_status_code, expected_response",
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
                {},
                400,
                {"detail": "Only slots and included_deferred can be modified on Default Pool"},
            ),
            (
                "unknown_pool",
                {},
                {},
                404,
                {"detail": "The Pool with name: `unknown_pool` was not found"},
            ),
            (
                POOL1_NAME,
                {},
                {},
                422,
                {
                    "detail": [
                        {
                            "input": {},
                            "loc": ["pool"],
                            "msg": "Field required",
                            "type": "missing",
                        },
                        {
                            "input": {},
                            "loc": ["slots"],
                            "msg": "Field required",
                            "type": "missing",
                        },
                        {
                            "input": {},
                            "loc": ["description"],
                            "msg": "Field required",
                            "type": "missing",
                        },
                        {
                            "input": {},
                            "loc": ["include_deferred"],
                            "msg": "Field required",
                            "type": "missing",
                        },
                    ],
                },
            ),
            # Success
            # Partial body
            (
                POOL1_NAME,
                {"update_mask": ["name"]},
                {"slots": 150, "name": "pool_1_updated"},
                200,
                {
                    "deferred_slots": 0,
                    "description": None,
                    "include_deferred": True,
                    "name": "pool_1_updated",
                    "occupied_slots": 0,
                    "open_slots": 3,
                    "queued_slots": 0,
                    "running_slots": 0,
                    "scheduled_slots": 0,
                    "slots": 3,
                },
            ),
            # Partial body on default_pool
            (
                Pool.DEFAULT_POOL_NAME,
                {"update_mask": ["slots"]},
                {"slots": 150},
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
                {"slots": 150, "include_deferred": True},
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
                    "name": "pool_1_updated",
                    "include_deferred": False,
                },
                200,
                {
                    "deferred_slots": 0,
                    "description": "Description Updated",
                    "include_deferred": False,
                    "name": "pool_1_updated",
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
        response = test_client.patch(f"/public/pools/{pool_name}", params=query_params, json=body)
        assert response.status_code == expected_status_code

        body = response.json()

        if response.status_code == 422:
            for error in body["detail"]:
                # pydantic version can vary in tests (lower constraints), we do not assert the url.
                del error["url"]

        assert body == expected_response


class TestPostPool(TestPoolsEndpoint):
    @pytest.mark.parametrize(
        "body, expected_status_code, expected_response",
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
        n_pools = session.query(Pool).count()
        response = test_client.post("/public/pools", json=body)
        assert response.status_code == expected_status_code

        assert response.json() == expected_response
        assert session.query(Pool).count() == n_pools + 1

    @pytest.mark.parametrize(
        "body,first_expected_status_code, first_expected_response, second_expected_status_code, second_expected_response",
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
        n_pools = session.query(Pool).count()
        response = test_client.post("/public/pools", json=body)
        assert response.status_code == first_expected_status_code
        assert response.json() == first_expected_response
        assert session.query(Pool).count() == n_pools + 1
        response = test_client.post("/public/pools", json=body)
        assert response.status_code == second_expected_status_code
        if second_expected_status_code == 201:
            assert response.json() == second_expected_response
        else:
            response_json = response.json()
            assert "detail" in response_json
            assert list(response_json["detail"].keys()) == ["reason", "statement", "orig_error"]

        assert session.query(Pool).count() == n_pools + 1


class TestBulkPools(TestPoolsEndpoint):
    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "actions, expected_results",
        [
            # Test successful create
            (
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
            ),
            # Test successful create with skip
            (
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
            ),
            # Test successful create with overwrite
            (
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
            ),
            # Test create conflict
            (
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
            ),
            # Test successful update
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [{"name": "pool2", "slots": 10, "description": "New Description"}],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {"update": {"success": ["pool2"], "errors": []}},
            ),
            # Test update with skip
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [{"name": "pool4", "slots": 20, "description": "New Description"}],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {"update": {"success": [], "errors": []}},
            ),
            # Test update not found
            (
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [{"name": "pool4", "slots": 10, "description": "New Description"}],
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
            ),
            # Test successful delete
            (
                {"actions": [{"action": "delete", "entities": ["pool1"], "action_on_non_existence": "skip"}]},
                {"delete": {"success": ["pool1"], "errors": []}},
            ),
            # Test delete with skip
            (
                {"actions": [{"action": "delete", "entities": ["pool3"], "action_on_non_existence": "skip"}]},
                {"delete": {"success": [], "errors": []}},
            ),
            # Test delete not found
            (
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
            ),
            # Test Create, Update, and Delete combined
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool6", "slots": 10, "description": "New Description"}],
                            "action_on_existence": "skip",
                        },
                        {
                            "action": "update",
                            "entities": [{"name": "pool1", "slots": 10, "description": "New Description"}],
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
            ),
            # Test Fail on conflicting create and handle others
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool1", "slots": 10, "description": "New Description"}],
                            "action_on_existence": "fail",
                        },
                        {
                            "action": "update",
                            "entities": [{"name": "pool1", "slots": 100, "description": "New Description"}],
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
            ),
            # Test all skipping actions
            (
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [{"name": "pool1", "slots": 10, "description": "New Description"}],
                            "action_on_existence": "skip",
                        },
                        {
                            "action": "update",
                            "entities": [{"name": "pool5", "slots": 10, "description": "New Description"}],
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
            ),
            # Test Dependent actions
            (
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
                                {"name": "pool5", "slots": 100, "description": "New test Description"}
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
            ),
            # Test Repeated actions
            (
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
                                {"name": "pool1", "slots": 100, "description": "New test Description"}
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
                                {"name": "pool8", "slots": 100, "description": "New test Description"}
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
                                {"name": "pool9", "slots": 100, "description": "New test Description"}
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
            ),
        ],
    )
    def test_bulk_pools(self, test_client, actions, expected_results, session):
        self.create_pools()
        response = test_client.patch("/public/pools", json=actions)
        response_data = response.json()
        for key, value in expected_results.items():
            assert response_data[key] == value
