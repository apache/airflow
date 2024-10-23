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

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

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
        assert "Default Pool can't be deleted" == body["detail"]

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
        response = test_client.get("/public/pools/", params=query_params)
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
                            "input": None,
                            "loc": ["pool"],
                            "msg": "Input should be a valid string",
                            "type": "string_type",
                        },
                        {
                            "input": None,
                            "loc": ["slots"],
                            "msg": "Input should be a valid integer",
                            "type": "int_type",
                        },
                        {
                            "input": None,
                            "loc": ["include_deferred"],
                            "msg": "Input should be a valid boolean",
                            "type": "bool_type",
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
