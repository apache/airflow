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
import unittest

from parameterized import parameterized

from airflow.models.pool import Pool
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.db import clear_db_pools


class TestBasePoolEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        super().setUp()
        clear_db_pools()

    def tearDown(self) -> None:
        clear_db_pools()


class TestGetPools(TestBasePoolEndpoints):
    @provide_session
    def test_response_200(self, session):
        pool_model = Pool(pool="test_pool_a", slots=3)
        session.add(pool_model)
        session.commit()
        result = session.query(Pool).all()
        assert len(result) == 2  # accounts for the default pool as well
        response = self.client.get("/api/v1/pools")
        assert response.status_code == 200
        self.assertEqual(
            {
                "pools": [
                    {
                        "name": "default_pool",
                        "slots": 128,
                        "occupied_slots": 0,
                        "running_slots": 0,
                        "queued_slots": 0,
                        "open_slots": 128,
                    },
                    {
                        "name": "test_pool_a",
                        "slots": 3,
                        "occupied_slots": 0,
                        "running_slots": 0,
                        "queued_slots": 0,
                        "open_slots": 3,
                    },
                ],
                "total_entries": 2,
            },
            response.json,
        )


class TestGetPoolsPagination(TestBasePoolEndpoints):
    @parameterized.expand(
        [
            # Offset test data
            ("/api/v1/pools?offset=1", [f"test_pool{i}" for i in range(1, 101)]),
            ("/api/v1/pools?offset=3", [f"test_pool{i}" for i in range(3, 103)]),
            # Limit test data
            ("/api/v1/pools?limit=2", ["default_pool", "test_pool1"]),
            ("/api/v1/pools?limit=1", ["default_pool"]),
            # Limit and offset test data
            (
                "/api/v1/pools?limit=120&offset=1",
                [f"test_pool{i}" for i in range(1, 101)],
            ),
            ("/api/v1/pools?limit=2&offset=1", ["test_pool1", "test_pool2"]),
            (
                "/api/v1/pools?limit=3&offset=2",
                ["test_pool2", "test_pool3", "test_pool4"],
            ),
        ]
    )
    @provide_session
    def test_limit_and_offset(self, url, expected_pool_ids, session):
        pools = [Pool(pool=f"test_pool{i}", slots=1) for i in range(1, 121)]
        session.add_all(pools)
        session.commit()
        result = session.query(Pool).count()
        self.assertEqual(result, 121)  # accounts for default pool as well
        response = self.client.get(url)
        assert response.status_code == 200
        pool_ids = [pool["name"] for pool in response.json["pools"]]
        self.assertEqual(pool_ids, expected_pool_ids)


class TestGetPool(TestBasePoolEndpoints):
    @provide_session
    def test_response_200(self, session):
        pool_model = Pool(pool="test_pool_a", slots=3)
        session.add(pool_model)
        session.commit()
        response = self.client.get("/api/v1/pools/test_pool_a")
        assert response.status_code == 200
        self.assertEqual(
            {
                "name": "test_pool_a",
                "slots": 3,
                "occupied_slots": 0,
                "running_slots": 0,
                "queued_slots": 0,
                "open_slots": 3,
            },
            response.json,
        )

    def test_response_404(self):
        response = self.client.get("/api/v1/pools/invalid_pool")
        assert response.status_code == 404
        self.assertEqual(
            {
                "detail": None,
                "status": 404,
                "title": "Pool with name:'invalid_pool' not found",
                "type": "about:blank",
            },
            response.json,
        )


class TestDeletePool(TestBasePoolEndpoints):
    @provide_session
    def test_response_204(self, session):
        pool_name = "test_pool"
        pool_instance = Pool(pool=pool_name, slots=3)
        session.add(pool_instance)
        session.commit()

        response = self.client.delete(f"api/v1/pools/{pool_name}")
        assert response.status_code == 204
        # Check if the pool is deleted from the db
        response = self.client.get(f"api/v1/pools/{pool_name}")
        self.assertEqual(response.status_code, 404)

    def test_response_404(self):
        response = self.client.delete("api/v1/pools/invalid_pool")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            {
                "detail": None,
                "status": 404,
                "title": "Pool with name:'invalid_pool' not found",
                "type": "about:blank",
            },
            response.json,
        )


class TestPostPool(TestBasePoolEndpoints):
    def test_response_200(self):
        response = self.client.post(
            "api/v1/pools", json={"name": "test_pool_a", "slots": 3}
        )
        assert response.status_code == 200
        self.assertEqual(
            {
                "name": "test_pool_a",
                "slots": 3,
                "occupied_slots": 0,
                "running_slots": 0,
                "queued_slots": 0,
                "open_slots": 3,
            },
            response.json,
        )

    @provide_session
    def test_response_409(self, session):
        pool_name = "test_pool_a"
        pool_instance = Pool(pool=pool_name, slots=3)
        session.add(pool_instance)
        session.commit()
        response = self.client.post(
            "api/v1/pools", json={"name": "test_pool_a", "slots": 3}
        )
        assert response.status_code == 409
        self.assertEqual(
            {
                "detail": None,
                "status": 409,
                "title": f"Pool: {pool_name} already exists",
                "type": "about:blank",
            },
            response.json,
        )

    @parameterized.expand(
        [
            ("for missing pool name", {"slots": 3}, "name"),
            ("for missing slots", {"name": "invalid_pool"}, "slots"),
        ]
    )
    def test_response_400(self, name, request_json, missing_field):
        del name
        response = self.client.post("api/v1/pools", json=request_json)
        assert response.status_code == 400
        self.assertEqual(
            {
                "detail": f"'{missing_field}' is a required property",
                "status": 400,
                "title": "Bad Request",
                "type": "about:blank",
            },
            response.json,
        )


class TestPatchPool(TestBasePoolEndpoints):
    @provide_session
    def test_response_200(self, session):
        pool = Pool(pool="test_pool", slots=3)
        session.add(pool)
        session.commit()
        response = self.client.patch(
            "api/v1/pools/test_pool", json={"name": "test_pool_a", "slots": 3}
        )
        assert response.status_code == 200
        self.assertEqual(
            {
                "occupied_slots": 0,
                "queued_slots": 0,
                "name": "test_pool_a",
                "open_slots": 3,
                "running_slots": 0,
                "slots": 3,
            },
            response.json,
        )

    def test_response_404(self):
        response = self.client.patch(
            "api/v1/pools/test_pool", json={"name": "test_pool_a", "slots": 3}
        )
        assert response.status_code == 404
        self.assertEqual(
            {
                "detail": None,
                "status": 404,
                "title": "Pool with name:'test_pool' not found",
                "type": "about:blank",
            },
            response.json,
        )

    @provide_session
    def test_response_400(self, session):
        pool = Pool(pool="test_pool", slots=3)
        session.add(pool)
        session.commit()
        response = self.client.patch("api/v1/pools/test_pool", json={"slots": 3})
        assert response.status_code == 400
        self.assertEqual(
            {
                "detail": "'name' is a required property",
                "status": 400,
                "title": "Bad Request",
                "type": "about:blank",
            },
            response.json,
        )


class TestPatchPoolWithUpdateMask(TestBasePoolEndpoints):
    @parameterized.expand(
        [
            (
                "api/v1/pools/test_pool?update_mask=name, slots",
                {"name": "test_pool_a", "slots": 2},
                "test_pool_a",
                2,
            ),
            (
                "api/v1/pools/test_pool?update_mask=name",
                {"name": "test_pool_a", "slots": 2},
                "test_pool_a",
                3,
            ),
            (
                "api/v1/pools/test_pool?update_mask=slots",
                {"name": "test_pool_a", "slots": 2},
                "test_pool",
                2,
            ),
        ]
    )
    @provide_session
    def test_response_200(
        self, url, patch_json, expected_name, expected_slots, session
    ):
        pool = Pool(pool="test_pool", slots=3)
        session.add(pool)
        session.commit()
        response = self.client.patch(url, json=patch_json)
        assert response.status_code == 200
        self.assertEqual(
            {
                "name": expected_name,
                "slots": expected_slots,
                "occupied_slots": 0,
                "running_slots": 0,
                "queued_slots": 0,
                "open_slots": expected_slots,
            },
            response.json,
        )

    @parameterized.expand(
        [
            (
                "Patching read only field",
                "Property is read-only - 'occupied_slots'",
                "api/v1/pools/test_pool?update_mask=slots, name, occupied_slots",
                {"name": "test_pool_a", "slots": 2, "occupied_slots": 1},
            ),
            (
                "Patching read only field",
                "Property is read-only - 'queued_slots'",
                "api/v1/pools/test_pool?update_mask=slots, name, queued_slots",
                {"name": "test_pool_a", "slots": 2, "queued_slots": 1},
            ),
            (
                "Invalid update mask",
                "'names' is not a valid Pool field",
                "api/v1/pools/test_pool?update_mask=slots, names,",
                {"name": "test_pool_a", "slots": 2},
            ),
            (
                "Invalid update mask",
                "'slot' is not a valid Pool field",
                "api/v1/pools/test_pool?update_mask=slot, name,",
                {"name": "test_pool_a", "slots": 2},
            ),
        ]
    )
    @provide_session
    def test_response_400(self, name, error_detail, url, patch_json, session):
        del name
        pool = Pool(pool="test_pool", slots=3)
        session.add(pool)
        session.commit()
        response = self.client.patch(url, json=patch_json)
        assert response.status_code == 400
        self.assertEqual
        (
            {
                "detail": error_detail,
                "status": 400,
                "title": "Bad Request",
                "type": "about:blank",
            },
            response.json,
        )
