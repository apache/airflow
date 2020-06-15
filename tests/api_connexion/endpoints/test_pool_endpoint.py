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
                "title": "Pool not found",
                "type": "about:blank",
            },
            response.json,
        )
