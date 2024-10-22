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


class TestPools:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_pools()

    def teardown_method(self) -> None:
        clear_db_pools()

    def create_pools(self):
        _create_pools()


class TestDeletePool(TestPools):
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


class TestGetPool(TestPools):
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
