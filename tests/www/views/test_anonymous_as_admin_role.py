#
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

from urllib.parse import quote_plus

import pytest

from airflow.models import Pool
from airflow.utils.session import create_session

from dev.tests_common.test_utils.www import check_content_in_response

pytestmark = pytest.mark.db_test

POOL = {
    "pool": "test-pool",
    "slots": 777,
    "description": "test-pool-description",
    "include_deferred": False,
}


@pytest.fixture(autouse=True)
def clear_pools():
    with create_session() as session:
        session.query(Pool).delete()


@pytest.fixture
def pool_factory(session):
    def factory(**values):
        pool = Pool(**{**POOL, **values})  # Passed in values override defaults.
        session.add(pool)
        session.commit()
        return pool

    return factory


def test_delete_pool_anonymous_user_no_role(anonymous_client, pool_factory):
    pool = pool_factory()
    resp = anonymous_client.post(f"pool/delete/{pool.id}")
    assert 302 == resp.status_code
    assert f"/login/?next={quote_plus(f'http://localhost/pool/delete/{pool.id}')}" == resp.headers["Location"]


def test_delete_pool_anonymous_user_as_admin(anonymous_client_as_admin, pool_factory):
    pool = pool_factory()
    resp = anonymous_client_as_admin.post(f"pool/delete/{pool.id}", follow_redirects=True)
    check_content_in_response("Deleted Row", resp)
