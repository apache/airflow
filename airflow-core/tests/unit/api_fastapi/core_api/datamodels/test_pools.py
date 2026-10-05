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
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.pools import PoolCollectionResponse
from airflow.models.pool import Pool

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_pools

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def default_pool():
    clear_db_pools()
    yield
    clear_db_pools()


def test_pool_collection_response_serializes_without_queries(session):
    """FastAPI serializes on the event loop, so the slot queries must run when the response is built."""
    response = PoolCollectionResponse(pools=session.scalars(select(Pool)), total_entries=1)

    with assert_queries_count(0):
        response.model_dump_json()
