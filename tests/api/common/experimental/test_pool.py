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

import random
import string

import pytest

from airflow import models
from airflow.api.common.experimental import pool as pool_api
from airflow.exceptions import AirflowBadRequest, PoolNotFound
from airflow.models.pool import Pool
from airflow.utils.session import create_session
from tests.test_utils.db import clear_db_pools


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
class TestPool:

    USER_POOL_COUNT = 2
    TOTAL_POOL_COUNT = USER_POOL_COUNT + 1  # including default_pool

    def setup_method(self):
        clear_db_pools()
        self.pools = [Pool.get_default_pool()]
        for i in range(self.USER_POOL_COUNT):
            name = f'experimental_{i + 1}'
            pool = models.Pool(
                pool=name,
                slots=i,
                description=name,
            )
            self.pools.append(pool)
        with create_session() as session:
            session.add_all(self.pools)

    def test_get_pool(self):
        pool = pool_api.get_pool(name=self.pools[0].pool)
        assert pool.pool == self.pools[0].pool

    def test_get_pool_non_existing(self):
        with pytest.raises(PoolNotFound, match="^Pool 'test' doesn't exist$"):
            pool_api.get_pool(name='test')

    def test_get_pool_bad_name(self):
        for name in ('', '    '):
            with pytest.raises(AirflowBadRequest, match="^Pool name shouldn't be empty$"):
                pool_api.get_pool(name=name)

    def test_get_pools(self):
        pools = sorted(pool_api.get_pools(), key=lambda p: p.pool)
        assert pools[0].pool == self.pools[0].pool
        assert pools[1].pool == self.pools[1].pool

    def test_create_pool(self):
        pool = pool_api.create_pool(name='foo', slots=5, description='')
        assert pool.pool == 'foo'
        assert pool.slots == 5
        assert pool.description == ''
        with create_session() as session:
            assert session.query(models.Pool).count() == self.TOTAL_POOL_COUNT + 1

    def test_create_pool_existing(self):
        pool = pool_api.create_pool(name=self.pools[0].pool, slots=5, description='')
        assert pool.pool == self.pools[0].pool
        assert pool.slots == 5
        assert pool.description == ''
        with create_session() as session:
            assert session.query(models.Pool).count() == self.TOTAL_POOL_COUNT

    def test_create_pool_bad_name(self):
        for name in ('', '    '):
            with pytest.raises(AirflowBadRequest, match="^Pool name shouldn't be empty$"):
                pool_api.create_pool(
                    name=name,
                    slots=5,
                    description='',
                )

    def test_create_pool_name_too_long(self):
        long_name = ''.join(random.choices(string.ascii_lowercase, k=300))
        column_length = models.Pool.pool.property.columns[0].type.length
        with pytest.raises(
            AirflowBadRequest, match="^Pool name can't be more than %d characters$" % column_length
        ):
            pool_api.create_pool(
                name=long_name,
                slots=5,
                description='',
            )

    def test_create_pool_bad_slots(self):
        with pytest.raises(AirflowBadRequest, match="^Bad value for `slots`: foo$"):
            pool_api.create_pool(
                name='foo',
                slots='foo',
                description='',
            )

    def test_delete_pool(self):
        pool = pool_api.delete_pool(name=self.pools[-1].pool)
        assert pool.pool == self.pools[-1].pool
        with create_session() as session:
            assert session.query(models.Pool).count() == self.TOTAL_POOL_COUNT - 1

    def test_delete_pool_non_existing(self):
        with pytest.raises(pool_api.PoolNotFound, match="^Pool 'test' doesn't exist$"):
            pool_api.delete_pool(name='test')

    def test_delete_pool_bad_name(self):
        for name in ('', '    '):
            with pytest.raises(AirflowBadRequest, match="^Pool name shouldn't be empty$"):
                pool_api.delete_pool(name=name)

    def test_delete_default_pool_not_allowed(self):
        with pytest.raises(AirflowBadRequest, match="^default_pool cannot be deleted$"):
            pool_api.delete_pool(Pool.DEFAULT_POOL_NAME)
