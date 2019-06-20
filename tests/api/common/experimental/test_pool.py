# -*- coding: utf-8 -*-
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

import unittest

from airflow import models
from airflow.api.common.experimental import pool as pool_api
from airflow.exceptions import AirflowBadRequest, PoolNotFound
from airflow.utils.db import create_session
from tests.test_utils.db import clear_db_pools


class TestPool(unittest.TestCase):

    def setUp(self):
        self.pools = []
        for i in range(2):
            name = 'experimental_%s' % (i + 1)
            pool = models.Pool(
                pool=name,
                slots=i,
                description=name,
            )
            self.pools.append(pool)
        with create_session() as session:
            session.add_all(self.pools)

    def tearDown(self):
        clear_db_pools()

    def test_get_pool(self):
        pool = pool_api.get_pool(name=self.pools[0].pool)
        self.assertEqual(pool.pool, self.pools[0].pool)

    def test_get_pool_non_existing(self):
        self.assertRaisesRegex(PoolNotFound,
                               "^Pool 'test' doesn't exist$",
                               pool_api.get_pool,
                               name='test')

    def test_get_pool_bad_name(self):
        for name in ('', '    '):
            self.assertRaisesRegex(AirflowBadRequest,
                                   "^Pool name shouldn't be empty$",
                                   pool_api.get_pool,
                                   name=name)

    def test_get_pools(self):
        pools = sorted(pool_api.get_pools(),
                       key=lambda p: p.pool)
        self.assertEqual(pools[0].pool, self.pools[0].pool)
        self.assertEqual(pools[1].pool, self.pools[1].pool)

    def test_create_pool(self):
        pool = pool_api.create_pool(name='foo',
                                    slots=5,
                                    description='')
        self.assertEqual(pool.pool, 'foo')
        self.assertEqual(pool.slots, 5)
        self.assertEqual(pool.description, '')
        with create_session() as session:
            self.assertEqual(session.query(models.Pool).count(), 3)

    def test_create_pool_existing(self):
        pool = pool_api.create_pool(name=self.pools[0].pool,
                                    slots=5,
                                    description='')
        self.assertEqual(pool.pool, self.pools[0].pool)
        self.assertEqual(pool.slots, 5)
        self.assertEqual(pool.description, '')
        with create_session() as session:
            self.assertEqual(session.query(models.Pool).count(), 2)

    def test_create_pool_bad_name(self):
        for name in ('', '    '):
            self.assertRaisesRegex(AirflowBadRequest,
                                   "^Pool name shouldn't be empty$",
                                   pool_api.create_pool,
                                   name=name,
                                   slots=5,
                                   description='')

    def test_create_pool_bad_slots(self):
        self.assertRaisesRegex(AirflowBadRequest,
                               "^Bad value for `slots`: foo$",
                               pool_api.create_pool,
                               name='foo',
                               slots='foo',
                               description='')

    def test_delete_pool(self):
        pool = pool_api.delete_pool(name=self.pools[0].pool)
        self.assertEqual(pool.pool, self.pools[0].pool)
        with create_session() as session:
            self.assertEqual(session.query(models.Pool).count(), 1)

    def test_delete_pool_non_existing(self):
        self.assertRaisesRegex(pool_api.PoolNotFound,
                               "^Pool 'test' doesn't exist$",
                               pool_api.delete_pool,
                               name='test')

    def test_delete_pool_bad_name(self):
        for name in ('', '    '):
            self.assertRaisesRegex(AirflowBadRequest,
                                   "^Pool name shouldn't be empty$",
                                   pool_api.delete_pool,
                                   name=name)


if __name__ == '__main__':
    unittest.main()
