# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from airflow.api.common.experimental import pool as pool_api
from airflow import models
from airflow import settings


class TestPool(unittest.TestCase):

    def setUp(self):
        super(TestPool, self).setUp()
        self.session = settings.Session()
        self.pools = []
        for i in range(2):
            name = 'experimental_%s' % (i + 1)
            pool = models.Pool(
                pool=name,
                slots=i,
                description=name,
            )
            self.session.add(pool)
            self.pools.append(pool)
        self.session.commit()

    def tearDown(self):
        self.session.query(models.Pool).delete()
        self.session.commit()
        self.session.close()
        super(TestPool, self).tearDown()

    def test_get_pool(self):
        pool = pool_api.get_pool(name=self.pools[0].pool, session=self.session)
        self.assertEqual(pool.pool, self.pools[0].pool)

    def test_get_pool_non_existing(self):
        self.assertRaisesRegexp(pool_api.PoolNotFound,
                                "^Pool 'test' doesn't exist$",
                                pool_api.get_pool,
                                name='test',
                                session=self.session)

    def test_get_pool_bad_name(self):
        for name in ('', '    '):
            self.assertRaisesRegexp(pool_api.PoolBadRequest,
                                    "^Pool name shouldn't be empty$",
                                    pool_api.get_pool,
                                    name=name,
                                    session=self.session)

    def test_get_pools(self):
        pools = sorted(pool_api.get_pools(session=self.session),
                       key=lambda p: p.pool)
        self.assertEqual(pools[0].pool, self.pools[0].pool)
        self.assertEqual(pools[1].pool, self.pools[1].pool)

    def test_create_pool(self):
        pool = pool_api.create_pool(name='foo',
                                    slots=5,
                                    description='',
                                    session=self.session)
        self.assertEqual(pool.pool, 'foo')
        self.assertEqual(pool.slots, 5)
        self.assertEqual(pool.description, '')
        self.assertEqual(self.session.query(models.Pool).count(), 3)

    def test_create_pool_existing(self):
        pool = pool_api.create_pool(name=self.pools[0].pool,
                                    slots=5,
                                    description='',
                                    session=self.session)
        self.assertEqual(pool.pool, self.pools[0].pool)
        self.assertEqual(pool.slots, 5)
        self.assertEqual(pool.description, '')
        self.assertEqual(self.session.query(models.Pool).count(), 2)

    def test_create_pool_bad_name(self):
        for name in ('', '    '):
            self.assertRaisesRegexp(pool_api.PoolBadRequest,
                                    "^Pool name shouldn't be empty$",
                                    pool_api.create_pool,
                                    name=name,
                                    slots=5,
                                    description='',
                                    session=self.session)

    def test_create_pool_bad_slots(self):
        self.assertRaisesRegexp(pool_api.PoolBadRequest,
                                "^Bad value for `slots`: foo$",
                                pool_api.create_pool,
                                name='foo',
                                slots='foo',
                                description='',
                                session=self.session)

    def test_delete_pool(self):
        pool = pool_api.delete_pool(name=self.pools[0].pool,
                                    session=self.session)
        self.assertEqual(pool.pool, self.pools[0].pool)
        self.assertEqual(self.session.query(models.Pool).count(), 1)

    def test_delete_pool_non_existing(self):
        self.assertRaisesRegexp(pool_api.PoolNotFound,
                                "^Pool 'test' doesn't exist$",
                                pool_api.delete_pool,
                                name='test',
                                session=self.session)

    def test_delete_pool_bad_name(self):
        for name in ('', '    '):
            self.assertRaisesRegexp(pool_api.PoolBadRequest,
                                    "^Pool name shouldn't be empty$",
                                    pool_api.delete_pool,
                                    name=name,
                                    session=self.session)


if __name__ == '__main__':
    unittest.main()
