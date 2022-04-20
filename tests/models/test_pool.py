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

import pytest

from airflow import settings
from airflow.exceptions import AirflowException, PoolNotFound
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils.db import clear_db_dags, clear_db_pools, clear_db_runs, set_default_pool_slots

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestPool:

    USER_POOL_COUNT = 2
    TOTAL_POOL_COUNT = USER_POOL_COUNT + 1  # including default_pool

    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_pools()

    def setup_method(self):
        self.clean_db()
        self.pools = []

    def add_pools(self):
        self.pools = [Pool.get_default_pool()]
        for i in range(self.USER_POOL_COUNT):
            name = f'experimental_{i + 1}'
            pool = Pool(
                pool=name,
                slots=i,
                description=name,
            )
            self.pools.append(pool)
        with create_session() as session:
            session.add_all(self.pools)

    def teardown_method(self):
        self.clean_db()

    def test_open_slots(self, dag_maker):
        pool = Pool(pool='test_pool', slots=5)
        with dag_maker(
            dag_id='test_open_slots',
            start_date=DEFAULT_DATE,
        ):
            op1 = EmptyOperator(task_id='dummy1', pool='test_pool')
            op2 = EmptyOperator(task_id='dummy2', pool='test_pool')
        dag_maker.create_dagrun()
        ti1 = TI(task=op1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=op2, execution_date=DEFAULT_DATE)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED

        session = settings.Session()
        session.add(pool)
        session.merge(ti1)
        session.merge(ti2)
        session.commit()
        session.close()

        assert 3 == pool.open_slots()
        assert 1 == pool.running_slots()
        assert 1 == pool.queued_slots()
        assert 2 == pool.occupied_slots()
        assert {
            "default_pool": {
                "open": 128,
                "queued": 0,
                "total": 128,
                "running": 0,
            },
            "test_pool": {
                "open": 3,
                "queued": 1,
                "running": 1,
                "total": 5,
            },
        } == pool.slots_stats()

    def test_infinite_slots(self, dag_maker):
        pool = Pool(pool='test_pool', slots=-1)
        with dag_maker(
            dag_id='test_infinite_slots',
        ):
            op1 = EmptyOperator(task_id='dummy1', pool='test_pool')
            op2 = EmptyOperator(task_id='dummy2', pool='test_pool')
        dag_maker.create_dagrun()
        ti1 = TI(task=op1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=op2, execution_date=DEFAULT_DATE)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED

        session = settings.Session()
        session.add(pool)
        session.merge(ti1)
        session.merge(ti2)
        session.commit()
        session.close()

        assert float('inf') == pool.open_slots()
        assert 1 == pool.running_slots()
        assert 1 == pool.queued_slots()
        assert 2 == pool.occupied_slots()
        assert {
            "default_pool": {
                "open": 128,
                "queued": 0,
                "total": 128,
                "running": 0,
            },
            "test_pool": {
                "open": float('inf'),
                "queued": 1,
                "running": 1,
                "total": float('inf'),
            },
        } == pool.slots_stats()

    def test_default_pool_open_slots(self, dag_maker):
        set_default_pool_slots(5)
        assert 5 == Pool.get_default_pool().open_slots()

        with dag_maker(
            dag_id='test_default_pool_open_slots',
        ):
            op1 = EmptyOperator(task_id='dummy1')
            op2 = EmptyOperator(task_id='dummy2', pool_slots=2)
        dag_maker.create_dagrun()
        ti1 = TI(task=op1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=op2, execution_date=DEFAULT_DATE)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED

        session = settings.Session()
        session.merge(ti1)
        session.merge(ti2)
        session.commit()
        session.close()

        assert 2 == Pool.get_default_pool().open_slots()
        assert {
            "default_pool": {
                "open": 2,
                "queued": 2,
                "total": 5,
                "running": 1,
            }
        } == Pool.slots_stats()

    def test_get_pool(self):
        self.add_pools()
        pool = Pool.get_pool(pool_name=self.pools[0].pool)
        assert pool.pool == self.pools[0].pool

    def test_get_pool_non_existing(self):
        self.add_pools()
        assert not Pool.get_pool(pool_name='test')

    def test_get_pool_bad_name(self):
        for name in ('', '    '):
            assert not Pool.get_pool(pool_name=name)

    def test_get_pools(self):
        self.add_pools()
        pools = sorted(Pool.get_pools(), key=lambda p: p.pool)
        assert pools[0].pool == self.pools[0].pool
        assert pools[1].pool == self.pools[1].pool

    def test_create_pool(self, session):
        self.add_pools()
        pool = Pool.create_or_update_pool(name='foo', slots=5, description='')
        assert pool.pool == 'foo'
        assert pool.slots == 5
        assert pool.description == ''
        assert session.query(Pool).count() == self.TOTAL_POOL_COUNT + 1

    def test_create_pool_existing(self, session):
        self.add_pools()
        pool = Pool.create_or_update_pool(name=self.pools[0].pool, slots=5, description='')
        assert pool.pool == self.pools[0].pool
        assert pool.slots == 5
        assert pool.description == ''
        assert session.query(Pool).count() == self.TOTAL_POOL_COUNT

    def test_delete_pool(self, session):
        self.add_pools()
        pool = Pool.delete_pool(name=self.pools[-1].pool)
        assert pool.pool == self.pools[-1].pool
        assert session.query(Pool).count() == self.TOTAL_POOL_COUNT - 1

    def test_delete_pool_non_existing(self):
        with pytest.raises(PoolNotFound, match="^Pool 'test' doesn't exist$"):
            Pool.delete_pool(name='test')

    def test_delete_default_pool_not_allowed(self):
        with pytest.raises(AirflowException, match="^default_pool cannot be deleted$"):
            Pool.delete_pool(Pool.DEFAULT_POOL_NAME)

    def test_is_default_pool(self):
        pool = Pool.create_or_update_pool(name="not_default_pool", slots=1, description="test")
        default_pool = Pool.get_default_pool()
        assert not Pool.is_default_pool(id=pool.id)
        assert Pool.is_default_pool(str(default_pool.id))
