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

from airflow import settings
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from tests.test_utils.db import clear_db_dags, clear_db_pools, clear_db_runs, set_default_pool_slots

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestPool:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_pools()

    def setup_method(self):
        self.clean_db()

    def teardown_method(self):
        self.clean_db()

    def test_open_slots(self, dag_maker):
        pool = Pool(pool='test_pool', slots=5)
        with dag_maker(
            dag_id='test_open_slots',
            start_date=DEFAULT_DATE,
        ):
            op1 = DummyOperator(task_id='dummy1', pool='test_pool')
            op2 = DummyOperator(task_id='dummy2', pool='test_pool')
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
            op1 = DummyOperator(task_id='dummy1', pool='test_pool')
            op2 = DummyOperator(task_id='dummy2', pool='test_pool')
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
            op1 = DummyOperator(task_id='dummy1')
            op2 = DummyOperator(task_id='dummy2', pool_slots=2)
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
