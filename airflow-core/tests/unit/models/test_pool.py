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

from typing import TYPE_CHECKING

import pendulum
import pytest
from sqlalchemy import func, select

from airflow import settings
from airflow.exceptions import AirflowException, PoolNotFound
from airflow.models.dag_version import DagVersion
from airflow.models.pool import Pool
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_pools,
    clear_db_runs,
    set_default_pool_slots,
)
from tests_common.test_utils.taskinstance import create_task_instance

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.team import Team

pytestmark = pytest.mark.db_test

DEFAULT_DATE = pendulum.datetime(2016, 1, 1, tz="UTC")


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
            name = f"experimental_{i + 1}"
            pool = Pool(
                pool=name,
                slots=i,
                description=name,
                include_deferred=False,
            )
            self.pools.append(pool)
        with create_session() as session:
            session.add_all(self.pools)

    def teardown_method(self):
        self.clean_db()

    def test_open_slots(self, dag_maker):
        pool = Pool(pool="test_pool", slots=5, include_deferred=False)
        with dag_maker(
            dag_id="test_open_slots",
            start_date=DEFAULT_DATE,
        ):
            op1 = EmptyOperator(task_id="dummy1", pool="test_pool")
            op2 = EmptyOperator(task_id="dummy2", pool="test_pool")
            op3 = EmptyOperator(task_id="dummy3", pool="test_pool")

        dr = dag_maker.create_dagrun()

        ti1 = dr.get_task_instance(task_id=op1.task_id)
        ti2 = dr.get_task_instance(task_id=op2.task_id)
        ti3 = dr.get_task_instance(task_id=op3.task_id)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.DEFERRED

        session = settings.Session()
        session.add(pool)
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)
        session.commit()
        session.close()

        assert pool.open_slots() == 3
        assert pool.running_slots() == 1
        assert pool.queued_slots() == 1
        assert pool.occupied_slots() == 2
        assert pool.deferred_slots() == 1
        assert pool.slots_stats() == {
            "default_pool": {
                "open": 128,
                "queued": 0,
                "total": 128,
                "running": 0,
                "scheduled": 0,
                "deferred": 0,
            },
            "test_pool": {
                "open": 3,
                "queued": 1,
                "running": 1,
                "deferred": 1,
                "scheduled": 0,
                "total": 5,
            },
        }

    def test_open_slots_including_deferred(self, dag_maker):
        pool = Pool(pool="test_pool", slots=5, include_deferred=True)
        with dag_maker(
            dag_id="test_open_slots_including_deferred",
            start_date=DEFAULT_DATE,
        ):
            op1 = EmptyOperator(task_id="dummy1", pool="test_pool")
            op2 = EmptyOperator(task_id="dummy2", pool="test_pool")

        dr = dag_maker.create_dagrun()

        ti1 = dr.get_task_instance(task_id=op1.task_id)
        ti2 = dr.get_task_instance(task_id=op2.task_id)
        ti1.state = State.RUNNING
        ti2.state = State.DEFERRED

        session = settings.Session()
        session.add(pool)
        session.merge(ti1)
        session.merge(ti2)
        session.commit()
        session.close()

        assert pool.open_slots() == 3
        assert pool.running_slots() == 1
        assert pool.queued_slots() == 0
        assert pool.deferred_slots() == 1
        assert pool.occupied_slots() == 2
        assert pool.slots_stats() == {
            "default_pool": {
                "open": 128,
                "queued": 0,
                "total": 128,
                "running": 0,
                "scheduled": 0,
                "deferred": 0,
            },
            "test_pool": {
                "open": 3,
                "queued": 0,
                "running": 1,
                "deferred": 1,
                "scheduled": 0,
                "total": 5,
            },
        }

    def test_infinite_slots(self, dag_maker):
        pool = Pool(pool="test_pool", slots=-1, include_deferred=False)
        with dag_maker(
            dag_id="test_infinite_slots",
        ):
            op1 = EmptyOperator(task_id="dummy1", pool="test_pool")
            op2 = EmptyOperator(task_id="dummy2", pool="test_pool")

        dr = dag_maker.create_dagrun()
        dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti1 = create_task_instance(task=op1, run_id=dr.run_id, dag_version_id=dag_version.id)
        ti1.refresh_from_db()
        ti2 = create_task_instance(task=op2, run_id=dr.run_id, dag_version_id=dag_version.id)
        ti2.refresh_from_db()
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED

        session = settings.Session()
        session.add(pool)
        session.merge(ti1)
        session.merge(ti2)
        session.commit()
        session.close()

        assert float("inf") == pool.open_slots()
        assert pool.running_slots() == 1
        assert pool.queued_slots() == 1
        assert pool.occupied_slots() == 2
        assert pool.slots_stats() == {
            "default_pool": {
                "open": 128,
                "queued": 0,
                "total": 128,
                "running": 0,
                "scheduled": 0,
                "deferred": 0,
            },
            "test_pool": {
                "open": float("inf"),
                "queued": 1,
                "running": 1,
                "total": float("inf"),
                "scheduled": 0,
                "deferred": 0,
            },
        }

    def test_default_pool_open_slots(self, dag_maker):
        set_default_pool_slots(5)
        assert Pool.get_default_pool().open_slots() == 5

        with dag_maker(
            dag_id="test_default_pool_open_slots",
        ):
            op1 = EmptyOperator(task_id="dummy1")
            op2 = EmptyOperator(task_id="dummy2", pool_slots=2)
            op3 = EmptyOperator(task_id="dummy3")

        dr = dag_maker.create_dagrun()
        dag_version = DagVersion.get_latest_version(dr.dag_id)
        ti1 = create_task_instance(task=op1, run_id=dr.run_id, dag_version_id=dag_version.id)
        ti2 = create_task_instance(task=op2, run_id=dr.run_id, dag_version_id=dag_version.id)
        ti3 = create_task_instance(task=op3, run_id=dr.run_id, dag_version_id=dag_version.id)
        ti1.refresh_from_db()
        ti1.state = State.RUNNING
        ti2.refresh_from_db()
        ti2.state = State.QUEUED
        ti3.refresh_from_db()
        ti3.state = State.SCHEDULED

        session = settings.Session()
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)
        session.commit()
        session.close()

        assert Pool.get_default_pool().open_slots() == 2
        assert Pool.slots_stats() == {
            "default_pool": {
                "open": 2,
                "queued": 2,
                "total": 5,
                "running": 1,
                "scheduled": 1,
                "deferred": 0,
            }
        }

    def test_get_pool(self):
        self.add_pools()
        pool = Pool.get_pool(pool_name=self.pools[0].pool)
        assert pool.pool == self.pools[0].pool

    def test_get_pool_non_existing(self):
        self.add_pools()
        assert not Pool.get_pool(pool_name="test")

    def test_get_pool_bad_name(self):
        for name in ("", "    "):
            assert not Pool.get_pool(pool_name=name)

    def test_get_pools(self):
        self.add_pools()
        pools = sorted(Pool.get_pools(), key=lambda p: p.pool)
        assert pools[0].pool == self.pools[0].pool
        assert pools[1].pool == self.pools[1].pool

    def test_create_pool(self, session):
        self.add_pools()
        pool = Pool.create_or_update_pool(name="foo", slots=5, description="", include_deferred=True)
        assert pool.pool == "foo"
        assert pool.slots == 5
        assert pool.description == ""
        assert pool.include_deferred is True
        assert session.scalar(select(func.count()).select_from(Pool)) == self.TOTAL_POOL_COUNT + 1

    def test_create_pool_existing(self, session):
        self.add_pools()
        pool = Pool.create_or_update_pool(
            name=self.pools[0].pool, slots=5, description="", include_deferred=False
        )
        assert pool.pool == self.pools[0].pool
        assert pool.slots == 5
        assert pool.description == ""
        assert pool.include_deferred is False
        assert session.scalar(select(func.count()).select_from(Pool)) == self.TOTAL_POOL_COUNT

    def test_delete_pool(self, session):
        self.add_pools()
        pool = Pool.delete_pool(name=self.pools[-1].pool)
        assert pool.pool == self.pools[-1].pool
        assert session.scalar(select(func.count()).select_from(Pool)) == self.TOTAL_POOL_COUNT - 1

    def test_delete_pool_non_existing(self):
        with pytest.raises(PoolNotFound, match="^Pool 'test' doesn't exist$"):
            Pool.delete_pool(name="test")

    def test_delete_default_pool_not_allowed(self):
        with pytest.raises(AirflowException, match="^default_pool cannot be deleted$"):
            Pool.delete_pool(Pool.DEFAULT_POOL_NAME)

    def test_is_default_pool(self):
        pool = Pool.create_or_update_pool(
            name="not_default_pool", slots=1, description="test", include_deferred=False
        )
        default_pool = Pool.get_default_pool()
        assert not Pool.is_default_pool(id=pool.id)
        assert Pool.is_default_pool(str(default_pool.id))

    def test_get_team_name(self, testing_team: Team, session: Session):
        pool = Pool(pool="test", include_deferred=False, team_name=testing_team.name)
        session.add(pool)
        session.flush()

        assert Pool.get_team_name("test", session=session) == "testing"

    def test_get_name_to_team_name_mapping(self, testing_team: Team, session: Session):
        pool1 = Pool(pool="pool1", include_deferred=False, team_name=testing_team.name)
        pool2 = Pool(pool="pool2", include_deferred=False)
        session.add(pool1)
        session.add(pool2)
        session.flush()

        assert Pool.get_name_to_team_name_mapping(["pool1", "pool2"], session=session) == {
            "pool1": "testing",
            "pool2": None,
        }
