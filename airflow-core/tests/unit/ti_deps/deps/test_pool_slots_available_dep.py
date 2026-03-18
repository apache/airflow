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

from unittest.mock import Mock, patch

import pytest

from airflow.models import Pool
from airflow.ti_deps.dependencies_states import EXECUTION_STATES
from airflow.ti_deps.deps.pool_slots_available_dep import PoolSlotsAvailableDep
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils import db
from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestPoolSlotsAvailableDep:
    def setup_method(self):
        db.clear_db_pools()
        db.clear_db_teams()
        with create_session() as session:
            test_pool = Pool(pool="test_pool", include_deferred=False)
            test_includes_deferred_pool = Pool(pool="test_includes_deferred_pool", include_deferred=True)
            session.add_all([test_pool, test_includes_deferred_pool])
            session.commit()

    def teardown_method(self):
        db.clear_db_pools()
        db.clear_db_teams()

    @patch("airflow.models.Pool.open_slots", return_value=0)
    def test_pooled_task_reached_concurrency(self, mock_open_slots):
        ti = Mock(pool="test_pool", pool_slots=1)
        assert not PoolSlotsAvailableDep().is_met(ti=ti)

    @patch("airflow.models.Pool.open_slots", return_value=1)
    def test_pooled_task_pass(self, mock_open_slots):
        ti = Mock(pool="test_pool", pool_slots=1)
        assert PoolSlotsAvailableDep().is_met(ti=ti)

    @patch("airflow.models.Pool.open_slots", return_value=0)
    def test_running_pooled_task_pass(self, mock_open_slots):
        for state in EXECUTION_STATES:
            ti = Mock(pool="test_pool", state=state, pool_slots=1)
            assert PoolSlotsAvailableDep().is_met(ti=ti)

    @patch("airflow.models.Pool.open_slots", return_value=0)
    def test_deferred_pooled_task_pass(self, mock_open_slots):
        ti = Mock(pool="test_includes_deferred_pool", state=TaskInstanceState.DEFERRED, pool_slots=1)
        assert PoolSlotsAvailableDep().is_met(ti=ti)
        ti_to_fail = Mock(pool="test_pool", state=TaskInstanceState.DEFERRED, pool_slots=1)
        assert not PoolSlotsAvailableDep().is_met(ti=ti_to_fail)

    def test_task_with_nonexistent_pool(self):
        ti = Mock(pool="nonexistent_pool", pool_slots=1)
        assert not PoolSlotsAvailableDep().is_met(ti=ti)

    @conf_vars({("core", "multi_team"): "True"})
    def test_pool_team_mismatch(self):
        """Test that a task from one team cannot use a pool assigned to another team."""
        from airflow.models.dag import DagModel
        from airflow.models.team import Team

        with create_session() as session:
            # Create teams
            team_a = Team(name="teamA")
            team_b = Team(name="teamB")
            session.add_all([team_a, team_b])
            session.commit()

            # Create a pool assigned to teamA
            pool_team_a = Pool(pool="pool_team_a", slots=10, include_deferred=False, team_name="teamA")
            session.add(pool_team_a)
            session.commit()

        # Mock a task instance from a DAG belonging to teamB
        ti = Mock(pool="pool_team_a", pool_slots=1, dag_id="test_dag")

        with patch.object(DagModel, "get_team_name", return_value="teamB"):
            assert not PoolSlotsAvailableDep().is_met(ti=ti)

    @conf_vars({("core", "multi_team"): "True"})
    def test_pool_team_match(self):
        """Test that a task from a team can use a pool assigned to the same team."""
        from airflow.models.dag import DagModel
        from airflow.models.team import Team

        with create_session() as session:
            # Create team
            team_a = Team(name="teamA")
            session.add(team_a)
            session.commit()

            # Create a pool assigned to teamA
            pool_team_a = Pool(pool="pool_team_a", slots=10, include_deferred=False, team_name="teamA")
            session.add(pool_team_a)
            session.commit()

        # Mock a task instance from a DAG belonging to teamA
        ti = Mock(pool="pool_team_a", pool_slots=1, dag_id="test_dag", state=None)

        with patch.object(DagModel, "get_team_name", return_value="teamA"):
            with patch("airflow.models.Pool.open_slots", return_value=5):
                assert PoolSlotsAvailableDep().is_met(ti=ti)

    def test_pool_no_team_assignment(self):
        """Test that a pool without team assignment can be used by any DAG."""
        from airflow.models.dag import DagModel

        with create_session() as session:
            # Create a pool without team assignment
            pool_no_team = Pool(pool="pool_no_team", slots=10, include_deferred=False, team_name=None)
            session.add(pool_no_team)
            session.commit()

        # Mock a task instance from any DAG
        ti = Mock(pool="pool_no_team", pool_slots=1, dag_id="test_dag", state=None)

        with patch.object(DagModel, "get_team_name", return_value="teamA"):
            with patch("airflow.models.Pool.open_slots", return_value=5):
                assert PoolSlotsAvailableDep().is_met(ti=ti)
