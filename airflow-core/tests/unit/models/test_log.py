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

from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import joinedload

from airflow.models.dag import DagModel, clear_team_name_cache
from airflow.models.dagbundle import DagBundleModel
from airflow.models.log import Log
from airflow.models.team import Team
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_logs,
    clear_db_teams,
)

pytestmark = pytest.mark.db_test


class TestLogTaskInstanceReproduction:
    def test_coordinate_join_selects_only_current_task_instance(self, dag_maker, session):
        with dag_maker("log_attempt_history", session=session):
            EmptyOperator(task_id="task")
        run = dag_maker.create_dagrun()
        ti = run.get_task_instance("task")
        ti.state = TaskInstanceState.SUCCESS
        ti = session.merge(ti)
        session.flush()
        log = Log(event="attempt_event", task_instance=ti)
        session.add(log)
        session.flush()
        log_id = log.id
        old_id = ti.id
        successor = ti.prepare_db_for_next_try(session)
        successor_id = successor.id
        session.commit()
        session.expunge_all()

        rows = session.execute(
            select(Log).where(Log.id == log_id).options(joinedload(Log.task_instance))
        ).all()

        assert len(rows) == 1
        assert rows[0][0].task_instance.id == successor_id
        assert rows[0][0].task_instance.id != old_id

    def test_log_task_instance_raises_without_joinedload(self, dag_maker, session):
        """Accessing Log.task_instance without joinedload should raise."""
        with dag_maker("dag_raise_test", session=session):
            EmptyOperator(task_id="task_1")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("task_1")
        session.merge(ti)
        session.commit()

        log = Log(event="test_event", task_instance=ti)
        session.add(log)
        session.commit()

        session.expire_all()
        stmt = select(Log).where(Log.id == log.id)
        loaded_log = session.scalar(stmt)

        with pytest.raises(InvalidRequestError):
            loaded_log.task_instance

    def test_log_task_instance_join_correctness(self, dag_maker, session):
        # Create dag_1 with a task
        with dag_maker("dag_1", session=session):
            EmptyOperator(task_id="common_task_id")

        dr1 = dag_maker.create_dagrun()
        ti1 = dr1.get_task_instance("common_task_id")
        ti1.state = TaskInstanceState.SUCCESS
        session.merge(ti1)
        session.commit()

        # Create dag_2 with the SAME task_id
        with dag_maker("dag_2", session=session):
            EmptyOperator(task_id="common_task_id")

        dr2 = dag_maker.create_dagrun()
        ti2 = dr2.get_task_instance("common_task_id")
        ti2.state = TaskInstanceState.FAILED
        session.merge(ti2)
        session.commit()

        # Create a log entry specifically for dag_1's task instance
        log = Log(
            event="test_event",
            task_instance=ti1,
        )
        session.add(log)
        session.commit()

        # Query with joinedload to trigger the relationship join

        stmt = select(Log).where(Log.id == log.id).options(joinedload(Log.task_instance))
        loaded_log = session.scalar(stmt)

        assert loaded_log.task_instance is not None
        assert loaded_log.task_instance.dag_id == "dag_1"
        assert loaded_log.task_instance.run_id == ti1.run_id

        # Verify incorrect join for second dag
        log2 = Log(
            event="test_event_2",
            task_instance=ti2,
        )
        session.add(log2)
        session.commit()

        stmt2 = select(Log).where(Log.id == log2.id).options(joinedload(Log.task_instance))
        loaded_log2 = session.scalar(stmt2)

        # This should fail if the join is ambiguous and picks the first one (dag_1)
        assert loaded_log2.task_instance is not None
        assert loaded_log2.task_instance.dag_id == "dag_2"
        assert loaded_log2.task_instance.run_id == ti2.run_id


DAG_IN_TEAM = "dag_owned_by_a_team"


class TestLogTaskInstanceId:
    @pytest.fixture
    def ti(self, dag_maker, session):
        with dag_maker("log_task_instance_id", session=session):
            EmptyOperator(task_id="task")
        return dag_maker.create_dagrun().get_task_instance("task")

    def test_records_the_attempt_that_wrote_the_event(self, ti):
        assert Log(event="event", task_instance=ti).task_instance_id == ti.id

    def test_task_instance_key_has_no_attempt(self, ti):
        assert Log(event="event", task_instance=ti.key).task_instance_id is None

    def test_explicit_attempt_id_overrides_the_task_instance(self, ti):
        attempt_id = uuid4()
        assert (
            Log(event="event", task_instance=ti, task_instance_id=attempt_id).task_instance_id == attempt_id
        )


class TestLogTeamName:
    def teardown_method(self):
        clear_db_logs()
        clear_db_dags()
        clear_db_dag_bundles()
        clear_db_teams()

    @staticmethod
    def _create_dag_owned_by_team(session, team_name: str, *, dag_id=DAG_IN_TEAM, bundle_name="team-bundle"):
        bundle = DagBundleModel(name=bundle_name)
        bundle.teams.append(Team(name=team_name))
        session.add(bundle)
        session.flush()
        session.add(DagModel(dag_id=dag_id, bundle_name=bundle_name, is_stale=False))
        session.commit()
        clear_team_name_cache()

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_owning_the_dag_is_recorded(self, session):
        self._create_dag_owned_by_team(session, "payments")

        log = Log(event="test_event", dag_id=DAG_IN_TEAM)
        session.add(log)
        session.commit()

        assert log.team_name == "payments"

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_named_by_the_caller_is_kept(self, session):
        self._create_dag_owned_by_team(session, "payments")

        log = Log(event="test_event", dag_id=DAG_IN_TEAM, team_name="infra")
        session.add(log)
        session.commit()

        assert log.team_name == "infra"

    @conf_vars({("core", "multi_team"): "True"})
    def test_recorded_team_outlives_the_dag_changing_teams(self, session):
        self._create_dag_owned_by_team(session, "payments")
        log = Log(event="test_event", dag_id=DAG_IN_TEAM)
        session.add(log)
        session.commit()

        other_bundle = DagBundleModel(name="other-team-bundle")
        other_bundle.teams.append(Team(name="infra"))
        session.add(other_bundle)
        session.flush()
        session.scalar(
            select(DagModel).where(DagModel.dag_id == DAG_IN_TEAM)
        ).bundle_name = "other-team-bundle"
        session.commit()
        clear_team_name_cache()

        assert log.team_name == "payments"

    @conf_vars({("core", "multi_team"): "True"})
    def test_no_team_is_recorded_for_an_event_owning_no_dag(self, session):
        log = Log(event="test_event")
        session.add(log)
        session.commit()

        assert log.team_name is None

    def test_no_team_is_recorded_when_multi_team_is_off(self, session):
        self._create_dag_owned_by_team(session, "payments")

        log = Log(event="test_event", dag_id=DAG_IN_TEAM)
        session.add(log)
        session.commit()

        assert log.team_name is None
