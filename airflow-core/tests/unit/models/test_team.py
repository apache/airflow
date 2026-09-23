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
from sqlalchemy import delete, inspect as sa_inspect, select, update

from airflow.api_fastapi.common.db.dags import eager_load_teams
from airflow.models.dag import DagModel, clear_team_name_cache
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

DAG_ID = "team_owned_dag"
BUNDLE_NAME = "team-owned-bundle"
TEAM_NAME = "owning-team"


class TestTeam:
    """Unit tests for Team model class methods."""

    @pytest.mark.db_test
    def test_get_name_if_exists_returns_name(self, testing_team):
        assert Team.get_name_if_exists("testing") == "testing"

    @pytest.mark.db_test
    def test_get_name_if_exists_returns_none(self):
        assert Team.get_name_if_exists("nonexistent") is None

    @pytest.mark.db_test
    def test_get_all_team_names_with_teams(self, testing_team):
        result = Team.get_all_team_names()

        assert result == {"testing"}
        assert isinstance(result, set)


class TestTeamName:
    """``team_name`` resolution on the models that reach a team through a relationship."""

    @pytest.fixture
    def team_owned_run(self, dag_maker, session):
        """A Dag run and task instance whose Dag is owned by ``TEAM_NAME`` via its bundle."""
        with dag_maker(DAG_ID, session=session):
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="task")
        dag_run = dag_maker.create_dagrun()
        original_bundle_name = session.scalar(select(DagModel.bundle_name).where(DagModel.dag_id == DAG_ID))

        session.execute(delete(DagBundleModel).where(DagBundleModel.name == BUNDLE_NAME))
        session.execute(delete(Team).where(Team.name == TEAM_NAME))
        session.flush()
        bundle = DagBundleModel(name=BUNDLE_NAME)
        bundle.teams.append(Team(name=TEAM_NAME))
        session.add(bundle)
        session.flush()
        session.execute(update(DagModel).where(DagModel.dag_id == DAG_ID).values(bundle_name=BUNDLE_NAME))
        session.commit()
        clear_team_name_cache()

        yield dag_run

        # bundle_name is a foreign key with no ON DELETE action, so restore it before
        # dropping the bundle this fixture introduced.
        session.execute(
            update(DagModel).where(DagModel.dag_id == DAG_ID).values(bundle_name=original_bundle_name)
        )
        session.execute(delete(DagBundleModel).where(DagBundleModel.name == BUNDLE_NAME))
        session.execute(delete(Team).where(Team.name == TEAM_NAME))
        session.commit()
        clear_team_name_cache()

    def get_run(self, session, *, eager_load: bool) -> DagRun:
        """Re-fetch the run, with or without the team eager loading options."""
        session.expunge_all()
        options = eager_load_teams(DagRun.dag_model) if eager_load else ()
        return session.scalar(select(DagRun).where(DagRun.dag_id == DAG_ID).options(*options))

    def get_dag_model(self, session, *, eager_load: bool) -> DagModel:
        """Re-fetch the Dag, with or without the team eager loading options."""
        session.expunge_all()
        options = eager_load_teams() if eager_load else ()
        return session.scalar(select(DagModel).where(DagModel.dag_id == DAG_ID).options(*options))

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_name_uses_eager_loaded_relationships(self, team_owned_run, session):
        dag_run = self.get_run(session, eager_load=True)

        with assert_queries_count(0, session=session):
            assert dag_run.team_name == TEAM_NAME

    @conf_vars({("core", "multi_team"): "True"})
    @pytest.mark.parametrize("entity", ["dag", "dag_run"])
    def test_team_name_falls_back_when_not_eager_loaded(self, team_owned_run, session, entity):
        """Paths that cannot eager load resolve via the cached resolver, not ``lazy="raise"``."""
        if entity == "dag":
            assert self.get_dag_model(session, eager_load=False).team_name == TEAM_NAME
        else:
            assert self.get_run(session, eager_load=False).team_name == TEAM_NAME

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_name_fallback_keeps_caller_objects_attached(self, team_owned_run, session):
        """The fallback must not close the caller's scoped session out from under it."""
        dag_model = self.get_dag_model(session, eager_load=False)

        assert dag_model.team_name == TEAM_NAME
        assert not sa_inspect(dag_model).detached
        # Would raise DetachedInstanceError if the resolver had closed the shared session.
        assert dag_model.dag_versions is not None

    @conf_vars({("core", "multi_team"): "False"})
    def test_team_name_is_none_without_multi_team(self, team_owned_run, session):
        """Single-team deployments answer ``None`` without loading anything."""
        dag_run = self.get_run(session, eager_load=False)

        with assert_queries_count(0, session=session):
            assert dag_run.team_name is None

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_name_on_task_instance(self, team_owned_run, session):
        task_instance = session.scalar(select(TaskInstance).where(TaskInstance.dag_id == DAG_ID))

        assert task_instance.team_name == TEAM_NAME

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_name_is_none_for_bundle_without_team(self, dag_maker, session):
        with dag_maker("unteamed_dag", session=session):
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="task")
        dag_maker.create_dagrun()
        session.commit()
        clear_team_name_cache()

        dag_run = session.scalar(
            select(DagRun).where(DagRun.dag_id == "unteamed_dag").options(*eager_load_teams(DagRun.dag_model))
        )

        assert dag_run.team_name is None
        clear_team_name_cache()

    def test_eager_load_teams_is_a_no_op_without_multi_team(self):
        with conf_vars({("core", "multi_team"): "False"}):
            assert eager_load_teams() == ()
            assert eager_load_teams(DagRun.dag_model) == ()

        with conf_vars({("core", "multi_team"): "True"}):
            assert eager_load_teams() != ()
            assert eager_load_teams(DagRun.dag_model) != ()
