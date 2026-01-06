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

import uuid
from unittest import mock

import pytest
from sqlalchemy import select

from airflow.models import DagModel, TaskInstance
from airflow.models.connection import Connection
from airflow.models.dagbundle import DagBundleModel
from airflow.models.team import Team
from airflow.models.variable import Variable
from airflow.models.xcom import XComModel
from airflow.utils.state import TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_connections, clear_db_dags, clear_db_variables, clear_db_runs, clear_db_teams, clear_db_dag_bundles

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def setup_method():
    clear_db_variables()
    clear_db_connections()
    clear_db_dags()
    clear_db_runs()
    clear_db_teams()
    clear_db_dag_bundles()
    yield
    clear_db_variables()
    clear_db_connections()
    clear_db_dags()
    clear_db_runs()
    clear_db_teams()
    clear_db_dag_bundles()


def setup_dag_run(session, dag_id, run_id, bundle_name="test_bundle"):
    from airflow.utils import timezone
    from airflow.models.dagrun import DagRun
    from airflow.models.dag import DagModel
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dag_version import DagVersion

    bundle = session.get(DagBundleModel, bundle_name)
    if not bundle:
        bundle = DagBundleModel(name=bundle_name)
        session.add(bundle)
        session.flush()

    dag = session.get(DagModel, dag_id)
    if not dag:
        dag = DagModel(dag_id=dag_id, bundle_name=bundle_name)
        session.add(dag)
        session.flush()

    dv = session.scalar(select(DagVersion).where(DagVersion.dag_id == dag_id))
    if not dv:
        dv = DagVersion(dag_id=dag_id, bundle_name=bundle_name)
        session.add(dv)
        session.flush()

    dr = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))
    if not dr:
        dr = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            run_type=DagRunType.MANUAL,
            logical_date=timezone.utcnow(),
        )
        session.add(dr)
        session.flush()
    return dr, dv


def create_task_instance(session, dag_id, task_id, run_id, ti_id=None, bundle_name="test_bundle"):
    from unittest.mock import MagicMock
    
    dr, dv = setup_dag_run(session, dag_id, run_id, bundle_name)
    
    # Mock task to satisfy TaskInstance constructor
    task = MagicMock()
    task.dag_id = dag_id
    task.task_id = task_id
    task.retries = 0
    task.queue = "default"
    task.pool = "default_pool"
    task.pool_slots = 1
    task.executor = None
    task.executor_config = {}
    task.task_type = "EmptyOperator"
    task.weight_rule = MagicMock()
    task.weight_rule.get_weight.return_value = 1
    
    ti = TaskInstance(task=task, run_id=run_id, dag_version_id=dv.id)
    if ti_id:
        ti.id = ti_id
    ti.state = TaskInstanceState.RUNNING
    session.add(ti)
    session.commit()
    return ti


class TestAccessControl:
    @pytest.fixture
    def setup_multi_team(self):
        from airflow.configuration import conf
        with mock.patch.object(conf, "getboolean", side_effect=lambda section, key, **kwargs: True if (section == "core" and key == "multi_team") else conf.getboolean(section, key, **kwargs)):
            yield

    def test_variable_access_allowed_same_team(self, client, session):
        from airflow.configuration import conf
        # Enable multi_team
        with mock.patch.object(conf, "getboolean", side_effect=lambda s, k, **kwargs: True if s=="core" and k=="multi_team" else False):
            # Setup Team A
            team_a = Team(name="team_a")
            session.add(team_a)
            session.flush()
            
            bundle = DagBundleModel(name="bundle_a")
            bundle.teams.append(team_a)
            session.add(bundle)
            session.flush()
            
            # create_task_instance will create DagModel if not exists
            ti = create_task_instance(session, "dag_a", "task_a", "run_a", bundle_name="bundle_a")
            
            # Setup Variable for Team A
            Variable.set(key="var_a", value="val_a", team_name="team_a", session=session)
            session.commit()

            ti.id = "00000000-0000-0000-0000-000000000000"
            session.merge(ti)
            session.commit()

            response = client.get("/execution/variables/var_a")
            assert response.status_code == 200

    def test_variable_access_denied_different_team(self, client, session):
        from airflow.configuration import conf
        with mock.patch.object(conf, "getboolean", side_effect=lambda s, k, **kwargs: True if s=="core" and k=="multi_team" else False):
            # Team A
            team_a = Team(name="team_a")
            session.add(team_a)
            bundle_a = DagBundleModel(name="bundle_a")
            bundle_a.teams.append(team_a)
            session.add(bundle_a)
            session.flush()
            
            # Team B
            team_b = Team(name="team_b")
            session.add(team_b)
            session.flush()
            
            ti = create_task_instance(session, "dag_a", "task_a", "run_a", bundle_name="bundle_a")
            ti.id = "00000000-0000-0000-0000-000000000000"
            session.merge(ti)
            
            # Variable for Team B
            Variable.set(key="var_b", value="val_b", team_name="team_b", session=session)
            session.commit()

            response = client.get("/execution/variables/var_b")
            assert response.status_code == 403

    def test_xcom_access_denied_different_dag(self, client, session):
        # TI 1 in DAG 1
        ti1 = create_task_instance(session, "dag_1", "task_1", "run_1")
        ti1.id = "00000000-0000-0000-0000-000000000000"
        session.merge(ti1)
        
        # TI 2 in DAG 2 (needed for XCom foreign key)
        create_task_instance(session, "dag_2", "task_2", "run_2")
        
        # XCom in DAG 2
        XComModel.set(key="key2", value="val2", dag_id="dag_2", task_id="task_2", run_id="run_2", session=session)
        session.commit()

        response = client.get("/execution/xcoms/dag_2/run_2/task_2/key2")
        assert response.status_code == 403
