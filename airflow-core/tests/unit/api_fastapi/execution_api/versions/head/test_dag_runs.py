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

from airflow.models import DagModel
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import DagRunState

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


class TestDagRunTrigger:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_trigger_dag_run(self, client, session, dag_maker):
        dag_id = "test_trigger_dag_run"
        run_id = "test_run_id"
        logical_date = timezone.datetime(2025, 2, 20)

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        session.commit()

        response = client.post(
            f"/execution/dag-runs/{dag_id}/{run_id}",
            json={"logical_date": logical_date.isoformat(), "conf": {"key1": "value1"}},
        )

        assert response.status_code == 204

        dag_run = session.query(DagRun).filter(DagRun.run_id == run_id).one()
        assert dag_run.conf == {"key1": "value1"}
        assert dag_run.logical_date == logical_date

    def test_trigger_dag_run_dag_not_found(self, client):
        """Test that a DAG that does not exist cannot be triggered."""
        dag_id = "dag_not_found"
        logical_date = timezone.datetime(2025, 2, 20)

        response = client.post(
            f"/execution/dag-runs/{dag_id}/test_run_id", json={"logical_date": logical_date.isoformat()}
        )

        assert response.status_code == 404

    def test_trigger_dag_run_import_error(self, client, session, dag_maker):
        """Test that a DAG with import errors cannot be triggered."""

        dag_id = "test_trigger_dag_run_import_error"
        run_id = "test_run_id"
        logical_date = timezone.datetime(2025, 2, 20)

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        session.query(DagModel).filter(DagModel.dag_id == dag_id).update({"has_import_errors": True})

        session.commit()

        response = client.post(
            f"/execution/dag-runs/{dag_id}/{run_id}",
            json={"logical_date": logical_date.isoformat()},
        )

        assert response.status_code == 400
        assert response.json() == {
            "detail": {
                "message": "DAG with dag_id: 'test_trigger_dag_run_import_error' has import errors and cannot be triggered",
                "reason": "import_errors",
            }
        }

    def test_trigger_dag_run_already_exists(self, client, session, dag_maker):
        """Test that error is raised when a DAG Run already exists."""

        dag_id = "test_trigger_dag_run_already_exists"
        run_id = "test_run_id"
        logical_date = timezone.datetime(2025, 2, 20)

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        session.commit()

        response = client.post(
            f"/execution/dag-runs/{dag_id}/{run_id}",
            json={"logical_date": logical_date.isoformat()},
        )

        assert response.status_code == 204

        response = client.post(
            f"/execution/dag-runs/{dag_id}/{run_id}",
            json={"logical_date": logical_date.isoformat()},
        )

        assert response.status_code == 409
        assert response.json() == {
            "detail": {
                "message": "A DAG Run already exists for DAG test_trigger_dag_run_already_exists with run id test_run_id",
                "reason": "already_exists",
            }
        }


class TestDagRunClear:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_dag_run_clear(self, client, session, dag_maker):
        dag_id = "test_trigger_dag_run_clear"
        run_id = "test_run_id"

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        dag_maker.create_dagrun(run_id=run_id, state=DagRunState.SUCCESS)

        session.commit()

        response = client.post(f"/execution/dag-runs/{dag_id}/{run_id}/clear")

        assert response.status_code == 204

        session.expire_all()
        dag_run = session.query(DagRun).filter(DagRun.run_id == run_id).one()
        assert dag_run.state == DagRunState.QUEUED

    def test_dag_run_import_error(self, client, session, dag_maker):
        """Test that a DAG with import errors cannot be cleared."""

        dag_id = "test_trigger_dag_run_import_error"
        run_id = "test_run_id"

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        session.query(DagModel).filter(DagModel.dag_id == dag_id).update({"has_import_errors": True})

        session.commit()

        response = client.post(f"/execution/dag-runs/{dag_id}/{run_id}/clear")

        assert response.status_code == 400
        assert response.json() == {
            "detail": {
                "message": "DAG with dag_id: 'test_trigger_dag_run_import_error' has import errors and cannot be triggered",
                "reason": "import_errors",
            }
        }

    def test_dag_run_not_found(self, client):
        """Test that a DAG that does not exist cannot be cleared."""
        dag_id = "dag_not_found"
        run_id = "test_run_id"

        response = client.post(f"/execution/dag-runs/{dag_id}/{run_id}/clear")

        assert response.status_code == 404


class TestDagRunState:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_get_state(self, client, session, dag_maker):
        dag_id = "test_get_state"
        run_id = "test_run_id"

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        dag_maker.create_dagrun(run_id=run_id, state=DagRunState.SUCCESS)

        session.commit()

        response = client.get(f"/execution/dag-runs/{dag_id}/{run_id}/state")

        assert response.status_code == 200
        assert response.json() == {"state": "success"}

    def test_dag_run_not_found(self, client):
        dag_id = "dag_not_found"
        run_id = "test_run_id"

        response = client.post(f"/execution/dag-runs/{dag_id}/{run_id}/clear")

        assert response.status_code == 404
