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

from datetime import UTC, datetime
from unittest.mock import ANY

import pytest
from sqlalchemy import event, update
from sqlalchemy.orm import Session

from airflow.models.dag import DagModel, DagTag
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


class TestDag:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            pytest.param(True, True),
            pytest.param(False, False),
        ],
    )
    def test_get_dag(self, client, session, dag_maker, state, expected):
        """Test getting a DAG."""

        dag_id = "test_get_dag"
        next_dagrun = datetime(2026, 4, 13, tzinfo=UTC)

        with dag_maker(dag_id=dag_id, session=session, serialized=True, tags=["z_tag", "a_tag"]):
            EmptyOperator(task_id="test_task")

        session.execute(
            update(DagModel)
            .where(DagModel.dag_id == dag_id)
            .values(
                is_paused=state,
                bundle_version="bundle-version",
                relative_fileloc="dags/example.py",
                owners="owner_1",
                next_dagrun=next_dagrun,
            )
        )

        session.commit()

        response = client.get(
            f"/execution/dags/{dag_id}",
        )

        assert response.status_code == 200
        assert response.json() == {
            "dag_id": dag_id,
            "is_paused": expected,
            "bundle_name": "dag_maker",
            "bundle_version": "bundle-version",
            "relative_fileloc": "dags/example.py",
            "owners": "owner_1",
            "tags": ["a_tag", "z_tag"],
            "next_dagrun": "2026-04-13T00:00:00Z",
        }

    def test_dag_not_found(self, client, session, dag_maker):
        """Test Dag not found"""

        dag_id = "test_get_dag"

        response = client.get(
            f"/execution/dags/{dag_id}",
        )

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "The Dag with dag_id: `test_get_dag` was not found",
                "reason": "not_found",
            }
        }

    def test_get_dag_defaults(self, client, session, dag_maker):
        """Test getting a DAG with default model values."""

        dag_id = "test_get_dag_defaults"

        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")

        session.commit()

        response = client.get(
            f"/execution/dags/{dag_id}",
        )

        assert response.status_code == 200
        assert response.json() == {
            "dag_id": dag_id,
            "is_paused": False,
            "bundle_name": "dag_maker",
            "bundle_version": None,
            "relative_fileloc": "test_dags.py",
            "owners": "airflow",
            "tags": [],
            "next_dagrun": ANY,
        }

    def test_get_dag_does_not_hydrate_orm_entities(self, client, session, dag_maker):
        dag_id = "test_get_dag_no_orm_hydration"

        with dag_maker(dag_id=dag_id, session=session, serialized=True, tags=["tag_a", "tag_b"]):
            EmptyOperator(task_id="test_task")
        session.commit()
        session.expunge_all()

        loaded: list[str] = []

        def record(_, instance):
            if isinstance(instance, (DagModel, DagTag)):
                loaded.append(type(instance).__name__)

        event.listen(Session, "loaded_as_persistent", record)
        try:
            response = client.get(f"/execution/dags/{dag_id}")
        finally:
            event.remove(Session, "loaded_as_persistent", record)

        assert response.status_code == 200
        assert response.json()["tags"] == ["tag_a", "tag_b"]
        assert loaded == []
