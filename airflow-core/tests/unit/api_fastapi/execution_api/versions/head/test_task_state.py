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
from uuid import uuid4

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.app import cached_app
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.security import _jwt_bearer
from airflow.models.dagrun import DagRun
from airflow.models.task_state import TaskStateModel
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from tests_common.pytest_plugin import CreateTaskInstance


pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def reset_state_tables():
    with create_session() as session:
        session.execute(delete(TaskStateModel))
        session.execute(delete(DagRun))


def _api_url(ti_id, key: str | None = None) -> str:
    base = f"/execution/state/ti/{ti_id}"
    return f"{base}/{key}" if key else base


class TestGetTaskState:
    def test_get_returns_value(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()
        client.put(_api_url(ti.id, "job_id"), json={"value": "spark_001"})

        response = client.get(_api_url(ti.id, "job_id"))

        assert response.status_code == 200
        assert response.json() == {"value": "spark_001"}

    def test_get_missing_key_returns_404(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()

        response = client.get(_api_url(ti.id, "never_set"))

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"

    def test_get_missing_ti_returns_404(self, client: TestClient):
        response = client.get(_api_url(uuid4(), "any_key"))

        assert response.status_code == 404
        assert "Task instance" in response.json()["detail"]["message"]


class TestPutTaskState:
    def test_put_creates_row(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()

        response = client.put(_api_url(ti.id, "job_id"), json={"value": "spark_001"})

        assert response.status_code == 204
        with create_session() as session:
            row = session.scalar(
                select(TaskStateModel).where(
                    TaskStateModel.dag_id == ti.dag_id,
                    TaskStateModel.run_id == ti.run_id,
                    TaskStateModel.task_id == ti.task_id,
                    TaskStateModel.key == "job_id",
                )
            )
            assert row is not None
            assert row.value == "spark_001"

    def test_put_overwrites_existing(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()
        client.put(_api_url(ti.id, "job_id"), json={"value": "spark_001"})

        response = client.put(_api_url(ti.id, "job_id"), json={"value": "spark_002"})

        assert response.status_code == 204
        assert client.get(_api_url(ti.id, "job_id")).json() == {"value": "spark_002"}

    def test_put_empty_body_returns_422(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()

        response = client.put(_api_url(ti.id, "job_id"), json={})

        assert response.status_code == 422

    def test_put_extra_field_returns_422(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()

        response = client.put(_api_url(ti.id, "job_id"), json={"value": "x", "extra": "y"})

        assert response.status_code == 422

    def test_put_null_value_returns_422(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()

        response = client.put(_api_url(ti.id, "job_id"), json={"value": None})

        assert response.status_code == 422

    def test_put_missing_ti_returns_404(self, client: TestClient):
        response = client.put(_api_url(uuid4(), "job_id"), json={"value": "x"})

        assert response.status_code == 404


class TestDeleteTaskState:
    def test_delete_removes_key(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()
        client.put(_api_url(ti.id, "job_id"), json={"value": "spark_001"})

        response = client.delete(_api_url(ti.id, "job_id"))

        assert response.status_code == 204
        assert client.get(_api_url(ti.id, "job_id")).status_code == 404

    def test_delete_missing_key_is_noop(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()

        response = client.delete(_api_url(ti.id, "never_existed"))

        assert response.status_code == 204

    def test_delete_only_targets_one_key(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()
        client.put(_api_url(ti.id, "job_id"), json={"value": "a"})
        client.put(_api_url(ti.id, "checkpoint"), json={"value": "b"})

        client.delete(_api_url(ti.id, "job_id"))

        assert client.get(_api_url(ti.id, "job_id")).status_code == 404
        assert client.get(_api_url(ti.id, "checkpoint")).json() == {"value": "b"}


class TestClearTaskState:
    def test_clear_removes_all_keys(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()
        for k, v in [("job_id", "a"), ("checkpoint", "b"), ("retry_count", "c")]:
            client.put(_api_url(ti.id, k), json={"value": v})

        response = client.delete(_api_url(ti.id))

        assert response.status_code == 204
        with create_session() as session:
            remaining = session.scalars(
                select(TaskStateModel.key).where(
                    TaskStateModel.dag_id == ti.dag_id,
                    TaskStateModel.task_id == ti.task_id,
                )
            ).all()
            assert remaining == []

    def test_clear_when_empty_is_noop(self, client: TestClient, create_task_instance: CreateTaskInstance):
        ti = create_task_instance()

        response = client.delete(_api_url(ti.id))

        assert response.status_code == 204

    def _seed_fleet_rows(self, ti, indices: tuple[int, ...]) -> None:
        with create_session() as session:
            now = timezone.utcnow()
            for idx in indices:
                session.add(
                    TaskStateModel(
                        dag_run_id=ti.dag_run.id,
                        dag_id=ti.dag_id,
                        run_id=ti.run_id,
                        task_id=ti.task_id,
                        map_index=idx,
                        key="job_id",
                        value=f"app_{idx}",
                        updated_at=now,
                    )
                )
            session.commit()

    def test_clear_default_only_clears_this_map_index(
        self, client: TestClient, create_task_instance: CreateTaskInstance
    ):
        """Clear without the query param only wipes the requesting TI's own map_index."""
        ti = create_task_instance(map_index=2)
        self._seed_fleet_rows(ti, (0, 1, 2))

        response = client.delete(_api_url(ti.id))

        assert response.status_code == 204
        with create_session() as session:
            remaining_indices = sorted(
                session.scalars(
                    select(TaskStateModel.map_index).where(
                        TaskStateModel.dag_id == ti.dag_id,
                        TaskStateModel.task_id == ti.task_id,
                    )
                ).all()
            )
            assert remaining_indices == [0, 1]

    def test_clear_with_all_map_indices_query_param_wipes_fleet(
        self, client: TestClient, create_task_instance: CreateTaskInstance
    ):
        """Clear with ?all_map_indices=true wipes state for every mapped instance."""
        ti = create_task_instance(map_index=2)
        self._seed_fleet_rows(ti, (0, 1, 2))

        response = client.delete(_api_url(ti.id), params={"all_map_indices": "true"})

        assert response.status_code == 204
        with create_session() as session:
            remaining = session.scalars(
                select(TaskStateModel).where(
                    TaskStateModel.dag_id == ti.dag_id,
                    TaskStateModel.task_id == ti.task_id,
                )
            ).all()
            assert remaining == []


class TestTiSelfEnforcement:
    @pytest.fixture
    def wrong_ti_client(self):
        """TestClient using the real require_auth, JWT bound to a random TI UUID."""
        app = cached_app(apps="execution")
        other_ti_id = uuid4()

        async def mock_jwt(request: Request) -> TIToken:
            return TIToken(id=other_ti_id, claims=TIClaims(scope="execution"))

        app.dependency_overrides[_jwt_bearer] = mock_jwt
        with TestClient(app, headers={"Authorization": "Bearer fake"}) as client:
            yield client
        app.dependency_overrides.pop(_jwt_bearer, None)

    def test_get_with_wrong_ti_token_returns_403(self, wrong_ti_client: TestClient, create_task_instance):
        ti = create_task_instance()
        response = wrong_ti_client.get(_api_url(ti.id, "some_key"))
        assert response.status_code == 403

    def test_put_with_wrong_ti_token_returns_403(self, wrong_ti_client: TestClient, create_task_instance):
        ti = create_task_instance()
        response = wrong_ti_client.put(_api_url(ti.id, "some_key"), json={"value": "x"})
        assert response.status_code == 403

    def test_clear_with_wrong_ti_token_returns_403(self, wrong_ti_client: TestClient, create_task_instance):
        ti = create_task_instance()
        response = wrong_ti_client.delete(_api_url(ti.id))
        assert response.status_code == 403
