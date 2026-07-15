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

import asyncio
import datetime
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import time_machine
from fastapi import HTTPException
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app, purge_cached_app
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.providers.common.dataquality.api.app import (
    _get_backend,
    dataquality_app,
    health,
    run_detail_by_task_instance,
    task_rule_history,
    task_runs,
)
from airflow.providers.common.dataquality.backends.object_storage import ObjectStorageResultsBackend
from airflow.providers.common.dataquality.results import DQRun, RuleResult

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

BASE_URL = "http://testserver"

pytestmark = pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS,
    reason="Plugin endpoints are not supported before Airflow 3.1",
)


def run(coro):
    return asyncio.run(coro)


class TestGetBackend:
    def test_raises_503_when_unconfigured(self):
        with mock.patch(
            "airflow.providers.common.dataquality.api.app.get_backend_from_config", return_value=None
        ):
            with pytest.raises(HTTPException) as exc_info:
                _get_backend()
        assert exc_info.value.status_code == 503

    def test_returns_configured_backend(self, tmp_path):
        configured = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
        with mock.patch(
            "airflow.providers.common.dataquality.api.app.get_backend_from_config", return_value=configured
        ):
            assert _get_backend() is configured


class TestHealth:
    def test_health(self):
        assert run(health()) == {"status": "ok"}


class TestTaskRuns:
    def test_reads_task_runs_from_object_storage_backend(self, tmp_path):
        backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
        dq_run = DQRun(
            dag_id="orders_pipeline",
            task_id="dq",
            run_id="manual__2026-07-04",
            started_at="2026-07-04T00:00:00+00:00",
        )
        backend.write_run(dq_run, [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")])

        result = run(task_runs(dag_id="orders_pipeline", task_id="dq", backend=backend))

        assert result.items[0].run.dag_id == "orders_pipeline"
        assert result.items[0].summary.passed == 1
        assert result.next_cursor is None

    def test_passes_before_cursor_through_to_backend(self, tmp_path):
        backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
        for day in range(1, 4):
            backend.write_run(
                DQRun(
                    dag_id="orders_pipeline",
                    task_id="dq",
                    run_id=f"manual__2026-07-0{day}",
                    run_uid=f"run{day}",
                    started_at=f"2026-07-0{day}T00:00:00+00:00",
                ),
                [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")],
            )

        first_page = run(task_runs(dag_id="orders_pipeline", task_id="dq", backend=backend, limit=2))
        second_page = run(
            task_runs(
                dag_id="orders_pipeline",
                task_id="dq",
                backend=backend,
                limit=2,
                before=first_page.next_cursor,
            )
        )

        assert [item.run.run_uid for item in first_page.items] == ["run3", "run2"]
        assert [item.run.run_uid for item in second_page.items] == ["run1"]
        assert second_page.next_cursor is None
        assert first_page.next_cursor == "2026-07-02T00:00:00+00:00|run2"


class TestTaskRuleHistory:
    def test_reads_task_rule_history_from_object_storage_backend(self, tmp_path):
        backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
        dq_run = DQRun(
            dag_id="orders_pipeline",
            task_id="dq",
            run_id="manual__2026-07-04",
            started_at="2026-07-04T00:00:00+00:00",
        )
        backend.write_run(dq_run, [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")])

        result = run(
            task_rule_history(
                dag_id="orders_pipeline",
                task_id="dq",
                rule_uid="r1",
                backend=backend,
            )
        )

        assert result.items[0].rule_name == "nulls"
        assert result.items[0].run.task_id == "dq"
        assert result.next_cursor is None


class TestRunDetailByTaskInstance:
    def test_reads_run_by_task_instance_route_params(self, tmp_path):
        backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
        dq_run = DQRun(
            dag_id="orders_pipeline",
            task_id="dq",
            run_id="scheduled__2026-07-04",
            started_at="2026-07-04T00:00:00+00:00",
        )
        backend.write_run(dq_run, [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")])

        result = run(
            run_detail_by_task_instance(
                dag_id="orders_pipeline",
                task_id="dq",
                run_id="scheduled__2026-07-04",
                backend=backend,
            )
        )

        assert result.run.dag_id == "orders_pipeline"
        assert result.results[0].rule_name == "nulls"
        assert result.summary.passed == 1

    def test_missing_task_instance_run_raises_404(self, tmp_path):
        backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")

        with pytest.raises(HTTPException) as exc_info:
            run(
                run_detail_by_task_instance(
                    dag_id="orders_pipeline",
                    task_id="dq",
                    run_id="does_not_exist",
                    backend=backend,
                )
            )

        assert exc_info.value.status_code == 404


@pytest.fixture
def dq_backend(tmp_path):
    """Override the route-level backend dependency so requests hit an isolated tmp_path backend."""
    backend = ObjectStorageResultsBackend(results_path=f"file://{tmp_path}")
    dataquality_app.dependency_overrides[_get_backend] = lambda: backend
    yield backend
    del dataquality_app.dependency_overrides[_get_backend]


def _make_test_client(*, role: str | None):
    with (
        conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
                ("core", "lazy_discover_providers"): "false",
            }
        ),
        mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False),
    ):
        purge_cached_app()
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        time_very_before = datetime.datetime(2014, 1, 1, 0, 0, 0)
        time_after = datetime.datetime.now() + datetime.timedelta(days=1)
        with time_machine.travel(time_very_before, tick=False):
            token = auth_manager._get_token_signer(
                expiration_time_in_seconds=(time_after - time_very_before).total_seconds()
            ).generate(
                auth_manager.serialize_user(SimpleAuthManagerUser(username="test", role=role)),
            )
        return TestClient(app, headers={"Authorization": f"Bearer {token}"}, base_url=BASE_URL)


@pytest.fixture
def test_client():
    """Authenticated, authorized (admin) test client for the mounted data quality sub-app."""
    return _make_test_client(role="admin")


@pytest.fixture
def unauthorized_test_client():
    """Authenticated but role-less test client, for 403 coverage."""
    return _make_test_client(role=None)


@pytest.fixture
def unauthenticated_test_client():
    """No Authorization header at all, for 401 coverage."""
    with (
        conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
                ("core", "lazy_discover_providers"): "false",
            }
        ),
        mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False),
    ):
        purge_cached_app()
        app = create_app()
        yield TestClient(app, base_url=BASE_URL)


class TestHealthEndpointHTTP:
    """``/health`` needs no auth and no backend, so it's tested against the bare sub-app."""

    def test_health_returns_ok(self):
        client = TestClient(dataquality_app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


@pytest.mark.db_test
class TestEndpointAuthorization:
    """401/403 coverage for every route gated by ``requires_access_dag``."""

    ROUTES = (
        ("get", "/dataquality/v1/dags/orders_pipeline/tasks/dq/runs"),
        ("get", "/dataquality/v1/dags/orders_pipeline/tasks/dq/rules/r1/history"),
        ("get", "/dataquality/v1/dags/orders_pipeline/tasks/dq/runs/by_run/manual__2026-07-04"),
    )

    @pytest.mark.parametrize(("method", "path"), ROUTES)
    def test_401_unauthenticated(self, unauthenticated_test_client, method, path):
        response = getattr(unauthenticated_test_client, method)(path)
        assert response.status_code == 401

    @pytest.mark.parametrize(("method", "path"), ROUTES)
    def test_403_forbidden(self, unauthorized_test_client, method, path):
        response = getattr(unauthorized_test_client, method)(path)
        assert response.status_code == 403


@pytest.mark.db_test
class TestTaskRunsHTTP:
    def test_returns_persisted_runs(self, test_client, dq_backend):
        dq_backend.write_run(
            DQRun(
                dag_id="orders_pipeline",
                task_id="dq",
                run_id="manual__2026-07-04",
                started_at="2026-07-04T00:00:00+00:00",
            ),
            [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")],
        )

        response = test_client.get("/dataquality/v1/dags/orders_pipeline/tasks/dq/runs")

        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["run"]["dag_id"] == "orders_pipeline"
        assert data["items"][0]["summary"]["passed"] == 1
        assert data["next_cursor"] is None

    def test_before_cursor_pages_further_back(self, test_client, dq_backend):
        for day in range(1, 4):
            dq_backend.write_run(
                DQRun(
                    dag_id="orders_pipeline",
                    task_id="dq",
                    run_id=f"manual__2026-07-0{day}",
                    run_uid=f"run{day}",
                    started_at=f"2026-07-0{day}T00:00:00+00:00",
                ),
                [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")],
            )

        first_page = test_client.get(
            "/dataquality/v1/dags/orders_pipeline/tasks/dq/runs", params={"limit": 2}
        ).json()
        second_page = test_client.get(
            "/dataquality/v1/dags/orders_pipeline/tasks/dq/runs",
            params={"limit": 2, "before": first_page["next_cursor"]},
        ).json()

        assert [item["run"]["run_uid"] for item in first_page["items"]] == ["run3", "run2"]
        assert [item["run"]["run_uid"] for item in second_page["items"]] == ["run1"]
        assert second_page["next_cursor"] is None
        assert first_page["next_cursor"] == "2026-07-02T00:00:00+00:00|run2"

    def test_503_when_backend_unconfigured(self, test_client):
        with mock.patch(
            "airflow.providers.common.dataquality.api.app.get_backend_from_config", return_value=None
        ):
            response = test_client.get("/dataquality/v1/dags/orders_pipeline/tasks/dq/runs")
        assert response.status_code == 503

    def test_limit_out_of_range_is_422(self, test_client, dq_backend):
        response = test_client.get("/dataquality/v1/dags/orders_pipeline/tasks/dq/runs", params={"limit": 0})
        assert response.status_code == 422


@pytest.mark.db_test
class TestTaskRuleHistoryHTTP:
    def test_returns_rule_history_for_task(self, test_client, dq_backend):
        dq_backend.write_run(
            DQRun(
                dag_id="orders_pipeline",
                task_id="dq",
                run_id="manual__2026-07-04",
                started_at="2026-07-04T00:00:00+00:00",
            ),
            [RuleResult(rule_uid="r1", rule_name="nulls", status="fail")],
        )

        response = test_client.get("/dataquality/v1/dags/orders_pipeline/tasks/dq/rules/r1/history")

        assert response.status_code == 200
        data = response.json()
        assert data["items"][0]["rule_name"] == "nulls"
        assert data["items"][0]["run"]["task_id"] == "dq"
        assert data["next_cursor"] is None


@pytest.mark.db_test
class TestRunDetailByTaskInstanceHTTP:
    def test_returns_run_for_task_instance(self, test_client, dq_backend):
        dq_backend.write_run(
            DQRun(
                dag_id="orders_pipeline",
                task_id="dq",
                run_id="scheduled__2026-07-04",
                started_at="2026-07-04T00:00:00+00:00",
            ),
            [RuleResult(rule_uid="r1", rule_name="nulls", status="pass")],
        )

        response = test_client.get(
            "/dataquality/v1/dags/orders_pipeline/tasks/dq/runs/by_run/scheduled__2026-07-04"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["run"]["dag_id"] == "orders_pipeline"
        assert data["results"][0]["rule_name"] == "nulls"
        assert data["summary"]["passed"] == 1

    def test_404_when_run_not_found(self, test_client, dq_backend):
        response = test_client.get("/dataquality/v1/dags/orders_pipeline/tasks/dq/runs/by_run/does_not_exist")
        assert response.status_code == 404
