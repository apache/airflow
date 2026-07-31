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

import json
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.datamodels.task_state_store import (
    TaskStateStoreBody,
    TaskStateStorePatchBody,
)
from airflow.models.dagrun import DagRun
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "test_task"
LOGICAL_DATE = timezone.datetime(2026, 1, 1)
RUN_ID = DagRun.generate_run_id(run_type=DagRunType.MANUAL, logical_date=LOGICAL_DATE, run_after=LOGICAL_DATE)

BASE_URL = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/state-store"


def _create_dag_run(dag_maker, session):
    with dag_maker(DAG_ID, schedule=None, start_date=LOGICAL_DATE):
        EmptyOperator(task_id=TASK_ID)
    dag_maker.create_dagrun(run_id=RUN_ID, run_type=DagRunType.MANUAL, logical_date=LOGICAL_DATE)
    dag_maker.sync_dagbag_to_db()
    session.merge(dag_maker.dag_model)
    session.commit()


def _create_task_state_store_row(session, key: str, value: str, dag_run: DagRun) -> None:
    row = TaskStateStoreModel(
        dag_run_id=dag_run.id,
        dag_id=DAG_ID,
        run_id=RUN_ID,
        task_id=TASK_ID,
        map_index=-1,
        key=key,
        value=json.dumps(value),
    )
    session.add(row)
    session.flush()


class TestTaskStateEndpoint:
    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_dag_bundles()

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker, session):
        _create_dag_run(dag_maker, session)
        self.dag_run = session.scalar(select(DagRun).where(DagRun.run_id == RUN_ID))
        self._session = session

    def teardown_method(self):
        self.clear_db()


class TestListTaskState(TestTaskStateEndpoint):
    def test_returns_empty_list_when_no_state(self, test_client):
        response = test_client.get(BASE_URL)
        assert response.status_code == 200
        assert response.json() == {"task_state_store": [], "total_entries": 0}

    def test_returns_all_keys(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "spark_001", self.dag_run)
        _create_task_state_store_row(self._session, "checkpoint", "step_3", self.dag_run)
        self._session.commit()

        response = test_client.get(BASE_URL)
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 2
        keys = {item["key"]: item["value"] for item in data["task_state_store"]}
        assert keys == {"job_id": "spark_001", "checkpoint": "step_3"}

    def test_returns_state_metadata_fields(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "spark_001", self.dag_run)
        self._session.commit()

        response = test_client.get(BASE_URL)
        item = response.json()["task_state_store"][0]
        assert "updated_at" in item
        assert "expires_at" in item

    def test_map_index_isolation(self, test_client):
        """map_index=-1 (default) doesn't return rows for other map indices."""
        row = TaskStateStoreModel(
            dag_run_id=self.dag_run.id,
            dag_id=DAG_ID,
            run_id=RUN_ID,
            task_id=TASK_ID,
            map_index=0,
            key="job_id",
            value=json.dumps("mapped_app"),
        )
        self._session.add(row)
        self._session.commit()

        response = test_client.get(BASE_URL)
        assert response.json()["total_entries"] == 0

    def test_pagination_limit(self, test_client):
        for k in ("a", "b", "c"):
            _create_task_state_store_row(self._session, k, "v", self.dag_run)
        self._session.commit()

        response = test_client.get(f"{BASE_URL}?limit=2")
        data = response.json()
        assert data["total_entries"] == 3
        assert len(data["task_state_store"]) == 2

    def test_pagination_offset(self, test_client):
        for k in ("a", "b", "c"):
            _create_task_state_store_row(self._session, k, "v", self.dag_run)
        self._session.commit()

        response = test_client.get(f"{BASE_URL}?limit=2&offset=2")
        data = response.json()
        assert data["total_entries"] == 3
        assert len(data["task_state_store"]) == 1

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(BASE_URL).status_code == 401


class TestGetTaskState(TestTaskStateEndpoint):
    def test_returns_value(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "spark_001", self.dag_run)
        self._session.commit()

        response = test_client.get(f"{BASE_URL}/job_id")
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "job_id"
        assert data["value"] == "spark_001"

    def test_missing_key_returns_404(self, test_client):
        response = test_client.get(f"{BASE_URL}/nonexistent")
        assert response.status_code == 404

    def test_key_with_slash_is_supported(self, test_client):
        """Keys containing slashes must work — route uses {key:path}."""
        _create_task_state_store_row(self._session, "workflow/step_1", "v", self.dag_run)
        self._session.commit()

        response = test_client.get(f"{BASE_URL}/workflow/step_1")
        assert response.status_code == 200
        assert response.json()["key"] == "workflow/step_1"

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(f"{BASE_URL}/job_id").status_code == 401


class TestSetTaskState(TestTaskStateEndpoint):
    def test_creates_new_key(self, test_client):
        response = test_client.put(f"{BASE_URL}/job_id", json={"value": "spark_001"})
        assert response.status_code == 204

        get_resp = test_client.get(f"{BASE_URL}/job_id")
        assert get_resp.json()["value"] == "spark_001"

    def test_overwrites_existing_key(self, test_client):
        test_client.put(f"{BASE_URL}/job_id", json={"value": "v1"})
        test_client.put(f"{BASE_URL}/job_id", json={"value": "v2"})

        assert test_client.get(f"{BASE_URL}/job_id").json()["value"] == "v2"

    def test_empty_body_returns_422(self, test_client):
        assert test_client.put(f"{BASE_URL}/job_id", json={}).status_code == 422

    def test_null_value_returns_422(self, test_client):
        assert test_client.put(f"{BASE_URL}/job_id", json={"value": None}).status_code == 422

    def test_oversized_value_returns_422(self, test_client):
        assert test_client.put(f"{BASE_URL}/job_id", json={"value": "x" * 65536}).status_code == 422

    @pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), {"a": float("nan")}, [float("inf")]])
    def test_non_finite_float_rejected_by_validator(self, bad_value):
        with pytest.raises(ValidationError, match="non-finite"):
            TaskStateStoreBody(value=bad_value)

    def test_set_nonexistent_dag_run_returns_404(self, test_client):
        """set() raises ValueError when DagRun doesn't exist — should surface as 404."""
        bad_url = f"/dags/{DAG_ID}/dagRuns/nonexistent_run/taskInstances/{TASK_ID}/state-store/job_id"
        response = test_client.put(bad_url, json={"value": "v"})
        assert response.status_code == 404

    def test_set_nonexistent_task_id_returns_404(self, test_client):
        """set() returns 404 when task_id doesn not match any TaskInstance in the run."""
        bad_url = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/nonexistent_task/state-store/job_id"
        response = test_client.put(bad_url, json={"value": "v"})
        assert response.status_code == 404

    @pytest.mark.parametrize(
        ("value", "expected_db"),
        [
            (42, "42"),
            ("hello", '"hello"'),
            ({"k": 1}, '{"k": 1}'),
            ([1, 2], "[1, 2]"),
        ],
    )
    def test_put_stores_json_encoded_value(self, test_client, value, expected_db):
        test_client.put(f"{BASE_URL}/k", json={"value": value})
        row = self._session.scalar(
            select(TaskStateStoreModel).where(
                TaskStateStoreModel.dag_id == DAG_ID,
                TaskStateStoreModel.run_id == RUN_ID,
                TaskStateStoreModel.task_id == TASK_ID,
                TaskStateStoreModel.key == "k",
            )
        )
        assert row is not None
        assert row.value == expected_db

    @pytest.mark.parametrize("value", [42, True, {"rows": 100}, [1, "two"], "hello"])
    def test_core_api_write_read_roundtrip(self, test_client, value):
        """Core API write then Core API read returns the same native value."""
        test_client.put(f"{BASE_URL}/k", json={"value": value})
        assert test_client.get(f"{BASE_URL}/k").json()["value"] == value

    @pytest.mark.parametrize("value", [42, True, {"rows": 100}, [1, "two"], "hello"])
    def test_worker_write_core_api_read_roundtrip(self, test_client, value):
        """Worker write (json.dumps in DB) then Core API read returns native value."""
        _create_task_state_store_row(self._session, "k", value, self.dag_run)
        self._session.commit()
        assert test_client.get(f"{BASE_URL}/k").json()["value"] == value

    def test_key_with_slash_is_supported(self, test_client):
        response = test_client.put(f"{BASE_URL}/workflow/step_1", json={"value": "v"})
        assert response.status_code == 204
        assert test_client.get(f"{BASE_URL}/workflow/step_1").json()["key"] == "workflow/step_1"

    def test_new_key_default_retention_applies_config(self, test_client, time_machine):
        time_machine.move_to("2026-01-01T00:00:00+00:00", tick=False)
        with conf_vars({("state_store", "default_retention_days"): "7"}):
            test_client.put(f"{BASE_URL}/job_id", json={"value": "v", "expires_at": "default"})

        resp = test_client.get(f"{BASE_URL}/job_id").json()
        assert resp["expires_at"] == "2026-01-08T00:00:00Z"

    def test_new_key_default_retention_zero_never_expires(self, test_client):
        """PUT with expires_at=default and default_retention_days=0 stores a key that never expires."""
        with conf_vars({("state_store", "default_retention_days"): "0"}):
            test_client.put(f"{BASE_URL}/job_id", json={"value": "v", "expires_at": "default"})
        assert test_client.get(f"{BASE_URL}/job_id").json()["expires_at"] is None

    def test_new_key_negative_retention_days_returns_400(self, test_client):
        """PUT with expires_at=default and default_retention_days<0 returns HTTP 400."""
        with conf_vars({("state_store", "default_retention_days"): "-1"}):
            resp = test_client.put(f"{BASE_URL}/job_id", json={"value": "v", "expires_at": "default"})
        assert resp.status_code == 400
        assert "default_retention_days" in resp.json()["detail"]

    def test_new_key_never_expiry(self, test_client):
        """PUT with expires_at=null stores a key that never expires."""
        test_client.put(f"{BASE_URL}/job_id", json={"value": "v", "expires_at": None})
        assert test_client.get(f"{BASE_URL}/job_id").json()["expires_at"] is None

    def test_new_key_explicit_expiry(self, test_client, time_machine):
        """PUT with an explicit datetime uses that as expires_at."""
        time_machine.move_to("2026-01-01T00:00:00+00:00", tick=False)
        target = "2026-01-31T00:00:00Z"
        test_client.put(f"{BASE_URL}/job_id", json={"value": "v", "expires_at": target})
        assert test_client.get(f"{BASE_URL}/job_id").json()["expires_at"] == target

    def test_put_overwrites_expiry_on_existing_key(self, test_client, time_machine):
        """PUT on an existing key replaces expires_at with whatever the body specifies."""
        time_machine.move_to("2026-01-01T00:00:00+00:00", tick=False)
        test_client.put(f"{BASE_URL}/job_id", json={"value": "v1", "expires_at": "2026-01-31T00:00:00Z"})

        # second request but with null expires_at
        test_client.put(f"{BASE_URL}/job_id", json={"value": "v2", "expires_at": None})

        resp = test_client.get(f"{BASE_URL}/job_id").json()
        assert resp["value"] == "v2"
        assert resp["expires_at"] is None

    def test_put_value_over_limit_returns_422(self, test_client):
        # default is 65535 bytes
        big = "x" * 70000
        resp = test_client.put(f"{BASE_URL}/job_id", json={"value": big})
        assert resp.status_code == 422

    @conf_vars({("state_store", "max_value_storage_bytes"): "0"})
    def test_put_value_over_limit_accepted_when_limit_disabled(self, test_client):
        big = "x" * 70000
        resp = test_client.put(f"{BASE_URL}/job_id", json={"value": big})
        assert resp.status_code == 204

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.put(f"{BASE_URL}/job_id", json={"value": "v"}).status_code == 401


class TestPatchTaskState(TestTaskStateEndpoint):
    def test_patch_updates_value(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "v1", self.dag_run)
        self._session.commit()

        assert test_client.patch(f"{BASE_URL}/job_id", json={"value": "v2"}).status_code == 200
        row = self._session.scalar(
            select(TaskStateStoreModel).where(
                TaskStateStoreModel.dag_id == DAG_ID,
                TaskStateStoreModel.run_id == RUN_ID,
                TaskStateStoreModel.task_id == TASK_ID,
                TaskStateStoreModel.key == "job_id",
            )
        )
        assert row.value == '"v2"'

    def test_patch_missing_key_returns_404(self, test_client):
        assert test_client.patch(f"{BASE_URL}/nonexistent", json={"value": "v"}).status_code == 404

    def test_patch_empty_body_returns_422(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "v", self.dag_run)
        self._session.commit()
        assert test_client.patch(f"{BASE_URL}/job_id", json={}).status_code == 422

    def test_patch_null_value_returns_422(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "v", self.dag_run)
        self._session.commit()
        assert test_client.patch(f"{BASE_URL}/job_id", json={"value": None}).status_code == 422

    def test_patch_value_over_limit_returns_422(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "v", self.dag_run)
        self._session.commit()
        big = "x" * 70000
        assert test_client.patch(f"{BASE_URL}/job_id", json={"value": big}).status_code == 422

    @conf_vars({("state_store", "max_value_storage_bytes"): "0"})
    def test_patch_value_over_limit_accepted_when_limit_disabled(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "v", self.dag_run)
        self._session.commit()
        big = "x" * 70000
        assert test_client.patch(f"{BASE_URL}/job_id", json={"value": big}).status_code == 200

    @pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), {"a": float("nan")}, [float("inf")]])
    def test_patch_non_finite_float_rejected_by_validator(self, bad_value):
        with pytest.raises(ValidationError, match="non-finite"):
            TaskStateStorePatchBody(value=bad_value)

    @pytest.mark.parametrize(
        ("value", "expected_db"),
        [
            (42, "42"),
            ("hello", '"hello"'),
            ({"k": 1}, '{"k": 1}'),
            ([1, 2], "[1, 2]"),
        ],
    )
    def test_patch_stores_json_encoded_value(self, test_client, value, expected_db):
        _create_task_state_store_row(self._session, "job_id", "initial", self.dag_run)
        self._session.commit()
        test_client.patch(f"{BASE_URL}/job_id", json={"value": value})
        row = self._session.scalar(
            select(TaskStateStoreModel).where(
                TaskStateStoreModel.dag_id == DAG_ID,
                TaskStateStoreModel.run_id == RUN_ID,
                TaskStateStoreModel.task_id == TASK_ID,
                TaskStateStoreModel.key == "job_id",
            )
        )
        self._session.refresh(row)
        assert row.value == expected_db

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.patch(f"{BASE_URL}/job_id", json={"value": "v"}).status_code == 401


class TestDeleteTaskState(TestTaskStateEndpoint):
    def test_deletes_key(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "spark_001", self.dag_run)
        self._session.commit()

        assert test_client.delete(f"{BASE_URL}/job_id").status_code == 204
        assert test_client.get(f"{BASE_URL}/job_id").status_code == 404

    def test_delete_noop_for_missing_key(self, test_client):
        assert test_client.delete(f"{BASE_URL}/nonexistent").status_code == 204

    def test_only_deletes_target_key(self, test_client):
        _create_task_state_store_row(self._session, "job_id", "a", self.dag_run)
        _create_task_state_store_row(self._session, "checkpoint", "b", self.dag_run)
        self._session.commit()

        test_client.delete(f"{BASE_URL}/job_id")

        assert test_client.get(f"{BASE_URL}/job_id").status_code == 404
        assert test_client.get(f"{BASE_URL}/checkpoint").json()["value"] == "b"

    def test_key_with_slash_is_supported(self, test_client):
        _create_task_state_store_row(self._session, "workflow/step_1", "v", self.dag_run)
        self._session.commit()

        assert test_client.delete(f"{BASE_URL}/workflow/step_1").status_code == 204
        assert test_client.get(f"{BASE_URL}/workflow/step_1").status_code == 404

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.delete(f"{BASE_URL}/job_id").status_code == 401


class TestClearTaskState(TestTaskStateEndpoint):
    def test_clears_all_keys(self, test_client):
        for k, v in [("job_id", "a"), ("checkpoint", "b"), ("retry_count", "c")]:
            _create_task_state_store_row(self._session, k, v, self.dag_run)
        self._session.commit()

        assert test_client.delete(BASE_URL).status_code == 204
        assert test_client.get(BASE_URL).json()["total_entries"] == 0

    def test_all_map_indices_clears_across_mapped_instances(self, test_client):
        """all_map_indices=true wipes state for every map index of the task."""
        for map_index in (-1, 0, 1):
            row = TaskStateStoreModel(
                dag_run_id=self.dag_run.id,
                dag_id=DAG_ID,
                run_id=RUN_ID,
                task_id=TASK_ID,
                map_index=map_index,
                key="job_id",
                value=json.dumps(f"app_{map_index}"),
            )
            self._session.add(row)
        self._session.commit()

        # Default clear only wipes map_index=-1
        assert test_client.delete(BASE_URL).status_code == 204
        # map_index=0 and map_index=1 rows still exist
        assert test_client.get(f"{BASE_URL}?map_index=0").json()["total_entries"] == 1
        assert test_client.get(f"{BASE_URL}?map_index=1").json()["total_entries"] == 1

        # all_map_indices=true wipes everything
        assert test_client.delete(f"{BASE_URL}?all_map_indices=true").status_code == 204
        assert test_client.get(f"{BASE_URL}?map_index=0").json()["total_entries"] == 0
        assert test_client.get(f"{BASE_URL}?map_index=1").json()["total_entries"] == 0

    def test_key_with_slash_is_supported(self, test_client):
        _create_task_state_store_row(self._session, "workflow/step_1", "v", self.dag_run)
        self._session.commit()

        assert test_client.delete(BASE_URL).status_code == 204
        assert test_client.get(BASE_URL).json()["total_entries"] == 0

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.delete(BASE_URL).status_code == 401


class TestRoutesNeverCallCustomBackend(TestTaskStateEndpoint):
    """Tests to validate that core API routes must use MetastoreStateBackend directly."""

    @pytest.mark.parametrize(
        ("method", "path", "kwargs"),
        [
            ("get", BASE_URL, {}),
            ("get", f"{BASE_URL}/job_id", {}),
            ("put", f"{BASE_URL}/job_id", {"json": {"value": "v2"}}),
            ("patch", f"{BASE_URL}/job_id", {"json": {"value": "v3"}}),
            ("delete", f"{BASE_URL}/job_id", {}),
            ("delete", BASE_URL, {}),
        ],
    )
    def test_route_never_calls_get_state_backend(self, test_client, method, path, kwargs):
        _create_task_state_store_row(self._session, "job_id", "v1", self.dag_run)
        self._session.commit()

        with patch("airflow.state.get_state_backend") as mock_get_backend:
            getattr(test_client, method)(path, **kwargs)

        mock_get_backend.assert_not_called()
