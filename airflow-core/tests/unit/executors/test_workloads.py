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

import dataclasses
from pathlib import PurePosixPath
from uuid import uuid4

import jwt
import pytest

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.executors import workloads
from airflow.executors.workloads import TaskInstance, TaskInstanceDTO, base as workloads_base
from airflow.executors.workloads.base import BaseWorkloadSchema, BundleInfo
from airflow.executors.workloads.callback import CallbackDTO, CallbackFetchMethod, ExecuteCallback
from airflow.executors.workloads.task import ExecuteTask
from airflow.executors.workloads.types import state_class_for_key
from airflow.models.callback import CallbackKey
from airflow.sdk.api.datamodels._generated import TaskInstance as GeneratedTaskInstance


def test_task_instance_alias_keeps_backwards_compat():
    assert TaskInstance is TaskInstanceDTO
    assert workloads.TaskInstance is TaskInstanceDTO
    assert workloads.TaskInstanceDTO is TaskInstanceDTO


def test_token_excluded_from_workload_repr():
    """Ensure JWT tokens do not leak into log output via repr()."""
    fake_token = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.secret_payload.signature"
    ti = TaskInstanceDTO(
        id=uuid4(),
        dag_version_id=uuid4(),
        task_id="test_task",
        dag_id="test_dag",
        run_id="test_run",
        try_number=1,
        map_index=-1,
        pool_slots=1,
        queue="default",
        priority_weight=1,
    )
    workload = ExecuteTask(
        ti=ti,
        dag_rel_path=PurePosixPath("test_dag.py"),
        token=fake_token,
        bundle_info=BundleInfo(name="dags-folder", version=None),
        log_path="test.log",
    )

    workload_repr = repr(workload)

    # Token MUST NOT appear in repr (prevents leaking into logs)
    assert fake_token not in workload_repr, f"JWT token leaked into repr! Found token in: {workload_repr}"
    # But token should still be accessible as an attribute
    assert workload.token == fake_token


def test_generate_token_produces_workload_scope(monkeypatch):
    """generate_token should create a JWT with scope 'workload' and [scheduler] task_queued_timeout expiry."""
    monkeypatch.setattr(workloads_base.conf, "getfloat", lambda section, key: 86400.0)

    generator = JWTGenerator(secret_key="test-secret", audience="test", valid_for=60)
    token = BaseWorkloadSchema.generate_token("ti-123", generator)

    claims = jwt.decode(token, "test-secret", algorithms=["HS512"], audience="test")
    assert claims["sub"] == "ti-123"
    assert claims["scope"] == "workload"
    assert claims["exp"] - claims["iat"] == 86400


def test_generate_token_without_generator():
    """generate_token should return empty string when no generator is provided."""
    assert BaseWorkloadSchema.generate_token("ti-123", None) == ""


def test_callback_key_is_frozen_and_hashable():
    """CallbackKey must be usable as a dict key (hashable) and immutable (frozen)."""
    cid = "some-uuid-value"

    key = CallbackKey(id=cid)
    assert hash(key) == hash(CallbackKey(id=cid))
    assert key == CallbackKey(id=cid)
    assert key != CallbackKey(id="other")

    # Frozen: assignment raises
    with pytest.raises(dataclasses.FrozenInstanceError):
        key.id = "mutated"  # type: ignore[misc]


def test_callback_key_str_returns_id():
    """str(CallbackKey) should return the raw id string."""
    cid = "some-uuid-value"

    key = CallbackKey(id=cid)
    assert str(key) == cid


def test_callback_key_is_not_a_string():
    """CallbackKey must NOT pass isinstance(x, str)."""

    key = CallbackKey(id="some-uuid-value")
    assert not isinstance(key, str)


def test_state_class_for_key_raises_on_unknown_type():
    """state_class_for_key should raise TypeError for unrecognized key types."""

    with pytest.raises(TypeError, match="Unknown workload key type"):
        state_class_for_key("bare-string-is-not-a-key")  # type: ignore[arg-type]


def test_callback_dto_key_returns_callback_key_instance():
    """CallbackDTO.key should return a CallbackKey, not a bare string."""
    cid = "some-uuid-value"

    callback = CallbackDTO(id=cid, fetch_method=CallbackFetchMethod.IMPORT_PATH, data={})
    key = callback.key
    assert isinstance(key, CallbackKey)
    assert key.id == cid
    assert str(key) == cid


def test_workload_ti_round_trips_through_sdk_generated_model():
    """
    The executor-side DTO and the SDK's generated TaskInstance share the
    execution API schema; the serialized workload must carry the routing
    fields and exclude the executor-only ones.
    """
    ti = TaskInstanceDTO(
        id=uuid4(),
        dag_version_id=uuid4(),
        task_id="test_task",
        dag_id="test_dag",
        run_id="test_run",
        try_number=2,
        map_index=3,
        pool_slots=4,
        queue="jdk-17",
        priority_weight=5,
        external_executor_id="celery-id",
        executor_config={"KubernetesExecutor": {"image": "custom"}},
    )

    dumped = ti.model_dump(mode="json")
    assert "external_executor_id" not in dumped
    assert "executor_config" not in dumped
    # Executor-side scheduling fields stay on the workload wire (older workers
    # deserialize the workload with a model that requires them) but are not
    # part of the worker-facing schema.
    assert dumped["pool_slots"] == 4
    assert dumped["priority_weight"] == 5

    received = GeneratedTaskInstance.model_validate(dumped)
    assert received.queue == "jdk-17"
    assert received.map_index == 3
    assert not hasattr(received, "pool_slots")


class TestExecuteTaskMakeVersionData:
    """Tests for ExecuteTask.make() threading version_data through BundleInfo."""

    @pytest.fixture(autouse=True)
    def _stub_log_template(self, monkeypatch):
        monkeypatch.setattr(
            "airflow.utils.helpers.log_filename_template_renderer",
            lambda: lambda **kwargs: "test.log",
        )

    @staticmethod
    def _make_mock_ti(
        bundle_version,
        version_data,
        *,
        has_created_dag_version=True,
        ti_dag_version_data=None,
    ):
        """Build a mock TI with the attributes ExecuteTask.make() reads.

        ``version_data`` is the manifest on the run's pinned version
        (``dag_run.created_dag_version``) -- the source make() must use.
        ``ti_dag_version_data`` is the manifest on ``ti.dag_version``, which make()
        must IGNORE (it can diverge from the run's pin after a mid-run DAG re-parse).
        ``has_created_dag_version`` toggles whether the run has a pinned DagVersion
        (legacy/backfilled runs may not).
        """
        from unittest.mock import Mock

        ti = Mock()
        ti.id = uuid4()
        ti.dag_version_id = uuid4()
        ti.task_id = "test_task"
        ti.dag_id = "test_dag"
        ti.run_id = "test_run"
        ti.try_number = 1
        ti.map_index = -1
        ti.pool_slots = 1
        ti.queue = "default"
        ti.priority_weight = 1
        ti.executor_config = None
        ti.parent_context_carrier = None
        ti.context_carrier = None
        ti.hostname = None
        ti.external_executor_id = None

        ti.dag_model.bundle_name = "test-bundle"
        ti.dag_model.relative_fileloc = "dags/test_dag.py"

        ti.dag_run.bundle_version = bundle_version

        # make() must source version_data from the run's pinned version, never ti.dag_version.
        ti.dag_version.version_data = ti_dag_version_data

        if has_created_dag_version:
            ti.dag_run.created_dag_version.version_data = version_data
        else:
            ti.dag_run.created_dag_version = None

        return ti

    def test_pinned_run_populates_version_data(self):
        """When the run is pinned, version_data from the run's created_dag_version flows to BundleInfo."""
        version_data = {"schema_version": 1, "files": {"dags/my_dag.py": "ver123"}}
        ti = self._make_mock_ti(bundle_version="abc123", version_data=version_data)

        workload = ExecuteTask.make(ti)

        assert workload.bundle_info.version == "abc123"
        assert workload.bundle_info.version_data == version_data

    def test_unpinned_run_suppresses_present_version_data(self):
        """An unpinned run must not expose version_data even when created_dag_version carries it."""
        version_data = {"schema_version": 1, "files": {"dags/my_dag.py": "ver123"}}
        ti = self._make_mock_ti(bundle_version=None, version_data=version_data)

        workload = ExecuteTask.make(ti)

        assert workload.bundle_info.version is None
        assert workload.bundle_info.version_data is None

    def test_missing_created_dag_version_yields_none(self):
        """A pinned run whose DagRun has no created_dag_version yields no version_data."""
        ti = self._make_mock_ti(bundle_version="abc123", version_data=None, has_created_dag_version=False)

        workload = ExecuteTask.make(ti)

        assert workload.bundle_info.version == "abc123"
        assert workload.bundle_info.version_data is None

    def test_mid_run_dag_version_bump_uses_run_pinned_manifest(self):
        """Regression: a mid-run DAG re-parse can bump ti.dag_version to a newer version while the
        run stays pinned. make() must ship the run's pinned manifest (created_dag_version), not the
        TI's bumped one -- otherwise a versioned bundle would fetch the wrong (latest) code.
        """
        run_manifest = {"schema_version": 1, "files": {"dags/my_dag.py": "v1-object-id"}}
        bumped_manifest = {"schema_version": 1, "files": {"dags/my_dag.py": "v2-object-id"}}
        ti = self._make_mock_ti(
            bundle_version="v1hash",
            version_data=run_manifest,
            ti_dag_version_data=bumped_manifest,
        )

        workload = ExecuteTask.make(ti)

        assert workload.bundle_info.version == "v1hash"
        assert workload.bundle_info.version_data == run_manifest
        assert workload.bundle_info.version_data != bumped_manifest


class TestExecuteCallbackMakeVersionData:
    """Tests for ExecuteCallback.make() threading version_data through BundleInfo."""

    @staticmethod
    def _make_mocks(bundle_version, version_data, *, has_created_dag_version=True):
        """Build mock Callback + DagRun with the attributes ExecuteCallback.make() reads."""
        from unittest.mock import Mock

        callback = Mock()
        callback.id = uuid4()
        callback.fetch_method = CallbackFetchMethod.IMPORT_PATH
        callback.data = {"path": "my_module.my_callback"}

        dag_run = Mock()
        dag_run.dag_id = "test_dag"
        dag_run.run_id = "test_run"
        dag_run.bundle_version = bundle_version
        dag_run.dag_model.bundle_name = "test-bundle"
        dag_run.dag_model.relative_fileloc = "dags/test_dag.py"
        if has_created_dag_version:
            dag_run.created_dag_version.version_data = version_data
        else:
            dag_run.created_dag_version = None

        return callback, dag_run

    def test_pinned_run_populates_version_data(self):
        """When the run is pinned, version_data from created_dag_version flows to BundleInfo."""
        version_data = {"schema_version": 1, "files": {"dags/my_dag.py": "ver123"}}
        callback, dag_run = self._make_mocks(bundle_version="abc123", version_data=version_data)

        workload = ExecuteCallback.make(callback=callback, dag_run=dag_run)

        assert workload.bundle_info.version == "abc123"
        assert workload.bundle_info.version_data == version_data

    def test_unpinned_run_suppresses_present_version_data(self):
        """An unpinned run must not expose version_data even when created_dag_version carries it."""
        version_data = {"schema_version": 1, "files": {"dags/my_dag.py": "ver123"}}
        callback, dag_run = self._make_mocks(bundle_version=None, version_data=version_data)

        workload = ExecuteCallback.make(callback=callback, dag_run=dag_run)

        assert workload.bundle_info.version is None
        assert workload.bundle_info.version_data is None

    def test_missing_created_dag_version_yields_none(self):
        """A pinned run without a created_dag_version yields no version_data."""
        callback, dag_run = self._make_mocks(
            bundle_version="abc123", version_data=None, has_created_dag_version=False
        )

        workload = ExecuteCallback.make(callback=callback, dag_run=dag_run)

        assert workload.bundle_info.version == "abc123"
        assert workload.bundle_info.version_data is None
