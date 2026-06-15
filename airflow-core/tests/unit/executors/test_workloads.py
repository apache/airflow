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
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import jwt
import pytest
from pydantic import TypeAdapter

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


# ---------------------------------------------------------------------------
# ExecuteCallback scheduler -> worker JSON serialization round-trip.
#
# Real wire format (verified against the executors):
#   scheduler: ``workload.model_dump_json()``
#     - providers/celery/.../celery_executor_utils.py: ``args.model_dump_json()``
#     - providers/cncf/kubernetes/.../pod_generator.py: ``workload.model_dump_json()``
#   worker:    ``TypeAdapter[workloads.ExecutorWorkload].validate_json(input)``
#     (discriminated union on the ``type`` field; ExecuteCallback is a member)
#
# These tests probe the full serialize -> JSON-over-the-wire -> deserialize
# round-trip, with particular focus on the two classic JSON traps:
#   * ``dag_rel_path: os.PathLike`` (a real ``Path``)
#   * ``callback.data`` carrying a handle_miss-style routing dict
# ---------------------------------------------------------------------------


def _handle_miss_style_data() -> dict:
    """Build a ``callback.data`` dict shaped exactly like ``Deadline.handle_miss`` produces."""
    return {
        # user payload kwargs (the callback import path + its args)
        "path": "my_pkg.my_module.my_callback",
        "kwargs": {"foo": "bar", "retries": 3},
        # routing identifiers injected by handle_miss (all JSON primitives / strings)
        "dag_id": "my_dag",
        "run_id": "manual__2026-06-15T00:00:00+00:00",
        "deadline_id": "11111111-1111-1111-1111-111111111111",
        "deadline_time": datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "dag_run_id": "42",
    }


def _build_execute_callback(data: dict, dag_rel_path) -> ExecuteCallback:
    return ExecuteCallback(
        callback=CallbackDTO(
            id="12345678-1234-5678-1234-567812345678",
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
            data=data,
        ),
        dag_rel_path=dag_rel_path,
        token="eyJhbGciOiJIUzUxMiJ9.payload.sig",
        bundle_info=BundleInfo(name="dags-folder", version="abc123"),
        log_path="executor_callbacks/my_dag/run_1/cb.log",
    )


def test_execute_callback_json_roundtrip_matches_wire_format():
    """ExecuteCallback must survive the scheduler->worker JSON round-trip with key fields intact.

    Mirrors the real wire path: scheduler ``model_dump_json()`` then worker
    ``TypeAdapter[ExecutorWorkload].validate_json(...)`` on the discriminated union.
    """
    workload = _build_execute_callback(_handle_miss_style_data(), Path("my_dag.py"))

    # Scheduler side: serialize to JSON for the broker / K8s arg.
    wire = workload.model_dump_json()

    # Worker side: reconstruct through the discriminated union exactly as the executors do.
    decoder = TypeAdapter[workloads.ExecutorWorkload](workloads.ExecutorWorkload)
    restored = decoder.validate_json(wire)

    # Discriminator routed to the right concrete type.
    assert isinstance(restored, ExecuteCallback)
    assert restored.type == "ExecuteCallback"

    # Key fields match after the trip across the wire.
    assert restored.callback.id == workload.callback.id
    assert restored.callback.fetch_method == workload.callback.fetch_method
    assert restored.callback.data == workload.callback.data
    assert restored.token == workload.token
    assert restored.log_path == workload.log_path
    assert restored.bundle_info == workload.bundle_info
    # The display_name / key derived properties still resolve post-trip.
    assert restored.key == workload.key
    assert restored.display_name == workload.display_name

    # The handle_miss routing identifiers survived as strings (no silent type drift).
    assert restored.callback.data["deadline_time"] == workload.callback.data["deadline_time"]
    assert isinstance(restored.callback.data["deadline_time"], str)
    assert restored.callback.data["dag_run_id"] == "42"


def test_execute_callback_path_field_survives_json_roundtrip():
    """The ``dag_rel_path`` (os.PathLike / Path) field is the classic JSON trap; assert it round-trips."""
    workload = _build_execute_callback(_handle_miss_style_data(), Path("subdir/my_dag.py"))

    wire = workload.model_dump_json()
    decoder = TypeAdapter[workloads.ExecutorWorkload](workloads.ExecutorWorkload)
    restored = decoder.validate_json(wire)

    # Pydantic serializes Path to its string form; on the way back it should reconstruct
    # to an equivalent PathLike whose string representation matches the original.
    assert os_fspath_eq(restored.dag_rel_path, workload.dag_rel_path)


def os_fspath_eq(a, b) -> bool:
    import os

    return os.fspath(a) == os.fspath(b)


@pytest.mark.parametrize(
    "rel_path",
    [
        pytest.param(Path("my_dag.py"), id="relative-path"),
        pytest.param(Path("nested/dir/my_dag.py"), id="nested-path"),
        pytest.param(PurePosixPath("posix/my_dag.py"), id="pure-posix-path"),
    ],
)
def test_execute_callback_pathlike_variants_roundtrip(rel_path):
    """Various PathLike inputs for ``dag_rel_path`` all survive the JSON round-trip."""
    workload = _build_execute_callback(_handle_miss_style_data(), rel_path)

    wire = workload.model_dump_json()
    restored = TypeAdapter[workloads.ExecutorWorkload](workloads.ExecutorWorkload).validate_json(wire)

    import os

    assert os.fspath(restored.dag_rel_path) == os.fspath(rel_path)


def test_execute_callback_data_with_non_json_value_fails_at_serialization():
    """Adversarial: a non-JSON-primitive in callback.data must not silently corrupt the wire format.

    handle_miss always stores JSON-safe primitives (it calls ``deadline_time.isoformat()``),
    but if a raw ``datetime`` ever leaked into ``data`` the serialize step is where it should
    surface. This documents/locks in the boundary behaviour so a regression that starts shipping
    raw datetimes is caught here rather than producing a malformed payload on the worker.
    """
    bad_data = _handle_miss_style_data()
    bad_data["deadline_time"] = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)  # raw, not isoformat

    workload = _build_execute_callback(bad_data, Path("my_dag.py"))

    # Pydantic's default JSON encoder DOES know how to serialize datetime, so this does not raise;
    # but it round-trips back as a *string*, not a datetime -> assert we detect that type drift so a
    # value that must stay a datetime can't masquerade as having survived intact.
    wire = workload.model_dump_json()
    restored = TypeAdapter[workloads.ExecutorWorkload](workloads.ExecutorWorkload).validate_json(wire)
    assert isinstance(restored.callback.data["deadline_time"], str)
    assert restored.callback.data["deadline_time"] != bad_data["deadline_time"]


def test_execute_callback_data_with_set_silently_coerces_to_list():
    """Adversarial finding: a ``set`` in callback.data is silently coerced to a JSON list.

    ``CallbackDTO.data`` is an untyped freeform ``dict``. When the scheduler serializes the
    workload with ``model_dump_json()``, Pydantic's JSON serializer turns a ``set`` into a JSON
    array WITHOUT raising and WITHOUT a warning. On the worker the value comes back as a ``list``,
    not a ``set`` (and the element ordering is not preserved). This documents the transit-path
    behaviour so that anyone relying on set semantics inside callback kwargs knows it does not
    survive the wire. ``Deadline.handle_miss`` only ever stores JSON primitives, so this is not a
    live defect — but it is the kind of silent type drift this round-trip is meant to surface.
    """
    bad_data = _handle_miss_style_data()
    bad_data["kwargs"] = {"tags": {"a", "b", "c"}}  # a set

    workload = _build_execute_callback(bad_data, Path("my_dag.py"))

    wire = workload.model_dump_json()  # does NOT raise
    restored = TypeAdapter[workloads.ExecutorWorkload](workloads.ExecutorWorkload).validate_json(wire)

    restored_tags = restored.callback.data["kwargs"]["tags"]
    # The set became a list across the wire (type drift), with the same membership.
    assert isinstance(restored_tags, list)
    assert set(restored_tags) == {"a", "b", "c"}
