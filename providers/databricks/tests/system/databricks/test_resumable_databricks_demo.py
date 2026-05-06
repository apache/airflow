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
"""
Layer 1 e2e — unit test for ResumableDatabricksSubmitRunOperator (AIP-96 demo).

Mock-based: no real Databricks workspace required. Asserts the resume contract:

  - First execute(): submit_run called, run_id stored in task_state.
  - SIGTERM during poll raises AirflowTaskCheckpointed.
  - Second execute() (after CHECKPOINTED): submit_run NOT called; prior run_id reused.
  - Success path: task_state cleared.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.sdk.exceptions import AirflowTaskCheckpointed

from system.databricks.example_resumable_databricks import (
    ResumableDatabricksSubmitRunOperator,
)


class _FakeTaskState:
    """In-memory task_state for tests."""

    def __init__(self):
        self._d: dict[str, object] = {}

    def get(self, key):
        return self._d.get(key)

    def set(self, key, value):
        self._d[key] = value

    def delete(self, key):
        self._d.pop(key, None)


def _make_op(**kwargs):
    return ResumableDatabricksSubmitRunOperator(
        task_id="resumable_databricks_run",
        databricks_conn_id="databricks_default",
        new_cluster={
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
        },
        notebook_task={"notebook_path": "/Users/x/y"},
        **kwargs,
    )


@pytest.fixture
def fake_context():
    return {"task_state": _FakeTaskState()}


def _patch_hook(op, run_id: int = 12345):
    """Replace the operator's _hook property with a MagicMock returning run_id."""
    hook = MagicMock()
    hook.submit_run.return_value = run_id
    # Override the property descriptor on the instance via __dict__
    object.__setattr__(op, "_hook", hook)
    return hook


def test_first_attempt_submits_and_persists_run_id(fake_context):
    op = _make_op()
    hook = _patch_hook(op, run_id=12345)

    target = "system.databricks.example_resumable_databricks._handle_databricks_operator_execution"
    with patch(target):
        op.execute(fake_context)

    assert hook.submit_run.call_count == 1
    # Cleared on success.
    assert fake_context["task_state"].get("databricks_run_id") is None


def test_resume_after_checkpoint_skips_submit_and_reuses_run_id(fake_context):
    op = _make_op()
    hook = _patch_hook(op, run_id=99999)

    # Pre-seed task_state as if the prior attempt was disrupted.
    fake_context["task_state"].set("databricks_run_id", 12345)

    target = "system.databricks.example_resumable_databricks._handle_databricks_operator_execution"
    with patch(target):
        op.execute(fake_context)

    # submit_run NOT called — we reused the prior run_id.
    assert hook.submit_run.call_count == 0
    # Operator's run_id was restored from task_state.
    assert op.run_id == 12345
    # task_state cleared on success.
    assert fake_context["task_state"].get("databricks_run_id") is None


def test_disruption_during_poll_raises_checkpointed_and_run_id_persists(fake_context):
    op = _make_op()
    hook = _patch_hook(op, run_id=12345)

    def _simulate_disruption(operator, _hook, _log, _context):
        # Simulate a SIGTERM-handler raise during the poll loop.
        raise AirflowTaskCheckpointed(checkpoint_data={"run_id": operator.run_id})

    target = "system.databricks.example_resumable_databricks._handle_databricks_operator_execution"
    with patch(target, side_effect=_simulate_disruption):
        with pytest.raises(AirflowTaskCheckpointed) as exc_info:
            op.execute(fake_context)

    # submit_run called once (first attempt), run_id persisted before disruption.
    assert hook.submit_run.call_count == 1
    assert fake_context["task_state"].get("databricks_run_id") == 12345
    assert exc_info.value.checkpoint_data == {"run_id": 12345}


def test_on_kill_does_not_cancel_run_for_resumable_variant():
    op = _make_op()
    hook = _patch_hook(op)
    op.run_id = 12345

    op.on_kill()

    # Critical: the resumable variant must NOT cancel the Databricks run on kill.
    # That's the whole point — preserve the external job for next-attempt reconnection.
    assert hook.cancel_run.call_count == 0
