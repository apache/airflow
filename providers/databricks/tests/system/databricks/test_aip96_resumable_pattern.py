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
Layer 2 e2e — framework-level test of the AIP-96 resumable pattern.

Provider-agnostic. Uses an in-process simulator (no Databricks, no
Kubernetes, no real external job) to exercise the AIP-96 + AIP-103
primitives end-to-end:

  - AirflowTaskCheckpointed from airflow.sdk.exceptions
  - task_state.set/get/delete (AIP-103) — fake backend used here

Shows that the resume contract composes correctly without per-provider
plumbing. The same shape works for any operator that has a submit-then-poll
external-job structure (Databricks, EMR, Spark-on-K8s, Beam, Dataproc, etc.).

Co-located with the Databricks demo because the discussion lives there;
in a real upstream PR this test would live next to the AIP-96 supervisor
wiring code (#66445) so the framework primitives are tested without
provider deps.
"""

from __future__ import annotations

import secrets
from typing import Any

import pytest

from airflow.sdk import BaseOperator
from airflow.sdk.exceptions import AirflowTaskCheckpointed


class _FakeTaskState:
    """In-memory task_state — stand-in for AIP-103's context['task_state']."""

    def __init__(self):
        self._d: dict[str, Any] = {}

    def get(self, key):
        return self._d.get(key)

    def set(self, key, value):
        self._d[key] = value

    def delete(self, key):
        self._d.pop(key, None)


class _SimulatedResumableOp(BaseOperator):
    """
    Minimal resumable operator using an in-process external job simulator.

    Mirrors the structure of ResumableDatabricksSubmitRunOperator without
    depending on the Databricks provider. The 'external job' is just
    counter state on the operator instance.

    Test hooks:
      - ``_disrupt_at_step``: if set, raises AirflowTaskCheckpointed when
        the poll loop reaches that step (simulates worker SIGTERM mid-poll).
    """

    RESUME_KEY = "external_id"

    def __init__(self, *, total_steps: int = 3, **kwargs):
        super().__init__(**kwargs)
        self.total_steps = total_steps
        self.submit_calls: list[str] = []
        self.poll_calls: list[tuple[str, int]] = []
        self._disrupt_at_step: int | None = None

    def execute(self, context):
        external_id = context["task_state"].get(self.RESUME_KEY)
        if external_id is None:
            external_id = self._submit()
            context["task_state"].set(self.RESUME_KEY, external_id)

        for step in range(self.total_steps):
            self.poll_calls.append((external_id, step))
            if step == self._disrupt_at_step:
                raise AirflowTaskCheckpointed(checkpoint_data={"external_id": external_id})

        context["task_state"].delete(self.RESUME_KEY)
        return external_id

    def _submit(self) -> str:
        external_id = f"job-{secrets.token_hex(2)}"
        self.submit_calls.append(external_id)
        return external_id


@pytest.fixture
def ctx():
    return {"task_state": _FakeTaskState()}


def test_first_attempt_submits_persists_then_clears_on_success(ctx):
    op = _SimulatedResumableOp(task_id="t")

    result = op.execute(ctx)

    assert len(op.submit_calls) == 1
    assert result == op.submit_calls[0]
    # Polled all steps.
    assert len(op.poll_calls) == op.total_steps
    # Cleared on success.
    assert ctx["task_state"].get("external_id") is None


def test_disruption_raises_checkpointed_and_persists_external_id(ctx):
    op = _SimulatedResumableOp(task_id="t")
    op._disrupt_at_step = 1

    with pytest.raises(AirflowTaskCheckpointed) as exc_info:
        op.execute(ctx)

    persisted = ctx["task_state"].get("external_id")
    assert persisted is not None
    assert persisted == op.submit_calls[0]
    assert exc_info.value.checkpoint_data == {"external_id": persisted}
    # Polled up to and including the disruption step.
    assert len(op.poll_calls) == op._disrupt_at_step + 1


def test_resume_after_checkpoint_skips_submit_and_completes(ctx):
    op = _SimulatedResumableOp(task_id="t")

    # First attempt — disrupted.
    op._disrupt_at_step = 1
    with pytest.raises(AirflowTaskCheckpointed):
        op.execute(ctx)

    persisted = ctx["task_state"].get("external_id")
    assert persisted is not None

    # Second attempt — no disruption.
    op._disrupt_at_step = None
    result = op.execute(ctx)

    # _submit was NOT called again. The same external_id flowed through.
    assert len(op.submit_calls) == 1
    assert result == persisted
    # task_state cleared on success.
    assert ctx["task_state"].get("external_id") is None


def test_repeated_disruption_cycles_preserve_external_id(ctx):
    """Three cycles of disrupt → resume → disrupt → resume → success.

    Validates that the framework primitives compose under repeated
    interruption — the operator submits exactly once, all subsequent
    attempts read the same external_id from task_state.
    """
    op = _SimulatedResumableOp(task_id="t", total_steps=10)

    # Cycle 1: disrupt at step 2.
    op._disrupt_at_step = 2
    with pytest.raises(AirflowTaskCheckpointed):
        op.execute(ctx)

    # Cycle 2: disrupt at step 5.
    op._disrupt_at_step = 5
    with pytest.raises(AirflowTaskCheckpointed):
        op.execute(ctx)

    # Cycle 3: success.
    op._disrupt_at_step = None
    result = op.execute(ctx)

    # Exactly one submit across all attempts.
    assert len(op.submit_calls) == 1
    assert result == op.submit_calls[0]
    assert ctx["task_state"].get("external_id") is None
