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
"""Tests for the triggerer/deadline callback context-construction layers (PR #66608).

Covers ``TriggerRunner._build_context_from_dag_run_data`` ``_deadline`` handling
and SDK callback path resolution for partial / lambda / bound-method callables.
"""

from __future__ import annotations

import functools

import pytest

from airflow.jobs.triggerer_job_runner import TriggerRunner


async def _async_cb(context, **kwargs):
    return None


def _sync_cb(context, **kwargs):
    return None


class _Holder:
    async def amethod(self, context, **kwargs):
        return None

    def method(self, context, **kwargs):
        return None


# Base valid dag_run_data accepted by DRDataModel.
_BASE_DAG_RUN_DATA: dict = {
    "dag_id": "example_dag",
    "run_id": "manual__2024-01-01",
    "logical_date": "2024-01-01T00:00:00+00:00",
    "data_interval_start": None,
    "data_interval_end": None,
    "run_after": "2024-01-01T00:00:00+00:00",
    "start_date": "2024-01-01T00:01:00+00:00",
    "end_date": None,
    "run_type": "manual",
    "state": "running",
    "conf": {},
    "consumed_asset_events": [],
    "partition_key": None,
}


# ---------------------------------------------------------------------------
# _build_context_from_dag_run_data with the _deadline key variants
# ---------------------------------------------------------------------------
class TestDeadlineKey:
    def test_no_deadline_key_no_deadline_in_context(self):
        ctx = TriggerRunner._build_context_from_dag_run_data(dict(_BASE_DAG_RUN_DATA))
        assert "deadline" not in ctx
        assert ctx["run_id"] == "manual__2024-01-01"

    def test_deadline_none_value_not_injected(self):
        data = {**_BASE_DAG_RUN_DATA, "_deadline": None}
        ctx = TriggerRunner._build_context_from_dag_run_data(data)
        # falsy deadline_info => not injected (guarded by `if deadline_info`)
        assert "deadline" not in ctx

    @pytest.mark.parametrize(
        "bad_deadline",
        ["a-string", 42, ["list", "items"], 3.14],
        ids=["str", "int", "list", "float"],
    )
    def test_malformed_deadline_passed_through_verbatim(self, bad_deadline):
        """A non-dict _deadline is assigned verbatim to context['deadline'].

        The builder does NOT validate the shape; truthy non-dict values land in
        the context as-is. Template access like {{ deadline.deadline_time }}
        would later fail, but context construction itself is graceful (no crash).
        """
        data = {**_BASE_DAG_RUN_DATA, "_deadline": bad_deadline}
        ctx = TriggerRunner._build_context_from_dag_run_data(data)
        assert ctx["deadline"] == bad_deadline

    def test_extra_unexpected_key_raises_validation_error(self):
        """DRDataModel uses extra='forbid'; an unexpected key (not _deadline) raises.

        Documents that the triggerer context builder is NOT defensive against
        unexpected keys in dag_run_data - they propagate as a pydantic
        ValidationError. In the real path dag_run_data comes from
        model_dump() so this cannot happen, but the builder offers no guard.
        """
        from pydantic import ValidationError

        data = {**_BASE_DAG_RUN_DATA, "totally_unexpected": "x"}
        with pytest.raises(ValidationError):
            TriggerRunner._build_context_from_dag_run_data(data)

    def test_deadline_popped_before_model_construction(self):
        """_deadline must be popped so it does not trip extra='forbid'."""
        data = {**_BASE_DAG_RUN_DATA, "_deadline": {"id": "x", "deadline_time": "t"}}
        ctx = TriggerRunner._build_context_from_dag_run_data(data)
        assert ctx["deadline"] == {"id": "x", "deadline_time": "t"}
        # the input dict was mutated by pop - acceptable, but verify no leak into model
        assert "_deadline" not in ctx["dag_run"].model_dump()


# ---------------------------------------------------------------------------
# Deadline callback that is a partial / lambda / bound method
# ---------------------------------------------------------------------------
class TestCallableResolution:
    """SDK Callback.get_callback_path path resolution at definition time."""

    def test_async_function_resolves_to_dotpath(self):
        from airflow.sdk.definitions.callback import AsyncCallback

        cb = AsyncCallback(_async_cb)
        assert cb.path.endswith("_async_cb")

    def test_lambda_produces_invalid_path_but_does_not_raise(self):
        """A lambda has __qualname__ '<lambda>' -> path contains '<lambda>'.

        get_callback_path does NOT raise for a lambda even though the resulting
        path can never be re-imported. Documents the known TODO limitation.
        """
        from airflow.sdk.definitions.callback import SyncCallback

        cb = SyncCallback(lambda context, **kw: None)
        assert "<lambda>" in cb.path

    def test_functools_partial_raises_attributeerror(self):
        """functools.partial has no __qualname__ -> AttributeError at definition.

        This is a clean fail at definition time (not silent corruption).
        """
        from airflow.sdk.definitions.callback import SyncCallback

        p = functools.partial(_sync_cb, extra=1)
        with pytest.raises(AttributeError):
            SyncCallback(p)

    def test_bound_method_resolves_with_dotted_qualname(self):
        """A bound method resolves to module.Class.method - not importable as-is.

        verify_callable passes (it's callable/awaitable) and the path embeds the
        class via __qualname__. No crash at definition time.
        """
        from airflow.sdk.definitions.callback import AsyncCallback

        h = _Holder()
        cb = AsyncCallback(h.amethod)
        assert cb.path.endswith("_Holder.amethod")

    def test_partial_via_create_from_sdk_def_fails_at_definition(self):
        """End-to-end through the SDK def: partial async callback fails cleanly."""
        from airflow.sdk.definitions.callback import AsyncCallback

        ap = functools.partial(_async_cb)
        with pytest.raises(AttributeError):
            AsyncCallback(ap)
