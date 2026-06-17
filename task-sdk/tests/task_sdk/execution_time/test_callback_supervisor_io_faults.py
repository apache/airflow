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
"""
IO-boundary fault-injection tests for the callback supervisor.

Each test injects a fault at the IO boundary (socket recv/send, file open, fork,
comms decode) into the REAL code path under test, NOT by replacing the logic,
to verify the deadline-callback runtime surfaces faults cleanly rather than
hanging or crashing the supervisor.
"""

from __future__ import annotations

from unittest import mock

import structlog

from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, DagRunType
from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context
from airflow.sdk.execution_time.comms import DagRunResult

log = structlog.get_logger()


def _mk_dagrun(**over):
    from airflow.sdk._shared.timezones import timezone

    base = dict(
        dag_id="d",
        run_id="r",
        run_after=timezone.parse("2024-01-01T00:00:00+00:00"),
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        consumed_asset_events=[],
    )
    base.update(over)
    return DagRun(**base)


# ---------------------------------------------------------------------------
# Scenario 1: comms send/recv raises mid-callback (socket error / broken pipe)
# ---------------------------------------------------------------------------
class TestScenario1CommsFailureMidCallback:
    """SUPERVISOR_COMMS.send raises a socket error during the callback."""

    def test_fetch_and_build_context_swallows_comms_failure(self):
        """_fetch_and_build_context must return None (not raise) when comms.send raises."""
        comms = mock.MagicMock()
        comms.send.side_effect = BrokenPipeError("pipe gone")
        result = _fetch_and_build_context(comms, "d", "r", log)
        assert result is None


# ---------------------------------------------------------------------------
# Scenario 2: GetDagRun comms returns malformed / partial / wrong-type payload
# ---------------------------------------------------------------------------
class TestScenario2MalformedDagRunPayload:
    """_fetch_and_build_context fed a bad response from comms.send."""

    def test_wrong_response_type_returns_none(self):
        comms = mock.MagicMock()
        comms.send.return_value = {"not": "a DagRunResult"}
        assert _fetch_and_build_context(comms, "d", "r", log) is None

    def test_none_response_returns_none(self):
        comms = mock.MagicMock()
        comms.send.return_value = None
        assert _fetch_and_build_context(comms, "d", "r", log) is None

    def test_dagrunresult_missing_logical_date_builds_minimal_context(self):
        """logical_date None -> context built with only run_id (graceful degrade, no crash)."""
        comms = mock.MagicMock()
        dr = DagRunResult.from_api_response(_mk_dagrun(logical_date=None))
        comms.send.return_value = dr
        ctx = _fetch_and_build_context(comms, "d", "r", log)
        assert ctx is not None
        assert ctx["run_id"] == "r"
        # task-specific / date fields must be absent, never half-built
        assert "logical_date" not in ctx


# ---------------------------------------------------------------------------
# Scenario 8: SUPERVISOR_COMMS not initialized (init-order race)
# ---------------------------------------------------------------------------
class TestScenario8CommsNotInitialized:
    def test_fetch_context_with_none_comms(self):
        """Passing None comms -> AttributeError is caught, returns None (no crash bubbling)."""
        # _fetch_and_build_context calls comms.send(...); None.send -> AttributeError,
        # which the broad except in the function must swallow into None.
        assert _fetch_and_build_context(None, "d", "r", log) is None
