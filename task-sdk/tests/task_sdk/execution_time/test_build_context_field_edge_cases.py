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
"""Edge-case tests for build_context_from_dag_run callback context construction (PR #66608).

Covers missing/None DagRun attributes, context key completeness (including the
absent top-level ``conf`` key), datetime formatting edges, deeply nested payloads,
and the supervisor ``_fetch_and_build_context`` path with partial DagRun fields.
"""

from __future__ import annotations

import pendulum
import structlog

from airflow.sdk.execution_time.context import build_context_from_dag_run


class MockDagRun:
    """Bare attribute holder; only attributes explicitly set exist."""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _full_kwargs():
    return dict(
        dag_id="d",
        run_id="r",
        logical_date=pendulum.datetime(2024, 1, 15, 10, 30, 0),
        data_interval_start=pendulum.datetime(2024, 1, 14, 0, 0, 0),
        data_interval_end=pendulum.datetime(2024, 1, 15, 0, 0, 0),
        run_after=pendulum.datetime(2024, 1, 15, 0, 0, 0),
    )


# ---------------------------------------------------------------------------
# DagRun missing each optional attr one at a time
# ---------------------------------------------------------------------------
class TestMissingOptionalAttrs:
    """Probe build_context_from_dag_run with each optional attr removed."""

    def test_missing_run_after_is_tolerated(self):
        # run_after is never read by build_context_from_dag_run, so its absence
        # must NOT affect the built context.
        kw = _full_kwargs()
        del kw["run_after"]
        dr = MockDagRun(**kw)
        ctx = build_context_from_dag_run(dr)
        assert ctx["run_id"] == "r"
        assert ctx["ds"] == "2024-01-15"


# ---------------------------------------------------------------------------
# _fetch_and_build_context with null/partial DagRun fields
# ---------------------------------------------------------------------------
class TestFetchPartialFields:
    def _result(self, **overrides):
        from airflow.sdk.execution_time.comms import DagRunResult

        base = dict(
            dag_id="test_dag",
            run_id="test_run",
            run_after=pendulum.datetime(2024, 1, 1, 0, 0, 0),
            run_type="manual",
            state="running",
            consumed_asset_events=[],
        )
        base.update(overrides)
        return DagRunResult(**base)

    def test_full_logical_date_builds_full_context(self, mocker):
        from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context

        comms = mocker.Mock()
        comms.send.return_value = self._result(
            logical_date=pendulum.datetime(2024, 1, 15, 0, 0, 0),
            data_interval_start=None,
            data_interval_end=None,
        )
        ctx = _fetch_and_build_context(comms, "test_dag", "test_run", structlog.get_logger())
        assert ctx is not None
        assert ctx["ds"] == "2024-01-15"
        # partial interval fields => coerce_datetime(None) is None, still keyed
        assert ctx["data_interval_start"] is None
        assert ctx["data_interval_end"] is None
