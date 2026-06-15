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
import pytest
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

    def test_missing_data_interval_start_raises_attributeerror(self):
        kw = _full_kwargs()
        del kw["data_interval_start"]
        dr = MockDagRun(**kw)
        # logical_date is present so the branch that reads data_interval_start runs.
        with pytest.raises(AttributeError):
            build_context_from_dag_run(dr)

    def test_missing_data_interval_end_raises_attributeerror(self):
        kw = _full_kwargs()
        del kw["data_interval_end"]
        dr = MockDagRun(**kw)
        with pytest.raises(AttributeError):
            build_context_from_dag_run(dr)

    def test_missing_logical_date_raises_attributeerror(self):
        # logical_date is read first (coerce_datetime(dag_run.logical_date));
        # absence => AttributeError, not a graceful partial context.
        kw = _full_kwargs()
        del kw["logical_date"]
        dr = MockDagRun(**kw)
        with pytest.raises(AttributeError):
            build_context_from_dag_run(dr)

    def test_missing_run_after_is_tolerated(self):
        # run_after is never read by build_context_from_dag_run, so its absence
        # must NOT affect the built context.
        kw = _full_kwargs()
        del kw["run_after"]
        dr = MockDagRun(**kw)
        ctx = build_context_from_dag_run(dr)
        assert ctx["run_id"] == "r"
        assert ctx["ds"] == "2024-01-15"

    def test_logical_date_none_then_missing_run_id_raises(self):
        # When logical_date is None the else-branch reads run_id; if run_id is
        # also absent it raises AttributeError.
        dr = MockDagRun(logical_date=None)
        with pytest.raises(AttributeError):
            build_context_from_dag_run(dr)


# ---------------------------------------------------------------------------
# Context dict key completeness / documented `conf` key
# ---------------------------------------------------------------------------
class TestKeyCompleteness:
    def test_documented_keys_present(self):
        dr = MockDagRun(**_full_kwargs())
        ctx = build_context_from_dag_run(dr)
        for key in (
            "dag_run",
            "run_id",
            "logical_date",
            "ds",
            "ts",
            "data_interval_start",
            "data_interval_end",
        ):
            assert key in ctx, f"documented key {key!r} missing"

    def test_conf_key_absent_doc_impl_mismatch(self):
        """PR description promises a top-level ``conf`` key; impl does not add it.

        This asserts the CURRENT (buggy-doc) behaviour: ``conf`` is absent even
        though DRDataModel carries a ``conf`` attribute. Documents the
        doc/impl mismatch flagged in the wave-1 audit.
        """
        kw = _full_kwargs()
        dr = MockDagRun(conf={"k": "v"}, **kw)
        ctx = build_context_from_dag_run(dr)
        assert "conf" not in ctx, (
            "If this fails, conf was added to the context - update the PR docs and this test together."
        )


# ---------------------------------------------------------------------------
# datetime edge cases
# ---------------------------------------------------------------------------
class TestDatetimeEdges:
    @pytest.mark.parametrize(
        ("ld", "exp_ds", "exp_ds_nodash"),
        [
            (pendulum.datetime(9999, 12, 31, 23, 59, 59), "9999-12-31", "99991231"),
            (pendulum.datetime(1969, 7, 20, 20, 17, 0), "1969-07-20", "19690720"),
            (pendulum.datetime(1900, 1, 1, 0, 0, 0), "1900-01-01", "19000101"),
            (pendulum.datetime(2024, 1, 1, 0, 0, 0, 123456), "2024-01-01", "20240101"),
            (pendulum.datetime(2024, 12, 31, 23, 59, 59), "2024-12-31", "20241231"),
        ],
    )
    def test_ds_formatting_edge_dates(self, ld, exp_ds, exp_ds_nodash):
        kw = _full_kwargs()
        kw["logical_date"] = ld
        dr = MockDagRun(**kw)
        ctx = build_context_from_dag_run(dr)
        assert ctx["ds"] == exp_ds
        assert ctx["ds_nodash"] == exp_ds_nodash
        # ts must be a valid isoformat round-trip
        assert ctx["ts"] == ld.isoformat()

    def test_year_below_1000_matches_existing_strftime_convention(self):
        """ds for a year < 1000 is NOT zero-padded to 4 digits.

        build_context_from_dag_run uses ``logical_date.strftime("%Y-%m-%d")`` -
        the SAME pattern as the task-bound context path in task_runner.py. On
        glibc, ``%Y`` is not zero-padded for years < 1000, so year 1 yields
        '1-01-01'. This is a long-standing, consistent convention (not a
        #66608 regression), asserted here so the behaviour is pinned.
        """
        kw = _full_kwargs()
        ld = pendulum.datetime(1, 1, 1, 0, 0, 0)
        kw["logical_date"] = ld
        dr = MockDagRun(**kw)
        ctx = build_context_from_dag_run(dr)
        # Whatever the platform produces, it must equal the raw strftime output
        assert ctx["ds"] == ld.strftime("%Y-%m-%d")
        assert ctx["ds_nodash"] == ld.strftime("%Y-%m-%d").replace("-", "")

    def test_microseconds_dropped_from_ts_nodash(self):
        kw = _full_kwargs()
        kw["logical_date"] = pendulum.datetime(2024, 1, 1, 1, 2, 3, 999999)
        dr = MockDagRun(**kw)
        ctx = build_context_from_dag_run(dr)
        # %H%M%S has no microseconds
        assert ctx["ts_nodash"] == "20240101T010203"


# ---------------------------------------------------------------------------
# very deeply nested callback_kwargs through context path
# ---------------------------------------------------------------------------
class TestDeepNesting:
    def _nested(self, depth):
        d: dict = {"leaf": 1}
        for _ in range(depth):
            d = {"n": d}
        return d

    def test_deeply_nested_kwargs_on_dag_run_attr(self):
        """A 50-level nested structure carried on the dag_run object does not
        affect build_context_from_dag_run (it never recurses into payload)."""
        kw = _full_kwargs()
        dr = MockDagRun(conf=self._nested(50), **kw)
        ctx = build_context_from_dag_run(dr)
        # conf is not copied; no recursion / no RecursionError
        assert "conf" not in ctx
        assert ctx["ds"] == "2024-01-15"

    def test_deeply_nested_kwargs_survive_dict_identity(self):
        """The deeply nested object is reachable only via dag_run, intact."""
        kw = _full_kwargs()
        nested = self._nested(50)
        dr = MockDagRun(conf=nested, **kw)
        ctx = build_context_from_dag_run(dr)
        assert ctx["dag_run"].conf is nested


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

    def test_null_logical_date_builds_minimal_context(self, mocker):
        from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context

        comms = mocker.Mock()
        comms.send.return_value = self._result(logical_date=None)
        ctx = _fetch_and_build_context(comms, "test_dag", "test_run", structlog.get_logger())
        assert ctx is not None
        assert ctx["run_id"] == "test_run"
        # logical_date None => no ds/ts keys, but no crash
        assert "ds" not in ctx
        assert "logical_date" not in ctx

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
