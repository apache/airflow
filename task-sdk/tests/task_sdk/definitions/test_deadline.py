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

from datetime import datetime, timedelta
from unittest import mock

import pytest
from task_sdk.definitions.test_callback import TEST_CALLBACK_KWARGS, TEST_CALLBACK_PATH, UNIMPORTABLE_DOT_PATH

from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.deadline import (
    DeadlineAlert,
    DeadlineReference,
    FixedDatetimeDeadline,
    VariableInterval,
)
from airflow.sdk.definitions.variable import Variable
from airflow.sdk.exceptions import AirflowRuntimeError

DAG_ID = "dag_id_1"
RUN_ID = 1
DEFAULT_DATE = datetime(2025, 6, 26)

REFERENCE_TYPES = [
    pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
    pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
    pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
    pytest.param(DeadlineReference.AVERAGE_RUNTIME, id="average_runtime"),
]


TEST_DEADLINE_CALLBACK = AsyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)


class TestAverageRuntimeReference:
    def test_min_runs_cannot_exceed_max_runs(self):
        """``min_runs > max_runs`` is unsatisfiable (the evaluator samples at most ``max_runs``
        rows then requires ``min_runs`` of them), so it must be rejected at authoring time rather
        than producing a deadline that silently never fires."""
        with pytest.raises(ValueError, match="cannot exceed max_runs"):
            DeadlineReference.AVERAGE_RUNTIME(max_runs=5, min_runs=10)


class TestDeadlineAlert:
    @pytest.mark.parametrize(
        ("test_alert", "should_equal"),
        [
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=1),
                    callback=TEST_DEADLINE_CALLBACK,
                ),
                True,
                id="same_alert",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
                    interval=timedelta(hours=1),
                    callback=TEST_DEADLINE_CALLBACK,
                ),
                False,
                id="different_reference",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=2),
                    callback=TEST_DEADLINE_CALLBACK,
                ),
                False,
                id="different_interval",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=1),
                    callback=AsyncCallback(UNIMPORTABLE_DOT_PATH, kwargs=TEST_CALLBACK_KWARGS),
                ),
                False,
                id="different_callback",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=1),
                    callback=AsyncCallback(TEST_CALLBACK_PATH, kwargs={"arg2": "value2"}),
                ),
                False,
                id="different_kwargs",
            ),
            pytest.param("not a DeadlineAlert", False, id="non_deadline_alert"),
        ],
    )
    def test_deadline_alert_equality(self, test_alert, should_equal):
        base_alert = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(hours=1),
            callback=TEST_DEADLINE_CALLBACK,
        )

        assert (base_alert == test_alert) == should_equal

    def test_deadline_alert_hash(self):
        std_interval = timedelta(hours=1)
        std_callback = TEST_CALLBACK_PATH
        std_kwargs = TEST_CALLBACK_KWARGS

        alert1 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=AsyncCallback(std_callback, kwargs=std_kwargs),
        )
        alert2 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=AsyncCallback(std_callback, kwargs=std_kwargs),
        )

        assert hash(alert1) == hash(alert1)
        assert hash(alert1) == hash(alert2)

    def test_deadline_alert_eq_is_symmetric_and_hash_consistent_for_subclassed_reference(self):
        """``__eq__`` must compare the reference by EXACT type, not ``isinstance``. A custom
        reference subclassing a builtin (``register_custom_reference`` permits any
        ``BaseDeadlineReference`` subclass) otherwise made equality ASYMMETRIC
        (``alert(SubRef) == alert(BaseRef)`` True but the reverse False) and INCONSISTENT with
        ``__hash__`` (which keys on ``type(...).__name__``) — two "equal" alerts hashing
        differently, breaking the eq/hash invariant and set/dict use.
        """
        import datetime

        class _MyFixedRef(FixedDatetimeDeadline):
            pass

        dt = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        common = dict(interval=timedelta(hours=1), callback=AsyncCallback(TEST_CALLBACK_PATH))
        a_sub = DeadlineAlert(reference=_MyFixedRef(dt), **common)
        a_base = DeadlineAlert(reference=FixedDatetimeDeadline(dt), **common)

        # A subclass-vs-builtin reference are DISTINCT types → not equal, and equality is symmetric.
        assert (a_sub == a_base) == (a_base == a_sub)  # symmetric
        assert a_sub != a_base  # exact-type: different reference types are not equal
        # Two alerts with the SAME exact subclass reference are equal AND hash-equal.
        a_sub2 = DeadlineAlert(reference=_MyFixedRef(dt), **common)
        assert a_sub == a_sub2
        assert hash(a_sub) == hash(a_sub2)
        # eq/hash invariant: if (unequal) they may differ; the broken case was equal-but-unequal-hash.
        if a_sub == a_base:
            assert hash(a_sub) == hash(a_base)

    def test_deadline_alert_in_set(self):
        std_interval = timedelta(hours=1)
        std_callback = TEST_CALLBACK_PATH
        std_kwargs = TEST_CALLBACK_KWARGS

        alert1 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=AsyncCallback(std_callback, kwargs=std_kwargs),
        )
        alert2 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=AsyncCallback(std_callback, kwargs=std_kwargs),
        )

        alert_set = {alert1, alert2}
        assert len(alert_set) == 1

    def test_deadline_alert_hash_with_nested_callback_kwargs(self):
        """A DeadlineAlert whose callback has NESTED dict/list kwargs must be hashable. Previously
        Callback.__hash__ only flattened the top level, so hashing the alert raised
        TypeError: unhashable type: 'dict'/'list' on a perfectly legal (JSON-serializable) config."""
        nested_kwargs = {"config": {"retries": 3, "endpoints": ["a", "b"]}, "flat": "x"}

        def _make():
            return DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=timedelta(hours=1),
                callback=AsyncCallback(TEST_CALLBACK_PATH, kwargs=nested_kwargs),
            )

        a1, a2 = _make(), _make()
        # Must not raise, and equal alerts must collapse in a set.
        assert hash(a1) == hash(a2)
        assert len({a1, a2}) == 1

    @pytest.mark.parametrize(
        ("callback_class"),
        [
            pytest.param(AsyncCallback, id="async_callback"),
            pytest.param(SyncCallback, id="sync_callback"),
        ],
    )
    def test_deadline_alert_accepts_all_callbacks(self, callback_class):
        alert = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(hours=1),
            callback=callback_class(TEST_CALLBACK_PATH),
        )
        assert alert.callback is not None
        assert isinstance(alert.callback, callback_class)

    def test_deadline_alert_rejects_invalid_callback(self):
        """Test that DeadlineAlert rejects non-callback types."""
        with pytest.raises(ValueError, match="Callbacks of type str are not currently supported"):
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=timedelta(hours=1),
                callback="not_a_callback",  # type: ignore
            )


class TestVariableInterval:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("3", timedelta(seconds=3)),
            ("05", timedelta(seconds=5)),  # leading zero
        ],
    )
    def test_resolve_valid(self, mocker, value, expected):
        mocker.patch.object(Variable, "get", return_value=value)

        interval = VariableInterval(key="test_interval")

        assert interval.resolve() == expected

    @pytest.mark.parametrize(
        ("value", "raise_runtime", "match"),
        [
            (None, True, "not found"),
            # Out-of-range: timedelta caps at ~999999999 days, so a wildly large value raises
            # OverflowError (NOT a ValueError subclass) and would escape the method's contract.
            # It must be translated to the same clean ValueError as every other bad input.
            ("99999999999999", False, "too large to be a valid interval"),
            (str(10**30), False, "too large to be a valid interval"),
        ],
    )
    def test_resolve_invalid(self, mocker, value, raise_runtime, match):

        if raise_runtime:
            mock_err = mock.Mock()
            mock_err.error.value = "MISSING"
            mock_err.detail = "missing"

            mocker.patch.object(
                Variable,
                "get",
                side_effect=AirflowRuntimeError(mock_err),
            )
        else:
            mocker.patch.object(Variable, "get", return_value=value)

        interval = VariableInterval(key="test_interval")

        with pytest.raises(ValueError, match=match):
            interval.resolve()

    @pytest.mark.parametrize(
        ("value", "match"),
        [
            ("abc", "must be an integer"),
            ("", "must be an integer"),
            (None, "must be an integer"),
            ("0", "must be > 0"),
            ("-5", "must be > 0"),
            ("99999999999999", "too large to be a valid interval"),
        ],
    )
    def test_coerce_to_timedelta_invalid(self, value, match):
        with pytest.raises(ValueError, match=match):
            VariableInterval(key="k").coerce_to_timedelta(value)
