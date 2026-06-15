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
Authoring-time validation tests for the DeadlineAlert / DeadlineReference / Callback SDK surface.

These tests exercise the *DAG authoring* surface (``airflow.sdk.definitions``), where a user
constructs a deadline in their DAG file. The intent is that bad input is rejected cleanly at
parse time, rather than being accepted silently and blowing up later when the scheduler evaluates
the deadline.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, VariableInterval


async def _async_callback():
    pass


def _sync_callback():
    pass


VALID_ASYNC_CB = AsyncCallback(_async_callback)


class TestDeadlineAlertIntervalValidation:
    """Scenario 1: the ``interval`` argument must be a timedelta or VariableInterval."""

    @pytest.mark.parametrize(
        "interval",
        [
            pytest.param(5, id="int"),
            pytest.param("1h", id="str"),
            pytest.param(None, id="none"),
            pytest.param(3.5, id="float"),
        ],
    )
    def test_non_timedelta_interval_rejected_at_authoring(self, interval):
        """
        A non-timedelta interval must raise at construction time.

        Regression test: previously accepted silently and produced a broken deadline that
        crashed at scheduler runtime with ``TypeError`` when computing ``base_time + interval``.
        """
        with pytest.raises(ValueError, match="interval must be a timedelta or VariableInterval"):
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=interval,
                callback=VALID_ASYNC_CB,
            )

    def test_negative_timedelta_interval_accepted(self):
        """A negative timedelta is a valid (if unusual) timedelta and is accepted."""
        alert = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(seconds=-5),
            callback=VALID_ASYNC_CB,
        )
        assert alert.interval == timedelta(seconds=-5)

    def test_timedelta_interval_accepted(self):
        alert = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(hours=1),
            callback=VALID_ASYNC_CB,
        )
        assert alert.interval == timedelta(hours=1)

    def test_variable_interval_accepted(self):
        interval = VariableInterval("deadline_seconds")
        alert = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=interval,
            callback=VALID_ASYNC_CB,
        )
        assert alert.interval is interval


class TestCallbackTypeValidation:
    """Scenarios 3 & 4: callback callable validation at definition time."""

    def test_async_callback_rejects_sync_function(self):
        with pytest.raises(AttributeError, match="is not awaitable"):
            AsyncCallback(_sync_callback)

    def test_async_callback_accepts_async_function(self):
        cb = AsyncCallback(_async_callback)
        assert cb.path.endswith("_async_callback")

    @pytest.mark.parametrize(
        "subclass",
        [pytest.param(AsyncCallback, id="async"), pytest.param(SyncCallback, id="sync")],
    )
    @pytest.mark.parametrize(
        "non_callable",
        [pytest.param(None, id="none"), pytest.param(123, id="int"), pytest.param("", id="empty_str")],
    )
    def test_non_callable_rejected(self, subclass, non_callable):
        """A non-callable, non-dotpath value is rejected cleanly with ImportError."""
        with pytest.raises(ImportError, match="doesn't look like a valid dot path"):
            subclass(non_callable)


class TestDeadlineAlertCallbackTypeValidation:
    """Scenario 4 (continued): a non-Callback callback is rejected by DeadlineAlert itself."""

    @pytest.mark.parametrize(
        "bad_callback",
        [
            pytest.param("some.callable", id="raw_str"),
            pytest.param(_async_callback, id="raw_callable"),
            pytest.param(None, id="none"),
        ],
    )
    def test_non_callback_object_rejected(self, bad_callback):
        with pytest.raises(ValueError, match="are not currently supported"):
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=timedelta(hours=1),
                callback=bad_callback,
            )


class TestSyncCallbackAcceptsAsync:
    """
    Gap #2 (left as documented): SyncCallback does not reject coroutine functions.

    AsyncCallback strictly rejects sync callables, but SyncCallback does *not* symmetrically reject
    coroutine functions. This asymmetry is left in place deliberately: rejecting at authoring time
    risks breaking valid edge cases (e.g. a coroutine function the user intends to drive via
    ``asyncio.run`` inside a sync wrapper), and the failure mode is not silent data corruption.
    Documented here so the current behaviour is intentional and any future tightening is a tested,
    deliberate decision.
    """

    def test_sync_callback_accepts_async_function(self):
        cb = SyncCallback(_async_callback)
        assert cb.path.endswith("_async_callback")
