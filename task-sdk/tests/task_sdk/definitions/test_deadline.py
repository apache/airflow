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

import pendulum
import pytest
from task_sdk.definitions.test_callback import TEST_CALLBACK_KWARGS, TEST_CALLBACK_PATH, UNIMPORTABLE_DOT_PATH

from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, VariableInterval
from airflow.sdk.definitions.variable import Variable
from airflow.sdk.exceptions import AirflowRuntimeError, RemovedInAirflow4Warning

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


class TestDeadlineAlert:
    class SubclassedCallback(SyncCallback):
        """Stand-in for a Dag author's own Callback subclass, which DeadlineAlert must reject."""

        ...

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

    @pytest.mark.parametrize(
        ("test_callback", "expected_name", "expected_pass"),
        [
            pytest.param(
                SyncCallback(TEST_CALLBACK_PATH),
                "SyncCallback",
                True,
                id="sync_callback_passes",
            ),
            pytest.param(
                AsyncCallback(TEST_CALLBACK_PATH),
                "AsyncCallback",
                True,
                id="async_callback_passes",
            ),
            pytest.param(
                type(TEST_CALLBACK_PATH),
                "type",
                False,
                id="non_callback_callable_fails",
            ),
            pytest.param(
                SubclassedCallback(TEST_CALLBACK_PATH),
                "SubclassedCallback",
                False,
                id="subclassed_callback_fails",
            ),
            pytest.param(
                "not_a_callback",
                "str",
                False,
                id="non_callback_fails",
            ),
            pytest.param(
                None,
                "NoneType",
                False,
                id="can_not_be_none",
            ),
        ],
    )
    def test_deadline_init_callback_type_checks(self, test_callback, expected_name, expected_pass):
        if expected_pass:
            alert = DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=timedelta(hours=1),
                callback=test_callback,
            )

            assert alert.callback is test_callback
            assert type(alert.callback).__name__ == expected_name
        else:
            with pytest.raises(
                ValueError,
                match=f"Callbacks must be `AsyncCallback` or `SyncCallback`, received {expected_name}",
            ):
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=1),
                    callback=test_callback,
                )

    @pytest.mark.parametrize(
        ("test_interval", "expected_name", "expected_pass"),
        [
            pytest.param(timedelta(1), "timedelta", True, id="positive_timedelta_passes"),
            pytest.param(timedelta(-1), "timedelta", True, id="negative_timedelta_passes"),
            pytest.param(timedelta(0), "timedelta", True, id="zero_timedelta_passes"),
            pytest.param(VariableInterval("var"), "VariableInterval", True, id="VariableInterval_passes"),
            pytest.param(True, "bool", False, id="bool_fails"),
            pytest.param("str", "str", False, id="string_fails"),
            pytest.param(None, "NoneType", False, id="can_not_be_none"),
        ],
    )
    def test_deadline_init_interval_type_checks(self, test_interval, expected_name, expected_pass):
        if expected_pass:
            alert = DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=test_interval,
                callback=TEST_DEADLINE_CALLBACK,
            )

            assert alert.interval == test_interval
            assert type(alert.interval).__name__ == expected_name
        else:
            with pytest.raises(
                ValueError,
                match=f"Interval must be a `timedelta` or a `VariableInterval`, received {expected_name}",
            ):
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=test_interval,
                    callback=TEST_DEADLINE_CALLBACK,
                )

    @pytest.mark.parametrize(
        ("test_interval", "expected"),
        [
            pytest.param(3600, timedelta(seconds=3600), id="positive_int"),
            pytest.param(-5, timedelta(seconds=-5), id="negative_int"),
            pytest.param(0, timedelta(0), id="zero"),
            pytest.param(0.1, timedelta(seconds=0.1), id="float"),
        ],
    )
    def test_deadline_init_number_interval_is_deprecated_not_rejected(self, test_interval, expected):
        """A bare number was never a documented interval type, but it parses today, so it is warned about."""
        with pytest.warns(
            RemovedInAirflow4Warning, match="Passing a number as a deadline interval is deprecated"
        ):
            alert = DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=test_interval,
                callback=TEST_DEADLINE_CALLBACK,
            )

        assert alert.interval == expected
        assert type(alert.interval) is timedelta

    def test_deadline_init_normalizes_timedelta_subclass(self):
        """``pendulum.duration()`` has no serializer of its own, so it is flattened to a ``timedelta``.

        Without this it passes the isinstance check and then fails much later in serde, which
        dispatches on qualified class name and registers only ``datetime.timedelta``.
        """
        alert = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=pendulum.duration(hours=1),
            callback=TEST_DEADLINE_CALLBACK,
        )

        assert alert.interval == timedelta(hours=1)
        assert type(alert.interval) is timedelta


class TestVariableInterval:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("3", timedelta(seconds=3)),
            ("10", timedelta(seconds=10)),
            ("05", timedelta(seconds=5)),
            ("0", timedelta(0)),
            ("-5", timedelta(seconds=-5)),
        ],
    )
    def test_resolve_valid(self, mocker, value, expected):
        mocker.patch.object(Variable, "get", return_value=value)

        interval = VariableInterval(key="test_interval")

        with pytest.warns(DeprecationWarning, match="VariableInterval.resolve"):
            assert interval.resolve() == expected

    @pytest.mark.parametrize(
        ("value", "raise_runtime", "match"),
        [
            (None, True, "not found"),
            ("abc", False, "must be an integer"),
            ("", False, "must be an integer"),
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

        with pytest.warns(DeprecationWarning, match="VariableInterval.resolve"):
            with pytest.raises(ValueError, match=match):
                interval.resolve()
