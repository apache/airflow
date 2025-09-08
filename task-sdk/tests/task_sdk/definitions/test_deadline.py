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
from typing import cast

import pytest

from airflow.sdk.definitions.deadline import (
    AsyncCallback,
    Callback,
    DeadlineAlert,
    DeadlineReference,
    SyncCallback,
)
from airflow.sdk.module_loading import qualname
from airflow.serialization.serde import deserialize, serialize

UNIMPORTABLE_DOT_PATH = "valid.but.nonexistent.path"

DAG_ID = "dag_id_1"
RUN_ID = 1
DEFAULT_DATE = datetime(2025, 6, 26)

REFERENCE_TYPES = [
    pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
    pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
    pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
]


async def empty_async_callback_for_deadline_tests():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


def empty_sync_callback_for_deadline_tests():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


TEST_CALLBACK_PATH = qualname(empty_async_callback_for_deadline_tests)
TEST_CALLBACK_KWARGS = {"arg1": "value1"}
TEST_DEADLINE_CALLBACK = AsyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)


class TestDeadlineAlert:
    @pytest.mark.parametrize(
        "test_alert, should_equal",
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


class TestCallback:
    @pytest.mark.parametrize(
        "subclass, callable",
        [
            pytest.param(AsyncCallback, empty_async_callback_for_deadline_tests, id="async"),
            pytest.param(SyncCallback, empty_sync_callback_for_deadline_tests, id="sync"),
        ],
    )
    def test_init_error_reserved_kwarg(self, subclass, callable):
        with pytest.raises(ValueError, match="context is a reserved kwarg for this class"):
            subclass(callable, {"context": None})

    @pytest.mark.parametrize(
        "callback_callable, expected_path",
        [
            pytest.param(
                empty_sync_callback_for_deadline_tests,
                qualname(empty_sync_callback_for_deadline_tests),
                id="valid_sync_callable",
            ),
            pytest.param(
                empty_async_callback_for_deadline_tests,
                qualname(empty_async_callback_for_deadline_tests),
                id="valid_async_callable",
            ),
            pytest.param(TEST_CALLBACK_PATH, TEST_CALLBACK_PATH, id="valid_path_string"),
            pytest.param(lambda x: x, None, id="lambda_function"),
            pytest.param(TEST_CALLBACK_PATH + "  ", TEST_CALLBACK_PATH, id="path_with_whitespace"),
            pytest.param(UNIMPORTABLE_DOT_PATH, UNIMPORTABLE_DOT_PATH, id="valid_format_not_importable"),
        ],
    )
    def test_get_callback_path_happy_cases(self, callback_callable, expected_path):
        path = Callback.get_callback_path(callback_callable)
        if expected_path is None:
            assert path.endswith("<lambda>")
        else:
            assert path == expected_path

    @pytest.mark.parametrize(
        "callback_callable, error_type",
        [
            pytest.param(42, ImportError, id="not_a_string"),
            pytest.param("", ImportError, id="empty_string"),
            pytest.param("os.path", AttributeError, id="non_callable_module"),
        ],
    )
    def test_get_callback_path_error_cases(self, callback_callable, error_type):
        expected_message = ""
        if error_type is ImportError:
            expected_message = "doesn't look like a valid dot path."
        elif error_type is AttributeError:
            expected_message = "is not callable."

        with pytest.raises(error_type, match=expected_message):
            Callback.get_callback_path(callback_callable)

    @pytest.mark.parametrize(
        "callback1_args, callback2_args, should_equal",
        [
            pytest.param(
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                True,
                id="identical",
            ),
            pytest.param(
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (UNIMPORTABLE_DOT_PATH, TEST_CALLBACK_KWARGS),
                False,
                id="different_path",
            ),
            pytest.param(
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, {"other": "kwargs"}),
                False,
                id="different_kwargs",
            ),
            pytest.param((TEST_CALLBACK_PATH, None), (TEST_CALLBACK_PATH, None), True, id="both_no_kwargs"),
        ],
    )
    def test_callback_equality(self, callback1_args, callback2_args, should_equal):
        callback1 = AsyncCallback(*callback1_args)
        callback2 = AsyncCallback(*callback2_args)
        assert (callback1 == callback2) == should_equal

    @pytest.mark.parametrize(
        "callback_class, args1, args2, should_be_same_hash",
        [
            pytest.param(
                AsyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                True,
                id="async_identical",
            ),
            pytest.param(
                SyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                True,
                id="sync_identical",
            ),
            pytest.param(
                AsyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (UNIMPORTABLE_DOT_PATH, TEST_CALLBACK_KWARGS),
                False,
                id="async_different_path",
            ),
            pytest.param(
                SyncCallback,
                (TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
                (TEST_CALLBACK_PATH, {"other": "kwargs"}),
                False,
                id="sync_different_kwargs",
            ),
            pytest.param(
                AsyncCallback,
                (TEST_CALLBACK_PATH, None),
                (TEST_CALLBACK_PATH, None),
                True,
                id="async_no_kwargs",
            ),
        ],
    )
    def test_callback_hash_and_set_behavior(self, callback_class, args1, args2, should_be_same_hash):
        callback1 = callback_class(*args1)
        callback2 = callback_class(*args2)
        assert (hash(callback1) == hash(callback2)) == should_be_same_hash


class TestAsyncCallback:
    @pytest.mark.parametrize(
        "callback_callable, kwargs, expected_path",
        [
            pytest.param(
                empty_async_callback_for_deadline_tests,
                TEST_CALLBACK_KWARGS,
                TEST_CALLBACK_PATH,
                id="callable",
            ),
            pytest.param(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS, TEST_CALLBACK_PATH, id="string_path"),
            pytest.param(
                UNIMPORTABLE_DOT_PATH, TEST_CALLBACK_KWARGS, UNIMPORTABLE_DOT_PATH, id="unimportable_path"
            ),
        ],
    )
    def test_init(self, callback_callable, kwargs, expected_path):
        callback = AsyncCallback(callback_callable, kwargs=kwargs)
        assert callback.path == expected_path
        assert callback.kwargs == kwargs
        assert isinstance(callback, Callback)

    def test_init_error(self):
        with pytest.raises(AttributeError, match="is not awaitable."):
            AsyncCallback(empty_sync_callback_for_deadline_tests)

    def test_serialize_deserialize(self):
        callback = AsyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)
        serialized = serialize(callback)
        deserialized = cast("Callback", deserialize(serialized.copy()))
        assert callback == deserialized


class TestSyncCallback:
    @pytest.mark.parametrize(
        "callback_callable, executor",
        [
            pytest.param(empty_sync_callback_for_deadline_tests, "remote", id="with_executor"),
            pytest.param(empty_sync_callback_for_deadline_tests, None, id="without_executor"),
            pytest.param(qualname(empty_sync_callback_for_deadline_tests), None, id="importable_path"),
            pytest.param(UNIMPORTABLE_DOT_PATH, None, id="unimportable_path"),
        ],
    )
    def test_init(self, callback_callable, executor):
        callback = SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS, executor=executor)

        assert callback.path == TEST_CALLBACK_PATH
        assert callback.kwargs == TEST_CALLBACK_KWARGS
        assert callback.executor == executor
        assert isinstance(callback, Callback)

    def test_serialize_deserialize(self):
        callback = SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS, executor="local")
        serialized = serialize(callback)
        deserialized = cast("Callback", deserialize(serialized.copy()))
        assert callback == deserialized


# While DeadlineReference lives in the SDK package, the unit tests to confirm it
# works need database access so they live in the models/test_deadline.py module.
