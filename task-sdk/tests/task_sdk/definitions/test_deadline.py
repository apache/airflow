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

import pytest
from task_sdk.definitions.test_callback import TEST_CALLBACK_KWARGS, TEST_CALLBACK_PATH, UNIMPORTABLE_DOT_PATH

from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference

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

    def test_deadline_alert_unsupported_callback(self):
        with pytest.raises(ValueError, match="Callbacks of type SyncCallback are not currently supported"):
            DeadlineAlert(
                reference=DeadlineReference.DAGRUN_QUEUED_AT,
                interval=timedelta(hours=1),
                callback=SyncCallback(TEST_CALLBACK_PATH),
            )
