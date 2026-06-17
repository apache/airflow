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

import pytest

from airflow.sdk.definitions.callback import AsyncCallback
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference


async def _async_callback():
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
