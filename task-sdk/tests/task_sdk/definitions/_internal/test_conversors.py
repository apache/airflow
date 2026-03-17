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

from datetime import timedelta

import pytest

from airflow.sdk.definitions._internal.abstractoperator import convert_timedelta


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, None, id="none_passthrough"),
        pytest.param(timedelta(minutes=5), timedelta(minutes=5), id="timedelta_passthrough"),
        pytest.param(300.0, timedelta(seconds=300), id="float_to_timedelta"),
        pytest.param(60, timedelta(seconds=60), id="int_to_timedelta"),
        pytest.param(0, timedelta(0), id="zero_to_timedelta"),
        pytest.param(0.5, timedelta(seconds=0.5), id="fractional_seconds"),
    ],
)
def test_convert_timedelta(value, expected):
    result = convert_timedelta(value)
    assert result == expected
    if value is not None and isinstance(value, timedelta):
        assert result is value  # identity preserved for timedelta inputs


def test_convert_timedelta_identity_for_timedelta():
    """Passing a timedelta should return the same object (no copy)."""
    td = timedelta(hours=1)
    assert convert_timedelta(td) is td


def test_task_group_retry_delay_uses_convert_timedelta():
    """TaskGroup retry_delay and max_retry_delay should accept float and convert to timedelta."""
    from airflow.sdk import DAG, timezone
    from airflow.sdk.definitions.taskgroup import TaskGroup

    with DAG(dag_id="test_convert_td", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        tg = TaskGroup(group_id="g", retries=1, retry_delay=120, max_retry_delay=600)

    assert tg.retry_delay == timedelta(seconds=120)
    assert tg.max_retry_delay == timedelta(seconds=600)


def test_task_group_max_retry_delay_none():
    """max_retry_delay=None should pass through the converter unchanged."""
    from airflow.sdk import DAG, timezone
    from airflow.sdk.definitions.taskgroup import TaskGroup

    with DAG(dag_id="test_convert_td_none", schedule=None, start_date=timezone.datetime(2024, 1, 1)):
        tg = TaskGroup(group_id="g", retries=1, max_retry_delay=None)

    assert tg.max_retry_delay is None
