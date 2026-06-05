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

import datetime

import pytest
from tenacity import stop_after_attempt, wait_incrementing

from airflow.providers.databricks.utils.retry import validate_deferrable_databricks_retry_args

INVALID_RETRY_ARGS_PATTERN = (
    "does not support non-serializable retry_args/databricks_retry_args when deferrable=True"
)
UNSUPPORTED_RETRY_ARGS = [
    pytest.param({"wait": wait_incrementing(start=1, increment=1, max=3)}, id="wait_incrementing"),
    pytest.param({"stop": stop_after_attempt(3)}, id="stop_after_attempt"),
]


def test_validate_deferrable_databricks_retry_args_accepts_none():
    assert validate_deferrable_databricks_retry_args(None, owner="test-owner") is None


@pytest.mark.parametrize(
    "retry_args",
    [
        {},
        {"retry_limit": 3, "retry_delay": 10},
        {"retry_limit": 3, "retry_delay": 10.5, "retry_enabled": True, "retry_codes": ["429", "500"]},
    ],
)
def test_validate_deferrable_databricks_retry_args_accepts_serde_serializable_values(retry_args):
    assert validate_deferrable_databricks_retry_args(retry_args, owner="test-owner") is None


def test_validate_deferrable_databricks_retry_args_accepts_airflow_serde_serializable_values():
    retry_args = {"deadline": datetime.datetime(2026, 5, 29, 12, 30, tzinfo=datetime.timezone.utc)}

    assert validate_deferrable_databricks_retry_args(retry_args, owner="test-owner") is None


@pytest.mark.parametrize("retry_args", UNSUPPORTED_RETRY_ARGS)
def test_validate_deferrable_databricks_retry_args_rejects_non_serializable_values(retry_args):
    with pytest.raises(ValueError, match=INVALID_RETRY_ARGS_PATTERN):
        validate_deferrable_databricks_retry_args(retry_args, owner="test-owner")
