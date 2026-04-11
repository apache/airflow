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

import pytest

from airflow.providers.databricks.utils.retry import validate_deferrable_databricks_retry_args

from unit.databricks._retry_test_utils import UNSUPPORTED_RETRY_ARGS, assert_invalid_retry_args_raises


def test_validate_deferrable_databricks_retry_args_accepts_none():
    validate_deferrable_databricks_retry_args(None, owner="test-owner")


@pytest.mark.parametrize(
    "retry_args",
    [
        {},
        {"retry_limit": 3, "retry_delay": 10},
        {"retry_limit": 3, "retry_delay": 10.5, "retry_enabled": True, "retry_codes": ["429", "500"]},
    ],
)
def test_validate_deferrable_databricks_retry_args_accepts_json_serializable_values(retry_args):
    validate_deferrable_databricks_retry_args(retry_args, owner="test-owner")


@pytest.mark.parametrize("retry_args", UNSUPPORTED_RETRY_ARGS)
def test_validate_deferrable_databricks_retry_args_rejects_non_serializable_values(retry_args):
    assert_invalid_retry_args_raises(
        lambda: validate_deferrable_databricks_retry_args(retry_args, owner="test-owner")
    )
