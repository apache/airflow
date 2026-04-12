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
"""Tests for airflowctl command validation that happen before API calls."""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("command", "expected_error"),
    [
        # Pool validations
        ("pools create --name=test_pool", "Missing required field(s): Slots"),
        ("pools create --slots=5", "Missing required field(s): Name"),
        # Connection validations
        ("connections create --connection-id=test_conn", "Missing required field(s): Conn Type"),
        ("connections create --conn-type=mysql", "Missing required field(s): Connection Id"),
        # Variable validations
        ("variables create --key=test_key", "Missing required field(s): Value"),
        ("variables create --value=test_value", "Missing required field(s): Key"),
    ],
    ids=[
        "pools create missing slots",
        "pools create missing name",
        "connections create missing conn_type",
        "connections create missing connection_id",
        "variables create missing value",
        "variables create missing key",
    ],
)
def test_validation_errors_without_api(command: str, expected_error: str, run_command):
    """
    Test that validation errors are raised before making API calls.

    These tests verify that the validation logic catches missing required fields
    and returns appropriate error messages without attempting to connect to the API.
    This ensures that validation happens client-side before any network requests.
    """
    run_command(
        command=command,
        env_vars={"AIRFLOW_CLI_DEBUG_MODE": "true"},
        skip_login=True,
        expect_failure=True,
        expected_error_pattern=expected_error,
    )
