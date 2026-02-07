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

# Test commands for config sensitive masking verification
SENSITIVE_CONFIG_COMMANDS = [
    # Test that config list shows masked sensitive values
    "config list",
    # Test that getting specific sensitive config values are masked
    "config get --section core --option fernet_key",
    "config get --section database --option sql_alchemy_conn",
]


@pytest.mark.parametrize(
    "command",
    SENSITIVE_CONFIG_COMMANDS,
    ids=[" ".join(command.split(" ", 2)[:2]) for command in SENSITIVE_CONFIG_COMMANDS],
)
def test_config_sensitive_masking(command: str, run_command):
    """
    Test that sensitive config values are properly masked by airflowctl.

    This integration test verifies that when airflowctl retrieves config data from the
    Airflow API, sensitive values (like fernet_key, sql_alchemy_conn) appear masked
    as '< hidden >' and do not leak actual secret values.
    """
    stdout_result = run_command(command)

    # CRITICAL: Verify that sensitive values are masked
    # The Airflow API returns masked values as "< hidden >" for sensitive configs
    assert "< hidden >" in stdout_result, (
        f"❌ Expected masked value '< hidden >' not found in output for 'airflowctl {command}'\n"
        f"Output:\n{stdout_result}"
    )
