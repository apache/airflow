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

import os
from subprocess import PIPE, STDOUT, Popen

import pytest

from airflowctl_tests import console

LOGIN_COMMAND = "auth login --username airflow --password airflow"
LOGIN_OUTPUT = "Login successful! Welcome to airflowctl!"

# Test commands for config sensitive masking verification
SENSITIVE_CONFIG_COMMANDS = [
    # Test that config list shows masked sensitive values
    "config list",
    # Test that getting specific sensitive config values are masked
    "config get --section core --option fernet_key",
    "config get --section database --option sql_alchemy_conn",
]


@pytest.mark.flaky(reruns=3, reruns_delay=1)
@pytest.mark.parametrize(
    "command",
    SENSITIVE_CONFIG_COMMANDS,
    ids=[" ".join(command.split(" ", 2)[:2]) for command in SENSITIVE_CONFIG_COMMANDS],
)
def test_config_sensitive_masking(command: str):
    """
    Test that sensitive config values are properly masked by airflowctl.

    This integration test verifies that when airflowctl retrieves config data from the
    Airflow API, sensitive values (like fernet_key, sql_alchemy_conn) appear masked
    as '< hidden >' and do not leak actual secret values.
    """
    host_envs = os.environ.copy()
    host_envs["AIRFLOW_CLI_DEBUG_MODE"] = "true"

    command_from_config = f"airflowctl {command}"
    # Run auth login first, then the config command
    run_command = f"airflowctl {LOGIN_COMMAND} && {command_from_config}"
    console.print(f"[yellow]Running command: {command}")

    # Execute the command
    proc = Popen(run_command.encode(), stdout=PIPE, stderr=STDOUT, stdin=PIPE, shell=True, env=host_envs)
    stdout_bytes, stderr_result = proc.communicate(timeout=60)

    # CLI command gave errors
    assert not stderr_result, (
        f"Errors while executing command '{command_from_config}':\n{stderr_result.decode()}"
    )

    # Decode the output
    stdout_result = stdout_bytes.decode()

    # Trim auth login output and get the actual command output
    assert LOGIN_OUTPUT in stdout_result, (
        f"❌ Login output not found before command output for '{command_from_config}'",
        f"\nFull output:\n{stdout_result}",
    )
    stdout_result = stdout_result.split(f"{LOGIN_OUTPUT}\n")[1].strip()

    # Check for non-zero exit code
    assert proc.returncode == 0, (
        f"❌ Command '{command_from_config}' exited with code {proc.returncode}",
        f"\nOutput:\n{stdout_result}",
    )

    # Error patterns to detect failures
    error_patterns = [
        "Server error",
        "command error",
        "unrecognized arguments",
        "invalid choice",
        "Traceback (most recent call last):",
    ]
    matched_error = next((error for error in error_patterns if error in stdout_result), None)
    assert not matched_error, (
        f"❌ Output contained unexpected text for command '{command_from_config}'",
        f"\nMatched error pattern: {matched_error}",
        f"\nOutput:\n{stdout_result}",
    )

    # CRITICAL: Verify that sensitive values are masked
    # The Airflow API returns masked values as "< hidden >" for sensitive configs
    assert "< hidden >" in stdout_result, (
        f"❌ Expected masked value '< hidden >' not found in output for '{command_from_config}'",
        f"\nOutput:\n{stdout_result}",
    )

    console.print(f"[green]✅ Sensitive config values are properly masked in command '{command_from_config}'")
    console.print(f"[cyan]Result:\n{stdout_result}\n")
    proc.kill()
