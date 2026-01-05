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


def date_param():
    import random
    from datetime import datetime, timedelta

    from dateutil.relativedelta import relativedelta

    # original datetime string
    dt_str = "2025-10-25T00:02:00+00:00"

    # parse to datetime object
    dt = datetime.fromisoformat(dt_str)

    # boundaries
    start = dt - relativedelta(months=1)
    end = dt + relativedelta(months=1)

    # pick random time between start and end
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    random_dt = start + timedelta(seconds=random_seconds)
    return random_dt.isoformat()


LOGIN_COMMAND = "auth login --username airflow --password airflow"
LOGIN_OUTPUT = "Login successful! Welcome to airflowctl!"
ONE_DATE_PARAM = date_param()
TEST_COMMANDS = [
    # Passing password via command line is insecure but acceptable for testing purposes
    # Please do not do this in production, it enables possibility of exposing your credentials
    LOGIN_COMMAND,
    # Assets commands
    "assets list",
    "assets get --asset-id=1",
    "assets create-event --asset-id=1",
    # Backfill commands
    "backfill list",
    # Config commands
    "config get --section core --option executor",
    "config list",
    "config lint",
    # Connections commands
    "connections create --connection-id=test_con --conn-type=mysql --password=TEST_PASS -o json",
    "connections list",
    "connections list -o yaml",
    "connections list -o table",
    "connections get --conn-id=test_con",
    "connections get --conn-id=test_con -o json",
    "connections update --connection-id=test_con --conn-type=postgres",
    "connections import tests/airflowctl_tests/fixtures/test_connections.json",
    "connections delete --conn-id=test_con",
    "connections delete --conn-id=test_import_conn",
    # DAGs commands
    "dags list",
    "dags get --dag-id=example_bash_operator",
    "dags get-details --dag-id=example_bash_operator",
    "dags get-stats --dag-ids=example_bash_operator",
    "dags get-version --dag-id=example_bash_operator --version-number=1",
    "dags list-import-errors",
    "dags list-version --dag-id=example_bash_operator",
    "dags list-warning",
    # Order of trigger and pause/unpause is important for test stability because state checked
    f"dags trigger --dag-id=example_bash_operator --logical-date={ONE_DATE_PARAM} --run-after={ONE_DATE_PARAM}",
    "dags pause example_bash_operator",
    "dags unpause example_bash_operator",
    # DAG Run commands
    f'dagrun get --dag-id=example_bash_operator --dag-run-id="manual__{ONE_DATE_PARAM}"',
    "dagrun list --dag-id example_bash_operator --state success --limit=1",
    # Jobs commands
    "jobs list",
    # Pools commands
    "pools create --name=test_pool --slots=5",
    "pools list",
    "pools get --pool-name=test_pool",
    "pools get --pool-name=test_pool -o yaml",
    "pools update --pool=test_pool --slots=10",
    "pools import tests/airflowctl_tests/fixtures/test_pools.json",
    "pools delete --pool=test_pool",
    "pools delete --pool=test_import_pool",
    # Providers commands
    "providers list",
    # Variables commands
    "variables create --key=test_key --value=test_value",
    "variables list",
    "variables get --variable-key=test_key",
    "variables get --variable-key=test_key -o table",
    "variables update --key=test_key --value=updated_value",
    "variables import tests/airflowctl_tests/fixtures/test_variables.json",
    "variables delete --variable-key=test_key",
    "variables delete --variable-key=test_import_var",
    "variables delete --variable-key=test_import_var_with_desc",
    # Version command
    "version --remote",
]


@pytest.mark.parametrize(
    "command", TEST_COMMANDS, ids=[" ".join(id.split(" ", 2)[:2]) for id in TEST_COMMANDS]
)
def test_airflowctl_commands(command: str):
    """Test airflowctl commands using docker-compose environment."""
    host_envs = os.environ.copy()
    host_envs["AIRFLOW_CLI_DEBUG_MODE"] = "true"

    command_from_config = f"airflowctl {command}"
    # We need to run auth login first for all commands except login itself
    if command != LOGIN_COMMAND:
        run_command = f"airflowctl {LOGIN_COMMAND} && {command_from_config}"
    else:
        run_command = command_from_config
    console.print(f"[yellow]Running command: {command}")

    # Give some time for the command to execute and output to be ready
    proc = Popen(run_command.encode(), stdout=PIPE, stderr=STDOUT, stdin=PIPE, shell=True, env=host_envs)
    stdout_bytes, stderr_result = proc.communicate(timeout=60)

    # CLI command gave errors
    assert not stderr_result, (
        f"Errors while executing command '{command_from_config}':\n{stderr_result.decode()}"
    )

    # Decode the output
    stdout_result = stdout_bytes.decode()
    # We need to trim auth login output if the command is not login itself and clean backspaces
    if command != LOGIN_COMMAND:
        assert LOGIN_OUTPUT in stdout_result, (
            f"❌ Login output not found before command output for '{command_from_config}'",
            f"\nFull output:\n{stdout_result}",
        )
        stdout_result = stdout_result.split(f"{LOGIN_OUTPUT}\n")[1].strip()
    else:
        stdout_result = stdout_result.strip()

    # Check for non-zero exit code
    assert proc.returncode == 0, (
        f"❌ Command '{command_from_config}' exited with code {proc.returncode}",
        f"\nOutput:\n{stdout_result}",
    )

    # Error patterns to detect failures that might otherwise slip through
    # Please ensure it is aligning with airflowctl.api.client.get_json_error
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

    console.print(f"[green]✅ Output did not contain unexpected text for command '{command_from_config}'")
    console.print(f"[cyan]Result:\n{stdout_result}\n")
    proc.kill()
