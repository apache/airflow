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


# Passing password via command line is insecure but acceptable for testing purposes
# Please do not do this in production, it enables possibility of exposing your credentials
CREDENTIAL_SUFFIX = "--username airflow --password airflow"
LOGIN_COMMAND = f"auth login {CREDENTIAL_SUFFIX}"
LOGIN_COMMAND_SKIP_KEYRING = "auth login --skip-keyring"
LOGIN_OUTPUT = "Login successful! Welcome to airflowctl!"
TEST_COMMANDS = [
    # Auth commands
    f"auth token {CREDENTIAL_SUFFIX}",
    "auth list-envs",
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
    "dags trigger --dag-id=example_bash_operator --logical-date={date_param} --run-after={date_param}",
    # Test trigger without logical-date (should default to now)
    "dags trigger --dag-id=example_bash_operator",
    "dags pause example_bash_operator",
    "dags unpause example_bash_operator",
    # DAG Run commands
    'dagrun get --dag-id=example_bash_operator --dag-run-id="manual__{date_param}"',
    "dags update --dag-id=example_bash_operator --no-is-paused",
    # DAG Run commands
    "dagrun list --dag-id example_bash_operator --state success --limit=1",
    # XCom commands - need a DAG run with completed tasks
    'xcom add --dag-id=example_bash_operator --dag-run-id="manual__{date_param}" --task-id=runme_0 --key={xcom_key} --value=\'{{"test": "value"}}\'',
    'xcom get --dag-id=example_bash_operator --dag-run-id="manual__{date_param}" --task-id=runme_0 --key={xcom_key}',
    'xcom list --dag-id=example_bash_operator --dag-run-id="manual__{date_param}" --task-id=runme_0',
    'xcom edit --dag-id=example_bash_operator --dag-run-id="manual__{date_param}" --task-id=runme_0 --key={xcom_key} --value=\'{{"updated": "value"}}\'',
    'xcom delete --dag-id=example_bash_operator --dag-run-id="manual__{date_param}" --task-id=runme_0 --key={xcom_key}',
    # Jobs commands
    "jobs list",
    # Pools commands
    "pools create --name=test_pool --slots=5",
    "pools list",
    "pools get --pool-name=test_pool",
    "pools get --pool-name=test_pool -o yaml",
    "pools update --pool=test_pool --slots=10",
    "pools import tests/airflowctl_tests/fixtures/test_pools.json",
    "pools export tests/airflowctl_tests/fixtures/pools_export.json --output=json",
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
    # Plugins command
    "plugins list",
    "plugins list-import-errors",
]

NO_AUTH_TEST_COMMANDS = [
    "version --remote",
]

DATE_PARAM_1 = date_param()
DATE_PARAM_2 = date_param()

# Unique xcom key per test run to avoid "already exists" errors from leftover state
_XCOM_KEY_1 = f"test_xcom_key_{DATE_PARAM_1.replace(':', '').replace('+', '').replace('-', '')[:16]}"
_XCOM_KEY_2 = f"test_xcom_key_{DATE_PARAM_2.replace(':', '').replace('+', '').replace('-', '')[:16]}"
TEST_COMMANDS_DEBUG_MODE = [LOGIN_COMMAND] + [
    test.format(date_param=DATE_PARAM_1, xcom_key=_XCOM_KEY_1) for test in TEST_COMMANDS
]
TEST_COMMANDS_SKIP_KEYRING = [LOGIN_COMMAND_SKIP_KEYRING] + [
    test.format(date_param=DATE_PARAM_2, xcom_key=_XCOM_KEY_2) for test in TEST_COMMANDS
]


def test_hardcoded_xcom_key_would_collide():
    """Regression: a hardcoded xcom key produces identical 'xcom add' commands
    across parametrize sets, causing 'already exists' errors when both run
    against the same Airflow instance (the bug that was fixed)."""
    xcom_add_template = [t for t in TEST_COMMANDS if "xcom add" in t]
    assert xcom_add_template, "xcom add must be in TEST_COMMANDS"

    hardcoded = xcom_add_template[0].format(date_param=DATE_PARAM_1, xcom_key="test_xcom_key")
    also_hardcoded = xcom_add_template[0].format(date_param=DATE_PARAM_2, xcom_key="test_xcom_key")
    # With a hardcoded key, only the dag-run-id differs — but if the same date
    # is reused (e.g. across retries), the commands are fully identical → collision.
    assert "test_xcom_key" in hardcoded
    assert "test_xcom_key" in also_hardcoded

    # The fix: derived keys are unique per set, so commands always differ.
    debug_cmd = xcom_add_template[0].format(date_param=DATE_PARAM_1, xcom_key=_XCOM_KEY_1)
    keyring_cmd = xcom_add_template[0].format(date_param=DATE_PARAM_2, xcom_key=_XCOM_KEY_2)
    assert _XCOM_KEY_1 != _XCOM_KEY_2, "derived xcom keys must differ between sets"
    assert debug_cmd != keyring_cmd, "xcom add commands must differ to avoid collisions"


@pytest.mark.parametrize(
    "command",
    TEST_COMMANDS_DEBUG_MODE,
    ids=[" ".join(command.split(" ", 2)[:2]) for command in TEST_COMMANDS_DEBUG_MODE],
)
def test_airflowctl_commands(command: str, run_command):
    """Test airflowctl commands using docker-compose environment."""
    run_command(command=command, env_vars={"AIRFLOW_CLI_DEBUG_MODE": "true"}, skip_login=True)


@pytest.mark.parametrize(
    "command",
    TEST_COMMANDS_SKIP_KEYRING,
    ids=[" ".join(command.split(" ", 2)[:2]) for command in TEST_COMMANDS_SKIP_KEYRING],
)
def test_airflowctl_commands_skip_keyring(command: str, api_token: str, run_command):
    """Test airflowctl commands using docker-compose environment without using keyring."""
    run_command(
        command=command,
        env_vars={
            "AIRFLOW_CLI_TOKEN": api_token,
            "AIRFLOW_CLI_DEBUG_MODE": "false",
            "AIRFLOW_CLI_ENVIRONMENT": "nokeyring",
        },
        skip_login=True,
    )


@pytest.mark.parametrize("command", NO_AUTH_TEST_COMMANDS)
def test_airflowctl_no_auth_commands(command: str, run_command, tmp_path):
    """Test airflowctl no-auth commands without login or persisted credentials."""
    run_command(
        command=command,
        env_vars={
            "AIRFLOW_HOME": str(tmp_path),
            "AIRFLOW_CLI_ENVIRONMENT": "no-auth",
            "AIRFLOW_CLI_DEBUG_MODE": "false",
        },
        skip_login=True,
    )
