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

from airflowctl_tests.constants import LOGIN_COMMAND


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
    # Test trigger without logical-date (should default to now)
    "dags trigger --dag-id=example_bash_operator",
    "dags pause example_bash_operator",
    "dags unpause example_bash_operator",
    # DAG Run commands
    f'dagrun get --dag-id=example_bash_operator --dag-run-id="manual__{ONE_DATE_PARAM}"',
    "dags update --dag-id=example_bash_operator --no-is-paused",
    # DAG Run commands
    "dagrun list --dag-id example_bash_operator --state success --limit=1",
    # XCom commands - need a DAG run with completed tasks
    f'xcom add --dag-id=example_bash_operator --dag-run-id="manual__{ONE_DATE_PARAM}" --task-id=runme_0 --key=test_xcom_key --value=\'{{"test": "value"}}\'',
    f'xcom get --dag-id=example_bash_operator --dag-run-id="manual__{ONE_DATE_PARAM}" --task-id=runme_0 --key=test_xcom_key',
    f'xcom list --dag-id=example_bash_operator --dag-run-id="manual__{ONE_DATE_PARAM}" --task-id=runme_0',
    f'xcom edit --dag-id=example_bash_operator --dag-run-id="manual__{ONE_DATE_PARAM}" --task-id=runme_0 --key=test_xcom_key --value=\'{{"updated": "value"}}\'',
    f'xcom delete --dag-id=example_bash_operator --dag-run-id="manual__{ONE_DATE_PARAM}" --task-id=runme_0 --key=test_xcom_key',
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
    # Version command
    "version --remote",
]


@pytest.mark.flaky(reruns=3, reruns_delay=1)
@pytest.mark.parametrize(
    "command", TEST_COMMANDS, ids=[" ".join(command.split(" ", 2)[:2]) for command in TEST_COMMANDS]
)
def test_airflowctl_commands(command: str, run_command):
    """Test airflowctl commands using docker-compose environment."""
    run_command(command)
