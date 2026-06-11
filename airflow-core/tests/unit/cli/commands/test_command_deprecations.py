#
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
Single source of truth for the ``airflow`` CLI commands deprecated in favour of ``airflowctl``.

Every command decorated with ``deprecated_for_airflowctl`` must have one entry below. When a new
command is migrated and deprecated, add a row to ``DEPRECATED_CLI_COMMANDS`` -- the test then
verifies it emits ``RemovedInAirflow4Warning`` pointing at the right ``airflowctl`` command.
"""

from __future__ import annotations

import contextlib
import re

import pytest

from airflow.cli.commands import asset_command, dag_command, pool_command
from airflow.exceptions import RemovedInAirflow4Warning

# (command callable, argv to parse, expected airflowctl replacement named in the warning)
DEPRECATED_CLI_COMMANDS = [
    (dag_command.dag_trigger, ["dags", "trigger", "example_dag", "--run-id=x"], "airflowctl dags trigger"),
    (dag_command.dag_delete, ["dags", "delete", "example_dag", "--yes"], "airflowctl dags delete"),
    (pool_command.pool_list, ["pools", "list"], "airflowctl pools list"),
    (pool_command.pool_get, ["pools", "get", "foo"], "airflowctl pools get"),
    (pool_command.pool_set, ["pools", "set", "foo", "1", "desc"], "airflowctl pools create"),
    (pool_command.pool_delete, ["pools", "delete", "foo"], "airflowctl pools delete"),
    (pool_command.pool_import, ["pools", "import", "/nonexistent.json"], "airflowctl pools import"),
    (
        pool_command.pool_export,
        ["pools", "export", "/tmp/airflow_pools_export.json"],
        "airflowctl pools export",
    ),
    (
        asset_command.asset_materialize,
        ["assets", "materialize", "--name=foo"],
        "airflowctl assets materialize",
    ),
]


@pytest.mark.parametrize(
    ("command", "argv", "replacement"),
    DEPRECATED_CLI_COMMANDS,
    ids=[argv[0] + "-" + argv[1] for _, argv, _ in DEPRECATED_CLI_COMMANDS],
)
def test_deprecated_cli_command_points_to_airflowctl(command, argv, replacement, parser, mock_cli_api_client):
    """Each migrated command warns it will become an alias for its ``airflowctl`` counterpart.

    We only assert the deprecation warning fires (and names the right replacement); the command
    body itself is exercised by the per-command test modules, so any error it raises against the
    bare mocked client is irrelevant here and suppressed.
    """
    with pytest.warns(RemovedInAirflow4Warning, match=re.escape(replacement)):
        with contextlib.suppress(Exception, SystemExit):
            command(parser.parse_args(argv))
