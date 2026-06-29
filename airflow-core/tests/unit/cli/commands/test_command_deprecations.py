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

Every command decorated with ``deprecated_for_airflowctl`` must have one entry below. When a
command is deprecated, add a row to ``MIGRATED_CLI_COMMANDS`` -- the test then verifies the decorator
recorded the right ``airflowctl`` replacement for maintainers. The commands stay in the ``airflow``
CLI as supported entry points, so they emit no user-facing deprecation warning; they are simply no
longer developed here -- new work belongs in ``airflowctl``. See
``contributing-docs/27_cli_implementation_guide.rst`` for the CLI / ``airflowctl`` direction.
"""

from __future__ import annotations

import pytest

from airflow.cli.commands import asset_command, dag_command, pool_command

# (command callable, expected airflowctl replacement recorded by the decorator)
MIGRATED_CLI_COMMANDS = [
    (dag_command.dag_trigger, "airflowctl dags trigger"),
    (dag_command.dag_delete, "airflowctl dags delete"),
    (pool_command.pool_list, "airflowctl pools list"),
    (pool_command.pool_get, "airflowctl pools get"),
    (pool_command.pool_set, "airflowctl pools create"),
    (pool_command.pool_delete, "airflowctl pools delete"),
    (pool_command.pool_import, "airflowctl pools import"),
    (pool_command.pool_export, "airflowctl pools export"),
    (asset_command.asset_materialize, "airflowctl assets materialize"),
    (asset_command.asset_list, "airflowctl assets list / airflowctl assets list-aliases"),
    (asset_command.asset_details, "airflowctl assets get / airflowctl assets get-by-alias"),
]


@pytest.mark.parametrize(
    ("command", "replacement"),
    MIGRATED_CLI_COMMANDS,
    ids=[replacement for _, replacement in MIGRATED_CLI_COMMANDS],
)
def test_migrated_cli_command_records_airflowctl_replacement(command, replacement):
    """Each migrated command records its ``airflowctl`` counterpart for maintainers.

    The marker is the maintainer-facing trace of the migration; users see no runtime deprecation
    warning. The command body itself is exercised by the per-command test modules.
    ``functools.wraps`` on the outer ``action_cli`` decorator propagates the attribute up to the
    command object imported here.
    """
    assert getattr(command, "_migrated_to_airflowctl", None) == replacement
