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

__all__ = ["get_keycloak_cli_commands"]


def get_keycloak_cli_commands():
    """Return CLI commands for Keycloak auth manager."""
    from airflow.cli.cli_config import GroupCommand
    from airflow.providers.keycloak.auth_manager.cli.definition import KEYCLOAK_AUTH_MANAGER_COMMANDS

    return [
        GroupCommand(
            name="keycloak-auth-manager",
            help="Manage resources used by Keycloak auth manager",
            subcommands=KEYCLOAK_AUTH_MANAGER_COMMANDS,
        ),
    ]

