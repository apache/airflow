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

from airflow.providers.fab.auth_manager.cli_commands.definition import (
    ROLES_COMMANDS,
    SYNC_PERM_COMMAND,
    USERS_COMMANDS,
)


class TestCliDefinition:
    def test_users_commands(self):
        assert len(USERS_COMMANDS) == 8

    def test_roles_commands(self):
        assert len(ROLES_COMMANDS) == 7

    def test_sync_perm_command(self):
        assert SYNC_PERM_COMMAND.name == "sync-perm"
