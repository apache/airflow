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

from airflow.providers.fab.auth_manager.ctl_commands.definition import (
    PERMISSIONS_COMMANDS,
    ROLES_COMMANDS,
    USERS_COMMANDS,
    get_fab_airflowctl_commands,
)


class TestFabAirflowctlDefinition:
    def test_users_commands_match_routes(self):
        # 1:1 mapping with auth_manager/api_fastapi/routes/users.py.
        assert sorted(c.name for c in USERS_COMMANDS) == [
            "create",
            "delete",
            "get",
            "list",
            "update",
        ]

    def test_roles_commands_match_routes(self):
        # 1:1 mapping with auth_manager/api_fastapi/routes/roles.py (excluding /permissions).
        assert sorted(c.name for c in ROLES_COMMANDS) == [
            "create",
            "delete",
            "get",
            "list",
            "update",
        ]

    def test_permissions_commands_match_routes(self):
        assert sorted(c.name for c in PERMISSIONS_COMMANDS) == ["list"]

    def test_top_level_groups(self):
        groups = get_fab_airflowctl_commands()
        assert sorted(g.name for g in groups) == ["permissions", "roles", "users"]
