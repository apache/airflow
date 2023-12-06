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
from __future__ import annotations

import json
from contextlib import redirect_stdout
from io import StringIO
from typing import TYPE_CHECKING

import pytest

from airflow.auth.managers.fab.cli_commands import role_command
from airflow.auth.managers.fab.cli_commands.utils import get_application_builder
from airflow.cli import cli_parser
from airflow.security import permissions

pytestmark = pytest.mark.db_test

if TYPE_CHECKING:
    from airflow.auth.managers.fab.models import Role

TEST_USER1_EMAIL = "test-user1@example.com"
TEST_USER2_EMAIL = "test-user2@example.com"


class TestCliRoles:
    @pytest.fixture(autouse=True)
    def _set_attrs(self):
        self.parser = cli_parser.get_parser()
        with get_application_builder() as appbuilder:
            self.appbuilder = appbuilder
            self.clear_roles_and_roles()
            yield
            self.clear_roles_and_roles()

    def clear_roles_and_roles(self):
        for email in [TEST_USER1_EMAIL, TEST_USER2_EMAIL]:
            test_user = self.appbuilder.sm.find_user(email=email)
            if test_user:
                self.appbuilder.sm.del_register_user(test_user)
        for role_name in ["FakeTeamA", "FakeTeamB", "FakeTeamC"]:
            if self.appbuilder.sm.find_role(role_name):
                self.appbuilder.sm.delete_role(role_name)

    def test_cli_create_roles(self):
        assert self.appbuilder.sm.find_role("FakeTeamA") is None
        assert self.appbuilder.sm.find_role("FakeTeamB") is None

        args = self.parser.parse_args(["roles", "create", "FakeTeamA", "FakeTeamB"])
        role_command.roles_create(args)

        assert self.appbuilder.sm.find_role("FakeTeamA") is not None
        assert self.appbuilder.sm.find_role("FakeTeamB") is not None

    def test_cli_delete_roles(self):
        assert self.appbuilder.sm.find_role("FakeTeamA") is None
        assert self.appbuilder.sm.find_role("FakeTeamB") is None
        assert self.appbuilder.sm.find_role("FakeTeamC") is None

        self.appbuilder.sm.add_role("FakeTeamA")
        self.appbuilder.sm.add_role("FakeTeamB")
        self.appbuilder.sm.add_role("FakeTeamC")

        args = self.parser.parse_args(["roles", "delete", "FakeTeamA", "FakeTeamC"])
        role_command.roles_delete(args)

        assert self.appbuilder.sm.find_role("FakeTeamA") is None
        assert self.appbuilder.sm.find_role("FakeTeamB") is not None
        assert self.appbuilder.sm.find_role("FakeTeamC") is None

    def test_cli_create_roles_is_reentrant(self):
        assert self.appbuilder.sm.find_role("FakeTeamA") is None
        assert self.appbuilder.sm.find_role("FakeTeamB") is None

        args = self.parser.parse_args(["roles", "create", "FakeTeamA", "FakeTeamB"])

        role_command.roles_create(args)

        assert self.appbuilder.sm.find_role("FakeTeamA") is not None
        assert self.appbuilder.sm.find_role("FakeTeamB") is not None

    def test_cli_list_roles(self):
        self.appbuilder.sm.add_role("FakeTeamA")
        self.appbuilder.sm.add_role("FakeTeamB")

        with redirect_stdout(StringIO()) as stdout:
            role_command.roles_list(self.parser.parse_args(["roles", "list"]))
            stdout = stdout.getvalue()

        assert "FakeTeamA" in stdout
        assert "FakeTeamB" in stdout

    def test_cli_list_roles_with_args(self):
        role_command.roles_list(self.parser.parse_args(["roles", "list", "--output", "yaml"]))
        role_command.roles_list(self.parser.parse_args(["roles", "list", "-p", "--output", "yaml"]))

    def test_cli_roles_add_and_del_perms(self):
        assert self.appbuilder.sm.find_role("FakeTeamC") is None

        role_command.roles_create(self.parser.parse_args(["roles", "create", "FakeTeamC"]))
        assert self.appbuilder.sm.find_role("FakeTeamC") is not None
        role: Role = self.appbuilder.sm.find_role("FakeTeamC")
        assert len(role.permissions) == 0

        role_command.roles_add_perms(
            self.parser.parse_args(
                [
                    "roles",
                    "add-perms",
                    "FakeTeamC",
                    "-r",
                    permissions.RESOURCE_POOL,
                    "-a",
                    permissions.ACTION_CAN_EDIT,
                ]
            )
        )
        role: Role = self.appbuilder.sm.find_role("FakeTeamC")
        assert len(role.permissions) == 1
        assert role.permissions[0].resource.name == permissions.RESOURCE_POOL
        assert role.permissions[0].action.name == permissions.ACTION_CAN_EDIT

        role_command.roles_del_perms(
            self.parser.parse_args(
                [
                    "roles",
                    "del-perms",
                    "FakeTeamC",
                    "-r",
                    permissions.RESOURCE_POOL,
                    "-a",
                    permissions.ACTION_CAN_EDIT,
                ]
            )
        )
        role: Role = self.appbuilder.sm.find_role("FakeTeamC")
        assert len(role.permissions) == 0

    def test_cli_import_roles(self, tmp_path):
        fn = tmp_path / "import_roles.json"
        fn.touch()
        roles_list = ["FakeTeamA", "FakeTeamB"]
        with open(fn, "w") as outfile:
            json.dump(roles_list, outfile)
        role_command.roles_import(self.parser.parse_args(["roles", "import", str(fn)]))
        assert self.appbuilder.sm.find_role("FakeTeamA") is not None
        assert self.appbuilder.sm.find_role("FakeTeamB") is not None

    def test_cli_export_roles(self, tmp_path):
        fn = tmp_path / "export_roles.json"
        fn.touch()
        args = self.parser.parse_args(["roles", "create", "FakeTeamA", "FakeTeamB"])
        role_command.roles_create(args)

        assert self.appbuilder.sm.find_role("FakeTeamA") is not None
        assert self.appbuilder.sm.find_role("FakeTeamB") is not None

        role_command.roles_export(self.parser.parse_args(["roles", "export", str(fn)]))
        with open(fn) as outfile:
            roles_exported = json.load(outfile)
        assert "FakeTeamA" in roles_exported
        assert "FakeTeamB" in roles_exported
