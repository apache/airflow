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

import types
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from airflow.providers.fab.auth_manager.api_fastapi.services.roles import FABAuthManagerRoles


@pytest.fixture
def fab_auth_manager():
    return MagicMock()


@pytest.fixture
def security_manager():
    sm = MagicMock()
    sm.get_action.side_effect = lambda n: object() if n in {"can_read"} else None
    sm.get_resource.side_effect = lambda n: object() if n in {"DAG"} else None
    return sm


def _make_role_obj(name: str, perms: list[tuple[str, str]]):
    perm_objs = [
        types.SimpleNamespace(
            action=types.SimpleNamespace(name=a),
            resource=types.SimpleNamespace(name=r),
        )
        for (a, r) in perms
    ]
    return types.SimpleNamespace(name=name, permissions=perm_objs)


@patch("airflow.providers.fab.auth_manager.api_fastapi.services.roles.get_fab_auth_manager")
class TestRolesService:
    def setup_method(self):
        self.body_ok = types.SimpleNamespace(
            name="roleA",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_read"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )

        self.body_bad_action = types.SimpleNamespace(
            name="roleB",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="no_such_action"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )
        self.body_bad_resource = types.SimpleNamespace(
            name="roleC",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_read"),
                    resource=types.SimpleNamespace(name="NOPE"),
                )
            ],
        )

    def test_create_role_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.side_effect = [
            None,
            _make_role_obj("roleA", [("can_read", "DAG")]),
        ]
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerRoles.create_role(self.body_ok)

        assert out.name == "roleA"
        assert out.permissions
        assert out.permissions[0].action.name == "can_read"
        assert out.permissions[0].resource.name == "DAG"
        security_manager.bulk_sync_roles.assert_called_once_with(
            [{"role": "roleA", "perms": [("can_read", "DAG")]}]
        )

    def test_create_role_conflict(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = object()
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.create_role(self.body_ok)
        assert ex.value.status_code == 409

    def test_create_role_action_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.create_role(self.body_bad_action)
        assert ex.value.status_code == 400
        assert "action" in ex.value.detail

    def test_create_role_resource_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.create_role(self.body_bad_resource)
        assert ex.value.status_code == 400
        assert "resource" in ex.value.detail

    def test_create_role_unexpected_no_created(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        security_manager.find_role.side_effect = [None, None]
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.create_role(self.body_ok)
        assert ex.value.status_code == 500
