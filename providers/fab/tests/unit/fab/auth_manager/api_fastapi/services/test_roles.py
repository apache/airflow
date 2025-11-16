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
from sqlalchemy import column

from airflow.providers.fab.auth_manager.api_fastapi.services.roles import FABAuthManagerRoles


@pytest.fixture
def fab_auth_manager():
    return MagicMock()


@pytest.fixture
def security_manager():
    sm = MagicMock()
    sm.get_action.side_effect = lambda n: object() if n in {"can_read", "can_edit"} else None
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
    return types.SimpleNamespace(id=1, name=name, permissions=perm_objs)


class _FakeScalarCount:
    def __init__(self, value: int):
        self._value = value

    def one(self) -> int:
        return self._value


class _FakeScalarRoles:
    def __init__(self, items):
        self._items = items
        self._unique_called = False

    def unique(self):
        self._unique_called = True
        return self

    def all(self):
        return self._items


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

    # POST /roles

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

    # GET /roles

    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.roles.build_ordering")
    def test_get_roles_happy_path(self, build_ordering, get_fab_auth_manager):
        role1 = _make_role_obj("viewer", [("can_read", "DAG")])
        role2 = _make_role_obj("admin", [("can_read", "DAG")])
        fake_roles_result = _FakeScalarRoles([role1, role2])

        session = MagicMock()
        session.scalars.side_effect = [
            _FakeScalarCount(2),
            fake_roles_result,
        ]

        fab_auth_manager = MagicMock()
        fab_auth_manager.security_manager = MagicMock(session=session)
        get_fab_auth_manager.return_value = fab_auth_manager

        build_ordering.return_value = column("name").desc()

        out = FABAuthManagerRoles.get_roles(order_by="-name", limit=5, offset=3)

        assert out.total_entries == 2
        assert [r.name for r in out.roles] == ["viewer", "admin"]
        assert fake_roles_result._unique_called is True

        build_ordering.assert_called_once()
        args, kwargs = build_ordering.call_args
        assert args[0] == "-name"
        assert set(kwargs["allowed"].keys()) == {"name", "role_id"}

        assert session.scalars.call_count == 2

    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.roles.build_ordering")
    def test_get_roles_invalid_order_by_bubbles_400(self, build_ordering, get_fab_auth_manager):
        session = MagicMock()
        fab_auth_manager = MagicMock()
        fab_auth_manager.security_manager = MagicMock(session=session)
        get_fab_auth_manager.return_value = fab_auth_manager

        build_ordering.side_effect = HTTPException(status_code=400, detail="disallowed")

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.get_roles(order_by="nope", limit=10, offset=0)
        assert ex.value.status_code == 400

    # DELETE /roles/{name}

    def test_delete_role_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = _make_role_obj("roleA", [])
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        FABAuthManagerRoles.delete_role(name="roleA")

        security_manager.delete_role.assert_called_once()

    def test_delete_role_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.delete_role(name="roleA")
        assert ex.value.status_code == 404

    # GET /roles/{name}

    def test_get_role_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = _make_role_obj("roleA", [("can_read", "DAG")])
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerRoles.get_role(name="roleA")

        assert out.name == "roleA"
        assert out.permissions
        assert out.permissions[0].action.name == "can_read"
        assert out.permissions[0].resource.name == "DAG"

    def test_get_role_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.get_role(name="roleA")
        assert ex.value.status_code == 404

    # PATCH /roles/{name}

    def test_patch_role_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        role = _make_role_obj("viewer", [("can_read", "DAG")])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = types.SimpleNamespace(
            name="viewer",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_edit"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )
        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")
        assert out.name == "viewer"
        assert out.permissions
        assert out.permissions[0].action.name == "can_edit"
        assert out.permissions[0].resource.name == "DAG"

    def test_patch_role_rename_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        role = _make_role_obj("viewer", [("can_edit", "DAG")])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = types.SimpleNamespace(
            name="editor",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_edit"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )
        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")
        assert out.name == "editor"
        assert out.permissions
        assert out.permissions[0].action.name == "can_edit"
        assert out.permissions[0].resource.name == "DAG"

    def test_patch_role_with_update_mask(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        role = _make_role_obj("viewer", [("can_read", "DAG")])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = types.SimpleNamespace(
            name="viewer1",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_edit"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )
        out = FABAuthManagerRoles.patch_role(
            body=body,
            name="viewer",
            update_mask=["actions"],
        )
        assert out.name == "viewer"
        assert out.permissions
        assert out.permissions[0].action.name == "can_edit"
        assert out.permissions[0].resource.name == "DAG"

    def test_patch_role_rename_with_update_mask(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        role = _make_role_obj("viewer", [("can_read", "DAG")])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = types.SimpleNamespace(
            name="viewer1",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_edit"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )
        out = FABAuthManagerRoles.patch_role(
            body=body,
            name="viewer",
            update_mask=["name"],
        )
        assert out.name == "viewer1"
        assert out.permissions
        assert out.permissions[0].action.name == "can_read"
        assert out.permissions[0].resource.name == "DAG"

    def test_patch_role_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = types.SimpleNamespace(
            name="viewer",
            permissions=[
                types.SimpleNamespace(
                    action=types.SimpleNamespace(name="can_edit"),
                    resource=types.SimpleNamespace(name="DAG"),
                )
            ],
        )
        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.patch_role(body=body, name="viewer")
        assert ex.value.status_code == 404
