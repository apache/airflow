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

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import (
    Action,
    ActionResource,
    PermissionCollectionResponse,
    Resource,
    RoleBody,
)
from airflow.providers.fab.auth_manager.api_fastapi.services.roles import (
    FABAuthManagerRoles,
)


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


def _make_role_body(name: str, actions: list[tuple[str, str]] | None = None, *, include_actions: bool = True):
    """Build a real `RoleBody` so `model_fields_set` reflects which fields the caller supplied."""
    kwargs: dict = {"name": name}
    if include_actions:
        kwargs["actions"] = [
            ActionResource(action=Action(name=a), resource=Resource(name=r)) for (a, r) in (actions or [])
        ]
    return RoleBody(**kwargs)


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

        security_manager.delete_role.assert_called_once_with("roleA")

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

    def test_patch_role_rename_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        role = _make_role_obj("viewer", [("can_edit", "DAG")])
        security_manager.find_role.return_value = role
        security_manager.get_permission.return_value = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_edit"), resource=types.SimpleNamespace(name="DAG")
        )
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("editor", [("can_edit", "DAG")])

        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")

        assert out.name == "editor"
        assert out.permissions
        assert out.permissions[0].action.name == "can_edit"
        assert out.permissions[0].resource.name == "DAG"
        # The permission set is unchanged, so nothing should be added or removed.
        security_manager.add_permission_to_role.assert_not_called()
        security_manager.remove_permission_from_role.assert_not_called()
        security_manager.update_role.assert_called_once_with(role_id=role.id, name="editor")

    def test_patch_role_adds_missing_permission(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        """Regression test: permissions present in the body but not yet on the role are added."""
        role = _make_role_obj("viewer", [])
        security_manager.find_role.return_value = role
        new_permission = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_edit"), resource=types.SimpleNamespace(name="DAG")
        )
        security_manager.get_permission.return_value = new_permission
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("viewer", [("can_edit", "DAG")])

        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")

        assert out.permissions
        assert out.permissions[0].action.name == "can_edit"
        assert out.permissions[0].resource.name == "DAG"
        security_manager.get_permission.assert_called_once_with("can_edit", "DAG")
        security_manager.create_permission.assert_not_called()
        security_manager.add_permission_to_role.assert_called_once_with(role, new_permission)
        security_manager.remove_permission_from_role.assert_not_called()

    def test_patch_role_creates_permission_when_missing_from_db(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        """When the (action, resource) pair has no Permission row yet, one is created before adding."""
        role = _make_role_obj("viewer", [])
        security_manager.find_role.return_value = role
        security_manager.get_permission.return_value = None
        created_permission = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_edit"), resource=types.SimpleNamespace(name="DAG")
        )
        security_manager.create_permission.return_value = created_permission
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("viewer", [("can_edit", "DAG")])

        FABAuthManagerRoles.patch_role(body=body, name="viewer")

        security_manager.create_permission.assert_called_once_with("can_edit", "DAG")
        security_manager.add_permission_to_role.assert_called_once_with(role, created_permission)

    def test_patch_role_removes_permission_absent_from_body(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        """The core issue #18714 regression test: a permission missing from the PATCH body
        must be revoked from the role, not silently kept."""
        role = _make_role_obj("viewer", [("can_read", "DAG"), ("can_edit", "DAG")])
        security_manager.find_role.return_value = role
        security_manager.get_permission.return_value = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_edit"), resource=types.SimpleNamespace(name="DAG")
        )
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("viewer", [("can_edit", "DAG")])

        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")

        assert {(p.action.name, p.resource.name) for p in out.permissions} == {("can_edit", "DAG")}
        security_manager.add_permission_to_role.assert_not_called()
        removed_role, removed_permission = security_manager.remove_permission_from_role.call_args.args
        assert removed_role is role
        assert (removed_permission.action.name, removed_permission.resource.name) == ("can_read", "DAG")

    def test_patch_role_adds_and_removes_permissions_together(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        role = _make_role_obj("viewer", [("can_read", "DAG"), ("can_edit", "DAG")])
        security_manager.find_role.return_value = role
        security_manager.get_permission.return_value = None
        created_permission = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_read"), resource=types.SimpleNamespace(name="Connections")
        )
        security_manager.create_permission.return_value = created_permission
        security_manager.get_resource.side_effect = lambda n: (
            object() if n in {"DAG", "Connections"} else None
        )
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        # can_edit/DAG is kept as-is, can_read/DAG is dropped, can_read/Connections is added.
        body = _make_role_body("viewer", [("can_edit", "DAG"), ("can_read", "Connections")])

        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")

        assert {(p.action.name, p.resource.name) for p in out.permissions} == {
            ("can_edit", "DAG"),
            ("can_read", "Connections"),
        }
        security_manager.add_permission_to_role.assert_called_once_with(role, created_permission)
        removed_role, removed_permission = security_manager.remove_permission_from_role.call_args.args
        assert removed_role is role
        assert (removed_permission.action.name, removed_permission.resource.name) == ("can_read", "DAG")

    def test_patch_role_explicit_empty_actions_removes_all_permissions(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        """Sending `"actions": []` is an explicit instruction to clear the permission set."""
        role = _make_role_obj("viewer", [("can_read", "DAG"), ("can_edit", "DAG")])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("viewer", [])

        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")

        assert out.permissions == []
        assert security_manager.remove_permission_from_role.call_count == 2
        security_manager.add_permission_to_role.assert_not_called()

    def test_patch_role_without_actions_key_leaves_permissions_untouched(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        """A rename-only PATCH that never mentions "actions" must not touch permissions at
        all -- omitting the field is not the same as sending an empty list."""
        role = _make_role_obj("viewer", [("can_read", "DAG")])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("editor", include_actions=False)

        out = FABAuthManagerRoles.patch_role(body=body, name="viewer")

        assert out.name == "editor"
        assert [(p.action.name, p.resource.name) for p in out.permissions] == [("can_read", "DAG")]
        security_manager.add_permission_to_role.assert_not_called()
        security_manager.remove_permission_from_role.assert_not_called()
        security_manager.get_permission.assert_not_called()

    def test_patch_role_with_update_mask(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        role = _make_role_obj("viewer", [("can_read", "DAG")])
        security_manager.find_role.return_value = role
        security_manager.get_permission.return_value = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_edit"), resource=types.SimpleNamespace(name="DAG")
        )
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        # "name" differs from the role's current name but is excluded by the mask.
        body = _make_role_body("viewer1", [("can_edit", "DAG")])

        out = FABAuthManagerRoles.patch_role(
            body=body,
            name="viewer",
            update_mask="actions",
        )
        assert out.name == "viewer"
        assert out.permissions
        assert out.permissions[0].action.name == "can_edit"
        assert out.permissions[0].resource.name == "DAG"
        security_manager.update_role.assert_not_called()

    def test_patch_role_rename_with_update_mask_leaves_permissions_untouched(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        """update_mask=name must not apply the body's `actions`, even though the body
        includes a different permission set (the pre-fix code ignored the mask here)."""
        role = _make_role_obj("viewer", [("can_read", "DAG")])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("viewer1", [("can_edit", "DAG")])

        out = FABAuthManagerRoles.patch_role(
            body=body,
            name="viewer",
            update_mask="name",
        )
        assert out.name == "viewer1"
        assert out.permissions
        assert out.permissions[0].action.name == "can_read"
        assert out.permissions[0].resource.name == "DAG"
        security_manager.add_permission_to_role.assert_not_called()
        security_manager.remove_permission_from_role.assert_not_called()
        security_manager.get_permission.assert_not_called()

    def test_patch_role_unknown_update_mask_field(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        role = _make_role_obj("viewer", [])
        security_manager.find_role.return_value = role
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("viewer", [])

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.patch_role(body=body, name="viewer", update_mask="unknown_field")
        assert ex.value.status_code == 400
        assert ex.value.detail == "'unknown_field' in update_mask is unknown"

    def test_patch_role_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_role.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager
        body = _make_role_body("viewer", [("can_edit", "DAG")])
        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.patch_role(body=body, name="viewer")
        assert ex.value.status_code == 404

    def test_get_permissions_success(self, get_fab_auth_manager):
        session = MagicMock()
        perm_obj = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_read"),
            resource=types.SimpleNamespace(name="DAG"),
        )
        session.scalars.side_effect = [
            types.SimpleNamespace(one=lambda: 1),
            types.SimpleNamespace(all=lambda: [perm_obj]),
        ]
        fab_auth_manager = MagicMock()
        fab_auth_manager.security_manager = MagicMock(session=session)
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerRoles.get_permissions(order_by="id", limit=10, offset=0)
        assert isinstance(out, PermissionCollectionResponse)
        assert out.total_entries == 1
        assert len(out.permissions) == 1
        assert out.permissions[0] == ActionResource(
            action=Action(name="can_read"), resource=Resource(name="DAG")
        )

    def test_get_permissions_empty(self, get_fab_auth_manager):
        session = MagicMock()
        session.scalars.side_effect = [
            types.SimpleNamespace(one=lambda: 0),
            types.SimpleNamespace(all=lambda: []),
        ]
        fab_auth_manager = MagicMock()
        fab_auth_manager.security_manager = MagicMock(session=session)
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerRoles.get_permissions(order_by="id", limit=10, offset=0)
        assert out.total_entries == 0
        assert out.permissions == []

    def test_get_permissions_with_multiple(self, get_fab_auth_manager):
        session = MagicMock()
        perm_objs = [
            types.SimpleNamespace(
                action=types.SimpleNamespace(name="can_read"),
                resource=types.SimpleNamespace(name="DAG"),
            ),
            types.SimpleNamespace(
                action=types.SimpleNamespace(name="can_edit"),
                resource=types.SimpleNamespace(name="DAG"),
            ),
        ]
        session.scalars.side_effect = [
            types.SimpleNamespace(one=lambda: 2),
            types.SimpleNamespace(all=lambda: perm_objs),
        ]
        fab_auth_manager = MagicMock()
        fab_auth_manager.security_manager = MagicMock(session=session)
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerRoles.get_permissions(order_by="id", limit=10, offset=0)
        assert isinstance(out, PermissionCollectionResponse)
        assert out.total_entries == 2
        assert len(out.permissions) == 2
        assert out.permissions[0] == ActionResource(
            action=Action(name="can_read"), resource=Resource(name="DAG")
        )
        assert out.permissions[1] == ActionResource(
            action=Action(name="can_edit"), resource=Resource(name="DAG")
        )

    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.roles.build_ordering")
    def test_get_permissions_ordering_happy_path(self, build_ordering, get_fab_auth_manager):
        perm_obj = types.SimpleNamespace(
            action=types.SimpleNamespace(name="can_read"),
            resource=types.SimpleNamespace(name="DAG"),
        )
        session = MagicMock()
        session.scalars.side_effect = [
            types.SimpleNamespace(one=lambda: 1),
            types.SimpleNamespace(all=lambda: [perm_obj]),
        ]
        fab_auth_manager = MagicMock()
        fab_auth_manager.security_manager = MagicMock(session=session)
        get_fab_auth_manager.return_value = fab_auth_manager

        build_ordering.return_value = column("id").desc()

        out = FABAuthManagerRoles.get_permissions(order_by="-id", limit=10, offset=0)

        assert out.total_entries == 1
        assert len(out.permissions) == 1

        build_ordering.assert_called_once()
        args, kwargs = build_ordering.call_args
        assert args[0] == "-id"
        assert set(kwargs["allowed"].keys()) == {"id", "action_id", "resource_id"}

    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.roles.build_ordering")
    def test_get_permissions_invalid_order_by_bubbles_400(self, build_ordering, get_fab_auth_manager):
        session = MagicMock()
        fab_auth_manager = MagicMock()
        fab_auth_manager.security_manager = MagicMock(session=session)
        get_fab_auth_manager.return_value = fab_auth_manager

        build_ordering.side_effect = HTTPException(status_code=400, detail="disallowed")

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerRoles.get_permissions(order_by="nope", limit=10, offset=0)
        assert ex.value.status_code == 400
