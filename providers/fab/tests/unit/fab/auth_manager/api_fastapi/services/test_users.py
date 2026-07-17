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

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.roles import Role
from airflow.providers.fab.auth_manager.api_fastapi.services.users import FABAuthManagerUsers


@pytest.fixture
def fab_auth_manager():
    return MagicMock()


@pytest.fixture
def security_manager():
    sm = MagicMock()

    def _find_role(name: str):
        if name in {"Admin", "User"}:
            return types.SimpleNamespace(name=name)
        return None

    sm.find_role.side_effect = _find_role
    sm.auth_user_registration_role = "User"
    return sm


def _make_user_obj(
    *,
    username: str,
    email: str,
    first_name: str,
    last_name: str,
    roles: list[str] | None = None,
    active: bool = True,
):
    role_objs = [types.SimpleNamespace(name=r) for r in (roles or [])]
    return types.SimpleNamespace(
        username=username,
        email=email,
        first_name=first_name,
        last_name=last_name,
        roles=role_objs or None,
        active=active,
        login_count=0,
        fail_login_count=0,
        last_login=None,
        created_on=None,
        changed_on=None,
    )


@patch("airflow.providers.fab.auth_manager.api_fastapi.services.users.get_fab_auth_manager")
class TestUsersService:
    def setup_method(self):
        self.password_mock = MagicMock()
        self.password_mock.get_secret_value.return_value = "pw"

        self.body_base = types.SimpleNamespace(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
            password=self.password_mock,
            roles=None,
        )

        self.body_with_roles_admin_dupe = types.SimpleNamespace(
            username="bob",
            email="bob@example.com",
            first_name="Bob",
            last_name="Builder",
            password=MagicMock(get_secret_value=MagicMock(return_value="pw2")),
            roles=[types.SimpleNamespace(name="Admin"), types.SimpleNamespace(name="Admin")],
        )

        self.body_with_missing_role = types.SimpleNamespace(
            username="eve",
            email="eve@example.com",
            first_name="Eve",
            last_name="Adams",
            password=MagicMock(get_secret_value=MagicMock(return_value="pw3")),
            roles=[types.SimpleNamespace(name="NOPE")],
        )

    def test_create_user_success_with_default_role(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        security_manager.find_user.side_effect = [None, None]
        security_manager.add_user.return_value = _make_user_obj(
            username=self.body_base.username,
            email=self.body_base.email,
            first_name=self.body_base.first_name,
            last_name=self.body_base.last_name,
            roles=["User"],
        )
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerUsers.create_user(self.body_base)

        assert out.username == "alice"
        assert out.email == "alice@example.com"
        called_roles = security_manager.add_user.call_args.kwargs["role"]
        assert len(called_roles) == 1
        assert called_roles[0].name == "User"
        self.password_mock.get_secret_value.assert_called_once()

    def test_create_user_success_with_explicit_roles_and_dedup(
        self, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        security_manager.find_user.side_effect = [None, None]
        security_manager.add_user.return_value = _make_user_obj(
            username=self.body_with_roles_admin_dupe.username,
            email=self.body_with_roles_admin_dupe.email,
            first_name=self.body_with_roles_admin_dupe.first_name,
            last_name=self.body_with_roles_admin_dupe.last_name,
            roles=["Admin"],
        )
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerUsers.create_user(self.body_with_roles_admin_dupe)

        assert out.username == "bob"
        roles_arg = security_manager.add_user.call_args.kwargs["role"]
        assert len(roles_arg) == 1
        assert roles_arg[0].name == "Admin"

    def test_create_user_conflict_username(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.side_effect = [object()]
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.create_user(self.body_base)
        assert ex.value.status_code == 409
        assert "Username" in ex.value.detail

    def test_create_user_conflict_email(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.side_effect = [None, object()]
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.create_user(self.body_base)
        assert ex.value.status_code == 409
        assert "email" in ex.value.detail

    def test_create_user_unknown_roles(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.side_effect = [None, None]
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.create_user(self.body_with_missing_role)
        assert ex.value.status_code == 400
        assert "Unknown roles" in ex.value.detail

    def test_create_user_default_role_missing(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.side_effect = [None, None]
        security_manager.auth_user_registration_role = "MissingDefault"
        security_manager.find_role.side_effect = lambda n: None if n == "MissingDefault" else None

        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.create_user(self.body_base)
        assert ex.value.status_code == 500
        assert "Default registration role" in ex.value.detail

    def test_create_user_add_user_failed(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.side_effect = [None, None]
        security_manager.add_user.return_value = None

        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.create_user(self.body_base)
        assert ex.value.status_code == 500
        assert "Failed to add user" in ex.value.detail

    def test_resolve_roles_returns_found_and_missing(self, get_fab_auth_manager, security_manager):
        found, missing = FABAuthManagerUsers._resolve_roles(
            security_manager,
            [Role(name="Admin"), Role(name="NOPE"), Role(name="Admin")],
        )
        assert [r.name for r in found] == ["Admin"]
        assert missing == ["NOPE"]

    def test_get_user_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        user_obj = _make_user_obj(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
            roles=["User"],
        )
        security_manager.find_user.return_value = user_obj
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        out = FABAuthManagerUsers.get_user("alice")

        assert out.username == "alice"
        assert out.email == "alice@example.com"
        security_manager.find_user.assert_called_once_with(username="alice")

    def test_get_user_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.get_user("nonexistent")
        assert ex.value.status_code == 404
        assert "nonexistent" in ex.value.detail

    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.users.build_ordering")
    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.users.select")
    def test_get_users_success(
        self, mock_select, mock_build_ordering, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        user1 = _make_user_obj(
            username="alice", email="alice@example.com", first_name="Alice", last_name="Liddell"
        )
        user2 = _make_user_obj(username="bob", email="bob@example.com", first_name="Bob", last_name="Builder")

        mock_session = MagicMock()
        mock_session.scalars.return_value.one.return_value = 2
        mock_session.scalars.return_value.unique.return_value.all.return_value = [user1, user2]
        security_manager.session = mock_session
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        mock_build_ordering.return_value = "ordering"

        out = FABAuthManagerUsers.get_users(order_by="username", limit=10, offset=0)

        assert out.total_entries == 2
        assert len(out.users) == 2
        assert out.users[0].username == "alice"
        assert out.users[1].username == "bob"

    @patch("airflow.providers.fab.auth_manager.api_fastapi.services.users.build_ordering")
    def test_get_users_invalid_order_by(
        self, mock_build_ordering, get_fab_auth_manager, fab_auth_manager, security_manager
    ):
        mock_build_ordering.side_effect = HTTPException(
            status_code=400,
            detail="Ordering with 'invalid' is disallowed or the attribute does not exist on the model",
        )
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.get_users(order_by="invalid", limit=10, offset=0)
        assert ex.value.status_code == 400

    def test_update_user_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        user_obj = _make_user_obj(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
            roles=["User"],
        )
        security_manager.find_user.return_value = user_obj
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        patch_body = types.SimpleNamespace(
            username=None,
            email=None,
            first_name=None,
            last_name="Updated",
            roles=None,
            password=None,
        )

        out = FABAuthManagerUsers.update_user("alice", patch_body)

        assert out.last_name == "Updated"
        security_manager.update_user.assert_called_once()

    def test_update_user_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        patch_body = types.SimpleNamespace(
            username=None,
            email=None,
            first_name=None,
            last_name="Updated",
            roles=None,
            password=None,
        )

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.update_user("nonexistent", patch_body)
        assert ex.value.status_code == 404

    def test_update_user_conflict_username(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        user_obj = _make_user_obj(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
        )

        security_manager.find_user.side_effect = [user_obj, object()]
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        patch_body = types.SimpleNamespace(
            username="bob",
            email=None,
            first_name=None,
            last_name=None,
            roles=None,
            password=None,
        )

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.update_user("alice", patch_body)
        assert ex.value.status_code == 409
        assert "username" in ex.value.detail

    def test_update_user_conflict_email(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        user_obj = _make_user_obj(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
        )
        security_manager.find_user.side_effect = [user_obj, object()]
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        patch_body = types.SimpleNamespace(
            username=None,
            email="taken@example.com",
            first_name=None,
            last_name=None,
            roles=None,
            password=None,
        )

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.update_user("alice", patch_body)
        assert ex.value.status_code == 409
        assert "email" in ex.value.detail

    def test_update_user_unknown_update_mask(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        user_obj = _make_user_obj(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
        )
        security_manager.find_user.return_value = user_obj
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        patch_body = types.SimpleNamespace(
            username=None,
            email=None,
            first_name=None,
            last_name="Updated",
            roles=None,
            password=None,
        )

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.update_user("alice", patch_body, update_mask="invalid_field")
        assert ex.value.status_code == 400
        assert "Unknown update masks" in ex.value.detail

    def test_update_user_with_password(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        user_obj = _make_user_obj(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
        )
        security_manager.find_user.return_value = user_obj
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        password_mock = MagicMock()
        password_mock.get_secret_value.return_value = "newpassword"

        patch_body = types.SimpleNamespace(
            username=None,
            email=None,
            first_name=None,
            last_name=None,
            roles=None,
            password=password_mock,
        )

        with patch(
            "airflow.providers.fab.auth_manager.api_fastapi.services.users.generate_password_hash"
        ) as mock_hash:
            mock_hash.return_value = "hashed_password"
            FABAuthManagerUsers.update_user("alice", patch_body, update_mask="password")

        password_mock.get_secret_value.assert_called_once()
        mock_hash.assert_called_once_with("newpassword")

    # delete_user tests

    def test_delete_user_success(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        user_obj = _make_user_obj(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Liddell",
            roles=["User"],
        )
        security_manager.find_user.return_value = user_obj
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        FABAuthManagerUsers.delete_user("alice")

        assert user_obj.roles == []
        security_manager.session.delete.assert_called_once_with(user_obj)
        security_manager.session.commit.assert_called_once()

    def test_delete_user_not_found(self, get_fab_auth_manager, fab_auth_manager, security_manager):
        security_manager.find_user.return_value = None
        fab_auth_manager.security_manager = security_manager
        get_fab_auth_manager.return_value = fab_auth_manager

        with pytest.raises(HTTPException) as ex:
            FABAuthManagerUsers.delete_user("nonexistent")
        assert ex.value.status_code == 404
        assert "nonexistent" in ex.value.detail
