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

from unittest import mock
from unittest.mock import Mock, call

import pytest
from flask_appbuilder import const
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from airflow.providers.fab.auth_manager.models import (
    Action,
    Permission,
    Resource,
    Role,
)
from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride


class EmptySecurityManager(FabAirflowSecurityManagerOverride):
    # noinspection PyMissingConstructor
    # super() not called on purpose to avoid the whole chain of init calls
    def __init__(self):
        pass


class TestFabAirflowSecurityManagerOverride:
    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_add_permission_to_role_ignores_duplicate_from_concurrent_worker(self, mock_log):
        sm = EmptySecurityManager()
        role = Mock(spec=Role, id=1, name="test_admin", permissions=[])
        permission = Mock(spec=Permission, id=2)

        mock_session = Mock(spec=Session)
        mock_session.commit.side_effect = IntegrityError("stmt", {}, Exception("Duplicate entry"))

        sm._is_permission_assigned_to_role = Mock(return_value=True)

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            sm.add_permission_to_role(role, permission)

        assert mock_session.rollback.mock_calls == [call()]
        assert sm._is_permission_assigned_to_role.mock_calls == [call(role_id=1, permission_view_id=2)]
        assert mock_log.error.mock_calls == []

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_add_permission_to_role_logs_error_when_duplicate_not_persisted(self, mock_log):
        sm = EmptySecurityManager()
        role = Mock(spec=Role, id=1, name="Admin", permissions=[])
        permission = Mock(spec=Permission, id=2)

        mock_session = Mock(spec=Session)
        mock_error = IntegrityError("stmt", {}, Exception("duplicate key"))
        mock_session.commit.side_effect = mock_error

        sm._is_permission_assigned_to_role = Mock(return_value=False)

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            sm.add_permission_to_role(role, permission)

        mock_session.rollback.assert_called_once_with()
        sm._is_permission_assigned_to_role.assert_called_once_with(role_id=1, permission_view_id=2)
        mock_log.error.assert_called_once_with(
            const.LOGMSG_ERR_SEC_ADD_PERMROLE,
            f"Failed to add '{permission}' permission to the '{role}' role Error: {mock_error}",
        )

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_add_role_returns_existing_on_concurrent_insert(self, mock_log):
        sm = EmptySecurityManager()
        existing_role = Mock(spec=Role, name="Admin")

        mock_session = Mock(spec=Session)
        mock_session.commit.side_effect = IntegrityError("stmt", {}, Exception("Duplicate entry"))
        sm.find_role = Mock(side_effect=[None, existing_role])

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            result = sm.add_role("Admin")

        assert result is existing_role
        assert mock_session.rollback.called
        assert mock_log.error.call_count == 0

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_create_action_returns_existing_on_concurrent_insert(self, mock_log):
        sm = EmptySecurityManager()
        existing_action = Mock(spec=Action, name="can_read")

        mock_session = Mock(spec=Session)
        mock_session.commit.side_effect = IntegrityError("stmt", {}, Exception("Duplicate entry"))
        sm.get_action = Mock(side_effect=[None, existing_action])

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            result = sm.create_action("can_read")

        assert result is existing_action
        assert mock_session.rollback.called
        assert mock_log.error.call_count == 0

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_create_resource_returns_existing_on_concurrent_insert(self, mock_log):
        sm = EmptySecurityManager()
        existing_resource = Mock(spec=Resource, name="Connections")

        mock_session = Mock(spec=Session)
        mock_session.commit.side_effect = IntegrityError("stmt", {}, Exception("Duplicate entry"))
        sm.get_resource = Mock(side_effect=[None, existing_resource])

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            result = sm.create_resource("Connections")

        assert result is existing_resource
        assert mock_session.rollback.called
        assert mock_log.error.call_count == 0

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_create_permission_returns_existing_on_concurrent_insert(self, mock_log):
        sm = EmptySecurityManager()
        existing_perm = Mock(spec=Permission)
        existing_resource = Mock(spec=Resource, id=10)
        existing_action = Mock(spec=Action, id=20)

        mock_session = Mock(spec=Session)
        mock_session.commit.side_effect = IntegrityError("stmt", {}, Exception("Duplicate entry"))

        sm.get_permission = Mock(side_effect=[None, existing_perm])
        sm.create_resource = Mock(return_value=existing_resource)
        sm.create_action = Mock(return_value=existing_action)

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            result = sm.create_permission("can_read", "Connections")

        assert result is existing_perm
        assert mock_session.rollback.called
        assert mock_log.error.call_count == 0

    def test_load_user(self):
        sm = EmptySecurityManager()
        sm.get_user_by_id = Mock()

        sm.load_user("123")

        sm.get_user_by_id.assert_called_once_with(123)

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.g", spec={})
    def test_load_user_jwt(self, mock_g):
        sm = EmptySecurityManager()
        mock_user = Mock()
        sm.load_user = Mock(return_value=mock_user)

        actual_user = sm.load_user_jwt(None, {"sub": "test_identity"})

        sm.load_user.assert_called_once_with("test_identity")
        assert actual_user is mock_user
        assert mock_g.user is mock_user

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.check_password_hash")
    def test_check_password(self, check_password):
        sm = EmptySecurityManager()
        mock_user = Mock()
        sm.find_user = Mock(return_value=mock_user)
        check_password.return_value = True
        assert sm.check_password("test_user", "test_password")

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.check_password_hash")
    def test_check_password_user_not_found(self, check_password):
        sm = EmptySecurityManager()
        sm.find_user = Mock(return_value=None)
        check_password.return_value = False
        assert not sm.check_password("test_user", "test_password")

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.check_password_hash")
    def test_check_password_not_match(self, check_password):
        sm = EmptySecurityManager()
        mock_user = Mock()
        sm.find_user = Mock(return_value=mock_user)
        check_password.return_value = False
        assert not sm.check_password("test_user", "test_password")

    @pytest.mark.parametrize(
        ("provider", "resp", "user_info"),
        [
            ("github", {"login": "test"}, {"username": "github_test"}),
            ("githublocal", {"login": "test"}, {"username": "github_test"}),
            ("twitter", {"screen_name": "test"}, {"username": "twitter_test"}),
            (
                "linkedin",
                {"id": "test", "firstName": "John", "lastName": "Doe", "email-address": "test@example.com"},
                {
                    "username": "linkedin_test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                },
            ),
            (
                "google",
                {"id": "test", "given_name": "John", "family_name": "Doe", "email": "test@example.com"},
                {
                    "username": "google_test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                },
            ),
            (
                "azure",
                {
                    "oid": "test",
                    "given_name": "John",
                    "family_name": "Doe",
                    "email": "test@example.com",
                    "roles": ["admin"],
                },
                {
                    "username": "test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                    "role_keys": ["admin"],
                },
            ),
            (
                "azure",
                {
                    "oid": "test",
                    "given_name": "John",
                    "family_name": "Doe",
                    "upn": "test@example.com",
                    "roles": ["admin"],
                },
                {
                    "username": "test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                    "role_keys": ["admin"],
                },
            ),
            (
                "azure",
                {
                    "oid": "test",
                    "given_name": "John",
                    "family_name": "Doe",
                    "email": "test@example.com",
                    "groups": ["group1", "group2"],
                },
                {
                    "username": "test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                    "role_keys": [],
                },
            ),
            ("openshift", {"metadata": {"name": "test"}}, {"username": "openshift_test"}),
            (
                "okta",
                {
                    "sub": "test",
                    "given_name": "John",
                    "family_name": "Doe",
                    "email": "test@example.com",
                    "groups": ["admin"],
                },
                {
                    "username": "okta_test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                    "role_keys": ["admin"],
                },
            ),
            ("okta", {"error": "access_denied", "error_description": "Invalid bearer token."}, {}),
            (
                "auth0",
                {
                    "sub": "test",
                    "given_name": "John",
                    "family_name": "Doe",
                    "email": "test@example.com",
                    "groups": ["admin"],
                },
                {
                    "username": "auth0_test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                    "role_keys": ["admin"],
                },
            ),
            (
                "keycloak",
                {
                    "preferred_username": "test",
                    "given_name": "John",
                    "family_name": "Doe",
                    "email": "test@example.com",
                    "groups": ["admin"],
                },
                {
                    "username": "test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                    "role_keys": ["admin"],
                },
            ),
            (
                "keycloak_before_17",
                {
                    "preferred_username": "test",
                    "given_name": "John",
                    "family_name": "Doe",
                    "email": "test@example.com",
                    "groups": ["admin"],
                },
                {
                    "username": "test",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "test@example.com",
                    "role_keys": ["admin"],
                },
            ),
            (
                "authentik",
                {
                    "nickname": "test",
                    "given_name": "John",
                    "preferred_username": "test@example.com",
                    "groups": ["admin"],
                },
                {
                    "username": "test",
                    "first_name": "John",
                    "email": "test@example.com",
                    "role_keys": ["admin"],
                },
            ),
            (
                "other",
                {"preferred_username": "test", "email": "test@example.com"},
                {
                    "username": "test",
                    "first_name": "",
                    "last_name": "",
                    "email": "test@example.com",
                    "role_keys": [],
                },
            ),
        ],
    )
    def test_get_oauth_user_info(self, provider, resp, user_info):
        from flask import Flask

        app = Flask(__name__)
        with app.app_context():
            sm = EmptySecurityManager()
            sm.appbuilder = Mock(sm=sm)
            sm.oauth_remotes = {}
            sm.oauth_remotes[provider] = Mock(
                get=Mock(return_value=Mock(json=Mock(return_value=resp))),
                userinfo=Mock(return_value=resp),
            )
            sm._decode_and_validate_azure_jwt = Mock(return_value=resp)
            sm._get_authentik_token_info = Mock(return_value=resp)
            assert sm.get_oauth_user_info(provider, {"id_token": None}) == user_info

    def test_get_oauth_user_info_azure_with_groups_config(self):
        from flask import Flask

        app = Flask(__name__)
        app.config["AUTH_OAUTH_ROLE_KEYS"] = {"azure": "groups"}

        azure_response = {
            "oid": "user-123",
            "given_name": "Jane",
            "family_name": "Smith",
            "email": "jane.smith@example.com",
            "groups": ["admin-group", "viewer-group"],
        }

        with app.app_context():
            sm = EmptySecurityManager()
            sm.appbuilder = Mock(sm=sm)
            sm.oauth_remotes = {}
            sm._decode_and_validate_azure_jwt = Mock(return_value=azure_response)

            user_info = sm.get_oauth_user_info("azure", {"id_token": "test-token"})

            assert user_info["username"] == "user-123"
            assert user_info["email"] == "jane.smith@example.com"
            assert user_info["role_keys"] == ["admin-group", "viewer-group"]
