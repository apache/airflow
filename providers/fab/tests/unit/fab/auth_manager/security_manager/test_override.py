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
from unittest.mock import Mock

import pytest

from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride


class EmptySecurityManager(FabAirflowSecurityManagerOverride):
    # noinspection PyMissingConstructor
    # super() not called on purpose to avoid the whole chain of init calls
    def __init__(self):
        pass


class TestFabAirflowSecurityManagerOverride:
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
