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

from types import SimpleNamespace
from unittest import mock
from unittest.mock import Mock, call

import pytest
import requests
from authlib.jose import JsonWebKey, jwt as authlib_jwt
from authlib.jose.errors import InvalidClaimError
from flask_appbuilder import const
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from airflow.providers.fab.auth_manager.models import (
    Action,
    Group,
    Permission,
    Resource,
    Role,
    User,
)
from airflow.providers.fab.auth_manager.security_manager.override import (
    AzureTenantResolutionError,
    FabAirflowSecurityManagerOverride,
    FabException,
)

TENANT_GUID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
OTHER_GUID = "00000000-0000-0000-0000-000000000000"
CLIENT_ID = "app-xyz"


def _create_azure_jwt(
    key,
    iss=f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0",
    aud=CLIENT_ID,
    tid=TENANT_GUID,
    oid="user-oid",
    kid="test-kid",
) -> str:
    token = authlib_jwt.encode(
        {"alg": "RS256", "kid": kid},
        {"iss": iss, "aud": aud, "tid": tid, "oid": oid},
        key,
    )
    return token.decode("utf-8") if isinstance(token, bytes) else token


def _create_mock_response(*, status_code=200, json_data=None, json_side_effect=None) -> Mock:
    response = Mock(spec=requests.Response)
    response.status_code = status_code
    response.json.return_value = json_data
    response.json.side_effect = json_side_effect
    return response


class EmptySecurityManager(FabAirflowSecurityManagerOverride):
    # noinspection PyMissingConstructor
    # super() not called on purpose to avoid the whole chain of init calls
    def __init__(self):
        self._azure_tenant_guid_cache = {}


class TestFabAirflowSecurityManagerOverride:
    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.log")
    def test_delete_role_cleans_up_associations_before_delete(self, mock_log):
        """delete_role must remove association rows before the role row itself."""
        sm = EmptySecurityManager()
        role = Mock(spec=Role, id=42, name="TestRole")

        mock_session = Mock(spec=Session)
        mock_scalars = Mock()
        mock_scalars.first.return_value = role
        mock_session.scalars.return_value = mock_scalars

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            sm.delete_role("TestRole")

        # 4 deletes: permission-role, user-role, group-role, then the role itself
        assert mock_session.execute.call_count == 4
        mock_session.commit.assert_called_once()
        mock_log.info.assert_called_once_with("Deleting role '%s'", "TestRole")

    def test_delete_role_raises_for_missing_role(self):
        """delete_role must raise FabException when the role does not exist."""
        sm = EmptySecurityManager()

        mock_session = Mock(spec=Session)
        mock_scalars = Mock()
        mock_scalars.first.return_value = None
        mock_session.scalars.return_value = mock_scalars

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            with pytest.raises(FabException, match="Role named 'NoSuchRole' does not exist"):
                sm.delete_role("NoSuchRole")

        mock_session.execute.assert_not_called()
        mock_session.commit.assert_not_called()

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

    def test_update_user_clears_cached_permissions(self):
        sm = EmptySecurityManager()
        user = Mock(
            spec=User,
            id=1,
            roles=[Mock(spec=Role, id=2)],
            groups=[Mock(spec=Group, id=3)],
            _perms={("can_read", "DAG")},
        )
        existing_user = Mock(spec=User, roles=[Mock(spec=Role, id=4)], groups=[Mock(spec=Group, id=5)])
        mock_merged_user = Mock(spec=User, _perms={("can_edit", "DAG")})
        mock_session = Mock(spec=Session)
        mock_session.get.return_value = existing_user
        mock_session.merge.return_value = mock_merged_user

        with mock.patch.object(EmptySecurityManager, "session", mock_session):
            assert sm.update_user(user)

        assert user._perms == {("can_read", "DAG")}
        assert mock_merged_user._perms is None
        mock_session.commit.assert_called_once_with()

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

    def test_decode_and_validate_azure_jwt_verifies_signature_by_default(self):
        """Azure AD id_token signatures are verified by default (verify_signature defaults to True)."""
        sm = EmptySecurityManager()
        # client_kwargs does not set verify_signature -> it must default to verifying.
        # A resolvable tenant is required before the key set is fetched, so the mock
        # carries the tenant-specific endpoint the documented configuration uses.
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs={},
                client_id="app-xyz",
                api_base_url=f"https://login.microsoftonline.com/{TENANT_GUID}/oauth2/v2.0/",
            )
        }

        with mock.patch.object(
            EmptySecurityManager,
            "_get_microsoft_jwks",
            autospec=True,
            side_effect=RuntimeError("verify-branch-reached"),
        ) as mock_jwks:
            with pytest.raises(RuntimeError, match="verify-branch-reached"):
                sm._decode_and_validate_azure_jwt("header.payload.signature")

        # entering the verifying branch means the Microsoft JWKS were fetched
        mock_jwks.assert_called_once()

    def test_decode_and_validate_azure_jwt_skips_verification_when_opted_out(self):
        """With verify_signature explicitly False, the token is decoded without signature verification."""
        import base64
        import json as _json

        payload = base64.urlsafe_b64encode(_json.dumps({"oid": "user-1"}).encode()).decode().rstrip("=")
        id_token = f"header.{payload}.signature"

        sm = EmptySecurityManager()
        sm.oauth_remotes = {"azure": Mock(client_kwargs={"verify_signature": False})}

        with mock.patch.object(EmptySecurityManager, "_get_microsoft_jwks") as mock_jwks:
            result = sm._decode_and_validate_azure_jwt(id_token)

        mock_jwks.assert_not_called()
        assert result == {"oid": "user-1"}

    @pytest.mark.parametrize(
        ("remote_kwargs", "expected"),
        [
            pytest.param(
                {"client_kwargs": {"tenant_id": "explicit-tenant"}}, "explicit-tenant", id="explicit"
            ),
            pytest.param(
                {
                    "client_kwargs": {},
                    "api_base_url": "https://login.microsoftonline.com/tenant-from-url/oauth2/v2.0/",
                },
                "tenant-from-url",
                id="from-api-base-url",
            ),
            pytest.param(
                {
                    "client_kwargs": {},
                    "api_base_url": None,
                    "access_token_url": "https://login.microsoftonline.com/tenant-from-token-url/oauth2/v2.0/token",
                },
                "tenant-from-token-url",
                id="from-access-token-url",
            ),
            pytest.param(
                {
                    "client_kwargs": {},
                    "api_base_url": None,
                    "access_token_url": None,
                    "authorize_url": "https://login.microsoftonline.com/tenant-from-auth-url/oauth2/v2.0/authorize",
                },
                "tenant-from-auth-url",
                id="from-authorize-url",
            ),
        ],
    )
    def test_get_azure_tenant_identifier_resolves_configured_tenant(self, remote_kwargs, expected):
        """The tenant identifier is taken from client_kwargs when set, otherwise from configured endpoints."""
        sm = EmptySecurityManager()
        sm.oauth_remotes = {"azure": SimpleNamespace(**remote_kwargs)}

        assert sm._get_azure_tenant_identifier() == expected

    @pytest.mark.parametrize(
        "multi_tenant_segment",
        [
            pytest.param("common", id="common"),
            pytest.param("organizations", id="organizations"),
            pytest.param("consumers", id="consumers"),
            pytest.param("CONSUMERS", id="case-insensitive"),
        ],
    )
    @pytest.mark.parametrize("configuration_source", ["client-kwargs", "endpoint"])
    def test_get_azure_tenant_identifier_returns_none_for_tenant_agnostic_authorities(
        self, multi_tenant_segment, configuration_source
    ):
        """Shared authorities identify no deployment-specific tenant, so no issuer can be pinned."""
        sm = EmptySecurityManager()
        client_kwargs = {"tenant_id": multi_tenant_segment} if configuration_source == "client-kwargs" else {}
        api_base_url = (
            None
            if configuration_source == "client-kwargs"
            else f"https://login.microsoftonline.com/{multi_tenant_segment}/oauth2/v2.0/"
        )
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs=client_kwargs,
                api_base_url=api_base_url,
                access_token_url=None,
                authorize_url=None,
            )
        }

        assert sm._get_azure_tenant_identifier() is None

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            pytest.param(
                "https://LOGIN.MICROSOFTONLINE.COM/tenant-uppercase-host/oauth2/v2.0/",
                "tenant-uppercase-host",
                id="case-insensitive-hostname",
            ),
            pytest.param(
                "http://login.microsoftonline.com/tenant-http/oauth2/v2.0/",
                None,
                id="reject-non-https",
            ),
            pytest.param(
                "https://evil.com/login.microsoftonline.com/tenant-abc",
                None,
                id="reject-microsoft-in-path",
            ),
            pytest.param(
                "https://login.microsoftonline.com@evil.com/tenant-abc",
                None,
                id="reject-microsoft-in-userinfo",
            ),
            pytest.param(
                "https://other.microsoftonline.com/tenant-abc",
                None,
                id="reject-wrong-subdomain",
            ),
            pytest.param(
                "https://login.microsoftonline.com/",
                None,
                id="reject-empty-path",
            ),
        ],
    )
    def test_get_azure_tenant_identifier_endpoint_validation(self, url, expected):
        sm = EmptySecurityManager()
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs={},
                api_base_url=url,
                access_token_url=None,
                authorize_url=None,
            )
        }
        assert sm._get_azure_tenant_identifier() == expected

    @pytest.mark.parametrize(
        "remote_kwargs",
        [
            pytest.param(
                {
                    "client_kwargs": {},
                    "api_base_url": "https://login.microsoftonline.com/common/oauth2/v2.0/",
                    "access_token_url": None,
                    "authorize_url": None,
                },
                id="common-endpoint",
            ),
            pytest.param({"client_kwargs": {"tenant_id": "common"}}, id="common-client-kwargs"),
            pytest.param({"client_kwargs": {"tenant_id": "organizations"}}, id="organizations-client-kwargs"),
            pytest.param({"client_kwargs": {"tenant_id": "consumers"}}, id="consumers-client-kwargs"),
        ],
    )
    def test_decode_and_validate_azure_jwt_requires_a_resolvable_tenant(self, remote_kwargs):
        """Without a tenant there is no issuer to check, so the token is not accepted."""
        sm = EmptySecurityManager()
        sm.oauth_remotes = {"azure": SimpleNamespace(**remote_kwargs)}

        with mock.patch("requests.get", autospec=True) as mock_requests_get:
            with pytest.raises(AzureTenantResolutionError, match="tenant could not be determined"):
                sm._decode_and_validate_azure_jwt("header.payload.signature")

        mock_requests_get.assert_not_called()

    @pytest.mark.parametrize(
        "configured_tenant",
        [
            pytest.param(TENANT_GUID, id="lowercase-guid"),
            pytest.param(TENANT_GUID.upper(), id="uppercase-guid"),
        ],
    )
    def test_decode_and_validate_azure_jwt_guid_does_not_call_metadata(self, configured_tenant):
        """GUID tenant identifiers (lowercase and uppercase) canonicalize without metadata HTTP calls."""
        key = JsonWebKey.generate_key("RSA", 2048, options={"kid": "test-kid"}, is_private=True)
        public_key = key.as_dict(is_private=False, kid="test-kid")
        id_token = _create_azure_jwt(key=key)

        sm = EmptySecurityManager()
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs={"tenant_id": configured_tenant},
                client_id=CLIENT_ID,
            )
        }

        with (
            mock.patch.object(
                EmptySecurityManager,
                "_get_microsoft_jwks",
                autospec=True,
                return_value={"keys": [public_key]},
            ),
            mock.patch("requests.get", autospec=True) as mock_requests_get,
        ):
            claims = sm._decode_and_validate_azure_jwt(id_token)

        mock_requests_get.assert_not_called()
        assert claims["iss"] == f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"
        assert claims["aud"] == CLIENT_ID

    @pytest.mark.parametrize(
        "domain",
        [
            pytest.param("microsoft.onmicrosoft.com", id="onmicrosoft-domain"),
            pytest.param("custom.company.org", id="custom-domain"),
        ],
    )
    @pytest.mark.parametrize("configuration_source", ["client-kwargs", "endpoint"])
    def test_decode_and_validate_azure_jwt_domain_resolves_via_metadata(self, domain, configuration_source):
        """Domain tenant identifiers resolve canonical GUID via OIDC metadata and validate token."""
        key = JsonWebKey.generate_key("RSA", 2048, options={"kid": "test-kid"}, is_private=True)
        public_key = key.as_dict(is_private=False, kid="test-kid")
        id_token = _create_azure_jwt(key=key)

        sm = EmptySecurityManager()
        client_kwargs = {"tenant_id": domain} if configuration_source == "client-kwargs" else {}
        api_base_url = (
            None
            if configuration_source == "client-kwargs"
            else f"https://login.microsoftonline.com/{domain}/oauth2/v2.0/"
        )
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs=client_kwargs,
                client_id=CLIENT_ID,
                api_base_url=api_base_url,
                access_token_url=None,
                authorize_url=None,
            )
        }

        mock_resp = _create_mock_response(
            json_data={"issuer": f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"}
        )

        with (
            mock.patch.object(
                EmptySecurityManager,
                "_get_microsoft_jwks",
                autospec=True,
                return_value={"keys": [public_key]},
            ),
            mock.patch("requests.get", autospec=True, return_value=mock_resp) as mock_requests_get,
        ):
            claims = sm._decode_and_validate_azure_jwt(id_token)

        mock_requests_get.assert_called_once_with(
            f"https://login.microsoftonline.com/{domain}/v2.0/.well-known/openid-configuration",
            timeout=5,
            allow_redirects=False,
        )
        assert claims["iss"] == f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"

    def test_decode_and_validate_azure_jwt_v1_issuer_supported_with_domain(self):
        """v1 STS issuer is accepted when domain is configured and resolves to canonical GUID."""
        key = JsonWebKey.generate_key("RSA", 2048, options={"kid": "test-kid"}, is_private=True)
        public_key = key.as_dict(is_private=False, kid="test-kid")
        id_token = _create_azure_jwt(key=key, iss=f"https://sts.windows.net/{TENANT_GUID}/")

        sm = EmptySecurityManager()
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs={"tenant_id": "microsoft.onmicrosoft.com"},
                client_id=CLIENT_ID,
            )
        }

        mock_resp = _create_mock_response(
            json_data={"issuer": f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"}
        )

        with (
            mock.patch.object(
                EmptySecurityManager,
                "_get_microsoft_jwks",
                autospec=True,
                return_value={"keys": [public_key]},
            ),
            mock.patch("requests.get", autospec=True, return_value=mock_resp),
        ):
            claims = sm._decode_and_validate_azure_jwt(id_token)

        assert claims["iss"] == f"https://sts.windows.net/{TENANT_GUID}/"

    def test_decode_and_validate_azure_jwt_rejects_issuer_mismatch(self):
        """Tokens issued for a different tenant are rejected with InvalidClaimError."""
        key = JsonWebKey.generate_key("RSA", 2048, options={"kid": "test-kid"}, is_private=True)
        public_key = key.as_dict(is_private=False, kid="test-kid")
        id_token = _create_azure_jwt(key=key, iss=f"https://login.microsoftonline.com/{OTHER_GUID}/v2.0")

        sm = EmptySecurityManager()
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs={"tenant_id": "microsoft.onmicrosoft.com"},
                client_id=CLIENT_ID,
            )
        }

        mock_resp = _create_mock_response(
            json_data={"issuer": f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"}
        )

        with (
            mock.patch.object(
                EmptySecurityManager,
                "_get_microsoft_jwks",
                autospec=True,
                return_value={"keys": [public_key]},
            ),
            mock.patch("requests.get", autospec=True, return_value=mock_resp),
        ):
            with pytest.raises(InvalidClaimError, match="invalid_claim: Invalid claim 'iss'"):
                sm._decode_and_validate_azure_jwt(id_token)

    def test_decode_and_validate_azure_jwt_rejects_audience_mismatch(self):
        """Tokens minted for another client/application are rejected."""
        key = JsonWebKey.generate_key("RSA", 2048, options={"kid": "test-kid"}, is_private=True)
        public_key = key.as_dict(is_private=False, kid="test-kid")
        id_token = _create_azure_jwt(key=key, aud="wrong-audience")

        sm = EmptySecurityManager()
        sm.oauth_remotes = {
            "azure": SimpleNamespace(
                client_kwargs={"tenant_id": TENANT_GUID},
                client_id=CLIENT_ID,
            )
        }

        with mock.patch.object(
            EmptySecurityManager,
            "_get_microsoft_jwks",
            autospec=True,
            return_value={"keys": [public_key]},
        ):
            with pytest.raises(InvalidClaimError, match="invalid_claim: Invalid claim 'aud'"):
                sm._decode_and_validate_azure_jwt(id_token)

    @pytest.mark.parametrize(
        ("response_kwargs", "request_side_effect", "error_match"),
        [
            pytest.param({"status_code": 404}, None, "HTTP 404", id="http-404"),
            pytest.param({"status_code": 500}, None, "HTTP 500", id="http-500"),
            pytest.param({"status_code": 302}, None, "HTTP 302", id="http-302-redirect"),
            pytest.param(
                {},
                requests.exceptions.Timeout("network timeout"),
                "via OpenID discovery",
                id="network-timeout",
            ),
            pytest.param(
                {"json_side_effect": ValueError("bad json")},
                None,
                "via OpenID discovery",
                id="malformed-json",
            ),
            pytest.param(
                {"json_data": ["not", "dict"]},
                None,
                "not a JSON object",
                id="json-not-dict",
            ),
            pytest.param({"json_data": {}}, None, "missing 'issuer'", id="missing-issuer"),
            pytest.param(
                {
                    "json_data": {"issuer": f"http://login.microsoftonline.com/{TENANT_GUID}/v2.0"},
                },
                None,
                "unexpected issuer",
                id="non-https-issuer",
            ),
            pytest.param(
                {
                    "json_data": {"issuer": f"https://evil.com/{TENANT_GUID}/v2.0"},
                },
                None,
                "unexpected issuer",
                id="wrong-issuer-host",
            ),
            pytest.param(
                {
                    "json_data": {"issuer": "https://login.microsoftonline.com/not-a-uuid/v2.0"},
                },
                None,
                "does not contain a valid tenant GUID",
                id="non-uuid-issuer",
            ),
            pytest.param(
                {
                    "json_data": {"issuer": f"https://login.microsoftonline.com/{TENANT_GUID}/v3.0"},
                },
                None,
                "unexpected issuer",
                id="wrong-issuer-path",
            ),
        ],
    )
    def test_resolve_azure_tenant_guid_metadata_failures(
        self, response_kwargs, request_side_effect, error_match
    ):
        """All metadata discovery failures fail closed with AzureTenantResolutionError."""
        sm = EmptySecurityManager()

        if request_side_effect:
            mock_get = mock.patch("requests.get", autospec=True, side_effect=request_side_effect)
        else:
            mock_resp = _create_mock_response(**response_kwargs)
            mock_get = mock.patch("requests.get", autospec=True, return_value=mock_resp)

        with mock_get:
            with pytest.raises(AzureTenantResolutionError, match=error_match):
                sm._resolve_azure_tenant_guid("contoso.onmicrosoft.com")

    def test_resolve_azure_tenant_guid_encodes_identifier_as_one_path_segment(self):
        sm = EmptySecurityManager()
        mock_resp = _create_mock_response(
            json_data={"issuer": f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"}
        )

        with mock.patch("requests.get", autospec=True, return_value=mock_resp) as mock_requests_get:
            result = sm._resolve_azure_tenant_guid("tenant/name?query#fragment")

        assert result == TENANT_GUID
        mock_requests_get.assert_called_once_with(
            "https://login.microsoftonline.com/tenant%2Fname%3Fquery%23fragment/"
            "v2.0/.well-known/openid-configuration",
            timeout=5,
            allow_redirects=False,
        )

    def test_resolve_azure_tenant_guid_caches_successful_result(self):
        """Successful domain resolution is cached and does not make repeated HTTP requests."""
        sm = EmptySecurityManager()
        mock_resp = _create_mock_response(
            json_data={"issuer": f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"}
        )

        with mock.patch("requests.get", autospec=True, return_value=mock_resp) as mock_requests_get:
            res1 = sm._resolve_azure_tenant_guid("contoso.onmicrosoft.com")
            res2 = sm._resolve_azure_tenant_guid("contoso.onmicrosoft.com")

        assert res1 == TENANT_GUID
        assert res2 == TENANT_GUID
        mock_requests_get.assert_called_once()

    def test_resolve_azure_tenant_guid_does_not_cache_transient_failures(self):
        """Transient discovery failures are not cached; subsequent calls retry HTTP request."""
        sm = EmptySecurityManager()
        mock_resp_success = _create_mock_response(
            json_data={"issuer": f"https://login.microsoftonline.com/{TENANT_GUID}/v2.0"}
        )

        with mock.patch(
            "requests.get",
            autospec=True,
            side_effect=[requests.exceptions.Timeout("temporary connection failure"), mock_resp_success],
        ) as mock_requests_get:
            with pytest.raises(AzureTenantResolutionError, match="via OpenID discovery"):
                sm._resolve_azure_tenant_guid("contoso.onmicrosoft.com")

            res = sm._resolve_azure_tenant_guid("contoso.onmicrosoft.com")

        assert res == TENANT_GUID
        assert mock_requests_get.call_count == 2


def test_ldap_search_escapes_username_and_validates_filter():
    """Test that LDAP search properly escapes username and validates search filter."""
    mock_ldap = Mock()
    mock_ldap.SCOPE_SUBTREE = 2

    def escape_chars(text):
        # Escape backslash first, then special chars
        result = text.replace("\\", "\\5c")
        result = result.replace("*", "\\2a")
        result = result.replace("(", "\\28")
        result = result.replace(")", "\\29")
        return result

    mock_ldap.filter.escape_filter_chars = escape_chars
    mock_con = Mock()
    mock_con.search_s = Mock(return_value=[("cn=test,dc=example,dc=com", {})])

    sm = EmptySecurityManager()
    with (
        mock.patch.object(
            type(sm), "auth_ldap_search", new_callable=mock.PropertyMock, return_value="dc=example,dc=com"
        ),
        mock.patch.object(
            type(sm),
            "auth_ldap_search_filter",
            new_callable=mock.PropertyMock,
            return_value="(objectClass=person)",
        ),
        mock.patch.object(
            type(sm), "auth_ldap_uid_field", new_callable=mock.PropertyMock, return_value="uid"
        ),
        mock.patch.object(
            type(sm), "auth_ldap_firstname_field", new_callable=mock.PropertyMock, return_value="givenName"
        ),
        mock.patch.object(
            type(sm), "auth_ldap_lastname_field", new_callable=mock.PropertyMock, return_value="sn"
        ),
        mock.patch.object(
            type(sm), "auth_ldap_email_field", new_callable=mock.PropertyMock, return_value="mail"
        ),
        mock.patch.object(type(sm), "auth_roles_mapping", new_callable=mock.PropertyMock, return_value=None),
        mock.patch.object(
            type(sm),
            "auth_ldap_use_nested_groups_for_roles",
            new_callable=mock.PropertyMock,
            return_value=False,
        ),
    ):
        # Test with special characters in username - should be escaped
        sm._search_ldap(mock_ldap, mock_con, "test*user")

        # Verify the filter was constructed with escaped username
        call_args = mock_con.search_s.call_args
        actual_filter = call_args[0][2]
        assert "test\\2auser" in actual_filter  # * should be escaped

        # Test that invalid filter raises ValueError
        with mock.patch.object(
            type(sm), "auth_ldap_search_filter", new_callable=mock.PropertyMock, return_value="invalid"
        ):
            with pytest.raises(ValueError, match="AUTH_LDAP_SEARCH_FILTER"):
                sm._search_ldap(mock_ldap, mock_con, "testuser")
