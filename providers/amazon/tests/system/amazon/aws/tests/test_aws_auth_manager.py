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

from functools import cache
from pathlib import Path
from unittest.mock import Mock, patch

import boto3
import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("AWS auth manager is only compatible with Airflow >= 3.0.0", allow_module_level=True)

from fastapi.testclient import TestClient
from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX, create_app
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN

from system.amazon.aws.utils import set_env_id
from tests_common.test_utils.config import conf_vars

SAML_METADATA_URL = "/saml/metadata"
SAML_METADATA_PARSED = {
    "idp": {
        "entityId": "https://portal.sso.us-east-1.amazonaws.com/saml/assertion/<assertion>",
        "singleSignOnService": {
            "url": "https://portal.sso.us-east-1.amazonaws.com/saml/assertion/<assertion>",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
        },
        "singleLogoutService": {
            "url": "https://portal.sso.us-east-1.amazonaws.com/saml/logout/<assertion>",
            "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
        },
        "x509cert": "<cert>",
    },
    "security": {"authnRequestsSigned": False},
    "sp": {"NameIDFormat": "urn:oasis:names:tc:SAML:2.0:nameid-format:transient"},
}

AVP_POLICY_ADMIN = """
permit (
    principal in Airflow::Group::"Admin",
    action,
    resource
);
"""

TEST_REQUEST_ID = "ONELOGIN_authn_request_id"


def create_avp_policy_store(env_id):
    description = f"Created by system test TestAwsAuthManager: {env_id}"
    client = boto3.client("verifiedpermissions")
    response = client.create_policy_store(
        validationSettings={"mode": "STRICT"},
        description=description,
    )
    policy_store_id = response["policyStoreId"]

    schema_path = (
        Path(__file__)
        .parents[5]
        .joinpath("src", "airflow", "providers", "amazon", "aws", "auth_manager", "avp", "schema.json")
        .resolve()
    )
    with open(schema_path) as schema_file:
        client.put_schema(
            policyStoreId=policy_store_id,
            definition={
                "cedarJson": schema_file.read(),
            },
        )

    client.create_policy(
        policyStoreId=policy_store_id,
        definition={
            "static": {"description": "Admin permissions", "statement": AVP_POLICY_ADMIN},
        },
    )

    return policy_store_id


@cache
def env_id():
    return set_env_id("test_aws_auth_manager")


@pytest.fixture
def region_name():
    return boto3.session.Session().region_name


@pytest.fixture
@cache
def avp_policy_store_id():
    return create_avp_policy_store(env_id())


@pytest.fixture
def base_app(region_name, avp_policy_store_id):
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
            ("aws_auth_manager", "conn_id"): "aws_default",
            ("aws_auth_manager", "region_name"): region_name,
            ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
            ("aws_auth_manager", "avp_policy_store_id"): avp_policy_store_id,
            ("api", "ssl_cert"): "",
        }
    ):
        with (
            patch.object(OneLogin_Saml2_IdPMetadataParser, "parse_remote") as mock_parse_remote,
            patch(
                "airflow.providers.amazon.aws.auth_manager.routes.login._init_saml_auth"
            ) as mock_init_saml_auth,
        ):
            mock_parse_remote.return_value = SAML_METADATA_PARSED
            yield mock_init_saml_auth


def _saml_auth_mock(nameid: str, attributes: dict) -> Mock:
    auth = Mock()
    auth.is_authenticated.return_value = True
    auth.get_nameid.return_value = nameid
    auth.get_attributes.return_value = attributes
    auth.login.return_value = "https://idp.example.com/sso"
    auth.get_last_request_id.return_value = TEST_REQUEST_ID
    return auth


def _complete_login(client: TestClient, saml_auth: Mock, login_path: str):
    """Make the whole login round trip and return the callback response."""
    client.get(AUTH_MANAGER_FASTAPI_APP_PREFIX + login_path, follow_redirects=False)
    relay_state = saml_auth.login.call_args[0][0]
    return client.post(
        AUTH_MANAGER_FASTAPI_APP_PREFIX + "/login_callback",
        follow_redirects=False,
        data={"RelayState": relay_state},
    )


@pytest.fixture
def client_no_permissions(base_app):
    base_app.return_value = _saml_auth_mock(
        "user_no_permissions",
        {"id": ["user_no_permissions"], "groups": [], "email": ["email"]},
    )
    return TestClient(create_app())


@pytest.fixture
def client_admin_permissions(base_app):
    base_app.return_value = _saml_auth_mock(
        "user_admin_permissions",
        {"id": ["user_admin_permissions"], "groups": ["Admin"]},
    )
    return TestClient(create_app())


@pytest.mark.system
class TestAwsAuthManager:
    """
    Run tests on Airflow using AWS auth manager with real credentials
    """

    @classmethod
    def teardown_class(cls):
        cls.delete_avp_policy_store()

    @classmethod
    def delete_avp_policy_store(cls):
        client = boto3.client("verifiedpermissions")

        paginator = client.get_paginator("list_policy_stores")
        pages = paginator.paginate()
        policy_store_ids = [
            store["policyStoreId"]
            for page in pages
            for store in page["policyStores"]
            if "description" in store
            and f"Created by system test TestAwsAuthManager: {env_id()}" in store["description"]
        ]

        for policy_store_id in policy_store_ids:
            client.delete_policy_store(policyStoreId=policy_store_id)

    def test_login_admin_redirect(self, base_app, client_admin_permissions):
        response = _complete_login(client_admin_permissions, base_app.return_value, "/login")
        token = response.cookies.get(COOKIE_NAME_JWT_TOKEN)
        assert response.status_code == 303
        assert "location" in response.headers
        assert response.headers["location"] == "/"
        assert token is not None

    def test_login_admin_token(self, base_app, client_admin_permissions):
        response = _complete_login(client_admin_permissions, base_app.return_value, "/login/token")
        assert response.status_code == 200
        assert response.json()["access_token"]
