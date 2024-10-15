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

from pathlib import Path
from unittest.mock import Mock, patch

import boto3
import pytest
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.www import check_content_in_response

from airflow.www import app as application

from providers.tests.system.amazon.aws.utils import set_env_id

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

env_id_cache: str | None = None
policy_store_id_cache: str | None = None


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
        .parents[6]
        .joinpath("airflow", "providers", "amazon", "aws", "auth_manager", "avp", "schema.json")
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


@pytest.fixture
def env_id():
    global env_id_cache
    if not env_id_cache:
        env_id_cache = set_env_id()
    return env_id_cache


@pytest.fixture
def region_name():
    return boto3.session.Session().region_name


@pytest.fixture
def avp_policy_store_id(env_id):
    global policy_store_id_cache
    if not policy_store_id_cache:
        policy_store_id_cache = create_avp_policy_store(env_id)
    return policy_store_id_cache


@pytest.fixture
def base_app(region_name, avp_policy_store_id):
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
            ("aws_auth_manager", "region_name"): region_name,
            ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
            ("aws_auth_manager", "avp_policy_store_id"): avp_policy_store_id,
        }
    ):
        with (
            patch(
                "airflow.providers.amazon.aws.auth_manager.views.auth.OneLogin_Saml2_IdPMetadataParser"
            ) as mock_parser,
            patch(
                "airflow.providers.amazon.aws.auth_manager.views.auth.AwsAuthManagerAuthenticationViews._init_saml_auth"
            ) as mock_init_saml_auth,
        ):
            mock_parser.parse_remote.return_value = SAML_METADATA_PARSED

            yield mock_init_saml_auth


@pytest.fixture
def client_no_permissions(base_app):
    auth = Mock()
    auth.is_authenticated.return_value = True
    auth.get_nameid.return_value = "user_no_permissions"
    auth.get_attributes.return_value = {
        "id": ["user_no_permissions"],
        "groups": [],
        "email": ["email"],
    }
    base_app.return_value = auth
    return application.create_app(testing=True)


@pytest.fixture
def client_admin_permissions(base_app):
    auth = Mock()
    auth.is_authenticated.return_value = True
    auth.get_nameid.return_value = "user_admin_permissions"
    auth.get_attributes.return_value = {
        "id": ["user_admin_permissions"],
        "groups": ["Admin"],
    }
    base_app.return_value = auth
    return application.create_app(testing=True)


@pytest.mark.system("amazon")
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
            and f"Created by system test TestAwsAuthManager: {env_id_cache}" in store["description"]
        ]

        for policy_store_id in policy_store_ids:
            client.delete_policy_store(policyStoreId=policy_store_id)

    def test_login_no_permissions(self, client_no_permissions):
        with client_no_permissions.test_client() as client:
            response = client.get("/login_callback", follow_redirects=True)
            check_content_in_response("Your user has no roles and/or permissions!", response, 403)

    def test_login_admin(self, client_admin_permissions):
        with client_admin_permissions.test_client() as client:
            response = client.get("/login_callback", follow_redirects=True)
            check_content_in_response("<h2>DAGs</h2>", response, 200)
