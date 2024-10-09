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

from unittest.mock import Mock, patch

import pytest
from flask import session, url_for

from airflow.exceptions import AirflowException
from airflow.www import app as application

from dev.tests_common.test_utils.compat import AIRFLOW_V_2_9_PLUS
from dev.tests_common.test_utils.config import conf_vars

pytestmark = [
    pytest.mark.skipif(not AIRFLOW_V_2_9_PLUS, reason="Test requires Airflow 2.9+"),
    pytest.mark.skip_if_database_isolation_mode,
]


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


@pytest.fixture
def aws_app():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
            ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
        }
    ):
        with (
            patch(
                "airflow.providers.amazon.aws.auth_manager.views.auth.OneLogin_Saml2_IdPMetadataParser"
            ) as mock_parser,
            patch(
                "airflow.providers.amazon.aws.auth_manager.avp.facade.AwsAuthManagerAmazonVerifiedPermissionsFacade.is_policy_store_schema_up_to_date"
            ) as mock_is_policy_store_schema_up_to_date,
        ):
            mock_is_policy_store_schema_up_to_date.return_value = True
            mock_parser.parse_remote.return_value = SAML_METADATA_PARSED
            return application.create_app(testing=True, config={"WTF_CSRF_ENABLED": False})


@pytest.mark.db_test
class TestAwsAuthManagerAuthenticationViews:
    def test_login_redirect_to_identity_center(self, aws_app):
        with aws_app.test_client() as client:
            response = client.get("/login")
            assert response.status_code == 302
            assert response.location.startswith("https://portal.sso.us-east-1.amazonaws.com/saml/assertion/")

    def test_logout_redirect_to_identity_center(self, aws_app):
        with aws_app.test_client() as client:
            response = client.post("/logout")
            assert response.status_code == 302
            assert response.location.startswith("https://portal.sso.us-east-1.amazonaws.com/saml/logout/")

    def test_login_metadata_return_xml_file(self, aws_app):
        with aws_app.test_client() as client:
            response = client.get("/login_metadata")
            assert response.status_code == 200
            assert response.headers["Content-Type"] == "text/xml"

    def test_login_callback_set_user_in_session(self):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
                ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
            }
        ):
            with (
                patch(
                    "airflow.providers.amazon.aws.auth_manager.views.auth.OneLogin_Saml2_IdPMetadataParser"
                ) as mock_parser,
                patch(
                    "airflow.providers.amazon.aws.auth_manager.views.auth.AwsAuthManagerAuthenticationViews._init_saml_auth"
                ) as mock_init_saml_auth,
                patch(
                    "airflow.providers.amazon.aws.auth_manager.avp.facade.AwsAuthManagerAmazonVerifiedPermissionsFacade.is_policy_store_schema_up_to_date"
                ) as mock_is_policy_store_schema_up_to_date,
            ):
                mock_is_policy_store_schema_up_to_date.return_value = True
                mock_parser.parse_remote.return_value = SAML_METADATA_PARSED

                auth = Mock()
                auth.is_authenticated.return_value = True
                auth.get_nameid.return_value = "user_id"
                auth.get_attributes.return_value = {
                    "id": ["1"],
                    "groups": ["group_1", "group_2"],
                    "email": ["email"],
                }
                mock_init_saml_auth.return_value = auth
                app = application.create_app(testing=True)
                with app.test_client() as client:
                    response = client.get("/login_callback")
                    assert response.status_code == 302
                    assert response.location == url_for("Airflow.index")
                    assert session["aws_user"] is not None
                    assert session["aws_user"].get_id() == "1"
                    assert session["aws_user"].get_name() == "user_id"

    def test_login_callback_raise_exception_if_errors(self):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.providers.amazon.aws.auth_manager.aws_auth_manager.AwsAuthManager",
                ("aws_auth_manager", "saml_metadata_url"): SAML_METADATA_URL,
            }
        ):
            with (
                patch(
                    "airflow.providers.amazon.aws.auth_manager.views.auth.OneLogin_Saml2_IdPMetadataParser"
                ) as mock_parser,
                patch(
                    "airflow.providers.amazon.aws.auth_manager.views.auth.AwsAuthManagerAuthenticationViews._init_saml_auth"
                ) as mock_init_saml_auth,
                patch(
                    "airflow.providers.amazon.aws.auth_manager.avp.facade.AwsAuthManagerAmazonVerifiedPermissionsFacade.is_policy_store_schema_up_to_date"
                ) as mock_is_policy_store_schema_up_to_date,
            ):
                mock_is_policy_store_schema_up_to_date.return_value = True
                mock_parser.parse_remote.return_value = SAML_METADATA_PARSED

                auth = Mock()
                auth.is_authenticated.return_value = False
                mock_init_saml_auth.return_value = auth
                app = application.create_app(testing=True)
                with app.test_client() as client:
                    with pytest.raises(AirflowException):
                        client.get("/login_callback")

    def test_logout_callback_raise_not_implemented_error(self, aws_app):
        with aws_app.test_client() as client:
            with pytest.raises(NotImplementedError):
                client.get("/logout_callback")
