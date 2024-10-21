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

import logging
from functools import cached_property

from flask import make_response, redirect, request, session, url_for
from flask_appbuilder import expose

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.auth_manager.constants import CONF_SAML_METADATA_URL_KEY, CONF_SECTION_NAME
from airflow.providers.amazon.aws.auth_manager.user import AwsAuthManagerUser
from airflow.www.app import csrf
from airflow.www.views import AirflowBaseView

try:
    from onelogin.saml2.auth import OneLogin_Saml2_Auth
    from onelogin.saml2.idp_metadata_parser import OneLogin_Saml2_IdPMetadataParser
except ImportError:
    raise ImportError(
        "AWS auth manager requires the python3-saml library but it is not installed by default. "
        "Please install the python3-saml library by running: "
        "pip install apache-airflow-providers-amazon[python3-saml]"
    )

logger = logging.getLogger(__name__)


class AwsAuthManagerAuthenticationViews(AirflowBaseView):
    """
    Views specific to AWS auth manager authentication mechanism.

    Some code below is inspired from
    https://github.com/SAML-Toolkits/python3-saml/blob/6988bdab7a203abfe8dc264992f7e350c67aef3d/demo-flask/index.py
    """

    @cached_property
    def idp_data(self) -> dict:
        saml_metadata_url = conf.get_mandatory_value(CONF_SECTION_NAME, CONF_SAML_METADATA_URL_KEY)
        return OneLogin_Saml2_IdPMetadataParser.parse_remote(saml_metadata_url)

    @expose("/login")
    def login(self):
        """Start login process."""
        saml_auth = self._init_saml_auth()
        return redirect(saml_auth.login())

    @expose("/logout", methods=("GET", "POST"))
    def logout(self):
        """Start logout process."""
        session.clear()
        saml_auth = self._init_saml_auth()

        return redirect(saml_auth.logout())

    @csrf.exempt
    @expose("/login_callback", methods=("GET", "POST"))
    def login_callback(self):
        """
        Redirect the user to this callback after successful login.

        CSRF protection needs to be disabled otherwise the callback won't work.
        """
        saml_auth = self._init_saml_auth()
        saml_auth.process_response()
        errors = saml_auth.get_errors()
        is_authenticated = saml_auth.is_authenticated()
        if not is_authenticated:
            error_reason = saml_auth.get_last_error_reason()
            logger.error("Failed to authenticate")
            logger.error("Errors: %s", errors)
            logger.error("Error reason: %s", error_reason)
            raise AirflowException(f"Failed to authenticate: {error_reason}")

        attributes = saml_auth.get_attributes()
        user = AwsAuthManagerUser(
            user_id=attributes["id"][0],
            groups=attributes["groups"],
            username=saml_auth.get_nameid(),
            email=attributes["email"][0] if "email" in attributes else None,
        )
        session["aws_user"] = user

        return redirect(url_for("Airflow.index"))

    @csrf.exempt
    @expose("/logout_callback", methods=("GET", "POST"))
    def logout_callback(self):
        raise NotImplementedError("AWS Identity center does not support SLO (Single Logout Service)")

    @expose("/login_metadata")
    def login_metadata(self):
        saml_auth = self._init_saml_auth()
        settings = saml_auth.get_settings()
        metadata = settings.get_sp_metadata()
        errors = settings.validate_metadata(metadata)

        if len(errors) == 0:
            resp = make_response(metadata, 200)
            resp.headers["Content-Type"] = "text/xml"
        else:
            resp = make_response(", ".join(errors), 500)
        return resp

    @staticmethod
    def _prepare_flask_request() -> dict:
        return {
            "https": "on" if request.scheme == "https" else "off",
            "http_host": request.host,
            "script_name": request.path,
            "get_data": request.args.copy(),
            "post_data": request.form.copy(),
        }

    def _init_saml_auth(self) -> OneLogin_Saml2_Auth:
        request_data = self._prepare_flask_request()
        base_url = conf.get(section="webserver", key="base_url")
        settings = {
            # We want to keep this flag on in case of errors.
            # It provides an error reasons, if turned off, it does not
            "debug": True,
            "sp": {
                "entityId": f"{base_url}/login_metadata",
                "assertionConsumerService": {
                    "url": f"{base_url}/login_callback",
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST",
                },
                "singleLogoutService": {
                    "url": f"{base_url}/logout_callback",
                    "binding": "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect",
                },
            },
        }
        merged_settings = OneLogin_Saml2_IdPMetadataParser.merge_settings(settings, self.idp_data)
        return OneLogin_Saml2_Auth(request_data, merged_settings)
