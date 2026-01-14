#
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
"""
Databricks Google Hook.

This hook extends BaseDatabricksHook to add Google-signed OpenID Connect (ID) token
authentication support for Databricks connections.
"""

from __future__ import annotations

import base64
import json
import time
from typing import TYPE_CHECKING, Any

import aiohttp
import requests
from requests import exceptions as requests_exceptions
from tenacity import RetryError

from airflow.exceptions import AirflowException, AirflowOptionalProviderFeatureException

from airflow.providers.databricks.hooks.databricks import DatabricksHook

if TYPE_CHECKING:
    from airflow.models import Connection

TOKEN_REFRESH_LEAD_TIME = 120


class DatabricksGoogleHook(DatabricksHook):
    """
    Databricks hook with Google ID token authentication support.

    Extends DatabricksHook to add Google-signed OpenID Connect (ID) token
    authentication for Databricks connections. This enables users to authenticate
    with Databricks using Google Cloud service accounts and ID tokens.

    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing-out.
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :param retry_delay: The number of seconds to wait between retries (it
        might be a floating point number).
    :param retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param caller: The name of the operator that is calling the hook.
    """

    conn_type = "databricks"

    extra_parameters = [
        "token",
        "host",
        "use_azure_managed_identity",
        "use_default_azure_credential",
        "azure_ad_endpoint",
        "azure_resource_id",
        "azure_tenant_id",
        "service_principal_oauth",
        "use_google_id_token",
        "google_id_token_target_principal",
        "google_id_token_target_audience",
    ]

    def __init__(
        self,
        databricks_conn_id: str = DatabricksHook.default_conn_name,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        retry_args: dict[Any, Any] | None = None,
        caller: str = "DatabricksGoogleHook",
    ) -> None:
        super().__init__(databricks_conn_id, timeout_seconds, retry_limit, retry_delay, retry_args, caller)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        widgets = super().get_connection_form_widgets()
        widgets.update(
            {
                "use_google_id_token": BooleanField(
                    lazy_gettext("Use Google ID Token"), default=False
                ),
                "google_id_token_target_principal": StringField(
                    lazy_gettext("Google ID Token Target Principal (Service Account Email)"),
                    widget=BS3TextFieldWidget(),
                ),
                "google_id_token_target_audience": StringField(
                    lazy_gettext("Google ID Token Target Audience (Databricks Workspace URL)"),
                    widget=BS3TextFieldWidget(),
                ),
            }
        )
        return widgets

    @staticmethod
    def _is_jwt_token_valid(token: str) -> bool:
        """
        Check if a JWT token is valid and hasn't expired yet.

        :param token: JWT token string
        :return: true if token is valid, false otherwise
        """
        try:
            # JWT tokens have 3 parts separated by dots: header.payload.signature
            parts = token.split(".")
            if len(parts) != 3:
                return False

            # Decode the payload (second part)
            payload = parts[1]
            # Add padding if needed for base64 decoding
            payload += "=" * (-len(payload) % 4)
            decoded = json.loads(base64.urlsafe_b64decode(payload))

            # Check if token has expired (with refresh lead time)
            exp = decoded.get("exp")
            if not exp:
                return False

            return int(exp) > (int(time.time()) + TOKEN_REFRESH_LEAD_TIME)
        except (ValueError, json.JSONDecodeError, IndexError, KeyError):
            # If we can't decode or validate, consider it invalid
            return False

    def _get_google_id_token(self, target_audience: str) -> str:
        """
        Get Google ID token for given target audience.

        Supports service account impersonation via target_principal.
        For non-impersonated cases, uses google.oauth2.id_token.fetch_id_token() directly.
        For impersonated cases, uses Google IAM Credentials API to generate ID tokens.
        :param target_audience: The intended audience for the ID token (typically Databricks workspace URL).
        :return: Google ID token, or raise an exception
        """
        target_principal = self.databricks_conn.extra_dejson.get("google_id_token_target_principal")
        # Create unique key for token cache (includes principal if impersonating)
        google_token_key = f"google_id_token_{target_audience}_{target_principal or 'default'}"

        # Check if we have a valid cached JWT token
        cached_token = self.jwt_tokens.get(google_token_key)
        if cached_token and self._is_jwt_token_valid(cached_token):
            return cached_token

        self.log.info("Existing Google ID token is expired, or going to expire soon. Refreshing...")
        try:
            import google.auth
            import google.auth.transport.requests
            from google.auth import impersonated_credentials
            from google.oauth2 import id_token

            for attempt in self._get_retry_object():
                with attempt:
                    # Get default credentials
                    credentials, _ = google.auth.default()

                    # Handle service account impersonation if target_principal is specified
                    if target_principal:
                        self.log.debug("Impersonating service account: %s", target_principal)
                        # For impersonation, we need to use IAM Credentials API
                        credentials = impersonated_credentials.Credentials(
                            source_credentials=credentials,
                            target_principal=target_principal,
                            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                        )

                        # Refresh credentials to get access token
                        request = google.auth.transport.requests.Request()
                        credentials.refresh(request)
                        access_token = credentials.token
                        if not access_token:
                            raise ValueError("Failed to obtain access token from Google credentials")

                        # Use IAM Credentials API to generate ID token
                        iam_url = f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{target_principal}:generateIdToken"
                        headers = {
                            "Authorization": f"Bearer {access_token}",
                            "Content-Type": "application/json",
                        }
                        payload = {"audience": target_audience}

                        response = requests.post(iam_url, json=payload, headers=headers, timeout=self.token_timeout_seconds)
                        response.raise_for_status()
                        response_json = response.json()
                        id_token_value = response_json["token"]
                    else:
                        # For non-impersonated case, use fetch_id_token() directly (simpler and more efficient)
                        self.log.debug("Using direct fetch_id_token() for non-impersonated credentials")
                        request = google.auth.transport.requests.Request()
                        credentials.refresh(request)
                        id_token_value = id_token.fetch_id_token(request, target_audience)

                    # Cache the JWT token
                    self.jwt_tokens[google_token_key] = id_token_value
                    break
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(
                "Google authentication libraries are not installed. "
                "Please install apache-airflow-providers-google to use Google ID token authentication."
            ) from e
        except RetryError:
            raise ConnectionError(f"API requests to Google failed {self.retry_limit} times. Giving up.")
        except requests_exceptions.HTTPError as e:
            # Re-raise with additional context using standard exception
            msg = f"Response: {e.response.content.decode()}, Status Code: {e.response.status_code}"
            raise OSError(msg) from e

        return id_token_value

    async def _a_get_google_id_token(self, target_audience: str) -> str:
        """
        Async version of `_get_google_id_token()`.

        Supports service account impersonation via target_principal.
        Uses Google IAM Credentials API to generate ID tokens (async version).
        :param target_audience: The intended audience for the ID token (typically Databricks workspace URL).
        :return: Google ID token, or raise an exception
        """
        target_principal = self.databricks_conn.extra_dejson.get("google_id_token_target_principal")
        # Create unique key for token cache (includes principal if impersonating)
        google_token_key = f"google_id_token_{target_audience}_{target_principal or 'default'}"

        # Check if we have a valid cached JWT token
        cached_token = self.jwt_tokens.get(google_token_key)
        if cached_token and self._is_jwt_token_valid(cached_token):
            return cached_token

        self.log.info("Existing Google ID token is expired, or going to expire soon. Refreshing...")
        try:
            import google.auth
            import google.auth.transport.requests
            from google.auth import impersonated_credentials

            async for attempt in self._a_get_retry_object():
                with attempt:
                    # Get default credentials (sync operation, but needed for auth)
                    credentials, _ = google.auth.default()

                    # Handle service account impersonation if target_principal is specified
                    if target_principal:
                        self.log.debug("Impersonating service account: %s", target_principal)
                        credentials = impersonated_credentials.Credentials(
                            source_credentials=credentials,
                            target_principal=target_principal,
                            target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                        )

                    # Refresh credentials to get access token
                    request = google.auth.transport.requests.Request()
                    credentials.refresh(request)
                    access_token = credentials.token
                    if not access_token:
                        raise ValueError("Failed to obtain access token from Google credentials")

                    # Determine service account email for IAM API
                    if target_principal:
                        service_account_email = target_principal
                    elif hasattr(credentials, "service_account_email"):
                        service_account_email = credentials.service_account_email
                    elif hasattr(credentials, "_target_principal"):
                        # For impersonated credentials
                        service_account_email = credentials._target_principal
                    else:
                        raise ValueError(
                            "Unable to determine service account email for ID token generation. "
                            "Please specify google_id_token_target_principal in connection extra."
                        )

                    # Use IAM Credentials API to generate ID token
                    iam_url = f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{service_account_email}:generateIdToken"
                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    }
                    payload = {"audience": target_audience}

                    async with self._session.post(iam_url, json=payload, headers=headers, timeout=self.token_timeout_seconds) as resp:
                        resp.raise_for_status()
                        response_json = await resp.json()
                        id_token_value = response_json["token"]

                    # Cache the JWT token
                    self.jwt_tokens[google_token_key] = id_token_value
                    break
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(
                "Google authentication libraries are not installed. "
                "Please install apache-airflow-providers-google to use Google ID token authentication."
            ) from e
        except RetryError:
            raise ConnectionError(f"API requests to Google failed {self.retry_limit} times. Giving up.")
        except aiohttp.ClientResponseError as err:
            # Re-raise with additional context using standard exception
            msg = f"Response: {err.message}, Status Code: {err.status}"
            raise OSError(msg) from err

        return id_token_value

    def _get_token(self, raise_error: bool = False) -> str | None:
        """Override to add Google ID token support."""
        # Check for Google ID token first
        if self.databricks_conn.extra_dejson.get("use_google_id_token", False):
            self.log.debug("Using Google ID Token.")
            # Get target audience from extra or use host as default
            target_audience = self.databricks_conn.extra_dejson.get(
                "google_id_token_target_audience", f"https://{self.host}"
            )
            return self._get_google_id_token(target_audience)

        # Fall back to parent implementation for other auth methods
        return super()._get_token(raise_error)

    async def _a_get_token(self, raise_error: bool = False) -> str | None:
        """Override to add Google ID token support."""
        # Check for Google ID token first
        if self.databricks_conn.extra_dejson.get("use_google_id_token", False):
            self.log.debug("Using Google ID Token.")
            # Get target audience from extra or use host as default
            target_audience = self.databricks_conn.extra_dejson.get(
                "google_id_token_target_audience", f"https://{self.host}"
            )
            return await self._a_get_google_id_token(target_audience)

        # Fall back to parent implementation for other auth methods
        return await super()._a_get_token(raise_error)

