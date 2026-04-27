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
"""Hook for Akeyless Vault Platform."""

from __future__ import annotations

from functools import cached_property
from typing import Any

import akeyless

from airflow.providers.common.compat.sdk import BaseHook

VALID_AUTH_TYPES = ("api_key", "aws_iam", "gcp", "azure_ad", "uid", "jwt", "k8s", "certificate")


class AkeylessHook(BaseHook):
    """
    Hook to interact with the Akeyless Vault Platform.

    Thin wrapper around the ``akeyless`` Python SDK.

    .. seealso::
        - https://docs.akeyless.io/
        - https://github.com/akeylesslabs/akeyless-python

    Connection fields:

    * **Host**     -> API URL (default ``https://api.akeyless.io``)
    * **Login**    -> Access ID
    * **Password** -> Access Key (for ``api_key`` auth)
    * **Extra**    -> JSON with ``access_type`` and auth-method-specific fields

    :param akeyless_conn_id: Airflow connection ID.
    """

    conn_name_attr = "akeyless_conn_id"
    default_conn_name = "akeyless_default"
    conn_type = "akeyless"
    hook_name = "Akeyless"

    def __init__(self, akeyless_conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__()
        self.akeyless_conn_id = akeyless_conn_id
        self._conn = self.get_connection(akeyless_conn_id)
        self._extra = self._conn.extra_dejson or {}

    @cached_property
    def client(self) -> akeyless.V2Api:
        """Return an ``akeyless.V2Api`` client (cached)."""
        api_url = self._conn.host or self._extra.get("api_url", "https://api.akeyless.io")
        if not api_url.startswith("http"):
            api_url = f"https://{api_url}"
        return akeyless.V2Api(akeyless.ApiClient(akeyless.Configuration(host=api_url)))

    def get_conn(self) -> akeyless.V2Api:
        """Return the underlying ``akeyless.V2Api`` client."""
        return self.client

    def authenticate(self) -> str:
        """
        Authenticate and return an API token.

        For ``uid`` auth the token is the UID token itself.
        For all other methods, calls ``akeyless.Auth``.
        """
        access_type = self._extra.get("access_type", "api_key")

        if access_type not in VALID_AUTH_TYPES:
            raise ValueError(
                f"Unsupported access_type {access_type!r}. Must be one of: {', '.join(VALID_AUTH_TYPES)}"
            )

        if access_type == "uid":
            return self._extra["uid_token"]

        body = akeyless.Auth(access_id=self._conn.login)

        if access_type == "api_key":
            body.access_key = self._conn.password
        elif access_type in ("aws_iam", "gcp", "azure_ad"):
            body.access_type = access_type
            body.cloud_id = self._get_cloud_id(access_type)
        elif access_type == "jwt":
            body.access_type = "jwt"
            body.jwt = self._extra.get("jwt")
        elif access_type == "k8s":
            body.access_type = "k8s"
            body.k8s_auth_config_name = self._extra.get("k8s_auth_config_name")
        elif access_type == "certificate":
            body.access_type = "cert"
            body.cert_data = self._extra.get("certificate_data")
            body.key_data = self._extra.get("private_key_data")

        return self.client.auth(body).token

    def _get_cloud_id(self, access_type: str) -> str:
        try:
            from akeyless_cloud_id import CloudId
        except ImportError:
            raise ImportError(
                "`akeyless_cloud_id` is required for aws_iam, gcp, and azure_ad "
                "authentication. Install it with: "
                "pip install apache-airflow-providers-akeyless[cloud_id]"
            )

        cid = CloudId()
        if access_type == "aws_iam":
            return cid.generate()
        if access_type == "gcp":
            return cid.generateGcp(self._extra.get("gcp_audience"))
        if access_type == "azure_ad":
            return cid.generateAzure(self._extra.get("azure_object_id"))
        raise ValueError(f"No cloud-id generator for {access_type!r}")

    # ------------------------------------------------------------------
    # Secret operations — thin delegates to the SDK
    # ------------------------------------------------------------------

    def get_secret_value(self, name: str) -> str | None:
        """Get a static secret value by path."""
        token = self.authenticate()
        res = self.client.get_secret_value(akeyless.GetSecretValue(names=[name], token=token))
        return res.get(name)

    def get_secret_values(self, names: list[str]) -> dict[str, str]:
        """Get multiple static secret values."""
        token = self.authenticate()
        return dict(self.client.get_secret_value(akeyless.GetSecretValue(names=names, token=token)))

    def create_secret(self, name: str, value: str, description: str | None = None) -> None:
        """Create a static secret."""
        token = self.authenticate()
        body = akeyless.CreateSecret(name=name, value=value, token=token)
        if description:
            body.description = description
        self.client.create_secret(body)

    def update_secret_value(self, name: str, value: str) -> None:
        """Update a static secret's value."""
        token = self.authenticate()
        self.client.update_secret_val(akeyless.UpdateSecretVal(name=name, value=value, token=token))

    def delete_item(self, name: str) -> None:
        """Delete a secret/item."""
        token = self.authenticate()
        self.client.delete_item(akeyless.DeleteItem(name=name, token=token))

    def describe_item(self, name: str) -> dict[str, Any] | None:
        """Describe a secret/item (returns metadata)."""
        token = self.authenticate()
        return self.client.describe_item(akeyless.DescribeItem(name=name, token=token)).to_dict()

    def list_items(self, path: str = "/") -> list[dict[str, Any]]:
        """List items under a path."""
        token = self.authenticate()
        res = self.client.list_items(akeyless.ListItems(token=token, path=path))
        return [item.to_dict() for item in (res.items or [])]

    def get_dynamic_secret_value(self, name: str) -> dict[str, Any]:
        """Generate a dynamic secret value (e.g. DB credentials)."""
        token = self.authenticate()
        res = self.client.get_dynamic_secret_value(akeyless.GetDynamicSecretValue(name=name, token=token))
        return res if isinstance(res, dict) else res.to_dict()

    def get_rotated_secret_value(self, name: str) -> dict[str, Any]:
        """Retrieve a rotated secret value."""
        token = self.authenticate()
        res = self.client.get_rotated_secret_value(akeyless.GetRotatedSecretValue(names=[name], token=token))
        return res.to_dict() if hasattr(res, "to_dict") else dict(res)

    # ------------------------------------------------------------------
    # Airflow UI
    # ------------------------------------------------------------------

    def test_connection(self) -> tuple[bool, str]:
        """Validate connectivity from the Airflow UI."""
        try:
            self.authenticate()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return widgets for the Airflow connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "access_type": StringField(
                lazy_gettext("Access type"),
                widget=BS3TextFieldWidget(),
                description="One of: " + ", ".join(VALID_AUTH_TYPES),
            ),
            "uid_token": StringField(lazy_gettext("UID Token"), widget=BS3TextFieldWidget()),
            "gcp_audience": StringField(lazy_gettext("GCP Audience"), widget=BS3TextFieldWidget()),
            "azure_object_id": StringField(lazy_gettext("Azure Object ID"), widget=BS3TextFieldWidget()),
            "jwt": StringField(lazy_gettext("JWT"), widget=BS3TextFieldWidget()),
            "k8s_auth_config_name": StringField(
                lazy_gettext("K8s Auth Config Name"), widget=BS3TextFieldWidget()
            ),
            "certificate_data": StringField(
                lazy_gettext("Certificate Data (PEM)"), widget=BS3TextFieldWidget()
            ),
            "private_key_data": StringField(
                lazy_gettext("Private Key Data (PEM)"), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour for the Airflow UI."""
        return {
            "hidden_fields": ["extra", "schema", "port"],
            "relabeling": {"login": "Access ID", "password": "Access Key", "host": "API URL"},
        }
