# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Hook for Akeyless Vault Platform."""

from __future__ import annotations

from typing import Any

from airflow.hooks.base import BaseHook
from airflow.providers.akeyless._internal_client.akeyless_client import (
    VALID_AUTH_TYPES,
    AkeylessClientError,
    _AkeylessClient,
)


class AkeylessHook(BaseHook):
    """Hook to interact with the Akeyless Vault Platform.

    Akeyless documentation:
      https://docs.akeyless.io/

    Connection parameters are read from an Airflow Connection whose
    ``conn_type`` is ``akeyless``.  The mapping is:

    * **Host**  -> Akeyless API URL (default ``https://api.akeyless.io``)
    * **Login** -> ``access_id``
    * **Password** -> ``access_key`` (for ``api_key`` auth) or token/secret
    * **Extra** fields (JSON):
        - ``access_type`` (default ``api_key``)
        - ``uid_token``
        - ``gcp_audience``
        - ``azure_object_id``
        - ``jwt``
        - ``k8s_auth_config_name``
        - ``certificate_data``
        - ``private_key_data``

    :param akeyless_conn_id: Airflow connection ID.
    :param access_type: Override the auth type stored in the connection.
    """

    conn_name_attr = "akeyless_conn_id"
    default_conn_name = "akeyless_default"
    conn_type = "akeyless"
    hook_name = "Akeyless"

    def __init__(
        self,
        akeyless_conn_id: str = default_conn_name,
        access_type: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.akeyless_conn_id = akeyless_conn_id
        self.connection = self.get_connection(akeyless_conn_id)

        extra = self.connection.extra_dejson or {}

        resolved_access_type = access_type or extra.get("access_type", "api_key")

        api_url = self.connection.host or extra.get("api_url")
        if api_url and not api_url.startswith("http"):
            api_url = f"https://{api_url}"

        self._client = _AkeylessClient(
            api_url=api_url,
            access_id=self.connection.login,
            access_key=self.connection.password,
            access_type=resolved_access_type,
            uid_token=extra.get("uid_token"),
            gcp_audience=extra.get("gcp_audience"),
            azure_object_id=extra.get("azure_object_id"),
            jwt=extra.get("jwt"),
            k8s_auth_config_name=extra.get("k8s_auth_config_name"),
            certificate_data=extra.get("certificate_data"),
            private_key_data=extra.get("private_key_data"),
            **kwargs,
        )

    def get_conn(self) -> _AkeylessClient:
        """Return the authenticated internal client."""
        return self._client

    def test_connection(self) -> tuple[bool, str]:
        """Validate connectivity from the Airflow UI."""
        try:
            self._client.authenticate()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)

    def get_secret_value(self, secret_path: str) -> str | None:
        """Get a single static-secret value.

        :param secret_path: Full path to the secret in Akeyless.
        :returns: The secret value or *None* if not found.
        """
        return self._client.get_secret_value(secret_path)

    def get_secret_values(self, secret_paths: list[str]) -> dict[str, str]:
        """Get multiple static-secret values at once.

        :param secret_paths: List of full secret paths.
        :returns: Mapping ``{path: value}``.
        """
        return self._client.get_secret_values(secret_paths)

    def create_secret(
        self,
        secret_path: str,
        value: str,
        description: str | None = None,
    ) -> None:
        """Create a new static secret.

        :param secret_path: Full path for the new secret.
        :param value: The secret value.
        :param description: Optional description.
        """
        self._client.create_secret(secret_path, value, description=description)

    def update_secret_value(self, secret_path: str, value: str) -> None:
        """Update the value of an existing static secret.

        :param secret_path: Full path to the secret.
        :param value: New value.
        """
        self._client.update_secret_value(secret_path, value)

    def describe_item(self, name: str) -> dict[str, Any] | None:
        """Describe/inspect a secret or item.

        :param name: Full path to the item.
        :returns: Item metadata dict, or *None* if not found.
        """
        return self._client.describe_item(name)

    def list_items(self, path: str = "/") -> list[dict[str, Any]]:
        """List items under a given path.

        :param path: Path prefix (default ``/``).
        :returns: List of item metadata dicts.
        """
        return self._client.list_items(path)

    def delete_item(self, name: str) -> None:
        """Delete a secret/item.

        :param name: Full path to the item.
        """
        self._client.delete_item(name)

    def get_rotated_secret_value(self, name: str) -> dict[str, Any] | None:
        """Retrieve a rotated-secret value.

        :param name: Full path to the rotated secret.
        :returns: The secret payload or *None*.
        """
        return self._client.get_rotated_secret_value(name)

    def get_dynamic_secret_value(self, name: str) -> dict[str, Any] | None:
        """Generate a dynamic-secret value.

        :param name: Full path to the dynamic secret producer.
        :returns: The generated credentials or *None*.
        """
        return self._client.get_dynamic_secret_value(name)

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
            "relabeling": {
                "login": "Access ID",
                "password": "Access Key",
                "host": "API URL",
            },
        }
