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
"""Internal client for Akeyless Vault Platform.

Shared authentication and secret-access logic used by both
AkeylessHook and AkeylessBackend.  This module should **not** be
imported directly in DAGs.
"""

from __future__ import annotations

import os
from functools import cached_property
from typing import Any

import akeyless

from airflow.utils.log.logging_mixin import LoggingMixin

DEFAULT_API_URL = "https://api.akeyless.io"

VALID_AUTH_TYPES: list[str] = [
    "api_key",
    "aws_iam",
    "gcp",
    "azure_ad",
    "uid",
    "jwt",
    "k8s",
    "certificate",
]


class AkeylessClientError(Exception):
    """Raised when an Akeyless API call fails."""


class _AkeylessClient(LoggingMixin):
    """Authenticated wrapper around the Akeyless Python SDK.

    :param api_url: Akeyless API gateway URL.  Defaults to the SaaS endpoint.
    :param access_id: Access ID used by most auth methods.
    :param access_key: Access Key (for ``api_key`` auth type).
    :param access_type: Authentication type.  One of ``VALID_AUTH_TYPES``.
    :param uid_token: Universal-Identity token (for ``uid`` auth type).
    :param gcp_audience: GCP audience (for ``gcp`` auth type).
    :param azure_object_id: Azure AD Object ID (for ``azure_ad`` auth type).
    :param jwt: Raw JWT string (for ``jwt`` auth type).
    :param k8s_auth_config_name: Kubernetes auth-config name (for ``k8s`` auth type).
    :param certificate_data: PEM-encoded client cert (for ``certificate`` auth type).
    :param private_key_data: PEM-encoded client key  (for ``certificate`` auth type).
    """

    def __init__(
        self,
        api_url: str | None = None,
        access_id: str | None = None,
        access_key: str | None = None,
        access_type: str = "api_key",
        uid_token: str | None = None,
        gcp_audience: str | None = None,
        azure_object_id: str | None = None,
        jwt: str | None = None,
        k8s_auth_config_name: str | None = None,
        certificate_data: str | None = None,
        private_key_data: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()

        if access_type not in VALID_AUTH_TYPES:
            raise AkeylessClientError(
                f"Unsupported access_type: {access_type!r}. "
                f"Must be one of {VALID_AUTH_TYPES}"
            )

        self.api_url = api_url or os.getenv("AKEYLESS_API_URL", DEFAULT_API_URL)
        self.access_id = access_id or os.getenv("AKEYLESS_ACCESS_ID")
        self.access_key = access_key or os.getenv("AKEYLESS_ACCESS_KEY")
        self.access_type = access_type
        self.uid_token = uid_token
        self.gcp_audience = gcp_audience
        self.azure_object_id = azure_object_id
        self.jwt = jwt
        self.k8s_auth_config_name = k8s_auth_config_name
        self.certificate_data = certificate_data
        self.private_key_data = private_key_data
        self.kwargs = kwargs

        self._validate_auth_params()

    def _validate_auth_params(self) -> None:
        t = self.access_type
        if t == "api_key":
            if not self.access_id or not self.access_key:
                raise AkeylessClientError(
                    "'api_key' auth requires both 'access_id' and 'access_key'"
                )
        elif t in ("aws_iam", "gcp", "azure_ad", "k8s"):
            if not self.access_id:
                raise AkeylessClientError(f"'{t}' auth requires 'access_id'")
        elif t == "uid":
            if not self.uid_token:
                raise AkeylessClientError("'uid' auth requires 'uid_token'")
        elif t == "jwt":
            if not self.access_id:
                raise AkeylessClientError("'jwt' auth requires 'access_id'")
            if not self.jwt:
                raise AkeylessClientError("'jwt' auth requires 'jwt'")
        elif t == "certificate":
            if not self.access_id:
                raise AkeylessClientError("'certificate' auth requires 'access_id'")
            if not self.certificate_data or not self.private_key_data:
                raise AkeylessClientError(
                    "'certificate' auth requires 'certificate_data' and 'private_key_data'"
                )

    @cached_property
    def _api(self) -> akeyless.V2Api:
        configuration = akeyless.Configuration(host=self.api_url)
        api_client = akeyless.ApiClient(configuration)
        return akeyless.V2Api(api_client)

    @property
    def api(self) -> akeyless.V2Api:
        """Return the underlying V2Api, authenticating on first access."""
        return self._api

    def _get_cloud_id(self) -> str:
        """Generate a Cloud ID token for AWS/GCP/Azure auth."""
        try:
            from akeyless_cloud_id import CloudId
        except ImportError:
            raise AkeylessClientError(
                "Cloud-based auth requires the 'akeyless_cloud_id' package. "
                "Install it with: pip install akeyless_cloud_id"
            )

        cid = CloudId()
        if self.access_type == "aws_iam":
            return cid.generate()
        elif self.access_type == "gcp":
            return cid.generateGcp(self.gcp_audience)
        elif self.access_type == "azure_ad":
            return cid.generateAzure(self.azure_object_id)
        raise AkeylessClientError(f"No cloud-id generator for {self.access_type!r}")

    def authenticate(self) -> str:
        """Perform authentication and return an Akeyless API token string."""
        t = self.access_type

        if t == "uid":
            return self.uid_token  # type: ignore[return-value]

        auth_body = akeyless.Auth(access_id=self.access_id)

        if t == "api_key":
            auth_body.access_key = self.access_key
        elif t in ("aws_iam", "gcp", "azure_ad"):
            auth_body.access_type = t
            auth_body.cloud_id = self._get_cloud_id()
        elif t == "jwt":
            auth_body.access_type = "jwt"
            auth_body.jwt = self.jwt
        elif t == "k8s":
            auth_body.access_type = "k8s"
            auth_body.k8s_auth_config_name = self.k8s_auth_config_name
        elif t == "certificate":
            auth_body.access_type = "cert"
            auth_body.cert_data = self.certificate_data
            auth_body.key_data = self.private_key_data

        try:
            res = self._api.auth(auth_body)
            return res.token
        except akeyless.ApiException as exc:
            raise AkeylessClientError(f"Akeyless authentication failed: {exc}") from exc

    def get_secret_value(self, name: str, token: str | None = None) -> str | None:
        """Retrieve a single static-secret value by *name*.

        :param name: Full path of the secret in Akeyless (e.g. ``/path/to/secret``).
        :param token: API token.  If *None*, ``authenticate()`` is called first.
        :returns: The secret value as a string, or *None* if not found.
        """
        token = token or self.authenticate()
        body_kwargs: dict[str, Any] = {"names": [name]}
        if self.access_type == "uid":
            body_kwargs["uid_token"] = token
        else:
            body_kwargs["token"] = token

        try:
            body = akeyless.GetSecretValue(**body_kwargs)
            res = self._api.get_secret_value(body)
            return res.get(name)
        except akeyless.ApiException as exc:
            if exc.status == 404:
                self.log.debug("Secret not found: %s", name)
                return None
            raise AkeylessClientError(
                f"Failed to get secret '{name}': {exc}"
            ) from exc

    def get_secret_values(
        self, names: list[str], token: str | None = None
    ) -> dict[str, str]:
        """Retrieve multiple secret values at once.

        :returns: Mapping of secret-name -> value.
        """
        token = token or self.authenticate()
        body_kwargs: dict[str, Any] = {"names": names}
        if self.access_type == "uid":
            body_kwargs["uid_token"] = token
        else:
            body_kwargs["token"] = token

        try:
            body = akeyless.GetSecretValue(**body_kwargs)
            return dict(self._api.get_secret_value(body))
        except akeyless.ApiException as exc:
            raise AkeylessClientError(
                f"Failed to get secrets {names}: {exc}"
            ) from exc

    def create_secret(
        self,
        name: str,
        value: str,
        description: str | None = None,
        token: str | None = None,
    ) -> None:
        """Create a new static secret.

        :param name: Full path for the new secret.
        :param value: Secret value.
        :param description: Optional description.
        :param token: API token.  If *None*, ``authenticate()`` is called first.
        """
        token = token or self.authenticate()
        body = akeyless.CreateSecret(name=name, value=value, token=token)
        if description:
            body.description = description
        try:
            self._api.create_secret(body)
        except akeyless.ApiException as exc:
            raise AkeylessClientError(
                f"Failed to create secret '{name}': {exc}"
            ) from exc

    def update_secret_value(
        self,
        name: str,
        value: str,
        token: str | None = None,
    ) -> None:
        """Update an existing static secret's value.

        :param name: Full path to the secret.
        :param value: New value.
        :param token: API token.  If *None*, ``authenticate()`` is called first.
        """
        token = token or self.authenticate()
        body = akeyless.UpdateSecretVal(name=name, value=value, token=token)
        try:
            self._api.update_secret_val(body)
        except akeyless.ApiException as exc:
            raise AkeylessClientError(
                f"Failed to update secret '{name}': {exc}"
            ) from exc

    def describe_item(
        self,
        name: str,
        token: str | None = None,
    ) -> dict[str, Any] | None:
        """Describe/inspect a secret or item by *name*.

        :returns: Item metadata dict, or *None* if not found.
        """
        token = token or self.authenticate()
        body = akeyless.DescribeItem(name=name, token=token)
        try:
            res = self._api.describe_item(body)
            return res.to_dict()
        except akeyless.ApiException as exc:
            if exc.status == 404:
                self.log.debug("Item not found: %s", name)
                return None
            raise AkeylessClientError(
                f"Failed to describe item '{name}': {exc}"
            ) from exc

    def list_items(
        self,
        path: str = "/",
        token: str | None = None,
    ) -> list[dict[str, Any]]:
        """List secrets/items under *path*.

        :returns: List of item metadata dicts.
        """
        token = token or self.authenticate()
        body = akeyless.ListItems(token=token, path=path)
        try:
            res = self._api.list_items(body)
            return [item.to_dict() for item in (res.items or [])]
        except akeyless.ApiException as exc:
            raise AkeylessClientError(
                f"Failed to list items at '{path}': {exc}"
            ) from exc

    def delete_item(
        self,
        name: str,
        token: str | None = None,
    ) -> None:
        """Delete a secret/item by *name*."""
        token = token or self.authenticate()
        body = akeyless.DeleteItem(name=name, token=token)
        try:
            self._api.delete_item(body)
        except akeyless.ApiException as exc:
            raise AkeylessClientError(
                f"Failed to delete item '{name}': {exc}"
            ) from exc

    def get_rotated_secret_value(
        self,
        names: str,
        token: str | None = None,
    ) -> dict[str, Any] | None:
        """Retrieve a rotated-secret value.

        :param names: Secret name.
        :returns: The secret payload dict, or *None* on 404.
        """
        token = token or self.authenticate()
        body = akeyless.GetRotatedSecretValue(names=names, token=token)
        try:
            res = self._api.get_rotated_secret_value(body)
            return res.to_dict() if hasattr(res, "to_dict") else dict(res)
        except akeyless.ApiException as exc:
            if exc.status == 404:
                self.log.debug("Rotated secret not found: %s", names)
                return None
            raise AkeylessClientError(
                f"Failed to get rotated secret '{names}': {exc}"
            ) from exc

    def get_dynamic_secret_value(
        self,
        name: str,
        token: str | None = None,
    ) -> dict[str, Any] | None:
        """Generate a dynamic-secret value.

        :param name: Dynamic secret name.
        :returns: The generated credentials dict, or *None* on 404.
        """
        token = token or self.authenticate()
        body = akeyless.GetDynamicSecretValue(name=name, token=token)
        try:
            res = self._api.get_dynamic_secret_value(body)
            return res if isinstance(res, dict) else res.to_dict()
        except akeyless.ApiException as exc:
            if exc.status == 404:
                self.log.debug("Dynamic secret not found: %s", name)
                return None
            raise AkeylessClientError(
                f"Failed to get dynamic secret '{name}': {exc}"
            ) from exc
