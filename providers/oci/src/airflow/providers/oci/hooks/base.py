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

import re
from collections.abc import Callable
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from airflow.providers.common.compat.sdk import (
    AirflowNotFoundException,
    AirflowOptionalProviderFeatureException,
    BaseHook,
)

if TYPE_CHECKING:
    from airflow.sdk import Connection

    OciSigner = Any

OciClient = TypeVar("OciClient")

OCI_REGION_IDENTIFIER_PATTERN = re.compile(r"[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?")


class OciAuthType(str, Enum):
    """Authentication types supported by OCI hooks."""

    API_KEY = "api_key"
    CONFIG_FILE = "config_file"
    INSTANCE_PRINCIPAL = "instance_principal"
    RESOURCE_PRINCIPAL = "resource_principal"


def _get_oci_sdk() -> Any:
    try:
        import oci
    except ImportError as e:
        raise AirflowOptionalProviderFeatureException(
            "OCI features require the optional OCI Python SDK. "
            "Install it with: pip install 'apache-airflow-providers-oci[oci]'"
        ) from e
    return oci


class OciBaseHook(BaseHook, Generic[OciClient]):
    """
    Base hook for Oracle Cloud Infrastructure services.

    The hook supports API key, OCI configuration file, instance principal, and resource principal
    authentication. API key credentials are read from the connection fields, while principal
    authentication is delegated to the OCI SDK.

    :param oci_conn_id: The :ref:`OCI connection id <howto/connection:oci>`.
        Defaults to ``oci_default``.
        Pass ``None`` to skip connection lookup for authentication methods that do not require it.
    :param auth_type: OCI authentication type selected by the Dag author.
        Defaults to ``api_key``.
    :param key_file: API signing private key path selected by the Dag author.
        Defaults to ``None``.
    :param config_file: OCI SDK configuration file selected by the Dag author.
        If not specified, the OCI SDK default location, ``~/.oci/config``, is used for
        configuration file authentication.
    :param profile: Profile to load from the OCI SDK configuration file.
        If not specified, the OCI SDK default profile, ``DEFAULT``, is used.
    :param service_endpoint: Optional service endpoint selected by the Dag author.
        If not specified, the OCI SDK derives the endpoint from the configured region.
    """

    conn_name_attr = "oci_conn_id"
    default_conn_name = "oci_default"
    conn_type = "oci"
    hook_name = "Oracle Cloud Infrastructure"

    def __init__(
        self,
        oci_conn_id: str | None = default_conn_name,
        *,
        auth_type: str = OciAuthType.API_KEY,
        key_file: str | None = None,
        config_file: str | None = None,
        profile: str | None = None,
        service_endpoint: str | None = None,
    ) -> None:
        super().__init__()
        self.oci_conn_id = oci_conn_id
        self.auth_type = auth_type
        self.key_file = key_file
        self.config_file = config_file
        self.profile = profile
        self.service_endpoint = service_endpoint

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to the connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "tenancy": StringField(lazy_gettext("Tenancy OCID"), widget=BS3TextFieldWidget()),
            "fingerprint": StringField(lazy_gettext("Key Fingerprint"), widget=BS3TextFieldWidget()),
            "private_key_content": PasswordField(
                lazy_gettext("Private Key Content"), widget=BS3PasswordFieldWidget()
            ),
            "region": StringField(lazy_gettext("Region"), widget=BS3TextFieldWidget()),
            "compartment_id": StringField(lazy_gettext("Compartment OCID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behavior for the connection form."""
        return {
            "hidden_fields": ["host", "schema", "port"],
            "relabeling": {
                "login": "User OCID",
                "password": "Private Key Passphrase",
            },
            "placeholders": {
                "login": "ocid1.user...",
                "password": "Optional API key passphrase",
                "tenancy": "ocid1.tenancy...",
                "fingerprint": "aa:bb:cc:...",
                "region": "us-chicago-1",
                "compartment_id": "ocid1.compartment...",
            },
        }

    @cached_property
    def connection(self) -> Connection:
        """Return the configured Airflow connection."""
        if not self.oci_conn_id:
            raise ValueError("An OCI connection ID is required for API key authentication.")
        return self.get_connection(self.oci_conn_id)

    def get_oci_config(self) -> tuple[dict[str, Any], OciSigner | None]:
        """Build OCI SDK configuration and an optional signer for the selected authentication type."""
        oci = _get_oci_sdk()
        auth_type = self.auth_type

        if auth_type == OciAuthType.CONFIG_FILE:
            extras = self._get_optional_connection_extras()
            config = oci.config.from_file(
                file_location=self.config_file or oci.config.DEFAULT_LOCATION,
                profile_name=self.profile or oci.config.DEFAULT_PROFILE,
            )
            if region := self._get_connection_region(extras):
                config["region"] = region
            return config, None

        if auth_type == OciAuthType.INSTANCE_PRINCIPAL:
            extras = self._get_optional_connection_extras()
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return self._build_principal_config(extras, signer), signer

        if auth_type == OciAuthType.RESOURCE_PRINCIPAL:
            extras = self._get_optional_connection_extras()
            signer = oci.auth.signers.get_resource_principals_signer()
            return self._build_principal_config(extras, signer), signer

        if auth_type != OciAuthType.API_KEY:
            raise ValueError(
                f"Unsupported OCI authentication type: {auth_type!r}. "
                f"Expected one of {tuple(auth_type.value for auth_type in OciAuthType)}."
            )

        conn = self.connection
        extras = conn.extra_dejson
        config = {
            "tenancy": extras.get("tenancy"),
            "user": conn.login,
            "fingerprint": extras.get("fingerprint"),
            "region": self._get_connection_region(extras),
            "pass_phrase": conn.password,
        }
        key_file = self.key_file
        private_key_content = extras.get("private_key_content")
        if key_file and private_key_content:
            raise ValueError(
                "OCI API key authentication cannot use both 'key_file' and 'private_key_content'."
            )
        if not key_file and not private_key_content:
            raise ValueError(
                "OCI API key authentication requires either 'key_file' or 'private_key_content'."
            )
        if key_file:
            config["key_file"] = key_file
        else:
            config["key_content"] = private_key_content
        return config, None

    def get_client(self, client_class: Callable[..., OciClient], **client_kwargs: Any) -> OciClient:
        """Return an authenticated OCI SDK client."""
        config, signer = self.get_oci_config()
        if signer is not None:
            client_kwargs["signer"] = signer
        if service_endpoint := self._get_service_endpoint():
            client_kwargs["service_endpoint"] = service_endpoint
        return client_class(config=config, **client_kwargs)

    @cached_property
    def conn(self) -> OciClient:
        """Return the configured OCI SDK client."""
        return self.get_client(self._get_client_class())

    def get_conn(self) -> OciClient:
        """Return the cached OCI SDK client."""
        return self.conn

    def test_connection(self) -> tuple[bool, str]:
        """Test OCI credentials against the Identity service."""
        try:
            oci = _get_oci_sdk()
            config, signer = self.get_oci_config()
            client_kwargs = {"signer": signer} if signer is not None else {}
            oci.identity.IdentityClient(config=config, **client_kwargs).list_regions()
        except Exception as e:
            return False, f"{type(e).__name__} error occurred while testing connection: {e}"
        return True, "Connection successfully tested"

    def get_compartment_id(self, compartment_id: str | None = None) -> str:
        """Return an explicit compartment OCID or the default from the connection."""
        resolved_compartment_id = compartment_id or self._get_optional_connection_extras().get(
            "compartment_id"
        )
        if not resolved_compartment_id:
            raise ValueError(
                "An OCI compartment OCID must be provided as a method argument or in the connection extra."
            )
        return resolved_compartment_id

    @classmethod
    def _build_principal_config(cls, extras: dict[str, Any], signer: OciSigner) -> dict[str, Any]:
        region = cls._get_connection_region(extras) or getattr(signer, "region", None)
        return {"region": region} if region else {}

    @staticmethod
    def _get_connection_region(extras: dict[str, Any]) -> str | None:
        region = extras.get("region")
        if region in (None, ""):
            return None
        if not isinstance(region, str) or OCI_REGION_IDENTIFIER_PATTERN.fullmatch(region) is None:
            raise ValueError(
                "The OCI connection region must be a region identifier containing only ASCII letters, "
                "digits, and hyphens. Dag authors can use service_endpoint for custom endpoints."
            )
        return region

    def _get_optional_connection_extras(self) -> dict[str, Any]:
        if not self.oci_conn_id:
            return {}
        try:
            return self.connection.extra_dejson
        except AirflowNotFoundException:
            self.log.warning(
                "Unable to find OCI Connection ID '%s'; continuing without connection defaults.",
                self.oci_conn_id,
            )
            return {}

    def _get_service_endpoint(self) -> str | None:
        return self.service_endpoint.rstrip("/") if self.service_endpoint else None

    def _get_client_class(self) -> Callable[..., OciClient]:
        raise NotImplementedError("OCI service hooks must implement _get_client_class().")
