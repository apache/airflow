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

from collections.abc import Callable
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, BaseHook

if TYPE_CHECKING:
    from airflow.sdk import Connection

    OciSigner = Any

OciClient = TypeVar("OciClient")

OCI_AUTH_TYPE_API_KEY = "api_key"
OCI_AUTH_TYPE_CONFIG_FILE = "config_file"
OCI_AUTH_TYPE_INSTANCE_PRINCIPAL = "instance_principal"
OCI_AUTH_TYPE_RESOURCE_PRINCIPAL = "resource_principal"
OCI_AUTH_TYPES = (
    OCI_AUTH_TYPE_API_KEY,
    OCI_AUTH_TYPE_CONFIG_FILE,
    OCI_AUTH_TYPE_INSTANCE_PRINCIPAL,
    OCI_AUTH_TYPE_RESOURCE_PRINCIPAL,
)


def _get_oci_sdk() -> Any:
    try:
        import oci
    except ImportError as e:
        raise AirflowOptionalProviderFeatureException(
            "OCI features require the optional OCI Python SDK. "
            "Install it with: pip install 'apache-airflow-providers-oracle[oci]'"
        ) from e
    return oci


class OciBaseHook(BaseHook, Generic[OciClient]):
    """
    Base hook for Oracle Cloud Infrastructure services.

    The hook supports API key, OCI configuration file, instance principal, and resource principal
    authentication. API key credentials are read from the connection fields, while principal
    authentication is delegated to the OCI SDK.

    :param oci_conn_id: The :ref:`OCI connection id <howto/connection:oci>`.
    :param auth_type: OCI authentication type selected by the Dag author.
    :param key_file: API signing private key path selected by the Dag author.
    :param config_file: OCI SDK configuration file selected by the Dag author.
    :param profile: Profile to load from the OCI SDK configuration file.
    :param service_endpoint: Optional service endpoint selected by the Dag author.
    """

    conn_name_attr = "oci_conn_id"
    default_conn_name = "oci_default"
    conn_type = "oci"
    hook_name = "Oracle Cloud Infrastructure"
    client_class: Callable[..., OciClient] | None = None

    def __init__(
        self,
        oci_conn_id: str = default_conn_name,
        *,
        auth_type: str = OCI_AUTH_TYPE_API_KEY,
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
            "key_content": PasswordField(
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
        return self.get_connection(self.oci_conn_id)

    def get_oci_config(self) -> tuple[dict[str, Any], OciSigner | None]:
        """Build OCI SDK configuration and an optional signer from the Airflow connection."""
        oci = _get_oci_sdk()
        conn = self.connection
        extras = conn.extra_dejson
        auth_type = self.auth_type

        if auth_type == OCI_AUTH_TYPE_CONFIG_FILE:
            config = oci.config.from_file(
                file_location=self.config_file or oci.config.DEFAULT_LOCATION,
                profile_name=self.profile or oci.config.DEFAULT_PROFILE,
            )
            if region := extras.get("region"):
                config["region"] = region
            return config, None

        if auth_type == OCI_AUTH_TYPE_INSTANCE_PRINCIPAL:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return self._build_principal_config(extras, signer), signer

        if auth_type == OCI_AUTH_TYPE_RESOURCE_PRINCIPAL:
            signer = oci.auth.signers.get_resource_principals_signer()
            return self._build_principal_config(extras, signer), signer

        if auth_type != OCI_AUTH_TYPE_API_KEY:
            raise ValueError(
                f"Unsupported OCI authentication type: {auth_type!r}. Expected one of {OCI_AUTH_TYPES}."
            )

        config = {
            "tenancy": extras.get("tenancy"),
            "user": conn.login,
            "fingerprint": extras.get("fingerprint"),
            "region": extras.get("region"),
            "pass_phrase": conn.password,
        }
        key_file = self.key_file
        key_content = extras.get("key_content")
        if key_file and key_content:
            raise ValueError("OCI API key authentication cannot use both 'key_file' and 'key_content'.")
        if not key_file and not key_content:
            raise ValueError("OCI API key authentication requires either 'key_file' or 'key_content'.")
        if key_file:
            config["key_file"] = key_file
        else:
            config["key_content"] = key_content
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
        resolved_compartment_id = compartment_id or self.connection.extra_dejson.get("compartment_id")
        if not resolved_compartment_id:
            raise ValueError(
                "An OCI compartment OCID must be provided as a method argument or in the connection extra."
            )
        return resolved_compartment_id

    @staticmethod
    def _build_principal_config(extras: dict[str, Any], signer: OciSigner) -> dict[str, Any]:
        region = extras.get("region") or getattr(signer, "region", None)
        return {"region": region} if region else {}

    def _get_service_endpoint(self) -> str | None:
        return self.service_endpoint.rstrip("/") if self.service_endpoint else None

    def _get_client_class(self) -> Callable[..., OciClient]:
        if self.client_class is None:
            raise ValueError("client_class must be specified by an OCI service hook.")
        return self.client_class
