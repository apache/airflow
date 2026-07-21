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

import subprocess
import sys
from unittest import mock

import pytest
from oci.generative_ai import GenerativeAiClient

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.oracle.get_provider_info import get_provider_info
from airflow.providers.oracle.hooks.base_oci import (
    OCI_AUTH_TYPE_CONFIG_FILE,
    OCI_AUTH_TYPE_INSTANCE_PRINCIPAL,
    OCI_AUTH_TYPE_RESOURCE_PRINCIPAL,
    OciBaseHook,
    _get_oci_sdk,
)


class TestOciBaseHook:
    def setup_method(self):
        self.hook = OciBaseHook()

    def set_connection(self, connection: Connection) -> None:
        self.hook.get_connection = mock.create_autospec(self.hook.get_connection, return_value=connection)

    @pytest.mark.parametrize(
        ("hook_kwargs", "connection_extra", "key_field", "key_value"),
        [
            ({"key_file": "/keys/oci.pem"}, {}, "key_file", "/keys/oci.pem"),
            ({}, {"key_content": "private-key-content"}, "key_content", "private-key-content"),
        ],
    )
    def test_get_oci_config_with_api_key(self, hook_kwargs, connection_extra, key_field, key_value):
        self.hook = OciBaseHook(**hook_kwargs)
        self.set_connection(
            Connection(
                login="ocid1.user.test",
                password="passphrase",
                extra={
                    "tenancy": "ocid1.tenancy.test",
                    "fingerprint": "fingerprint",
                    "region": "us-chicago-1",
                    **connection_extra,
                },
            )
        )

        config, signer = self.hook.get_oci_config()

        assert config == {
            "tenancy": "ocid1.tenancy.test",
            "user": "ocid1.user.test",
            "fingerprint": "fingerprint",
            "region": "us-chicago-1",
            "pass_phrase": "passphrase",
            key_field: key_value,
        }
        assert signer is None

    def test_connection_extra_cannot_control_hook_configuration(self):
        self.set_connection(
            Connection(
                login="ocid1.user.test",
                extra={
                    "auth_type": OCI_AUTH_TYPE_INSTANCE_PRINCIPAL,
                    "key_file": "/etc/hosts",
                    "config_file": "/etc/hosts",
                    "profile": "UNTRUSTED",
                    "service_endpoint": "https://untrusted.example.test",
                    "tenancy": "ocid1.tenancy.test",
                    "fingerprint": "fingerprint",
                    "region": "us-chicago-1",
                    "key_content": "private-key-content",
                },
            )
        )

        config, signer = self.hook.get_oci_config()

        assert config == {
            "tenancy": "ocid1.tenancy.test",
            "user": "ocid1.user.test",
            "fingerprint": "fingerprint",
            "region": "us-chicago-1",
            "pass_phrase": None,
            "key_content": "private-key-content",
        }
        assert signer is None

    @pytest.mark.parametrize(
        ("hook_kwargs", "extra", "error_message"),
        [
            (
                {},
                {},
                "OCI API key authentication requires either 'key_file' or 'key_content'",
            ),
            (
                {"key_file": "/keys/oci.pem"},
                {"key_content": "private-key-content"},
                "OCI API key authentication cannot use both 'key_file' and 'key_content'",
            ),
        ],
        ids=["missing-key", "conflicting-keys"],
    )
    def test_get_oci_config_rejects_invalid_api_key_material(self, hook_kwargs, extra, error_message):
        self.hook = OciBaseHook(**hook_kwargs)
        self.set_connection(Connection(extra=extra))

        with pytest.raises(ValueError, match=error_message):
            self.hook.get_oci_config()

    @mock.patch("oci.config.from_file", autospec=True)
    def test_get_oci_config_from_file_with_region_override(self, mock_from_file):
        self.hook = OciBaseHook(
            auth_type=OCI_AUTH_TYPE_CONFIG_FILE,
            config_file="/config/oci",
            profile="AIRFLOW",
        )
        mock_from_file.return_value = {"region": "us-ashburn-1"}
        self.set_connection(Connection(extra={"region": "eu-frankfurt-1"}))

        config, signer = self.hook.get_oci_config()

        mock_from_file.assert_called_once_with(
            file_location="/config/oci",
            profile_name="AIRFLOW",
        )
        assert config == {"region": "eu-frankfurt-1"}
        assert signer is None

    @pytest.mark.parametrize(
        ("config_file", "profile"),
        [
            (None, None),
            ("", ""),
        ],
    )
    @mock.patch("oci.config.from_file", autospec=True)
    def test_get_oci_config_from_default_file(self, mock_from_file, config_file, profile):
        self.hook = OciBaseHook(
            auth_type=OCI_AUTH_TYPE_CONFIG_FILE,
            config_file=config_file,
            profile=profile,
        )
        mock_from_file.return_value = {"region": "us-ashburn-1"}
        self.set_connection(Connection())

        config, signer = self.hook.get_oci_config()

        mock_from_file.assert_called_once_with(
            file_location="~/.oci/config",
            profile_name="DEFAULT",
        )
        assert config == {"region": "us-ashburn-1"}
        assert signer is None

    @mock.patch(
        "oci.auth.signers.InstancePrincipalsSecurityTokenSigner",
        autospec=True,
    )
    def test_get_oci_config_with_instance_principal_and_connection_region(self, mock_signer_class):
        self.hook = OciBaseHook(auth_type=OCI_AUTH_TYPE_INSTANCE_PRINCIPAL)
        signer = mock_signer_class.return_value
        signer.region = "us-ashburn-1"
        self.set_connection(Connection(extra={"region": "eu-frankfurt-1"}))

        config, actual_signer = self.hook.get_oci_config()

        assert config == {"region": "eu-frankfurt-1"}
        assert actual_signer is signer

    @mock.patch(
        "oci.auth.signers.get_resource_principals_signer",
        autospec=True,
    )
    def test_get_oci_config_with_resource_principal_region(self, mock_get_signer):
        self.hook = OciBaseHook(auth_type=OCI_AUTH_TYPE_RESOURCE_PRINCIPAL)
        signer = mock_get_signer.return_value
        signer.region = "us-phoenix-1"
        self.set_connection(Connection())

        config, actual_signer = self.hook.get_oci_config()

        assert config == {"region": "us-phoenix-1"}
        assert actual_signer is signer

    @mock.patch(
        "oci.auth.signers.get_resource_principals_signer",
        autospec=True,
    )
    def test_get_oci_config_with_resource_principal_without_region(self, mock_get_signer):
        self.hook = OciBaseHook(auth_type=OCI_AUTH_TYPE_RESOURCE_PRINCIPAL)
        signer = mock_get_signer.return_value
        del signer.region
        self.set_connection(Connection())

        config, actual_signer = self.hook.get_oci_config()

        assert config == {}
        assert actual_signer is signer

    def test_get_oci_config_rejects_unknown_auth_type(self):
        self.hook = OciBaseHook(auth_type="unknown")
        self.set_connection(Connection())

        with pytest.raises(ValueError, match="Unsupported OCI authentication type: 'unknown'"):
            self.hook.get_oci_config()

    def test_get_client_with_signer_and_explicit_endpoint(self):
        signer = mock.sentinel.signer
        client = mock.sentinel.client
        client_class = mock.create_autospec(GenerativeAiClient, return_value=client)
        self.hook.get_oci_config = mock.create_autospec(
            self.hook.get_oci_config, return_value=({"region": "us-chicago-1"}, signer)
        )
        self.hook.service_endpoint = "https://generativeai.example.test/"

        result = self.hook.get_client(client_class, timeout=30)

        assert result is client
        client_class.assert_called_once_with(
            config={"region": "us-chicago-1"},
            signer=signer,
            service_endpoint="https://generativeai.example.test",
            timeout=30,
        )

    def test_get_client_without_signer_or_endpoint(self):
        client = mock.sentinel.client
        client_class = mock.create_autospec(GenerativeAiClient, return_value=client)
        self.hook.get_oci_config = mock.create_autospec(
            self.hook.get_oci_config, return_value=({"region": "us-chicago-1"}, None)
        )
        self.set_connection(Connection())

        result = self.hook.get_client(client_class)

        assert result is client
        client_class.assert_called_once_with(config={"region": "us-chicago-1"})

    def test_get_conn_requires_service_client_class(self):
        with pytest.raises(ValueError, match="client_class must be specified by an OCI service hook"):
            self.hook.get_conn()

    def test_get_client_class_returns_configured_class(self):
        self.hook.client_class = GenerativeAiClient

        assert self.hook._get_client_class() is GenerativeAiClient

    @pytest.mark.parametrize("signer", [None, mock.sentinel.signer])
    @mock.patch("oci.identity.IdentityClient", autospec=True)
    def test_connection_success(self, mock_identity_client, signer):
        config = {"region": "us-chicago-1"}
        self.hook.get_oci_config = mock.create_autospec(
            self.hook.get_oci_config, return_value=(config, signer)
        )

        result = self.hook.test_connection()

        assert result == (True, "Connection successfully tested")
        expected_kwargs = {"config": config}
        if signer is not None:
            expected_kwargs["signer"] = signer
        mock_identity_client.assert_called_once_with(**expected_kwargs)
        mock_identity_client.return_value.list_regions.assert_called_once_with()

    @mock.patch("oci.identity.IdentityClient", autospec=True)
    def test_connection_failure(self, mock_identity_client):
        self.hook.get_oci_config = mock.create_autospec(
            self.hook.get_oci_config, return_value=({"region": "us-chicago-1"}, None)
        )
        mock_identity_client.return_value.list_regions.side_effect = ValueError("invalid credentials")

        result = self.hook.test_connection()

        assert result == (False, "ValueError error occurred while testing connection: invalid credentials")

    @pytest.mark.parametrize(
        ("hook_endpoint", "expected"),
        [
            ("https://hook.test/", "https://hook.test"),
            (None, None),
        ],
    )
    def test_service_endpoint_is_controlled_by_hook_argument(self, hook_endpoint, expected):
        self.hook = OciBaseHook(service_endpoint=hook_endpoint)
        self.set_connection(
            Connection(
                host="https://connection-host.test",
                extra={"service_endpoint": "https://connection-extra.test"},
            )
        )

        assert self.hook._get_service_endpoint() == expected

    def test_get_compartment_id_prefers_explicit_value(self):
        self.set_connection(Connection(extra={"compartment_id": "connection-compartment"}))

        assert self.hook.get_compartment_id("explicit-compartment") == "explicit-compartment"

    def test_get_compartment_id_from_connection(self):
        self.set_connection(Connection(extra={"compartment_id": "connection-compartment"}))

        assert self.hook.get_compartment_id() == "connection-compartment"

    def test_get_compartment_id_requires_value(self):
        self.set_connection(Connection())

        with pytest.raises(ValueError, match="An OCI compartment OCID must be provided"):
            self.hook.get_compartment_id()

    def test_connection_form_widgets(self):
        pytest.importorskip("flask_appbuilder")
        pytest.importorskip("flask_babel")
        password_field = pytest.importorskip("wtforms").PasswordField

        widgets = self.hook.get_connection_form_widgets()

        assert set(widgets) == {
            "tenancy",
            "fingerprint",
            "key_content",
            "region",
            "compartment_id",
        }
        assert widgets["key_content"].field_class is password_field

    def test_ui_field_behaviour(self):
        assert self.hook.get_ui_field_behaviour() == {
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

    def test_declarative_placeholders_match_legacy_hook(self):
        oci_connection = next(
            connection
            for connection in get_provider_info()["connection-types"]
            if connection["connection-type"] == "oci"
        )

        assert (
            oci_connection["ui-field-behaviour"]["placeholders"]
            == self.hook.get_ui_field_behaviour()["placeholders"]
        )
        assert oci_connection["conn-fields"]["key_content"]["schema"]["format"] == "password"


def test_get_oci_sdk_requires_optional_extra():
    with mock.patch.dict(sys.modules, {"oci": None}):
        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match=r"pip install 'apache-airflow-providers-oracle\[oci\]'",
        ):
            _get_oci_sdk()


def test_hook_modules_import_without_optional_oci_sdk():
    subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys

sys.modules["oci"] = None
import airflow.providers.oracle.hooks.base_oci
import airflow.providers.oracle.hooks.generative_ai
""",
        ],
        check=True,
    )
