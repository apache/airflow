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

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from datafusion.object_store import LocalFileSystem

from airflow.providers.common.compat.sdk import Connection
from airflow.providers.common.sql.config import ConnectionConfig, DataSourceConfig, StorageType
from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException
from airflow.providers.microsoft.azure.datafusion.object_storage import (
    AzureObjectStorageProvider,
    _resolve_wasb_account,
)

MODULE = "airflow.providers.microsoft.azure.datafusion.object_storage"


def _resolve_credentials(conn: Connection) -> dict:
    with patch(f"{MODULE}.BaseHook.get_connection", autospec=True, return_value=conn):
        return AzureObjectStorageProvider()._credentials_from_connection("wasb_default")


class TestAzureObjectStorageProvider:
    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_success(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "myaccount", "access_key": "fake_key"},
        )

        store = provider.create_object_store("az://demo-container/path", connection_config)

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="myaccount", access_key="fake_key"
        )
        assert store == mock_azure.return_value
        assert provider.get_storage_type == StorageType.AZURE
        assert provider.get_scheme() == "az://"

    @pytest.mark.parametrize(
        "uri",
        [
            "abfs://demo-container@myaccount.dfs.core.windows.net/path",
            "abfss://demo-container@myaccount.dfs.core.windows.net/path",
            "abfss://demo-container@myaccount.blob.core.windows.net/path",
            "abfss://demo-container@myaccount.DFS.Core.Windows.Net/path",
        ],
    )
    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_success_with_abfs_uri(self, mock_azure, uri, monkeypatch):
        # Without these, a public-cloud host check that is too strict would still pass.
        monkeypatch.delenv("AZURE_STORAGE_ENDPOINT", raising=False)
        monkeypatch.delenv("AZURE_ENDPOINT", raising=False)
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "myaccount", "access_key": "fake_key"},
        )

        store = provider.create_object_store(uri, connection_config)

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="myaccount", access_key="fake_key"
        )
        assert store == mock_azure.return_value
        assert provider.get_bucket(uri) == "demo-container"

    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_abfs_uri_account_mismatch_raises(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "connection-account", "access_key": "fake_key"},
        )

        with pytest.raises(ObjectStoreCreationException, match="names storage account 'uri-account'"):
            provider.create_object_store(
                "abfss://demo-container@uri-account.dfs.core.windows.net/path", connection_config
            )

        mock_azure.assert_not_called()

    @pytest.mark.parametrize(
        "uri",
        [
            "abfs://demo-container/path",  # missing @account entirely
            "abfss://demo-container@/path",  # empty account
        ],
    )
    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_malformed_abfs_uri_raises_clear_error(self, mock_azure, uri):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "myaccount", "access_key": "fake_key"},
        )

        with pytest.raises(ObjectStoreCreationException, match="does not match the required"):
            provider.create_object_store(uri, connection_config)

        mock_azure.assert_not_called()

    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_abfs_uri_account_used_when_connection_resolves_none(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"access_key": "fake_key"},
        )

        provider.create_object_store(
            "abfss://demo-container@uri-account.dfs.core.windows.net/path", connection_config
        )

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="uri-account", access_key="fake_key"
        )

    @pytest.mark.parametrize("account", [None, "usgovacct"])
    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_abfs_uri_sovereign_host_without_endpoint_raises(
        self, mock_azure, account, monkeypatch
    ):
        monkeypatch.delenv("AZURE_STORAGE_ENDPOINT", raising=False)
        monkeypatch.delenv("AZURE_ENDPOINT", raising=False)
        credentials = {"access_key": "fake_key"}
        if account:
            credentials["account"] = account
        connection_config = ConnectionConfig(conn_id="wasb_default", credentials=credentials)

        with pytest.raises(ObjectStoreCreationException, match="no endpoint override"):
            AzureObjectStorageProvider().create_object_store(
                "abfss://demo-container@usgovacct.dfs.core.usgovcloudapi.net/path", connection_config
            )

        mock_azure.assert_not_called()

    @pytest.mark.parametrize("env_var", ["AZURE_STORAGE_ENDPOINT", "AZURE_ENDPOINT"])
    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_abfs_uri_sovereign_host_with_endpoint_env_passes(
        self, mock_azure, env_var, monkeypatch
    ):
        monkeypatch.setenv(env_var, "https://usgovacct.blob.core.usgovcloudapi.net")
        connection_config = ConnectionConfig(conn_id="wasb_default", credentials={"access_key": "fake_key"})

        AzureObjectStorageProvider().create_object_store(
            "abfss://demo-container@usgovacct.dfs.core.usgovcloudapi.net/path", connection_config
        )

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="usgovacct", access_key="fake_key"
        )

    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    def test_azure_provider_abfs_uri_account_matches_case_insensitively(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "MyAccount", "access_key": "fake_key"},
        )

        provider.create_object_store(
            "abfss://demo-container@myaccount.dfs.core.windows.net/path", connection_config
        )

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="MyAccount", access_key="fake_key"
        )

    def test_azure_provider_failure(self):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(conn_id="wasb_default", credentials={"access_key": "fake_key"})

        with patch(
            f"{MODULE}.MicrosoftAzure",
            autospec=True,
            side_effect=Exception("Error"),
        ):
            with pytest.raises(ObjectStoreCreationException, match="Failed to create Azure object store"):
                provider.create_object_store("az://demo-container/path", connection_config)

    def test_azure_provider_requires_connection_config(self):
        provider = AzureObjectStorageProvider()

        with pytest.raises(ValueError, match="connection_config must be provided for azure"):
            provider.create_object_store("az://demo-container/path")

    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            (
                "abfs://demo-container@myaccount.dfs.core.windows.net/path/to/data.parquet",
                "az://myaccount.demo-container/path/to/data.parquet",
            ),
            (
                "abfss://demo-container@myaccount.dfs.core.windows.net/path/to/data.parquet",
                "az://myaccount.demo-container/path/to/data.parquet",
            ),
            ("abfss://demo-container@myaccount.dfs.core.windows.net", "az://myaccount.demo-container"),
            ("az://demo-container/path", "az://demo-container/path"),
            # Same container name on different accounts maps to distinct hosts.
            ("abfss://data@devacct.dfs.core.windows.net/data.csv", "az://devacct.data/data.csv"),
            ("abfss://data@prodacct.dfs.core.windows.net/data.csv", "az://prodacct.data/data.csv"),
        ],
    )
    def test_azure_provider_normalize_uri(self, uri, expected):
        provider = AzureObjectStorageProvider()

        assert provider.normalize_uri(uri) == expected

    def test_azure_provider_partial_service_principal_raises_clear_error(self):
        """Uses the real MicrosoftAzure binding, not a mock, since it's the one that panics
        on a partial client_id/client_secret/tenant_id combination."""
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"client_id": "only-this-one-set"},
        )

        with pytest.raises(ObjectStoreCreationException, match="Failed to create Azure object store"):
            provider.create_object_store("az://demo-container/path", connection_config)

    def test_get_storage_type_and_scheme(self):
        provider = AzureObjectStorageProvider()
        assert provider.get_storage_type == StorageType.AZURE
        assert provider.get_scheme() == "az://"

    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    @patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    def test_resolves_credentials_from_connection_when_none_given(self, mock_get_connection, mock_azure):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = "mykey"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        store = AzureObjectStorageProvider().create_object_store(
            "az://container/path", connection_config=ConnectionConfig(conn_id="wasb_default")
        )

        mock_get_connection.assert_called_once_with("wasb_default")
        mock_azure.assert_called_once_with(
            container_name="container", account="myaccount", access_key="mykey"
        )
        assert store == mock_azure.return_value

    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    @patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    def test_explicit_credentials_take_precedence_over_connection(self, mock_get_connection, mock_azure):
        connection_config = ConnectionConfig(
            conn_id="wasb_default", credentials={"account": "myaccount", "access_key": "explicit"}
        )

        AzureObjectStorageProvider().create_object_store("az://container/path", connection_config)

        mock_get_connection.assert_not_called()
        mock_azure.assert_called_once_with(
            container_name="container", account="myaccount", access_key="explicit"
        )

    @patch(f"{MODULE}.MicrosoftAzure", autospec=True)
    @patch(f"{MODULE}.WasbHook", autospec=True)
    @patch(f"{MODULE}.BaseHook.get_connection", autospec=True)
    def test_unknown_connection_type_raises_before_building_hook_or_store(
        self, mock_get_connection, mock_wasb_hook, mock_azure
    ):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "aws"
        mock_get_connection.return_value = mock_conn

        with pytest.raises(ValueError, match="Unknown connection type aws"):
            AzureObjectStorageProvider().create_object_store(
                "az://container/path", connection_config=ConnectionConfig(conn_id="aws_default")
            )

        mock_wasb_hook.assert_not_called()
        mock_azure.assert_not_called()


class TestCredentialsFromConnection:
    @pytest.mark.parametrize(
        ("password", "extra_dejson", "expected_access_key"),
        [
            ("mykey", {}, "mykey"),
            (None, {"shared_access_key": "extra-key"}, "extra-key"),
            (None, {"account_key": "extra-key"}, "extra-key"),
        ],
        ids=["password", "shared_access_key_extra", "account_key_extra"],
    )
    def test_with_shared_key(self, password, extra_dejson, expected_access_key):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = password
        mock_conn.extra_dejson = extra_dejson

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {"account": "myaccount", "access_key": expected_access_key}

    def test_with_service_principal(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "client-id"
        mock_conn.password = "client-secret"
        mock_conn.extra_dejson = {"tenant_id": "tenant-id"}

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {
            "client_id": "client-id",
            "client_secret": "client-secret",
            "tenant_id": "tenant-id",
        }

    def test_with_service_principal_and_host_prefers_host_account(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = "realaccount.blob.core.windows.net"
        mock_conn.login = "11111111-2222-3333-4444-555555555555"
        mock_conn.password = "client-secret"
        mock_conn.extra_dejson = {"tenant_id": "tenant-id"}

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {
            "account": "realaccount",
            "client_id": "11111111-2222-3333-4444-555555555555",
            "client_secret": "client-secret",
            "tenant_id": "tenant-id",
        }

    @pytest.mark.parametrize(
        ("login", "password", "missing"),
        [
            (None, "client-secret", "login"),
            ("client-id", None, "password"),
            (None, None, "login"),
        ],
    )
    def test_partial_service_principal_raises(self, login, password, missing):
        """A partial service-principal config must raise, not silently authenticate with a
        different identity (ambient auth, or the client secret sent as a shared key) --
        DataFusion's binding also panics on a partial client_id/client_secret/tenant_id
        combination."""
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = login
        mock_conn.password = password
        mock_conn.extra_dejson = {"tenant_id": "tenant-id"}

        with pytest.raises(ValueError, match=f"{missing}.*is not"):
            _resolve_credentials(mock_conn)

    def test_fully_empty_connection_omits_account(self):
        """Neither host nor login set (the shape of the ``wasb_default`` connection ``airflow
        db`` creates) must drop `account` entirely, not send the literal string 'None' --
        the binding then falls back to AZURE_STORAGE_ACCOUNT_NAME."""
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = None
        mock_conn.password = None
        mock_conn.extra_dejson = {}

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {}

    def test_shared_access_key_takes_priority_over_sas_token(self):
        """Matches WasbHook.get_conn, which checks the `shared_access_key` extra before
        `sas_token`."""
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {
            "sas_token": "?sv=2020-08-04&sp=rl&sig=abc",
            "shared_access_key": "extra-key",
        }

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {"account": "myaccount", "access_key": "extra-key"}

    def test_with_sas_token(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {"sas_token": "?sv=2020-08-04&sp=rl&sig=abc"}

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {
            "account": "myaccount",
            "sas_query_pairs": [("sv", "2020-08-04"), ("sp", "rl"), ("sig", "abc")],
        }

    def test_without_credentials_uses_ambient_auth(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {}

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {"account": "myaccount"}

    @pytest.mark.parametrize(
        "unsupported_field",
        ["connection_string", "managed_identity_client_id", "workload_identity_tenant_id"],
    )
    def test_rejects_unsupported_identity_fields(self, unsupported_field):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.extra_dejson = {unsupported_field: "some-value"}

        with pytest.raises(ValueError, match=f"{unsupported_field!r} is not supported"):
            _resolve_credentials(mock_conn)

    def test_reads_legacy_extra_prefixed_sas_token(self):
        """Older Airflow connection UIs wrote custom extra fields as
        extra__wasb__<field>; WasbHook still reads that spelling as a fallback."""
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {"extra__wasb__sas_token": "?sv=2020-08-04&sp=rl&sig=abc"}

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {
            "account": "myaccount",
            "sas_query_pairs": [("sv", "2020-08-04"), ("sp", "rl"), ("sig", "abc")],
        }

    def test_rejects_legacy_extra_prefixed_unsupported_field(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {"extra__wasb__connection_string": "some-conn-string"}

        with pytest.raises(ValueError, match="'connection_string' is not supported"):
            _resolve_credentials(mock_conn)

    def test_rejects_url_form_sas_token(self):
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {"sas_token": "https://myaccount.blob.core.windows.net/?sv=2020-08-04"}

        with pytest.raises(ValueError, match="URL-form `sas_token` is not supported"):
            _resolve_credentials(mock_conn)

    @pytest.mark.parametrize(
        ("env_vars", "should_raise"),
        [
            (["AZURE_STORAGE_ACCOUNT_KEY"], True),
            (["AZURE_STORAGE_ACCESS_KEY"], True),
            (["AZURE_STORAGE_MASTER_KEY"], True),
            (["AZURE_STORAGE_TOKEN"], True),
            (["AZURE_STORAGE_SAS_KEY"], False),
            (["AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE"], True),
            (["AZURE_FEDERATED_TOKEN_FILE"], False),
            (["AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID"], True),
            (["AZURE_STORAGE_CLIENT_ID", "AZURE_STORAGE_CLIENT_SECRET", "AZURE_STORAGE_TENANT_ID"], True),
            (["AZURE_STORAGE_CLIENT_ID", "AZURE_STORAGE_CLIENT_SECRET", "AZURE_STORAGE_AUTHORITY_ID"], True),
            (["AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_AUTHORITY_ID"], True),
            (["AZURE_CLIENT_ID"], False),
        ],
        ids=[
            "account-key",
            "access-key",
            "master-key",
            "bearer",
            "sas-key-checked-after-query-pairs",
            "workload-identity-triple",
            "workload-identity-partial",
            "client-secret-triple-bare",
            "client-secret-triple-storage-prefixed",
            "client-secret-triple-storage-authority",
            "client-secret-triple-bare-authority",
            "client-secret-partial",
        ],
    )
    def test_sas_env_precedence(self, env_vars, should_raise, monkeypatch):
        """A SAS connection sits at the bottom of object_store's precedence order and occupies
        none of the higher tiers' fields, so a bearer token or any access-key spelling always
        outranks it, and a complete workload-identity or client-secret triple (any recognized
        spelling) does too -- but a partial triple, or AZURE_STORAGE_SAS_KEY (checked only after
        the connection's own sas_query_pairs within SAS resolution itself), must not raise."""
        for var in env_vars:
            monkeypatch.setenv(var, "some-value")
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {"sas_token": "?sv=2020-08-04&sp=rl&sig=abc"}

        if should_raise:
            with pytest.raises(ValueError, match=", ".join(env_vars)):
                _resolve_credentials(mock_conn)
        else:
            credentials = _resolve_credentials(mock_conn)
            assert credentials == {
                "account": "myaccount",
                "sas_query_pairs": [("sv", "2020-08-04"), ("sp", "rl"), ("sig", "abc")],
            }

    @pytest.mark.parametrize(
        ("env_vars", "should_raise"),
        [
            (["AZURE_STORAGE_TOKEN"], True),
            (["AZURE_STORAGE_ACCOUNT_KEY"], False),
            (["AZURE_STORAGE_ACCESS_KEY"], False),
            (["AZURE_STORAGE_MASTER_KEY"], False),
            (["AZURE_FEDERATED_TOKEN_FILE"], False),
            (["AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID"], False),
        ],
        ids=[
            "bearer",
            "account-key",
            "access-key",
            "master-key",
            "federated-token-file",
            "client-secret-triple",
        ],
    )
    def test_access_key_env_precedence(self, env_vars, should_raise, monkeypatch):
        """A shared-key connection overwrites the same field object_store's build() would
        otherwise read from env, and build() picks access_key ahead of every lower tier -- so
        only a bearer token, the one tier above access_key, can actually outrank it, even on an
        AKS pod where the workload-identity webhook injects AZURE_FEDERATED_TOKEN_FILE into
        every labelled pod."""
        for var in env_vars:
            monkeypatch.setenv(var, "some-value")
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = "mykey"
        mock_conn.extra_dejson = {}

        if should_raise:
            with pytest.raises(ValueError, match=", ".join(env_vars)):
                _resolve_credentials(mock_conn)
        else:
            credentials = _resolve_credentials(mock_conn)
            assert credentials == {"account": "myaccount", "access_key": "mykey"}

    @pytest.mark.parametrize(
        ("env_vars", "should_raise"),
        [
            (["AZURE_STORAGE_TOKEN"], True),
            (["AZURE_STORAGE_ACCOUNT_KEY"], True),
            (["AZURE_STORAGE_ACCESS_KEY"], True),
            (["AZURE_STORAGE_MASTER_KEY"], True),
            (["AZURE_FEDERATED_TOKEN_FILE"], True),
            (["AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID"], False),
        ],
        ids=[
            "bearer",
            "account-key",
            "access-key",
            "master-key",
            "federated-token-file",
            "client-secret-triple",
        ],
    )
    def test_service_principal_env_precedence(self, env_vars, should_raise, monkeypatch):
        """A bearer token or any access-key spelling sits above client secret in object_store's
        precedence order, and the connection's own client_id and tenant_id already complete two
        of workload identity's three fields, so a federated token file alone is enough for env to
        complete that tier -- but a client-secret triple in env is moot, since the connection's
        own client_id/client_secret/tenant_id overwrite the same fields."""
        for var in env_vars:
            monkeypatch.setenv(var, "some-value")
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "client-id"
        mock_conn.password = "client-secret"
        mock_conn.extra_dejson = {"tenant_id": "tenant-id"}

        if should_raise:
            with pytest.raises(ValueError, match=", ".join(env_vars)):
                _resolve_credentials(mock_conn)
        else:
            credentials = _resolve_credentials(mock_conn)
            assert credentials == {
                "client_id": "client-id",
                "client_secret": "client-secret",
                "tenant_id": "tenant-id",
            }

    def test_env_precedence_guard_ignores_pure_ambient_auth(self, monkeypatch):
        """The guard only fires for an explicit connection credential -- a connection with none
        at all is meant to rely on the environment, so the same env vars must not raise here."""
        monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "some-value")
        mock_conn = MagicMock(spec=Connection)
        mock_conn.conn_type = "wasb"
        mock_conn.host = None
        mock_conn.login = "myaccount"
        mock_conn.password = None
        mock_conn.extra_dejson = {}

        credentials = _resolve_credentials(mock_conn)

        assert credentials == {"account": "myaccount"}

    @pytest.mark.parametrize(
        ("host", "login", "expected"),
        [
            ("myaccount.blob.core.windows.net", None, "myaccount"),
            ("https://myaccount.blob.core.windows.net/container/path", None, "myaccount"),
            (None, "myaccount", "myaccount"),
            ("justanadid", "myaccount", "myaccount"),
            ("a" * 40 + ".blob.core.windows.net", "x", "a" * 24),
            (None, None, None),
        ],
    )
    def test_resolve_wasb_account(self, host, login, expected):
        assert _resolve_wasb_account(host, login) == expected

    @pytest.mark.parametrize(
        "host",
        [
            "myaccount.blob.core.chinacloudapi.cn",
            "myaccount.blob.core.usgovcloudapi.net",
        ],
    )
    def test_resolve_wasb_account_rejects_non_public_cloud_hosts(self, host):
        """The binding has no endpoint override, so a sovereign-cloud host would otherwise be
        silently misrouted to the public *.blob.core.windows.net account of the same name."""
        with pytest.raises(ValueError, match="does not resolve to the public"):
            _resolve_wasb_account(host, None)

    @pytest.mark.parametrize("env_var", ["AZURE_STORAGE_ENDPOINT", "AZURE_ENDPOINT"])
    def test_resolve_wasb_account_allows_non_public_cloud_host_when_endpoint_set(self, env_var, monkeypatch):
        """Once the worker sets an endpoint override, object_store uses it verbatim instead of
        deriving the URL from the account name, so a real (dotted, portless) sovereign-cloud
        hostname no longer needs to match *.blob.core.windows.net."""
        monkeypatch.setenv(env_var, "https://myaccount.blob.core.chinacloudapi.cn")

        account = _resolve_wasb_account("myaccount.blob.core.chinacloudapi.cn", None)

        assert account == "myaccount"

    @pytest.mark.parametrize(
        ("host", "login"),
        [
            ("http://127.0.0.1:10000/devstoreaccount1", None),
            ("azurite:10000", None),
            ("azurite:10000", "devstoreaccount1"),
        ],
        ids=["ip-port-url", "dotless-host-port", "dotless-host-port-with-login"],
    )
    def test_resolve_wasb_account_rejects_emulator_style_hosts(self, host, login, monkeypatch):
        """A host:port netloc (Azurite's shape) is never a real DNS hostname -- resolving the
        account from it would otherwise silently produce the wrong value ("127", or "None" once
        the dotless fallback for an Active Directory ID kicks in), so this must always raise
        regardless of login or of the endpoint override, which does not help this shape either."""
        monkeypatch.setenv("AZURE_STORAGE_ENDPOINT", host.split("/")[0])

        with pytest.raises(ValueError, match="looks like an emulator address"):
            _resolve_wasb_account(host, login)

    def test_resolve_wasb_account_rejects_dotless_host_without_login(self):
        """The Active Directory ID fallback assumes login holds the real account name; without
        one there's nothing to build a netloc from, and silently using the literal string "None"
        is exactly the bug this function's own empty-connection case is meant to avoid."""
        with pytest.raises(ValueError, match="no `login` was given"):
            _resolve_wasb_account("azurite", None)

    def test_resolve_wasb_account_azurite_workaround_uses_login_with_empty_host(self):
        """The documented Azurite workaround -- the account name in login, host left empty --
        already resolves correctly without needing the non-public-suffix check at all."""
        assert _resolve_wasb_account(None, "devstoreaccount1") == "devstoreaccount1"


class TestEngineIntegration:
    @patch.object(DataFusionEngine, "_get_connection_config", autospec=True)
    def test_execute_query_with_abfs_uri_matches_real_registry(self, mock_get_conn):
        """Only object-store construction is mocked; normalize_uri and DataFusion's real
        registry run live, so a missing store surfaces as "No suitable object store found"."""
        mock_get_conn.return_value = None

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("name,age\nAlice,30\nBob,25\n")
            csv_path = f.name

        try:
            engine = DataFusionEngine()
            datasource_config = DataSourceConfig(
                table_name="abfs_csv",
                uri=f"abfss://demo-container@myaccount.dfs.core.windows.net{csv_path}",
                format="csv",
                conn_id="wasb_default",
            )

            with patch(
                f"{MODULE}.AzureObjectStorageProvider.create_object_store",
                autospec=True,
                return_value=LocalFileSystem(),
            ):
                engine.register_datasource(datasource_config)

            result = engine.execute_query("SELECT * FROM abfs_csv ORDER BY name")

            assert result == {"name": ["Alice", "Bob"], "age": [30, 25]}
            assert datasource_config.uri == f"abfss://demo-container@myaccount.dfs.core.windows.net{csv_path}"
            assert engine.registered_tables["abfs_csv"] == f"az://myaccount.demo-container{csv_path}"
        finally:
            os.unlink(csv_path)

    @pytest.mark.parametrize(
        ("uri_a", "uri_b"),
        [
            pytest.param(
                "abfss://bronze@myaccount.dfs.core.windows.net/data.csv",
                "abfss://silver@myaccount.dfs.core.windows.net/data.csv",
                id="same-account-different-containers",
            ),
            pytest.param(
                "abfss://data@devacct.dfs.core.windows.net/data.csv",
                "abfss://data@prodacct.dfs.core.windows.net/data.csv",
                id="same-container-different-accounts",
            ),
        ],
    )
    @patch.object(DataFusionEngine, "_get_connection_config", autospec=True)
    def test_execute_query_with_shared_abfs_identifiers_reads_each_store(self, mock_get_conn, uri_a, uri_b):
        mock_get_conn.return_value = None

        with (
            tempfile.TemporaryDirectory() as dir_a,
            tempfile.TemporaryDirectory() as dir_b,
        ):
            (Path(dir_a) / "data.csv").write_text("name,age\nAlice,30\n")
            (Path(dir_b) / "data.csv").write_text("name,age\nBob,25\n")

            engine = DataFusionEngine()
            config_a = DataSourceConfig(table_name="table_a", uri=uri_a, format="csv", conn_id="wasb_a")
            config_b = DataSourceConfig(table_name="table_b", uri=uri_b, format="csv", conn_id="wasb_b")

            with patch(
                f"{MODULE}.AzureObjectStorageProvider.create_object_store",
                autospec=True,
                side_effect=[LocalFileSystem(prefix=dir_a), LocalFileSystem(prefix=dir_b)],
            ):
                engine.register_datasource(config_a)
                engine.register_datasource(config_b)

            assert engine.execute_query("SELECT * FROM table_a") == {"name": ["Alice"], "age": [30]}
            assert engine.execute_query("SELECT * FROM table_b") == {"name": ["Bob"], "age": [25]}
