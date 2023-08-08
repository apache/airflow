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
from __future__ import annotations

import json
from unittest import mock

import pytest
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._models import BlobProperties

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# connection_string has a format
CONN_STRING = (
    "DefaultEndpointsProtocol=https;AccountName=testname;AccountKey=wK7BOz;EndpointSuffix=core.windows.net"
)

ACCESS_KEY_STRING = "AccountName=name;skdkskd"


class TestWasbHook:
    def setup_method(self):
        self.login = "login"
        self.wasb_test_key = "wasb_test_key"
        self.connection_type = "wasb"
        self.connection_string_id = "azure_test_connection_string"
        self.shared_key_conn_id = "azure_shared_key_test"
        self.shared_key_conn_id_without_host = "azure_shared_key_test_wihout_host"
        self.ad_conn_id = "azure_AD_test"
        self.sas_conn_id = "sas_token_id"
        self.extra__wasb__sas_conn_id = "extra__sas_token_id"
        self.http_sas_conn_id = "http_sas_token_id"
        self.extra__wasb__http_sas_conn_id = "extra__http_sas_token_id"
        self.public_read_conn_id = "pub_read_id"
        self.public_read_conn_id_without_host = "pub_read_id_without_host"
        self.managed_identity_conn_id = "managed_identity"
        self.authority = "https://test_authority.com"

        self.proxies = {"http": "http_proxy_uri", "https": "https_proxy_uri"}
        self.client_secret_auth_config = {
            "proxies": self.proxies,
            "connection_verify": False,
            "authority": self.authority,
        }
        self.connection_map = {
            self.wasb_test_key: Connection(
                conn_id="wasb_test_key",
                conn_type=self.connection_type,
                login=self.login,
                password="key",
            ),
            self.public_read_conn_id: Connection(
                conn_id=self.public_read_conn_id,
                conn_type=self.connection_type,
                host="https://accountname.blob.core.windows.net",
                extra=json.dumps({"proxies": self.proxies}),
            ),
            self.public_read_conn_id_without_host: Connection(
                conn_id=self.public_read_conn_id_without_host,
                conn_type=self.connection_type,
                login=self.login,
                extra=json.dumps({"proxies": self.proxies}),
            ),
            self.connection_string_id: Connection(
                conn_id=self.connection_string_id,
                conn_type=self.connection_type,
                extra=json.dumps({"connection_string": CONN_STRING, "proxies": self.proxies}),
            ),
            self.shared_key_conn_id: Connection(
                conn_id=self.shared_key_conn_id,
                conn_type=self.connection_type,
                host="https://accountname.blob.core.windows.net",
                extra=json.dumps({"shared_access_key": "token", "proxies": self.proxies}),
            ),
            self.shared_key_conn_id_without_host: Connection(
                conn_id=self.shared_key_conn_id_without_host,
                conn_type=self.connection_type,
                login=self.login,
                extra=json.dumps({"shared_access_key": "token", "proxies": self.proxies}),
            ),
            self.ad_conn_id: Connection(
                conn_id=self.ad_conn_id,
                conn_type=self.connection_type,
                host="conn_host",
                login="appID",
                password="appsecret",
                extra=json.dumps(
                    {
                        "tenant_id": "token",
                        "proxies": self.proxies,
                        "client_secret_auth_config": self.client_secret_auth_config,
                    }
                ),
            ),
            self.managed_identity_conn_id: Connection(
                conn_id=self.managed_identity_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps({"proxies": self.proxies}),
            ),
            self.sas_conn_id: Connection(
                conn_id=self.sas_conn_id,
                conn_type=self.connection_type,
                login=self.login,
                extra=json.dumps({"sas_token": "token", "proxies": self.proxies}),
            ),
            self.extra__wasb__sas_conn_id: Connection(
                conn_id=self.extra__wasb__sas_conn_id,
                conn_type=self.connection_type,
                login=self.login,
                extra=json.dumps({"extra__wasb__sas_token": "token", "proxies": self.proxies}),
            ),
            self.http_sas_conn_id: Connection(
                conn_id=self.http_sas_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps(
                    {"sas_token": "https://login.blob.core.windows.net/token", "proxies": self.proxies}
                ),
            ),
            self.extra__wasb__http_sas_conn_id: Connection(
                conn_id=self.extra__wasb__http_sas_conn_id,
                conn_type=self.connection_type,
                extra=json.dumps(
                    {
                        "extra__wasb__sas_token": "https://login.blob.core.windows.net/token",
                        "proxies": self.proxies,
                    }
                ),
            ),
        }

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_key(self, mock_get_conn, mock_blob_service_client):
        conn = self.connection_map[self.wasb_test_key]
        mock_get_conn.return_value = conn
        WasbHook(wasb_conn_id=self.wasb_test_key)
        assert mock_blob_service_client.call_args == mock.call(
            account_url=f"https://{self.login}.blob.core.windows.net/",
            credential=conn.password,
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_public_read(self, mock_get_conn, mock_blob_service_client):
        conn = self.connection_map[self.public_read_conn_id]
        mock_get_conn.return_value = conn
        WasbHook(wasb_conn_id=self.public_read_conn_id, public_read=True)
        assert mock_blob_service_client.call_args == mock.call(
            account_url=conn.host,
            proxies=conn.extra_dejson["proxies"],
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_connection_string(self, mock_get_conn, mock_blob_service_client):
        conn = self.connection_map[self.connection_string_id]
        mock_get_conn.return_value = conn
        WasbHook(wasb_conn_id=self.connection_string_id)
        mock_blob_service_client.from_connection_string.assert_called_once_with(
            CONN_STRING,
            proxies=conn.extra_dejson["proxies"],
            connection_string=conn.extra_dejson["connection_string"],
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_shared_key_connection(self, mock_get_conn, mock_blob_service_client):
        conn = self.connection_map[self.shared_key_conn_id]
        mock_get_conn.return_value = conn
        WasbHook(wasb_conn_id=self.shared_key_conn_id)
        mock_blob_service_client.assert_called_once_with(
            account_url=conn.host,
            credential=conn.extra_dejson["shared_access_key"],
            proxies=conn.extra_dejson["proxies"],
            shared_access_key=conn.extra_dejson["shared_access_key"],
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.DefaultAzureCredential")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_managed_identity(self, mock_get_conn, mock_credential, mock_blob_service_client):
        conn = self.connection_map[self.managed_identity_conn_id]
        mock_get_conn.return_value = conn
        WasbHook(wasb_conn_id=self.managed_identity_conn_id)
        mock_blob_service_client.assert_called_once_with(
            account_url=f"https://{conn.login}.blob.core.windows.net/",
            credential=mock_credential.return_value,
            proxies=conn.extra_dejson["proxies"],
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.ClientSecretCredential")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_azure_directory_connection(self, mock_get_conn, mock_credential, mock_blob_service_client):
        conn = self.connection_map[self.ad_conn_id]
        mock_get_conn.return_value = conn
        WasbHook(wasb_conn_id=self.ad_conn_id)
        mock_credential.assert_called_once_with(
            conn.extra_dejson["tenant_id"],
            conn.login,
            conn.password,
            proxies=self.client_secret_auth_config["proxies"],
            connection_verify=self.client_secret_auth_config["connection_verify"],
            authority=self.client_secret_auth_config["authority"],
        )
        mock_blob_service_client.assert_called_once_with(
            account_url=f"https://{conn.login}.blob.core.windows.net/",
            credential=mock_credential.return_value,
            tenant_id=conn.extra_dejson["tenant_id"],
            proxies=conn.extra_dejson["proxies"],
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.DefaultAzureCredential")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_active_directory_ID_used_as_host(self, mock_get_conn, mock_credential, mock_blob_service_client):
        mock_get_conn.return_value = Connection(
            conn_id="testconn",
            conn_type=self.connection_type,
            login="testaccountname",
            host="testaccountID",
        )
        WasbHook(wasb_conn_id="testconn")
        assert mock_blob_service_client.call_args == mock.call(
            account_url="https://testaccountname.blob.core.windows.net/",
            credential=mock_credential.return_value,
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_sas_token_provided_and_active_directory_ID_used_as_host(
        self, mock_get_conn, mock_blob_service_client
    ):
        mock_get_conn.return_value = Connection(
            conn_id="testconn",
            conn_type=self.connection_type,
            login="testaccountname",
            host="testaccountID",
            extra=json.dumps({"sas_token": "SAStoken"}),
        )
        WasbHook(wasb_conn_id="testconn")
        assert mock_blob_service_client.call_args == mock.call(
            account_url="https://testaccountname.blob.core.windows.net/SAStoken",
            sas_token="SAStoken",
        )

    @pytest.mark.parametrize(
        argnames="conn_id_str",
        argvalues=[
            "wasb_test_key",
            "shared_key_conn_id_without_host",
            "public_read_conn_id_without_host",
        ],
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.DefaultAzureCredential")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_account_url_without_host(
        self, mock_get_conn, mock_credential, mock_blob_service_client, conn_id_str
    ):
        conn_id = self.__getattribute__(conn_id_str)
        connection = self.connection_map[conn_id]
        mock_get_conn.return_value = connection
        WasbHook(wasb_conn_id=conn_id)
        if conn_id_str == "wasb_test_key":
            mock_blob_service_client.assert_called_once_with(
                account_url=f"https://{connection.login}.blob.core.windows.net/",
                credential=connection.password,
            )
        elif conn_id_str == "shared_key_conn_id_without_host":
            mock_blob_service_client.assert_called_once_with(
                account_url=f"https://{connection.login}.blob.core.windows.net/",
                credential=connection.extra_dejson["shared_access_key"],
                proxies=connection.extra_dejson["proxies"],
                shared_access_key=connection.extra_dejson["shared_access_key"],
            )
        else:
            mock_blob_service_client.assert_called_once_with(
                account_url=f"https://{connection.login}.blob.core.windows.net/",
                credential=mock_credential.return_value,
                proxies=connection.extra_dejson["proxies"],
            )

    @pytest.mark.parametrize(
        argnames="conn_id_str, extra_key",
        argvalues=[
            ("sas_conn_id", "sas_token"),
            ("extra__wasb__sas_conn_id", "extra__wasb__sas_token"),
            ("http_sas_conn_id", "sas_token"),
            ("extra__wasb__http_sas_conn_id", "extra__wasb__sas_token"),
        ],
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_sas_token_connection(self, mock_get_conn, conn_id_str, extra_key):
        conn_id = self.__getattribute__(conn_id_str)
        mock_get_conn.return_value = self.connection_map[conn_id]
        hook = WasbHook(wasb_conn_id=conn_id)
        conn = hook.get_conn()
        hook_conn = hook.get_connection(hook.conn_id)
        sas_token = hook_conn.extra_dejson[extra_key]
        assert isinstance(conn, BlobServiceClient)
        assert conn.url.startswith("https://")
        if hook_conn.login:
            assert conn.url.__contains__(hook_conn.login)
        assert conn.url.endswith(sas_token + "/")

    @pytest.mark.parametrize(
        argnames="conn_id_str",
        argvalues=[
            "connection_string_id",
            "shared_key_conn_id",
            "ad_conn_id",
            "managed_identity_conn_id",
            "sas_conn_id",
            "extra__wasb__sas_conn_id",
            "http_sas_conn_id",
            "extra__wasb__http_sas_conn_id",
        ],
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_connection_extra_arguments(self, mock_get_conn, conn_id_str):
        conn_id = self.__getattribute__(conn_id_str)
        mock_get_conn.return_value = self.connection_map[conn_id]
        hook = WasbHook(wasb_conn_id=conn_id)
        conn = hook.get_conn()
        assert conn._config.proxy_policy.proxies == self.proxies

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_connection_extra_arguments_public_read(self, mock_get_conn):
        conn_id = self.public_read_conn_id
        mock_get_conn.return_value = self.connection_map[conn_id]
        hook = WasbHook(wasb_conn_id=conn_id, public_read=True)
        conn = hook.get_conn()
        assert conn._config.proxy_policy.proxies == self.proxies

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_extra_client_secret_auth_config_ad_connection(self, mock_get_conn):
        mock_get_conn.return_value = self.connection_map[self.ad_conn_id]
        conn_id = self.ad_conn_id
        hook = WasbHook(wasb_conn_id=conn_id)
        conn = hook.get_conn()
        assert conn.credential._authority == self.authority

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_check_for_blob(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        assert hook.check_for_blob(container_name="mycontainer", blob_name="myblob")
        mock_blob_client = mock_service.return_value.get_blob_client
        mock_blob_client.assert_called_once_with(container="mycontainer", blob="myblob")
        mock_blob_client.return_value.get_blob_properties.assert_called()

    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_check_for_prefix(self, mock_get_conn, get_blobs_list):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        get_blobs_list.return_value = ["blobs"]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        assert hook.check_for_prefix("container", "prefix", timeout=3)
        get_blobs_list.assert_called_once_with(container_name="container", prefix="prefix", timeout=3)

    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_check_for_prefix_empty(self, mock_get_conn, get_blobs_list):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        get_blobs_list.return_value = []
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        assert not hook.check_for_prefix("container", "prefix", timeout=3)
        get_blobs_list.assert_called_once_with(container_name="container", prefix="prefix", timeout=3)

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_get_blobs_list(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.get_blobs_list(container_name="mycontainer", prefix="my", include=None, delimiter="/")
        mock_service.return_value.get_container_client.assert_called_once_with("mycontainer")
        mock_service.return_value.get_container_client.return_value.walk_blobs.assert_called_once_with(
            name_starts_with="my", include=None, delimiter="/"
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_get_blobs_list_recursive(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.get_blobs_list_recursive(
            container_name="mycontainer", prefix="test", include=None, endswith="file_extension"
        )
        mock_service.return_value.get_container_client.assert_called_once_with("mycontainer")
        mock_service.return_value.get_container_client.return_value.list_blobs.assert_called_once_with(
            name_starts_with="test", include=None
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_get_blobs_list_recursive_endswith(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        mock_service.return_value.get_container_client.return_value.list_blobs.return_value = [
            BlobProperties(name="test/abc.py"),
            BlobProperties(name="test/inside_test/abc.py"),
            BlobProperties(name="test/abc.csv"),
        ]
        blob_list_output = hook.get_blobs_list_recursive(
            container_name="mycontainer", prefix="test", include=None, endswith=".py"
        )
        assert blob_list_output == ["test/abc.py", "test/inside_test/abc.py"]

    @pytest.mark.parametrize(argnames="create_container", argvalues=[True, False])
    @mock.patch.object(WasbHook, "upload")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_load_file(self, mock_get_conn, mock_upload, create_container):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        with mock.patch("builtins.open", mock.mock_open(read_data="data")):
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.load_file("path", "container", "blob", create_container, max_connections=1)

        mock_upload.assert_called_with(
            container_name="container",
            blob_name="blob",
            data=mock.ANY,
            create_container=create_container,
            max_connections=1,
        )

    @pytest.mark.parametrize(argnames="create_container", argvalues=[True, False])
    @mock.patch.object(WasbHook, "upload")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_load_string(self, mock_get_conn, mock_upload, create_container):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.load_string("big string", "container", "blob", create_container, max_connections=1)
        mock_upload.assert_called_once_with(
            container_name="container",
            blob_name="blob",
            data="big string",
            create_container=create_container,
            max_connections=1,
        )

    @mock.patch.object(WasbHook, "download")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_get_file(self, mock_get_conn, mock_download):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        with mock.patch("builtins.open", mock.mock_open(read_data="data")):
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.get_file("path", "container", "blob", max_connections=1)
        mock_download.assert_called_once_with(container_name="container", blob_name="blob", max_connections=1)
        mock_download.return_value.readall.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch.object(WasbHook, "download")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_read_file(self, mock_get_conn, mock_download, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.read_file("container", "blob", max_connections=1)
        mock_download.assert_called_once_with("container", "blob", max_connections=1)

    @pytest.mark.parametrize(argnames="create_container", argvalues=[True, False])
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_upload(self, mock_get_conn, mock_service, create_container):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.upload(
            container_name="mycontainer",
            blob_name="myblob",
            data=b"mydata",
            create_container=create_container,
            blob_type="BlockBlob",
            length=4,
        )
        mock_blob_client = mock_service.return_value.get_blob_client
        mock_blob_client.assert_called_once_with(container="mycontainer", blob="myblob")
        mock_blob_client.return_value.upload_blob.assert_called_once_with(b"mydata", "BlockBlob", length=4)

        mock_container_client = mock_service.return_value.get_container_client
        if create_container:
            mock_container_client.assert_called_with("mycontainer")
        else:
            mock_container_client.assert_not_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_download(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        blob_client = mock_service.return_value.get_blob_client
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.download(container_name="mycontainer", blob_name="myblob", offset=2, length=4)
        blob_client.assert_called_once_with(container="mycontainer", blob="myblob")
        blob_client.return_value.download_blob.assert_called_once_with(offset=2, length=4)

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_get_container_client(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook._get_container_client("mycontainer")
        mock_service.return_value.get_container_client.assert_called_once_with("mycontainer")

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_get_blob_client(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook._get_blob_client(container_name="mycontainer", blob_name="myblob")
        mock_instance = mock_service.return_value.get_blob_client
        mock_instance.assert_called_once_with(container="mycontainer", blob="myblob")

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_create_container(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.create_container(container_name="mycontainer")
        mock_instance = mock_service.return_value.get_container_client
        mock_instance.assert_called_once_with("mycontainer")
        mock_instance.return_value.create_container.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_delete_container(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.delete_container("mycontainer")
        mock_service.return_value.get_container_client.assert_called_once_with("mycontainer")
        mock_service.return_value.get_container_client.return_value.delete_container.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch.object(WasbHook, "delete_blobs")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_delete_single_blob(self, mock_get_conn, delete_blobs, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.delete_file("container", "blob", is_prefix=False)
        delete_blobs.assert_called_once_with("container", "blob")

    @mock.patch.object(WasbHook, "delete_blobs")
    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch.object(WasbHook, "check_for_blob")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_delete_multiple_blobs(self, mock_get_conn, mock_check, mock_get_blobslist, mock_delete_blobs):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        mock_check.return_value = False
        mock_get_blobslist.return_value = ["blob_prefix/blob1", "blob_prefix/blob2"]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.delete_file("container", "blob_prefix", is_prefix=True)
        mock_get_blobslist.assert_called_once_with("container", prefix="blob_prefix", delimiter="")
        mock_delete_blobs.assert_any_call(
            "container",
            "blob_prefix/blob1",
            "blob_prefix/blob2",
        )
        assert mock_delete_blobs.call_count == 1

    @mock.patch.object(WasbHook, "delete_blobs")
    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch.object(WasbHook, "check_for_blob")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_delete_more_than_256_blobs(
        self, mock_get_conn, mock_check, mock_get_blobslist, mock_delete_blobs
    ):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        mock_check.return_value = False
        mock_get_blobslist.return_value = [f"blob_prefix/blob{i}" for i in range(300)]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.delete_file("container", "blob_prefix", is_prefix=True)
        mock_get_blobslist.assert_called_once_with("container", prefix="blob_prefix", delimiter="")
        # The maximum number of blobs that can be deleted in a single request is 256 using the underlying
        # `ContainerClient.delete_blobs()` method. Therefore the deletes need to be in batches of <= 256.
        # Therefore, providing a list of 300 blobs to delete should yield 2 calls of
        # `ContainerClient.delete_blobs()` in this test.
        assert mock_delete_blobs.call_count == 2

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch.object(WasbHook, "check_for_blob")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_delete_nonexisting_blob_fails(self, mock_get_conn, mock_check, mock_getblobs, mock_service):
        mock_getblobs.return_value = []
        mock_check.return_value = False
        with pytest.raises(Exception) as ctx:
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.delete_file("container", "nonexisting_blob", is_prefix=False, ignore_if_missing=False)
        assert isinstance(ctx.value, AirflowException)

    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_delete_multiple_nonexisting_blobs_fails(self, mock_get_conn, mock_getblobs):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        mock_getblobs.return_value = []
        with pytest.raises(Exception) as ctx:
            hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
            hook.delete_file("container", "nonexisting_blob_prefix", is_prefix=True, ignore_if_missing=False)
        assert isinstance(ctx.value, AirflowException)

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_connection_success(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.get_conn().get_account_information().return_value = {
            "sku_name": "Standard_RAGRS",
            "account_kind": "StorageV2",
        }
        status, msg = hook.test_connection()

        assert status is True
        assert msg == "Successfully connected to Azure Blob Storage."

    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient")
    @mock.patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_connection")
    def test_connection_failure(self, mock_get_conn, mock_service):
        mock_get_conn.return_value = self.connection_map[self.shared_key_conn_id]
        hook = WasbHook(wasb_conn_id=self.shared_key_conn_id)
        hook.get_conn().get_account_information = mock.PropertyMock(
            side_effect=Exception("Authentication failed.")
        )
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Authentication failed."
