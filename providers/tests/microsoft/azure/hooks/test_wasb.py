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

import re
from unittest import mock

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._models import BlobProperties

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

pytestmark = pytest.mark.db_test


# connection_string has a format
CONN_STRING = (
    "DefaultEndpointsProtocol=https;AccountName=testname;AccountKey=wK7BOz;EndpointSuffix=core.windows.net"
)

ACCESS_KEY_STRING = "AccountName=name;skdkskd"
PROXIES = {"http": "http_proxy_uri", "https": "https_proxy_uri"}


@pytest.fixture
def mocked_blob_service_client():
    with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.BlobServiceClient") as m:
        yield m


@pytest.fixture
def mocked_default_azure_credential():
    with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.get_sync_default_azure_credential") as m:
        yield m


@pytest.fixture
def mocked_client_secret_credential():
    with mock.patch("airflow.providers.microsoft.azure.hooks.wasb.ClientSecretCredential") as m:
        yield m


class TestWasbHook:
    @pytest.fixture(autouse=True)
    def setup_method(self, create_mock_connections):
        self.login = "login"
        self.wasb_test_key = "wasb_test_key"
        self.connection_type = "wasb"
        self.azure_test_connection_string = "azure_test_connection_string"
        self.azure_shared_key_test = "azure_shared_key_test"
        self.ad_conn_id = "ad_conn_id"
        self.sas_conn_id = "sas_token_id"
        self.extra__wasb__sas_conn_id = "extra__wasb__sas_conn_id"
        self.http_sas_conn_id = "http_sas_conn_id"
        self.extra__wasb__http_sas_conn_id = "extra__wasb__http_sas_conn_id"
        self.public_read_conn_id = "pub_read_id"
        self.public_read_conn_id_without_host = "pub_read_id_without_host"
        self.managed_identity_conn_id = "managed_identity_conn_id"
        self.authority = "https://test_authority.com"

        self.proxies = PROXIES
        self.client_secret_auth_config = {
            "proxies": self.proxies,
            "connection_verify": False,
            "authority": self.authority,
        }

        conns = create_mock_connections(
            Connection(
                conn_id="wasb_test_key",
                conn_type=self.connection_type,
                login=self.login,
                password="key",
            ),
            Connection(
                conn_id=self.public_read_conn_id,
                conn_type=self.connection_type,
                host="https://accountname.blob.core.windows.net",
                extra={"proxies": self.proxies},
            ),
            Connection(
                conn_id=self.public_read_conn_id_without_host,
                conn_type=self.connection_type,
                login=self.login,
                extra={"proxies": self.proxies},
            ),
            Connection(
                conn_id=self.azure_test_connection_string,
                conn_type=self.connection_type,
                extra={"connection_string": CONN_STRING, "proxies": self.proxies},
            ),
            Connection(
                conn_id=self.azure_shared_key_test,
                conn_type=self.connection_type,
                host="https://accountname.blob.core.windows.net",
                extra={"shared_access_key": "token", "proxies": self.proxies},
            ),
            Connection(
                conn_id=self.ad_conn_id,
                conn_type=self.connection_type,
                host="conn_host",
                login="appID",
                password="appsecret",
                extra={
                    "tenant_id": "token",
                    "proxies": self.proxies,
                    "client_secret_auth_config": self.client_secret_auth_config,
                },
            ),
            Connection(
                conn_id=self.managed_identity_conn_id,
                conn_type=self.connection_type,
                extra={"proxies": self.proxies},
            ),
            Connection(
                conn_id="sas_conn_id",
                conn_type=self.connection_type,
                login=self.login,
                extra={"sas_token": "token", "proxies": self.proxies},
            ),
            Connection(
                conn_id=self.extra__wasb__sas_conn_id,
                conn_type=self.connection_type,
                login=self.login,
                extra={"extra__wasb__sas_token": "token", "proxies": self.proxies},
            ),
            Connection(
                conn_id=self.http_sas_conn_id,
                conn_type=self.connection_type,
                extra={"sas_token": "https://login.blob.core.windows.net/token", "proxies": self.proxies},
            ),
            Connection(
                conn_id=self.extra__wasb__http_sas_conn_id,
                conn_type=self.connection_type,
                extra={
                    "extra__wasb__sas_token": "https://login.blob.core.windows.net/token",
                    "proxies": self.proxies,
                },
            ),
        )
        self.connection_map = {conn.conn_id: conn for conn in conns}

    def test_key(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.wasb_test_key)
        mocked_blob_service_client.assert_not_called()  # Not expected during initialisation
        hook.get_conn()
        mocked_blob_service_client.assert_called_once_with(
            account_url=f"https://{self.login}.blob.core.windows.net/", credential="key"
        )

    def test_public_read(self, mocked_blob_service_client):
        WasbHook(wasb_conn_id=self.public_read_conn_id, public_read=True).get_conn()
        mocked_blob_service_client.assert_called_once_with(
            account_url="https://accountname.blob.core.windows.net", proxies=self.proxies
        )

    def test_connection_string(self, mocked_blob_service_client):
        WasbHook(wasb_conn_id=self.azure_test_connection_string).get_conn()
        mocked_blob_service_client.from_connection_string.assert_called_once_with(
            CONN_STRING, proxies=self.proxies, connection_string=CONN_STRING
        )

    def test_shared_key_connection(self, mocked_blob_service_client):
        WasbHook(wasb_conn_id=self.azure_shared_key_test).get_conn()
        mocked_blob_service_client.assert_called_once_with(
            account_url="https://accountname.blob.core.windows.net",
            credential="token",
            proxies=self.proxies,
            shared_access_key="token",
        )

    def test_managed_identity(self, mocked_default_azure_credential, mocked_blob_service_client):
        mocked_default_azure_credential.assert_not_called()
        mocked_default_azure_credential.return_value = "foo-bar"
        WasbHook(wasb_conn_id=self.managed_identity_conn_id).get_conn()
        mocked_default_azure_credential.assert_called_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )
        mocked_blob_service_client.assert_called_once_with(
            account_url="https://None.blob.core.windows.net/",
            credential="foo-bar",
            proxies=self.proxies,
        )

    def test_azure_directory_connection(self, mocked_client_secret_credential, mocked_blob_service_client):
        mocked_client_secret_credential.return_value = "spam-egg"
        WasbHook(wasb_conn_id=self.ad_conn_id).get_conn()
        mocked_client_secret_credential.assert_called_once_with(
            tenant_id="token",
            client_id="appID",
            client_secret="appsecret",
            proxies=self.client_secret_auth_config["proxies"],
            connection_verify=self.client_secret_auth_config["connection_verify"],
            authority=self.client_secret_auth_config["authority"],
        )
        mocked_blob_service_client.assert_called_once_with(
            account_url="https://appID.blob.core.windows.net/",
            credential="spam-egg",
            tenant_id="token",
            proxies=self.proxies,
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                host="testaccountID",
            )
        ],
        indirect=True,
    )
    def test_active_directory_id_used_as_host(
        self, mocked_connection, mocked_default_azure_credential, mocked_blob_service_client
    ):
        mocked_default_azure_credential.return_value = "fake-credential"
        WasbHook(wasb_conn_id="testconn").get_conn()
        mocked_blob_service_client.assert_called_once_with(
            account_url="https://testaccountname.blob.core.windows.net/",
            credential="fake-credential",
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="testconn",
                conn_type="wasb",
                login="testaccountname",
                host="testaccountID",
                extra={"sas_token": "SAStoken"},
            )
        ],
        indirect=True,
    )
    def test_sas_token_provided_and_active_directory_id_used_as_host(
        self, mocked_connection, mocked_blob_service_client
    ):
        WasbHook(wasb_conn_id="testconn").get_conn()
        mocked_blob_service_client.assert_called_once_with(
            account_url="https://testaccountname.blob.core.windows.net/SAStoken",
            sas_token="SAStoken",
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            pytest.param(
                Connection(
                    conn_type="wasb",
                    login="foo",
                    extra={"shared_access_key": "token", "proxies": PROXIES},
                ),
                id="shared-key-without-host",
            ),
            pytest.param(
                Connection(conn_type="wasb", login="foo", extra={"proxies": PROXIES}),
                id="public-read-without-host",
            ),
        ],
        indirect=True,
    )
    def test_account_url_without_host(
        self, mocked_connection, mocked_blob_service_client, mocked_default_azure_credential
    ):
        mocked_default_azure_credential.return_value = "default-creds"
        WasbHook(wasb_conn_id=mocked_connection.conn_id).get_conn()
        if "shared_access_key" in mocked_connection.extra_dejson:
            mocked_blob_service_client.assert_called_once_with(
                account_url=f"https://{mocked_connection.login}.blob.core.windows.net/",
                credential=mocked_connection.extra_dejson["shared_access_key"],
                proxies=mocked_connection.extra_dejson["proxies"],
                shared_access_key=mocked_connection.extra_dejson["shared_access_key"],
            )
        else:
            mocked_blob_service_client.assert_called_once_with(
                account_url=f"https://{mocked_connection.login}.blob.core.windows.net/",
                credential="default-creds",
                proxies=mocked_connection.extra_dejson["proxies"],
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
    def test_sas_token_connection(self, conn_id_str, extra_key):
        hook = WasbHook(wasb_conn_id=conn_id_str)
        conn = hook.get_conn()
        hook_conn = hook.get_connection(hook.conn_id)
        sas_token = hook_conn.extra_dejson[extra_key]
        assert isinstance(conn, BlobServiceClient)
        assert conn.url.startswith("https://")
        if hook_conn.login:
            assert hook_conn.login in conn.url
        assert conn.url.endswith(sas_token + "/")

    @pytest.mark.parametrize(
        argnames="conn_id_str",
        argvalues=[
            "azure_test_connection_string",
            "azure_shared_key_test",
            "ad_conn_id",
            "managed_identity_conn_id",
            "sas_conn_id",
            "extra__wasb__sas_conn_id",
            "http_sas_conn_id",
            "extra__wasb__http_sas_conn_id",
        ],
    )
    def test_connection_extra_arguments(self, conn_id_str):
        conn = WasbHook(wasb_conn_id=conn_id_str).get_conn()
        assert conn._config.proxy_policy.proxies == self.proxies

    def test_connection_extra_arguments_public_read(self):
        hook = WasbHook(wasb_conn_id=self.public_read_conn_id, public_read=True)
        conn = hook.get_conn()
        assert conn._config.proxy_policy.proxies == self.proxies

    def test_extra_client_secret_auth_config_ad_connection(self):
        hook = WasbHook(wasb_conn_id=self.ad_conn_id)
        conn = hook.get_conn()
        assert conn.credential._authority == self.authority

    @pytest.mark.parametrize(
        "provided_host, expected_host",
        [
            (
                "https://testaccountname.blob.core.windows.net",
                "https://testaccountname.blob.core.windows.net",
            ),
            ("testhost", "https://accountlogin.blob.core.windows.net/"),
            ("testhost.dns", "https://testhost.dns"),
            ("testhost.blob.net", "https://testhost.blob.net"),
            (
                "testhostakjhdisdfbearioyo.blob.core.windows.net",
                "https://testhostakjhdisdfbearioy.blob.core.windows.net",
            ),  # more than 24 characters
        ],
    )
    def test_proper_account_url_update(
        self, mocked_blob_service_client, provided_host, expected_host, create_mock_connection
    ):
        conn = create_mock_connection(
            Connection(
                conn_type=self.connection_type,
                password="testpass",
                login="accountlogin",
                host=provided_host,
            )
        )
        WasbHook(wasb_conn_id=conn.conn_id).get_conn()
        mocked_blob_service_client.assert_called_once_with(account_url=expected_host, credential="testpass")

    def test_check_for_blob(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        assert hook.check_for_blob(container_name="mycontainer", blob_name="myblob")
        mock_blob_client = mocked_blob_service_client.return_value.get_blob_client
        mock_blob_client.assert_called_once_with(container="mycontainer", blob="myblob")
        mock_blob_client.return_value.get_blob_properties.assert_called()

    @mock.patch.object(WasbHook, "get_blobs_list", return_value=["blobs"])
    def test_check_for_prefix(self, get_blobs_list):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        assert hook.check_for_prefix("container", "prefix", timeout=3)
        get_blobs_list.assert_called_once_with(container_name="container", prefix="prefix", timeout=3)

    @mock.patch.object(WasbHook, "get_blobs_list", return_value=[])
    def test_check_for_prefix_empty(self, get_blobs_list):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        assert not hook.check_for_prefix("container", "prefix", timeout=3)
        get_blobs_list.assert_called_once_with(container_name="container", prefix="prefix", timeout=3)

    def test_get_blobs_list(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.get_blobs_list(container_name="mycontainer", prefix="my", include=None, delimiter="/")
        mock_container_client = mocked_blob_service_client.return_value.get_container_client
        mock_container_client.assert_called_once_with("mycontainer")
        mock_container_client.return_value.walk_blobs.assert_called_once_with(
            name_starts_with="my", include=None, delimiter="/"
        )

    def test_get_blobs_list_recursive(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.get_blobs_list_recursive(
            container_name="mycontainer", prefix="test", include=None, endswith="file_extension"
        )
        mock_container_client = mocked_blob_service_client.return_value.get_container_client
        mock_container_client.assert_called_once_with("mycontainer")
        mock_container_client.return_value.list_blobs.assert_called_once_with(
            name_starts_with="test", include=None
        )

    def test_get_blobs_list_recursive_endswith(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        mocked_blob_service_client.return_value.get_container_client.return_value.list_blobs.return_value = [
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
    def test_load_file(self, mock_upload, create_container):
        with mock.patch("builtins.open", mock.mock_open(read_data="data")):
            hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
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
    def test_load_string(self, mock_upload, create_container):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.load_string("big string", "container", "blob", create_container, max_connections=1)
        mock_upload.assert_called_once_with(
            container_name="container",
            blob_name="blob",
            data="big string",
            create_container=create_container,
            max_connections=1,
        )

    @mock.patch.object(WasbHook, "download")
    def test_get_file(self, mock_download):
        with mock.patch("builtins.open", mock.mock_open(read_data="data")):
            hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
            hook.get_file("path", "container", "blob", max_connections=1)
        mock_download.assert_called_once_with(container_name="container", blob_name="blob", max_connections=1)
        mock_download.return_value.readall.assert_called()

    @mock.patch.object(WasbHook, "download")
    def test_read_file(self, mock_download, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.read_file("container", "blob", max_connections=1)
        mock_download.assert_called_once_with("container", "blob", max_connections=1)

    @pytest.mark.parametrize(argnames="create_container", argvalues=[True, False])
    def test_upload(self, mocked_blob_service_client, create_container):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.upload(
            container_name="mycontainer",
            blob_name="myblob",
            data=b"mydata",
            create_container=create_container,
            blob_type="BlockBlob",
            length=4,
        )
        mock_blob_client = mocked_blob_service_client.return_value.get_blob_client
        mock_blob_client.assert_called_once_with(container="mycontainer", blob="myblob")
        mock_blob_client.return_value.upload_blob.assert_called_once_with(b"mydata", "BlockBlob", length=4)

        mock_container_client = mocked_blob_service_client.return_value.get_container_client
        if create_container:
            mock_container_client.assert_called_with("mycontainer")
        else:
            mock_container_client.assert_not_called()

    def test_download(self, mocked_blob_service_client):
        blob_client = mocked_blob_service_client.return_value.get_blob_client
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.download(container_name="mycontainer", blob_name="myblob", offset=2, length=4)
        blob_client.assert_called_once_with(container="mycontainer", blob="myblob")
        blob_client.return_value.download_blob.assert_called_once_with(offset=2, length=4)

    def test_get_container_client(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook._get_container_client("mycontainer")
        mocked_blob_service_client.return_value.get_container_client.assert_called_once_with("mycontainer")

    def test_get_blob_client(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook._get_blob_client(container_name="mycontainer", blob_name="myblob")
        mock_instance = mocked_blob_service_client.return_value.get_blob_client
        mock_instance.assert_called_once_with(container="mycontainer", blob="myblob")

    def test_create_container(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.create_container(container_name="mycontainer")
        mock_instance = mocked_blob_service_client.return_value.get_container_client
        mock_instance.assert_called_once_with("mycontainer")
        mock_instance.return_value.create_container.assert_called()

    def test_delete_container(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.delete_container("mycontainer")
        mocked_container_client = mocked_blob_service_client.return_value.get_container_client
        mocked_container_client.assert_called_once_with("mycontainer")
        mocked_container_client.return_value.delete_container.assert_called()

    @pytest.mark.parametrize("exc", [ValueError, RuntimeError])
    def test_delete_container_generic_exception(self, exc: type[Exception], caplog):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        with mock.patch.object(WasbHook, "_get_container_client") as m:
            m.return_value.delete_container.side_effect = exc("FakeException")
            caplog.clear()
            caplog.set_level("ERROR")
            with pytest.raises(exc, match="FakeException"):
                hook.delete_container("mycontainer")
            assert "Error deleting container: mycontainer" in caplog.text

    def test_delete_container_resource_not_found(self, caplog):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        with mock.patch.object(WasbHook, "_get_container_client") as m:
            m.return_value.delete_container.side_effect = ResourceNotFoundError("FakeException")
            caplog.clear()
            caplog.set_level("WARNING")
            hook.delete_container("mycontainer")
            assert "Unable to delete container mycontainer (not found)" in caplog.text

    @mock.patch.object(WasbHook, "delete_blobs")
    def test_delete_single_blob(self, delete_blobs, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.delete_file("container", "blob", is_prefix=False)
        delete_blobs.assert_called_once_with("container", "blob")

    @mock.patch.object(WasbHook, "delete_blobs")
    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch.object(WasbHook, "check_for_blob")
    def test_delete_multiple_blobs(self, mock_check, mock_get_blobslist, mock_delete_blobs):
        mock_check.return_value = False
        mock_get_blobslist.return_value = ["blob_prefix/blob1", "blob_prefix/blob2"]
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
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
    def test_delete_more_than_256_blobs(self, mock_check, mock_get_blobslist, mock_delete_blobs):
        mock_check.return_value = False
        mock_get_blobslist.return_value = [f"blob_prefix/blob{i}" for i in range(300)]
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.delete_file("container", "blob_prefix", is_prefix=True)
        mock_get_blobslist.assert_called_once_with("container", prefix="blob_prefix", delimiter="")
        # The maximum number of blobs that can be deleted in a single request is 256 using the underlying
        # `ContainerClient.delete_blobs()` method. Therefore the deletes need to be in batches of <= 256.
        # Therefore, providing a list of 300 blobs to delete should yield 2 calls of
        # `ContainerClient.delete_blobs()` in this test.
        assert mock_delete_blobs.call_count == 2

    @mock.patch.object("WasbHook._get_blob_client")
    def test_copy_blobs(self, mock_get_blob_client):
        # Arrange
        hook = WasbHook()
        source_container_name = "source-container"
        source_blob_name = "source-blob"
        destination_container_name = "destination-container"
        destination_blob_name = "destination-blob"

        # Mock the blob clients
        mock_source_blob_client = mock.MagicMock()
        mock_destination_blob_client = mock.MagicMock()
        mock_get_blob_client.side_effect = [mock_source_blob_client, mock_destination_blob_client]

        # Mock the URL of the source blob
        mock_source_blob_client.url = "https://source-url"

        hook.copy_blobs(
            source_container_name, source_blob_name, destination_container_name, destination_blob_name
        )

        mock_get_blob_client.assert_any_call(container_name=source_container_name, blob_name=source_blob_name)
        mock_get_blob_client.assert_any_call(
            container_name=destination_container_name, blob_name=destination_blob_name
        )
        mock_destination_blob_client.start_copy_from_url.assert_called_once_with("https://source-url")

    @mock.patch.object(WasbHook, "get_blobs_list")
    @mock.patch.object(WasbHook, "check_for_blob")
    def test_delete_nonexisting_blob_fails(self, mock_check, mock_getblobs, mocked_blob_service_client):
        mock_getblobs.return_value = []
        mock_check.return_value = False
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        with pytest.raises(AirflowException, match=re.escape("Blob(s) not found: nonexisting_blob")):
            hook.delete_file("container", "nonexisting_blob", is_prefix=False, ignore_if_missing=False)

    @mock.patch.object(WasbHook, "get_blobs_list")
    def test_delete_multiple_nonexisting_blobs_fails(self, mock_getblobs):
        mock_getblobs.return_value = []
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        with pytest.raises(AirflowException, match=re.escape("Blob(s) not found: nonexisting_blob_prefix")):
            hook.delete_file("container", "nonexisting_blob_prefix", is_prefix=True, ignore_if_missing=False)

    def test_connection_success(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.get_conn().get_account_information().return_value = {
            "sku_name": "Standard_RAGRS",
            "account_kind": "StorageV2",
        }
        status, msg = hook.test_connection()

        assert status is True
        assert msg == "Successfully connected to Azure Blob Storage."

    def test_connection_failure(self, mocked_blob_service_client):
        hook = WasbHook(wasb_conn_id=self.azure_shared_key_test)
        hook.get_conn().get_account_information = mock.PropertyMock(
            side_effect=Exception("Authentication failed.")
        )
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Authentication failed."

    @pytest.mark.parametrize(
        "conn_id_str",
        [
            "wasb_test_key",
            "pub_read_id",
            "pub_read_id_without_host",
            "azure_test_connection_string",
            "azure_shared_key_test",
            "ad_conn_id",
            "managed_identity_conn_id",
            "sas_conn_id",
            "extra__wasb__sas_conn_id",
            "http_sas_conn_id",
            "extra__wasb__http_sas_conn_id",
        ],
    )
    def test_extract_account_name_from_connection(self, conn_id_str, mocked_blob_service_client):
        expected_account_name = "testname"
        if conn_id_str == "azure_test_connection_string":
            mocked_blob_service_client.from_connection_string().account_name = expected_account_name
        else:
            mocked_blob_service_client.return_value.account_name = expected_account_name

        wasb_hook = WasbHook(wasb_conn_id=conn_id_str)
        account_name = wasb_hook.get_conn().account_name

        assert (
            account_name == expected_account_name
        ), f"Expected account name {expected_account_name} but got {account_name}"
