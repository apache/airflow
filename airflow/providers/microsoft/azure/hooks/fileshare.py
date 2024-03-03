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

from typing import IO, Any

from azure.storage.fileshare import FileProperties, ShareDirectoryClient, ShareFileClient, ShareServiceClient

from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_sync_default_azure_credential,
)


class AzureFileShareHook(BaseHook):
    """
    Interacts with Azure FileShare Storage.

    :param azure_fileshare_conn_id: Reference to the
        :ref:`Azure FileShare connection id<howto/connection:azure_fileshare>`
        of an Azure account of which file share should be used.
    """

    conn_name_attr = "azure_fileshare_conn_id"
    default_conn_name = "azure_fileshare_default"
    conn_type = "azure_fileshare"
    hook_name = "Azure FileShare"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "sas_token": PasswordField(lazy_gettext("SAS Token (optional)"), widget=BS3PasswordFieldWidget()),
            "connection_string": StringField(
                lazy_gettext("Connection String (optional)"), widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Blob Storage Login (optional)",
                "password": "Blob Storage Key (optional)",
            },
            "placeholders": {
                "login": "account name or account url",
                "password": "secret",
                "sas_token": "account url or token (optional)",
                "connection_string": "account url or token (optional)",
            },
        }

    def __init__(
        self,
        share_name: str | None = None,
        file_path: str | None = None,
        directory_path: str | None = None,
        azure_fileshare_conn_id: str = "azure_fileshare_default",
    ) -> None:
        super().__init__()
        self._conn_id = azure_fileshare_conn_id
        self.share_name = share_name
        self.file_path = file_path
        self.directory_path = directory_path
        self._account_url: str | None = None
        self._connection_string: str | None = None
        self._account_access_key: str | None = None
        self._sas_token: str | None = None

    def get_conn(self) -> None:
        conn = self.get_connection(self._conn_id)
        extras = conn.extra_dejson
        self._connection_string = extras.get("connection_string")
        if conn.login:
            self._account_url = self._parse_account_url(conn.login)
        self._sas_token = extras.get("sas_token")
        self._account_access_key = conn.password

    @staticmethod
    def _parse_account_url(account_url: str) -> str:
        if not account_url.lower().startswith("https"):
            return f"https://{account_url}.file.core.windows.net"
        return account_url

    def _get_sync_default_azure_credential(self):
        conn = self.get_connection(self._conn_id)
        extras = conn.extra_dejson
        managed_identity_client_id = extras.get("managed_identity_client_id")
        workload_identity_tenant_id = extras.get("workload_identity_tenant_id")
        return get_sync_default_azure_credential(
            managed_identity_client_id=managed_identity_client_id,
            workload_identity_tenant_id=workload_identity_tenant_id,
        )

    @property
    def share_service_client(self):
        self.get_conn()
        if self._connection_string:
            return ShareServiceClient.from_connection_string(
                conn_str=self._connection_string,
            )
        elif self._account_url and (self._sas_token or self._account_access_key):
            credential = self._sas_token or self._account_access_key
            return ShareServiceClient(account_url=self._account_url, credential=credential)
        else:
            return ShareServiceClient(
                account_url=self._account_url,
                credential=self._get_sync_default_azure_credential(),
                token_intent="backup",
            )

    @property
    def share_directory_client(self):
        self.get_conn()
        if self._connection_string:
            return ShareDirectoryClient.from_connection_string(
                conn_str=self._connection_string,
                share_name=self.share_name,
                directory_path=self.directory_path,
            )
        elif self._account_url and (self._sas_token or self._account_access_key):
            credential = self._sas_token or self._account_access_key
            return ShareDirectoryClient(
                account_url=self._account_url,
                share_name=self.share_name,
                directory_path=self.directory_path,
                credential=credential,
            )
        else:
            return ShareDirectoryClient(
                account_url=self._account_url,
                share_name=self.share_name,
                directory_path=self.directory_path,
                credential=self._get_sync_default_azure_credential(),
                token_intent="backup",
            )

    @property
    def share_file_client(self):
        self.get_conn()
        if self._connection_string:
            return ShareFileClient.from_connection_string(
                conn_str=self._connection_string,
                share_name=self.share_name,
                file_path=self.file_path,
            )
        elif self._account_url and (self._sas_token or self._account_access_key):
            credential = self._sas_token or self._account_access_key
            return ShareFileClient(
                account_url=self._account_url,
                share_name=self.share_name,
                file_path=self.file_path,
                credential=credential,
            )
        else:
            return ShareFileClient(
                account_url=self._account_url,
                share_name=self.share_name,
                file_path=self.file_path,
                credential=self._get_sync_default_azure_credential(),
                token_intent="backup",
            )

    def check_for_directory(self) -> bool:
        """Check if a directory exists on Azure File Share."""
        return self.share_directory_client.exists()

    def list_directories_and_files(self) -> list:
        """Return the list of directories and files stored on a Azure File Share."""
        return list(self.share_directory_client.list_directories_and_files())

    def list_files(self) -> list[str]:
        """Return the list of files stored on a Azure File Share."""
        return [obj.name for obj in self.list_directories_and_files() if isinstance(obj, FileProperties)]

    def create_share(self, share_name: str, **kwargs) -> bool:
        """
        Create new Azure File Share.

        :param share_name: Name of the share.
        :return: True if share is created, False if share already exists.
        """
        try:
            self.share_service_client.create_share(share_name, **kwargs)
        except Exception as e:
            self.log.warning(e)
            return False
        return True

    def delete_share(self, share_name: str, **kwargs) -> bool:
        """
        Delete existing Azure File Share.

        :param share_name: Name of the share.
        :return: True if share is deleted, False if share does not exist.
        """
        try:
            self.share_service_client.delete_share(share_name, **kwargs)
        except Exception as e:
            self.log.warning(e)
            return False
        return True

    def create_directory(self, **kwargs) -> Any:
        """Create a new directory on a Azure File Share."""
        return self.share_directory_client.create_directory(**kwargs)

    def get_file(self, file_path: str, **kwargs) -> None:
        """
        Download a file from Azure File Share.

        :param file_path: Where to store the file.
        """
        with open(file_path, "wb") as file_handle:
            data = self.share_file_client.download_file(**kwargs)
            data.readinto(file_handle)

    def get_file_to_stream(self, stream: IO, **kwargs) -> None:
        """
        Download a file from Azure File Share.

        :param stream: A filehandle to store the file to.
        """
        data = self.share_file_client.download_file(**kwargs)
        data.readinto(stream)

    def load_file(self, file_path: str, **kwargs) -> None:
        """
        Upload a file to Azure File Share.

        :param file_path: Path to the file to load.
        """
        with open(file_path, "rb") as source_file:
            self.share_file_client.upload_file(source_file, **kwargs)

    def load_data(self, string_data: bytes | str | IO, **kwargs) -> None:
        """
        Upload a string to Azure File Share.

        :param string_data: String/Stream to load.
        """
        self.share_file_client.upload_file(string_data, **kwargs)

    def test_connection(self):
        """Test Azure FileShare connection."""
        success = (True, "Successfully connected to Azure File Share.")

        try:
            # Attempt to retrieve file share information
            next(iter(self.share_service_client.list_shares()))
            return success
        except StopIteration:
            # If the iterator returned is empty it should still be considered a successful connection since
            # it's possible to create a storage account without any file share and none could
            # legitimately exist yet.
            return success
        except Exception as e:
            return False, str(e)
